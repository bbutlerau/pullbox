"""A source outage must not strand unrelated series or lose deferred source work."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, select, update

from pullbox.core.encryption import encrypt_secret
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Base, MetadataSeriesRetry, Series
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.metadata_source_account import MetadataSourceAccount
from pullbox.models.series import IssueCatalogState
from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.services.metadata_series_retry import admit_retry, retry_candidates, settle_retry
from pullbox.tasks import metadata_task
from pullbox.tasks.metadata_sweep_state import load_sweep, save_sweep
from tests.integration.metadata_identity.test_daily_issue_sync import (  # noqa: F401
    DailyAdapter,
    daily,
)
from tests.integration.metadata_identity.test_scheduled_series_refresh import (  # noqa: F401
    add_series,
    configured_sources,
    offer,
    scheduled,
)
from tests.unit.test_metadata_source_reads import registry


@pytest.fixture
async def isolated(daily):  # noqa: F811 - imported pytest fixture
    return daily


async def expire_retry(factory, task_id):
    async with factory.begin() as session:
        await session.execute(
            update(MetadataSourceAccount)
            .where(MetadataSourceAccount.retry_at.isnot(None))
            .values(retry_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.execute(
            update(MetadataSeriesRetry)
            .where(
                MetadataSeriesRetry.task_id == task_id,
                MetadataSeriesRetry.retry_at.isnot(None),
            )
            .values(retry_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        state = await load_sweep(session, task_id)
        state.retry_at = 0
        await save_sweep(session, task_id, state)


@pytest.mark.parametrize("task_id", ["sync_new_issues", "refresh_metadata"])
async def test_retryable_series_does_not_block_the_next_series(isolated, monkeypatch, task_id):
    factory, _, scheduler = isolated
    first, first_data = await add_series(factory)
    async with factory.begin() as session:
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == Source.COMICVINE_LOCAL.value)
            .values(enabled=True)
        )
    second, second_data = await add_series(factory, identifier=43, source=Source.COMICVINE_LOCAL)
    adapter = DailyAdapter(first_data)
    local = DailyAdapter(second_data)
    local.source = Source.COMICVINE_LOCAL
    offer(local, second_data)
    original = adapter.series

    async def fetch(identifier, **kwargs):
        if identifier == "42":
            adapter.calls.append(("failed", identifier))
            raise MetadataSourceError(SourceStatus.TIMEOUT, retry_after_seconds=720)
        return await original(identifier, **kwargs)

    adapter.series = fetch
    from pullbox.providers.metadata import sources

    monkeypatch.setattr(
        sources,
        "metadata_sources",
        lambda: {**registry(adapter).factories, **registry(local).factories},
    )
    result = await getattr(metadata_task, task_id)()
    async with factory() as session:
        assert (await session.get(Series, second)).issue_count == 2, (
            "A temporary failure in the first series must not block the second"
        )
        assert (await session.get(Series, first)).issue_count == 1
    assert result.status == "waiting"
    assert scheduler._scheduler.add_job.call_args.kwargs["args"] == [second]
    calls = list(adapter.calls)
    assert (await getattr(metadata_task, task_id)()).status == "waiting"
    assert adapter.calls == calls
    await expire_retry(factory, task_id)
    await factory.kw["bind"].dispose()
    adapter.series = original
    offer(adapter, first_data)
    assert (await getattr(metadata_task, task_id)()).status == "completed"
    assert all(call[1] == "42" for call in adapter.calls[len(calls) :])
    async with factory() as session:
        assert (await session.get(Series, first)).issue_count == 2
        assert list(await session.scalars(select(MetadataSeriesRetry))) == []
    assert scheduler._scheduler.add_job.call_count == 2


@pytest.mark.parametrize("task_id", ["sync_new_issues", "refresh_metadata"])
async def test_auth_failure_does_not_schedule_automatic_retry(isolated, monkeypatch, task_id):
    factory, adapter, scheduler = isolated
    series_id, data = await add_series(factory)
    offer(adapter, data)
    original = adapter.series

    async def rejected(identifier, **kwargs):
        adapter.calls.append(("auth", identifier))
        raise MetadataSourceError(SourceStatus.AUTHENTICATION_FAILED)

    adapter.series = rejected
    result = await getattr(metadata_task, task_id)()
    assert result.status == "completed", "Bad credentials require configuration, not timed retry"
    calls = list(adapter.calls)
    await getattr(metadata_task, task_id)()
    assert adapter.calls == calls, "Starting another sweep must not repeat known-bad credentials"
    scheduler._scheduler.add_job.assert_not_called()
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 1
        row = await session.scalar(select(MetadataSeriesRetry))
        assert row.retry_at is None and row.status == "authentication_failed"
    async with factory.begin() as session:
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == Source.METRON_API.value)
            .values(credential_secret=encrypt_secret("corrected-scheduled-token"))
        )
    adapter.series = original
    assert (await getattr(metadata_task, task_id)()).status == "completed"
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 2
        assert list(await session.scalars(select(MetadataSeriesRetry))) == []


@pytest.mark.parametrize("task_id", ["sync_new_issues", "refresh_metadata"])
async def test_successful_fallback_retries_only_the_failed_source(isolated, monkeypatch, task_id):
    from pullbox.core.comicvine_key import save_comicvine_api_key
    from pullbox.providers.metadata import sources

    factory, _, scheduler = isolated
    async with factory.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
        for source, rank in [(Source.COMICVINE_API, 0), (Source.COMICVINE_LOCAL, 1)]:
            await session.execute(
                update(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == source.value)
                .values(enabled=True, priority=rank)
            )
        await save_comicvine_api_key(session, "fallback-test-key")
    series_id, local_data = await add_series(factory, source=Source.COMICVINE_LOCAL)
    local = DailyAdapter(local_data)
    local.source = Source.COMICVINE_LOCAL
    offer(local, local_data)
    api = DailyAdapter(local_data)
    api.source = Source.COMICVINE_API
    api.data = replace(
        local.data,
        series=local.data.series.model_copy(update={"source": Source.COMICVINE_API}),
        issues=tuple(
            row.model_copy(update={"source": Source.COMICVINE_API}) for row in local.data.issues
        ),
    )
    original = api.series

    async def throttled(identifier, **kwargs):
        api.calls.append(("throttled", identifier))
        raise MetadataSourceError(SourceStatus.RATE_LIMITED, retry_after_seconds=720)

    api.series = throttled
    monkeypatch.setattr(
        sources,
        "metadata_sources",
        lambda: {**registry(api).factories, **registry(local).factories},
    )
    assert (await getattr(metadata_task, task_id)()).status == "waiting"
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 2
        assert (
            await session.scalar(select(MetadataSeriesRetry))
        ).source == Source.COMICVINE_API.value
    local_calls = list(local.calls)
    await expire_retry(factory, task_id)
    api.series = original
    assert (await getattr(metadata_task, task_id)()).status == "completed"
    assert local.calls == local_calls, "A retry must not repeat healthy source requests"
    async with factory() as session:
        assert list(await session.scalars(select(MetadataSeriesRetry))) == []
    scheduler._scheduler.add_job.assert_called_once()


async def test_retry_migration_matches_model_without_touching_library(identity_probe_db):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Integer, MetaData, Table

    from tests.integration.metadata_identity.test_production_migration import _revision

    engine, factory, _ = identity_probe_db
    async with factory.begin() as session:
        series = Series(title="Preserved", sort_title="Preserved")
        session.add(series)
        await session.flush()
        series_id = series.id
    async with engine.begin() as conn:

        def migrate(sync):
            revision = _revision("v3p4q5r6s789_add_metadata_series_retries", sync)
            revision.downgrade()
            revision.upgrade()
            expected = MetaData()
            Table("series", expected, Column("id", Integer, primary_key=True))
            name = "metadata_series_retries"
            Base.metadata.tables[name].to_metadata(expected)
            context = MigrationContext.configure(
                sync,
                opts={
                    "include_object": lambda obj, item, type_, reflected, compare_to: (
                        item == name if type_ == "table" else True
                    ),
                    "compare_server_default": True,
                },
            )
            assert compare_metadata(context, expected) == []

        await conn.run_sync(migrate)
    async with factory() as session:
        assert (await session.get(Series, series_id)).title == "Preserved"
        assert list(await session.scalars(select(MetadataSeriesRetry))) == []


async def seed_failure(isolated):
    factory, adapter, _ = isolated
    series_id, data = await add_series(factory)
    offer(adapter, data)
    original = adapter.series

    async def timeout(*args, **kwargs):
        raise MetadataSourceError(SourceStatus.TIMEOUT, retry_after_seconds=720)

    adapter.series = timeout
    assert (await metadata_task.refresh_metadata()).status == "waiting"
    adapter.series = original
    return factory, adapter, series_id


async def test_stale_attempt_cannot_restore_a_deleted_retry(isolated):
    factory, _, series_id = await seed_failure(isolated)
    await expire_retry(factory, "refresh_metadata")
    async with factory() as reader:
        admission = await admit_retry(
            reader, "refresh_metadata", series_id, gcd_api_enabled=False, retry_only=True
        )
    async with factory.begin() as writer:
        await writer.execute(delete(MetadataSeriesRetry))
    async with factory.begin() as writer:
        await settle_retry(
            writer,
            "refresh_metadata",
            series_id,
            admission,
            outcomes=(SourceOutcome(source=Source.METRON_API, status=SourceStatus.TIMEOUT),),
        )
    async with factory() as session:
        assert list(await session.scalars(select(MetadataSeriesRetry))) == [], (
            "An obsolete failure cannot recreate work that another attempt settled"
        )


async def test_failed_retry_commit_preserves_deferred_work_and_metadata(isolated, monkeypatch):
    factory, _, series_id = await seed_failure(isolated)
    await expire_retry(factory, "refresh_metadata")
    async with factory() as session:
        row = await session.scalar(select(MetadataSeriesRetry))
        previous = (row.id, row.revision, row.retry_at)
    original = metadata_task.save_sweep
    saves = 0

    async def fail(session, task_id, state):
        nonlocal saves
        saves += 1
        if saves == 1:
            raise RuntimeError("transaction failed")
        await original(session, task_id, state)

    monkeypatch.setattr(metadata_task, "save_sweep", fail)
    with pytest.raises(RuntimeError, match="transaction failed"):
        await metadata_task.refresh_metadata()
    async with factory() as session:
        row = await session.scalar(select(MetadataSeriesRetry))
        assert (row.id, row.revision, row.retry_at) == previous
        assert (await session.get(Series, series_id)).issue_count == 1


async def test_cancelled_retry_is_retained_for_resume(isolated):
    factory, adapter, series_id = await seed_failure(isolated)
    await expire_retry(factory, "refresh_metadata")
    adapter.started.clear()
    adapter.wait = asyncio.Event()
    task = asyncio.create_task(metadata_task.refresh_metadata())
    try:
        await asyncio.wait_for(adapter.started.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        assert await session.scalar(select(MetadataSeriesRetry.id))
        assert (await session.get(Series, series_id)).issue_count == 1
    adapter.wait = None
    assert (await metadata_task.refresh_metadata()).status == "completed"


async def test_hydrating_retry_does_not_hide_ready_work_behind_page_limit(isolated):
    factory, _, _ = isolated
    first, _ = await add_series(factory)
    second, _ = await add_series(factory, identifier=43)
    async with factory.begin() as session:
        (await session.get(Series, first)).issue_catalog_state = IssueCatalogState.HYDRATING
        for identifier in (first, second):
            session.add(
                MetadataSeriesRetry(
                    task_id="refresh_metadata",
                    series_id=identifier,
                    source="*",
                    config_key="",
                    status="timeout",
                    retry_at=datetime.now(UTC) - timedelta(hours=1),
                )
            )
    async with factory() as session:
        assert await retry_candidates(session, "refresh_metadata", [], limit=1) == [second]


@pytest.mark.parametrize("failure", [SourceStatus.RATE_LIMITED, SourceStatus.AUTHENTICATION_FAILED])
async def test_account_failure_is_shared_across_series_and_both_scheduled_tasks(isolated, failure):
    factory, adapter, _ = isolated
    first, _ = await add_series(factory)
    second, _ = await add_series(factory, identifier=43)

    async def failed(identifier, **kwargs):
        adapter.calls.append(("failed", identifier))
        raise MetadataSourceError(failure, retry_after_seconds=720)

    adapter.series = failed
    await metadata_task.sync_new_issues()
    await factory.kw["bind"].dispose()
    await metadata_task.refresh_metadata()
    assert adapter.calls == [("failed", "42")], "The second task and series share account admission"
    async with factory() as session:
        retries = list(await session.scalars(select(MetadataSeriesRetry)))
        assert {(row.task_id, row.series_id) for row in retries} == {
            (task_id, series_id)
            for task_id in ("sync_new_issues", "refresh_metadata")
            for series_id in (first, second)
        }
        assert {row.status for row in retries} == {failure.value}
