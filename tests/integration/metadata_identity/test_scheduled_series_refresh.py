"""Scheduled native-source refresh keeps durable progress and library ownership."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select, update

from pullbox.core.comicvine_key import save_comicvine_api_key
from pullbox.core.encryption import encrypt_secret
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Issue, Series
from pullbox.models.issue import IssueStatus
from pullbox.models.metadata_identity import SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.series import IssueCatalogState, SeriesStatusOverride
from pullbox.schemas.metadata_sources import SourceStatus
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.services.metadata_series_adoption import adopt_source_series_bundle
from pullbox.tasks import metadata_task
from pullbox.tasks.metadata_sweep_state import MetadataSweep, load_sweep, save_sweep
from tests.integration.metadata_identity.test_series_adoption import (  # noqa: F401
    bundle,
    configured_sources,
)
from tests.integration.metadata_identity.test_series_refresh import RefreshAdapter
from tests.unit.test_metadata_source_reads import registry


@pytest.fixture
async def scheduled(identity_probe_db, monkeypatch):
    from pullbox.providers.metadata import sources

    _, factory, _ = identity_probe_db
    scheduler = MagicMock()
    monkeypatch.setattr(metadata_task, "get_session_factory", lambda: factory)
    monkeypatch.setattr(metadata_task, "get_scheduler", lambda: scheduler)
    monkeypatch.setattr("pullbox.tasks.metadata_sweep_state.get_scheduler", lambda: scheduler)
    async with factory.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == Source.METRON_API.value)
            .values(enabled=True, credential_secret=encrypt_secret("scheduled-test-token"))
        )
    data = bundle(numbers=("1", "50-x"))
    adapter = RefreshAdapter(data)
    monkeypatch.setattr(sources, "metadata_sources", lambda: registry(adapter).factories)
    return factory, adapter, scheduler


async def add_series(
    factory, *, identifier=42, monitored=True, fresh=False, source=Source.METRON_API
):
    data = bundle(numbers=("1",))
    data.series.external_id = str(identifier)
    data.series.description = "Existing description"
    data.series.image_url = None
    data.issues[0].series_external_id = str(identifier)
    data.issues[0].external_id = str(identifier * 100)
    generation = datetime.now(UTC)
    for row in (data.series, *data.issues):
        row.source = source
        row.identity_namespace = source.identity_namespace
        if source is Source.COMICVINE_LOCAL:
            row.source_updated_at = generation
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, data, monitored=monitored)
        result.series.metadata_last_refreshed = datetime.now(UTC) - timedelta(
            days=0 if fresh else 60
        )
        result.series.path = "/read-only/original"
        series_id = result.series.id
        issue = await session.scalar(select(Issue).where(Issue.series_id == series_id))
        issue.status = IssueStatus.OWNED
        issue.manual_skip = True
    return series_id, data


def offer(adapter, data, *, count=2):
    rows = tuple(
        data.issues[0].model_copy(
            update={
                "external_id": str(int(data.issues[0].external_id) + i),
                "issue_number_text": "1" if i == 0 else "50-x",
                "issue_number_key": "1" if i == 0 else "50-X",
            }
        )
        for i in range(count)
    )
    adapter.data = replace(
        data,
        series=data.series.model_copy(update={"issue_count": count}),
        issues=rows,
        catalog_total=count,
    )


async def test_scheduled_native_refresh_fills_gaps_without_cv_key_and_searches_after_commit(
    scheduled,
):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    adapter.data.series.description = "Do not overwrite"
    adapter.data.issues[0].description = "Fill this missing value"
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        series.status_override = SeriesStatusOverride.ENDED
        series.status = "ended"
    result = await metadata_task.refresh_metadata()
    assert adapter.calls, "Native-only series must not require a ComicVine API key"
    assert result.status == "completed"
    async with factory() as session:
        series = await session.get(Series, series_id)
        assert series.description == "Existing description"
        assert series.status.value == "ended" and series.path == "/read-only/original"
        assert series.issue_count == 2 and series.issue_catalog_state is IssueCatalogState.COMPLETE
        issues = list(await session.scalars(select(Issue).order_by(Issue.id)))
        assert issues[0].status is IssueStatus.OWNED and issues[0].manual_skip
        assert issues[0].description == "Fill this missing value"
        assert issues[1].status is IssueStatus.WANTED and issues[1].issue_number_text == "50-X"
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 2
        state = await load_sweep(session, "refresh_metadata")
        assert not state.active and state.cursor == state.upper_bound == series_id
    scheduler._scheduler.add_job.assert_called_once()
    assert scheduler._scheduler.add_job.call_args.kwargs["args"] == [series_id]


async def test_total_deadline_is_retryable_without_fabricating_source_outcomes(
    scheduled, monkeypatch
):
    from pullbox.services import metadata_series_refresh as refresh

    factory, adapter, _ = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    monkeypatch.setattr(refresh, "fetch_metadata_snapshot", AsyncMock(side_effect=TimeoutError))
    async with factory() as session:
        with pytest.raises(refresh.SeriesRefreshError) as failure:
            await refresh.refresh_series_from_sources(session, series_id)
    assert failure.value.outcomes == (), (
        "An operation deadline cannot assign failures to unqueried sources"
    )
    assert metadata_task._provider_pause_seconds(failure.value) == 300


@pytest.mark.parametrize(
    "reason",
    ["disabled", "unconfigured", "stale", "unmonitored", "fresh", "hydrating", "locg", "gcd"],
)
async def test_ineligible_series_never_contact_a_provider(scheduled, reason):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(
        factory, monitored=reason != "unmonitored", fresh=reason == "fresh"
    )
    offer(adapter, data)
    async with factory.begin() as session:
        if reason in {"disabled", "unconfigured"}:
            await session.execute(
                update(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == Source.METRON_API.value)
                .values(
                    **({"enabled": False} if reason == "disabled" else {"credential_secret": None})
                )
            )
        elif reason == "stale":
            await session.execute(update(SeriesExternalIdentity).values(verification_state="stale"))
        elif reason in {"locg", "gcd"}:
            await session.execute(
                update(SeriesExternalIdentity).values(identity_namespace=Namespace(reason))
            )
            if reason == "gcd":
                await session.execute(
                    update(MetadataSourceConfig)
                    .where(MetadataSourceConfig.source == Source.GCD_API_V2.value)
                    .values(enabled=True, credential_secret=encrypt_secret("flagged-test-token"))
                )
                adapter.source = Source.GCD_API_V2
        elif reason == "hydrating":
            (await session.get(Series, series_id)).issue_catalog_state = IssueCatalogState.HYDRATING
    await metadata_task.refresh_metadata()
    assert adapter.calls == []
    scheduler._scheduler.add_job.assert_not_called()
    async with factory() as session:
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1


@pytest.mark.parametrize("operation", ["series", "issues"])
@pytest.mark.parametrize(
    "status",
    [
        SourceStatus.RATE_LIMITED,
        SourceStatus.AUTHENTICATION_FAILED,
        SourceStatus.TIMEOUT,
        SourceStatus.UNAVAILABLE,
    ],
)
async def test_scheduled_failure_is_atomic_and_stays_at_retry_cursor(scheduled, operation, status):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)

    async def throttled(*args, **kwargs):
        adapter.calls.append(("throttled",))
        raise MetadataSourceError(status, retry_after_seconds=720)

    setattr(adapter, operation, throttled)
    started = datetime.now(UTC).timestamp()
    result = await metadata_task.refresh_metadata()
    assert result.status == "waiting", "Structured source throttles must keep the sweep resumable"
    async with factory() as session:
        state = await load_sweep(session, "refresh_metadata")
        assert state.active and state.cursor == 0 and state.upper_bound == series_id
        assert started + 719 <= state.retry_at <= datetime.now(UTC).timestamp() + 721
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1
    calls = list(adapter.calls)
    await metadata_task.refresh_metadata()
    assert adapter.calls == calls, "A durable cooldown must survive a new task invocation"
    scheduler._scheduler.add_job.assert_not_called()


async def test_cancelling_refresh_keeps_uncommitted_cursor_and_resumes(scheduled):
    factory, adapter, _ = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    adapter.wait = asyncio.Event()
    task = asyncio.create_task(metadata_task.refresh_metadata())
    try:
        done, _ = await asyncio.wait([task], timeout=0.2)
        assert not done, (
            "The native refresh must reach its provider read rather than skip the series"
        )
        await asyncio.wait_for(adapter.started.wait(), 3)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    async with factory() as session:
        state = await load_sweep(session, "refresh_metadata")
        assert state.active and state.cursor == 0
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1
    adapter.wait = None
    await metadata_task.refresh_metadata()
    async with factory() as session:
        state = await load_sweep(session, "refresh_metadata")
        assert not state.active and state.cursor == series_id


async def test_structural_catalog_failure_does_not_search_or_change_owned_issues(scheduled):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    adapter.data.issues[0].issue_number_text = "99"
    result = await metadata_task.refresh_metadata()
    assert adapter.calls, "A native series must be evaluated rather than silently skipped"
    assert result.status == "completed"
    scheduler._scheduler.add_job.assert_not_called()
    async with factory() as session:
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1
        assert (await session.scalar(select(Issue))).issue_number_text == "1"
        assert (await load_sweep(session, "refresh_metadata")).cursor == series_id


async def test_disabling_last_source_clears_a_durable_cooldown(scheduled):
    factory, adapter, scheduler = scheduled
    series_id, _ = await add_series(factory)
    async with factory.begin() as session:
        await save_sweep(
            session,
            "refresh_metadata",
            MetadataSweep(
                upper_bound=series_id,
                active=True,
                retry_at=(datetime.now(UTC) + timedelta(hours=1)).timestamp(),
            ),
        )
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
    result = await metadata_task.refresh_metadata()
    assert result.status == "completed"
    assert adapter.calls == []
    async with factory() as session:
        state = await load_sweep(session, "refresh_metadata")
        assert not state.active and state.retry_at == 0
    scheduler.schedule_task_continuation.assert_not_called()


async def test_native_batch_preserves_initial_upper_bound_and_restarts(scheduled, monkeypatch):
    factory, adapter, _ = scheduled
    first, first_data = await add_series(factory)
    second, second_data = await add_series(factory, identifier=43)
    monkeypatch.setattr(metadata_task, "_METADATA_BATCH_SIZE", 1)
    offer(adapter, first_data, count=1)
    assert (await metadata_task.refresh_metadata()).status == "waiting"
    third, _ = await add_series(factory, identifier=44)
    async with factory() as session:
        state = await load_sweep(session, "refresh_metadata")
        assert state.cursor == first and state.upper_bound == second
    offer(adapter, second_data, count=1)
    assert (await metadata_task.refresh_metadata()).status == "completed"
    async with factory() as session:
        assert (await load_metadata_baseline(session, Kind.SERIES, first)).revision == 2
        assert (await load_metadata_baseline(session, Kind.SERIES, second)).revision == 2
        assert (await load_metadata_baseline(session, Kind.SERIES, third)).revision == 1


async def test_failed_metadata_commit_never_fetches_artwork_or_schedules_search(
    scheduled, monkeypatch
):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    adapter.data.series.image_url = "https://static.metron.cloud/media/cover.jpg"
    artwork = AsyncMock()
    monkeypatch.setattr(metadata_task, "refresh_series_artwork", artwork)
    original = metadata_task.save_sweep
    failed = False

    async def fail_checkpoint(session, task_id, state):
        nonlocal failed
        if state.cursor == series_id and not failed:
            failed = True
            raise RuntimeError("checkpoint failed before commit")
        await original(session, task_id, state)

    monkeypatch.setattr(metadata_task, "save_sweep", fail_checkpoint)
    await metadata_task.refresh_metadata()
    assert adapter.calls
    artwork.assert_not_awaited()
    scheduler._scheduler.add_job.assert_not_called()
    async with factory() as session:
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1


async def test_search_is_committed_before_optional_artwork_wait(scheduled, monkeypatch):
    factory, adapter, scheduler = scheduled
    series_id, data = await add_series(factory)
    offer(adapter, data)
    adapter.data.series.image_url = "https://static.metron.cloud/media/cover.jpg"

    async def artwork(session, sid, url, covers):
        assert sid == series_id
        assert not session.in_transaction()
        scheduler._scheduler.add_job.assert_called_once()
        async with factory() as reader:
            assert (await load_metadata_baseline(reader, Kind.SERIES, sid)).revision == 2
            assert (await load_sweep(reader, "refresh_metadata")).cursor == sid
        raise asyncio.CancelledError

    monkeypatch.setattr(metadata_task, "refresh_series_artwork", artwork)
    with pytest.raises(asyncio.CancelledError):
        await metadata_task.refresh_metadata()
    assert (await metadata_task.refresh_metadata()).status == "completed"
    scheduler._scheduler.add_job.assert_called_once()


@pytest.mark.parametrize("source", [Source.COMICVINE_LOCAL, Source.COMICVINE_API])
async def test_scheduled_comicvine_uses_shared_writer_and_configured_transport(scheduled, source):
    factory, adapter, scheduler = scheduled
    async with factory.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == source.value)
            .values(enabled=True)
        )
        if source is Source.COMICVINE_API:
            await save_comicvine_api_key(session, "scheduled-test-cv-token")
    series_id, data = await add_series(factory, source=source)
    adapter.source = source
    offer(adapter, data)
    await metadata_task.refresh_metadata()
    assert adapter.calls == [("series", "42"), ("issues", "42", 1)]
    async with factory() as session:
        series = await session.get(Series, series_id)
        assert series.comicvine_id == 42 and series.issue_count == 2
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 2
        assert (
            await session.scalar(select(Issue).where(Issue.issue_number_text == "50-X"))
        ).comicvine_id == 4201
    scheduler._scheduler.add_job.assert_called_once()
