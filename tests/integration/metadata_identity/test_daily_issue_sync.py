"""Daily sync uses real configured source reads, canonical writes and sweep progress."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from pullbox.core.comicvine_key import save_comicvine_api_key
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Issue, Series, SeriesCatalogCheckpoint
from pullbox.models.issue import IssueStatus
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.series import IssueCatalogState, SeriesStatus
from pullbox.schemas.metadata_sources import MetadataFetch, RecentIssueWindow, SourceStatus
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_catalog_checkpoints import (
    load_catalog_checkpoint,
    save_full_catalog_checkpoint,
)
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.tasks import metadata_task
from pullbox.tasks.metadata_sweep_state import load_sweep
from tests.integration.metadata_identity.test_scheduled_series_refresh import (  # noqa: F401
    add_series,
    configured_sources,
    offer,
    scheduled,
)
from tests.integration.metadata_identity.test_series_adoption import bundle
from tests.integration.metadata_identity.test_series_refresh import RefreshAdapter
from tests.unit.test_metadata_source_reads import registry


class DailyAdapter(RefreshAdapter):
    def __init__(self, data):
        super().__init__(data)
        self.scope = "modified_since"
        self.recent_failure = None
        self.recent_wait = None
        self.recent_started = asyncio.Event()
        self.window = None

    async def recent_issues(self, external_id, *, since):
        assert self.session is None or not self.session.in_transaction()
        self.calls.append(("recent", external_id, since))
        self.recent_started.set()
        if self.recent_wait:
            await self.recent_wait.wait()
        if self.recent_failure:
            raise MetadataSourceError(self.recent_failure, retry_after_seconds=720)
        rows = self.data.issues if self.scope == "recent_publication" else self.data.issues[1:]
        rows = [
            row.model_copy(update={"source_updated_at": datetime.now(UTC)})
            if self.scope == "modified_since"
            else row
            for row in rows
        ]
        window = self.window or RecentIssueWindow(
            results=rows[:100],
            matched_total=len(rows),
            truncated=len(rows) > 100,
            scope=self.scope,
            since=since if self.scope == "modified_since" else None,
            source_updated_at=self.data.series.source_updated_at,
        )
        return MetadataFetch(status=SourceStatus.OK, data=window)


@pytest.fixture
async def daily(scheduled, monkeypatch):  # noqa: F811 - imported pytest fixture
    from pullbox.providers.metadata import sources

    factory, _, scheduler = scheduled
    adapter = DailyAdapter(bundle(numbers=("1", "2")))
    monkeypatch.setattr(sources, "metadata_sources", lambda: registry(adapter).factories)
    return factory, adapter, scheduler


async def checkpoint(factory, series_id, data, *, days=2, full_days=None):
    started = datetime.now(UTC) - timedelta(days=days)
    async with factory.begin() as session:
        saved = await save_full_catalog_checkpoint(
            session,
            series_id,
            source=data.series.source,
            source_revision=1,
            identity_revision=1,
            external_id=data.series.external_id,
            started_at=started,
            source_updated_at=data.series.source_updated_at,
        )
        if full_days is not None:
            await session.execute(
                update(SeriesCatalogCheckpoint).values(
                    full_synced_at=datetime.now(UTC) - timedelta(days=full_days)
                )
            )
        series = await session.get(Series, series_id)
        series.issue_catalog_last_checked_at = started
        series.issue_catalog_last_synced_at = started
    return saved


async def test_daily_native_full_sync_without_cv_key_commits_before_search(daily, monkeypatch):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    offer(adapter, data)
    search = []
    original = metadata_task._schedule_new_issue_search

    def schedule(identifier):
        search.append(identifier)
        original(identifier)

    monkeypatch.setattr(metadata_task, "_schedule_new_issue_search", schedule)
    result = await metadata_task.sync_new_issues()
    assert adapter.calls, "Native daily synchronization must not require a ComicVine API key"
    assert result.status == "completed" and search == [series_id]
    async with factory() as session:
        series = await session.get(Series, series_id)
        assert series.issue_count == 2 and series.path == "/read-only/original"
        issues = list(await session.scalars(select(Issue).order_by(Issue.id)))
        assert issues[0].status is IssueStatus.OWNED and issues[0].manual_skip
        assert issues[1].issue_number_text == "50-X" and issues[1].status is IssueStatus.WANTED
        assert await load_catalog_checkpoint(session, series_id, Source.METRON_API)
        state = await load_sweep(session, "sync_new_issues")
        assert state.cursor == series_id and not state.active
    scheduler._scheduler.add_job.assert_called_once()


async def test_truncated_modification_window_requires_complete_reconciliation(daily):
    factory, adapter, _ = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    rows = tuple(
        data.issues[0].model_copy(
            update={
                "external_id": str(int(data.issues[0].external_id) + i),
                "issue_number_text": str(i + 1),
                "issue_number_key": str(i + 1),
            }
        )
        for i in range(102)
    )
    adapter.data = replace(
        data,
        series=data.series.model_copy(update={"issue_count": 102}),
        issues=rows,
        catalog_total=102,
    )
    await metadata_task.sync_new_issues()
    assert [call[0] for call in adapter.calls] == ["recent", "series", "issues", "issues"]
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 102
        saved = await load_catalog_checkpoint(session, series_id, Source.METRON_API)
        assert (
            saved.full_synced_at > old.full_synced_at and saved.full_synced_at == saved.checked_at
        )


async def test_empty_modified_window_updates_progress_without_rewriting_metadata(daily):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    adapter.data = data
    await metadata_task.sync_new_issues()
    assert [call[0] for call in adapter.calls] == ["recent"]
    async with factory() as session:
        assert (await load_metadata_baseline(session, Kind.SERIES, series_id)).revision == 1
        saved = await load_catalog_checkpoint(session, series_id, Source.METRON_API)
        assert saved.revision == 2 and saved.full_synced_at == old.full_synced_at
    scheduler._scheduler.add_job.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        SourceStatus.RATE_LIMITED,
        SourceStatus.AUTHENTICATION_FAILED,
        SourceStatus.TIMEOUT,
        SourceStatus.UNAVAILABLE,
    ],
)
async def test_daily_window_failure_preserves_issue_checkpoint_and_durable_retry(daily, failure):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    offer(adapter, data)
    adapter.recent_failure = failure
    result = await metadata_task.sync_new_issues()
    assert result.status == "waiting"
    async with factory() as session:
        assert await load_catalog_checkpoint(session, series_id, Source.METRON_API) == old
        assert (await session.get(Series, series_id)).issue_count == 1
        state = await load_sweep(session, "sync_new_issues")
        assert state.cursor == 0 and state.retry_at > datetime.now(UTC).timestamp()
    await metadata_task.sync_new_issues()
    assert len(adapter.calls) == 1
    scheduler._scheduler.add_job.assert_not_called()


@pytest.mark.parametrize("change", ["policy", "identity", "baseline", "monitor", "checkpoint"])
async def test_inflight_daily_change_rolls_back_entire_window(daily, change):
    from pullbox.models.metadata_baseline import SeriesMetadataBaseline
    from pullbox.models.metadata_identity import SeriesExternalIdentity

    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    offer(adapter, data)
    adapter.recent_wait = asyncio.Event()
    task = asyncio.create_task(metadata_task.sync_new_issues())
    try:
        await asyncio.wait_for(adapter.recent_started.wait(), 2)
        async with factory.begin() as session:
            if change == "policy":
                await session.execute(update(MetadataSourceConfig).values(revision=2))
            elif change == "identity":
                await session.execute(update(SeriesExternalIdentity).values(revision=2))
            elif change == "baseline":
                await session.execute(update(SeriesMetadataBaseline).values(revision=2))
            elif change == "monitor":
                (await session.get(Series, series_id)).monitored = False
            else:
                await session.execute(update(SeriesCatalogCheckpoint).values(revision=2))
        adapter.recent_wait.set()
        await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 1
        saved = await session.scalar(select(SeriesCatalogCheckpoint))
        assert saved.checked_at == old.checked_at and saved.full_synced_at == old.full_synced_at
    scheduler._scheduler.add_job.assert_not_called()


async def test_daily_source_reads_release_database_transaction(daily):
    from pullbox.services.metadata_daily_sync import sync_scheduled_issue_catalog

    factory, adapter, _ = daily
    series_id, data = await add_series(factory, fresh=True)
    await checkpoint(factory, series_id, data)
    offer(adapter, data)
    async with factory() as session:
        adapter.session = session
        result = await sync_scheduled_issue_catalog(session, series_id, refresh_days=30)
        assert result.added == 1
        await session.rollback()
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 1


@pytest.mark.parametrize("fallback_fails", [False, True])
async def test_daily_source_fallback_preserves_failed_source_progress(
    daily, monkeypatch, fallback_fails
):
    from pullbox.providers.metadata import sources

    factory, _, scheduler = daily
    async with factory.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
        for source, priority in [(Source.COMICVINE_API, 0), (Source.COMICVINE_LOCAL, 1)]:
            await session.execute(
                update(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == source.value)
                .values(enabled=True, domain_priorities={"issues": priority})
            )
        await save_comicvine_api_key(session, "daily-test-key")
    series_id, local_data = await add_series(factory, fresh=True, source=Source.COMICVINE_LOCAL)
    if not fallback_fails:
        await checkpoint(factory, series_id, local_data)
    api_data = replace(
        local_data,
        series=local_data.series.model_copy(update={"source": Source.COMICVINE_API}),
        issues=tuple(
            row.model_copy(update={"source": Source.COMICVINE_API}) for row in local_data.issues
        ),
    )
    old_api = await checkpoint(factory, series_id, api_data)
    local = DailyAdapter(local_data)
    local.source, local.scope = Source.COMICVINE_LOCAL, "recent_publication"
    offer(local, local_data)
    if fallback_fails:
        local.profile_failure = SourceStatus.NOT_FOUND
    api = DailyAdapter(api_data)
    api.source, api.scope = Source.COMICVINE_API, "recent_publication"
    api.recent_failure = SourceStatus.RATE_LIMITED
    factories = {**registry(api).factories, **registry(local).factories}
    monkeypatch.setattr(sources, "metadata_sources", lambda: factories)
    result = await metadata_task.sync_new_issues()
    assert result.status == ("waiting" if fallback_fails else "completed")
    assert [call[0] for call in api.calls] == ["recent"]
    assert [call[0] for call in local.calls] == (["series"] if fallback_fails else ["recent"])
    async with factory() as session:
        assert await load_catalog_checkpoint(session, series_id, Source.COMICVINE_API) == old_api
        assert (await session.get(Series, series_id)).issue_count == (1 if fallback_fails else 2)
    assert scheduler._scheduler.add_job.call_count == int(not fallback_fails)


async def test_daily_partial_window_advances_only_checked_floor_and_replays_safely(daily):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    offer(adapter, data)
    before = datetime.now(UTC)
    await metadata_task.sync_new_issues()
    assert [call[0] for call in adapter.calls] == ["recent"]
    since = adapter.calls[0][2]
    assert since < old.checked_at and old.checked_at - since <= timedelta(minutes=5)
    async with factory() as session:
        saved = await load_catalog_checkpoint(session, series_id, Source.METRON_API)
        assert saved.revision == 2 and saved.full_synced_at == old.full_synced_at
        assert before <= saved.checked_at <= datetime.now(UTC)
        series = await session.get(Series, series_id)
        assert series.issue_catalog_last_synced_at == old.full_synced_at
        assert series.issue_count == 2 and series.issue_catalog_state is IssueCatalogState.COMPLETE
        baseline = await load_metadata_baseline(session, Kind.SERIES, series_id)
        assert baseline.snapshot.values.issue_count == 2
    await metadata_task.sync_new_issues()
    assert len(adapter.calls) == 1
    scheduler._scheduler.add_job.assert_called_once()


@pytest.mark.parametrize("reason", ["stale", "invalidated", "partial", "metadata"])
async def test_daily_requires_full_read_for_stale_or_unproven_catalog(daily, reason):
    factory, adapter, _ = daily
    series_id, data = await add_series(factory, fresh=reason != "metadata")
    await checkpoint(factory, series_id, data, full_days=60 if reason == "stale" else None)
    async with factory.begin() as session:
        if reason == "invalidated":
            await session.execute(update(MetadataSourceConfig).values(revision=2))
        elif reason == "partial":
            (await session.get(Series, series_id)).issue_catalog_state = IssueCatalogState.PARTIAL
    offer(adapter, data)
    await metadata_task.sync_new_issues()
    assert [call[0] for call in adapter.calls] == ["series", "issues"]
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 2


@pytest.mark.parametrize("source", [Source.COMICVINE_LOCAL, Source.COMICVINE_API])
@pytest.mark.parametrize("changed", [False, True])
async def test_daily_cv_generation_and_publication_slice(daily, source, changed):
    factory, adapter, _ = daily
    async with factory.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False))
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == source.value)
            .values(enabled=True)
        )
        if source is Source.COMICVINE_API:
            await save_comicvine_api_key(session, "daily-test-key")
    series_id, data = await add_series(factory, fresh=True, source=source)
    await checkpoint(factory, series_id, data)
    adapter.source = source
    adapter.scope = "recent_publication"
    offer(adapter, data)
    if changed and source is Source.COMICVINE_LOCAL:
        newer = datetime.now(UTC)
        for row in (adapter.data.series, *adapter.data.issues):
            row.source_updated_at = newer
    if changed and source is Source.COMICVINE_API:
        adapter.window = RecentIssueWindow(
            results=[], matched_total=0, scope="recent_publication", truncated=False
        )
    await metadata_task.sync_new_issues()
    assert [call[0] for call in adapter.calls] == (
        ["recent", "series", "issues"] if changed else ["recent"]
    )
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 2


@pytest.mark.parametrize(
    "status,monitored,days,due",
    [
        (SeriesStatus.CONTINUING, True, 0, False),
        (SeriesStatus.CONTINUING, False, 2, True),
        (SeriesStatus.ENDED, True, 2, False),
        (SeriesStatus.ENDED, True, 15, True),
        (SeriesStatus.ENDED, False, 15, False),
        (SeriesStatus.ENDED, False, 31, True),
    ],
)
async def test_daily_preserves_cadence_and_unmonitored_issue_status(
    daily, status, monitored, days, due
):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True, monitored=monitored)
    await checkpoint(factory, series_id, data, days=days)
    async with factory.begin() as session:
        (await session.get(Series, series_id)).status = status
    offer(adapter, data)
    await metadata_task.sync_new_issues()
    assert bool(adapter.calls) is due
    if due:
        async with factory() as session:
            issue = await session.scalar(select(Issue).where(Issue.issue_number_text == "50-X"))
            assert issue.status is (IssueStatus.WANTED if monitored else IssueStatus.SKIPPED)
    assert scheduler._scheduler.add_job.call_count == int(due and monitored)


async def test_daily_failed_sweep_commit_does_not_publish_partial_metadata_or_search(
    daily, monkeypatch
):
    factory, adapter, scheduler = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    offer(adapter, data)
    original = metadata_task.save_sweep

    async def fail(session, task_id, state):
        if state.cursor == series_id:
            raise RuntimeError("sweep rejected")
        await original(session, task_id, state)

    monkeypatch.setattr(metadata_task, "save_sweep", fail)
    with pytest.raises(RuntimeError, match="sweep rejected"):
        await metadata_task.sync_new_issues()
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 1
        assert await load_catalog_checkpoint(session, series_id, Source.METRON_API) == old
    scheduler._scheduler.add_job.assert_not_called()


async def test_daily_cancelled_window_leaves_original_checkpoint_and_resumes(daily):
    factory, adapter, _ = daily
    series_id, data = await add_series(factory, fresh=True)
    old = await checkpoint(factory, series_id, data)
    offer(adapter, data)
    adapter.recent_wait = asyncio.Event()
    task = asyncio.create_task(metadata_task.sync_new_issues())
    try:
        await asyncio.wait_for(adapter.recent_started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        assert await load_catalog_checkpoint(session, series_id, Source.METRON_API) == old
        assert (await load_sweep(session, "sync_new_issues")).cursor == 0
    adapter.recent_wait = None
    await metadata_task.sync_new_issues()
    async with factory() as session:
        assert (await session.get(Series, series_id)).issue_count == 2
