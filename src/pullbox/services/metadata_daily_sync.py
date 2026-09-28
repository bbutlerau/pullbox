"""Bounded source-aware daily catalog checks with caller-owned progress."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.config import get_settings
from pullbox.core.exceptions import ValidationError
from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.models import Issue, Series
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.series import IssueCatalogState, SeriesStatus
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    RecentIssueWindow,
    SourceCapability,
    SourceOutcome,
    SourceStatus,
)
from pullbox.services.cover_resolver import resolve_covers_dir
from pullbox.services.metadata_assembly import assemble_metadata
from pullbox.services.metadata_baselines import MetadataBaselineWrite, save_metadata_baselines
from pullbox.services.metadata_catalog_checkpoints import (
    CatalogCheckpoint,
    advance_catalog_checkpoint,
    load_catalog_checkpoint,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_issue_catalog import SourceIssueBatch, apply_issue_batch
from pullbox.services.metadata_scheduled_refresh import ScheduledSeriesRefresh
from pullbox.services.metadata_series_refresh import (
    SeriesRefreshError,
    _with_derived,
    refresh_series_catalog_from_sources,
)
from pullbox.services.metadata_series_refresh_state import (
    SERIES_FIELDS,
    SeriesRefreshState,
    read_series_refresh_state,
)
from pullbox.services.metadata_sources import load_source_runtime
from pullbox.services.metadata_writer_identity import metadata_write_scope


@dataclass(frozen=True)
class _Cadence:
    monitored: bool
    catalog_state: IssueCatalogState
    metadata_refreshed: datetime | None
    checked: datetime | None
    synced: datetime | None


def _cadence(series: Series) -> _Cadence:
    return _Cadence(
        series.monitored,
        series.issue_catalog_state,
        series.metadata_last_refreshed,
        series.issue_catalog_last_checked_at,
        series.issue_catalog_last_synced_at,
    )


def _full_due(
    cadence: _Cadence, checkpoint: CatalogCheckpoint | None, now: datetime, days: int
) -> bool:
    return (
        checkpoint is None
        or cadence.catalog_state is not IssueCatalogState.COMPLETE
        or days <= 0
        or now - checkpoint.full_synced_at >= timedelta(days=days)
        or cadence.metadata_refreshed is None
        or now - cadence.metadata_refreshed >= timedelta(days=days)
    )


def _window_requires_full(
    window: RecentIssueWindow, checkpoint: CatalogCheckpoint, state: SeriesRefreshState
) -> bool:
    if (
        checkpoint.source in {MetadataSource.COMICVINE_LOCAL, MetadataSource.GCD_LOCAL}
        and window.source_updated_at != checkpoint.source_updated_at
    ):
        return True
    if window.scope == "modified_since":
        return window.truncated
    # Publication slices cannot hide a gap larger than the returned window.
    known = {
        ref.external_id
        for item in state.issues
        for ref in item.identities
        if ref.namespace is checkpoint.source.identity_namespace
    }
    additions = {item.external_id for item in window.results} - known
    return window.matched_total != len(known) + len(additions)


async def sync_scheduled_issue_catalog(
    session: AsyncSession,
    series_id: int,
    *,
    refresh_days: int,
    registry: MetadataSourceRegistry | None = None,
) -> ScheduledSeriesRefresh:
    """Read outside transactions, then save issues and progress for the task to commit."""
    if session.new or session.dirty or session.deleted:
        raise SeriesRefreshError("Finish pending library changes before synchronizing issues.")
    try:
        async with asyncio.timeout(120):
            return await _sync(session, series_id, refresh_days=refresh_days, registry=registry)
    except SeriesRefreshError:
        raise
    except (ValueError, IntegrityError, ValidationError) as exc:
        raise SeriesRefreshError(
            "The issue catalog could not be synchronized safely. Review its match or retry."
        ) from exc
    except TimeoutError as exc:
        raise SeriesRefreshError(
            "Issue synchronization timed out. Retry later.", retry_after_seconds=300
        ) from exc


async def _sync(
    session: AsyncSession,
    series_id: int,
    *,
    refresh_days: int,
    registry: MetadataSourceRegistry | None = None,
) -> ScheduledSeriesRefresh:
    before = await read_series_refresh_state(session, series_id)
    series = await session.get(Series, series_id)
    assert series is not None
    cadence = _cadence(series)
    covers = await resolve_covers_dir(session)
    enabled = get_settings().metadata_gcd_api_v2_enabled
    registry = registry or MetadataSourceRegistry(
        await load_source_runtime(session, gcd_api_enabled=enabled),
        gcd_api_enabled=enabled,
        revalidate_reads=True,
        total_timeout=60,
    )
    known = {ref.namespace: ref.external_id for ref in before.series.identities}
    sources = sorted(
        (
            source
            for source in registry.runtime
            if source.identity_namespace in known
            and registry.source_availability(source, SourceCapability.ISSUE_LIST) is None
            and registry.source_availability(source, SourceCapability.SERIES_DETAILS) is None
        ),
        key=lambda source: (
            registry.runtime[source].policy.domain_priorities.get(
                MetadataDomain.ISSUES, registry.runtime[source].policy.priority
            ),
            source.value,
        ),
    )
    checkpoints = {
        source: await load_catalog_checkpoint(session, series_id, source) for source in sources
    }
    await session.rollback()
    now = datetime.now(UTC)
    interval = timedelta(
        days=(14 if cadence.monitored else 30)
        if before.series.values.status == SeriesStatus.ENDED.value
        else 1
    )
    failures: list[SourceOutcome] = []
    for source in sources:
        checkpoint = checkpoints[source]
        full = _full_due(cadence, checkpoint, now, refresh_days)
        if not full and checkpoint is not None and now - checkpoint.checked_at < interval:
            return ScheduledSeriesRefresh(0, False, None, covers, tuple(failures))
        full = (
            full or registry.source_availability(source, SourceCapability.RECENT_ISSUES) is not None
        )
        if not full:
            assert checkpoint is not None
            started_at = datetime.now(UTC)
            # Overlap the strict modified_gt boundary, including second-resolution providers.
            since = checkpoint.checked_at.replace(microsecond=0) - timedelta(minutes=2)
            fetched = await registry.recent_issues(
                source, known[source.identity_namespace], since=since
            )
            if fetched.status is not SourceStatus.OK or fetched.data is None:
                outcome = SourceOutcome(
                    source=source,
                    status=fetched.status,
                    retry_after_seconds=fetched.retry_after_seconds,
                )
                if fetched.status is SourceStatus.INCOMPATIBLE_RESPONSE:
                    raise SeriesRefreshError(
                        "The provider issue window is inconsistent. Review its match.",
                        outcomes=(outcome,),
                    )
                failures.append(outcome)
                registry.runtime.pop(source)
                continue
            full = _window_requires_full(fetched.data, checkpoint, before)
            if not full:
                created = await _apply_window(
                    session, before, cadence, checkpoint, fetched.data, started_at
                )
                return ScheduledSeriesRefresh(
                    len(created), cadence.monitored and bool(created), None, covers, tuple(failures)
                )
        if full:
            try:
                result = await refresh_series_catalog_from_sources(
                    session, series_id, registry=registry, replace_managed=False
                )
            except SeriesRefreshError as exc:
                if not failures:
                    raise
                # A failed fallback must not discard an earlier source's retry window.
                raise SeriesRefreshError(
                    str(exc),
                    outcomes=(*failures, *exc.outcomes),
                    retry_after_seconds=exc.retry_after_seconds,
                ) from exc
            return ScheduledSeriesRefresh(
                len(result.created_issue_ids),
                result.series.monitored and bool(result.created_issue_ids),
                result.series.cover_url,
                covers,
                (*failures, *result.outcomes),
            )
    raise SeriesRefreshError(
        "No configured source could supply the issue catalog. Check source status and retry.",
        outcomes=tuple(failures),
    )


async def _apply_window(
    session: AsyncSession,
    before: SeriesRefreshState,
    cadence: _Cadence,
    checkpoint: CatalogCheckpoint,
    window: RecentIssueWindow,
    started_at: datetime,
) -> tuple[int, ...]:
    series_id = before.series.local_id
    async with metadata_write_scope(session):
        await session.execute(
            select(MetadataSourceConfig)
            .order_by(MetadataSourceConfig.source)
            .with_for_update(read=True)
        )
        await session.execute(select(Series.id).where(Series.id == series_id).with_for_update())
        await session.execute(
            select(Issue.id)
            .where(Issue.series_id == series_id)
            .order_by(Issue.id)
            .with_for_update()
        )
        current = await read_series_refresh_state(session, series_id)
        series = await session.get(Series, series_id)
        if current != before or series is None or _cadence(series) != cadence:
            raise SeriesRefreshError(
                "Library metadata or catalog progress changed. Retry synchronization."
            )
        created = await apply_issue_batch(
            session,
            current,
            SourceIssueBatch(
                checkpoint.source,
                checkpoint.external_id,
                tuple(window.results),
                checkpoint.source_revision,
            ),
            started_at,
        )
        if created:
            series.issue_count = len(current.issues) + len(created)
            snapshot = assemble_metadata(
                MetadataEntityKind.SERIES,
                current.series.identities,
                [],
                current.policies,
                now=started_at,
                current=current.series.values,
                previous=current.series.baseline,
                overrides=current.series.overrides,
                replace_managed=False,
                fields=SERIES_FIELDS,
            )
            snapshot = _with_derived(snapshot, "issue_count", series.issue_count, started_at)
            await save_metadata_baselines(
                session,
                [MetadataBaselineWrite(series_id, snapshot, current.series.baseline_revision)],
            )
        series.issue_catalog_last_checked_at = started_at
        series.issue_catalog_error = None
        await advance_catalog_checkpoint(
            session, checkpoint, started_at=started_at, source_updated_at=window.source_updated_at
        )
        await session.flush()
    return created
