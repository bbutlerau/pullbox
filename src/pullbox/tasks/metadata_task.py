"""Source-aware metadata sweeps with bounded batches and atomic library progress."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import or_, select

from pullbox.config import PullboxSettings, get_settings
from pullbox.core.exceptions import ProviderError
from pullbox.core.scheduler import TaskExecutionResult, get_scheduler
from pullbox.core.sqlite_lock import is_sqlite_locked_error
from pullbox.database import get_session_factory
from pullbox.models.series import IssueCatalogState, Series
from pullbox.providers.metadata.comicvine import ComicVineError
from pullbox.schemas.metadata_sources import SourceStatus
from pullbox.services.metadata_daily_sync import sync_scheduled_issue_catalog
from pullbox.services.metadata_scheduled_refresh import (
    ScheduledSeriesRefresh,
    refresh_scheduled_series,
    scheduled_series_eligibility,
)
from pullbox.services.metadata_series_refresh import SeriesRefreshError, refresh_series_artwork
from pullbox.services.metadata_series_retry import (
    admit_retry,
    prune_retries,
    retry_candidates,
    retry_deadline,
    retry_runtime,
    settle_retry,
)
from pullbox.tasks.metadata_sweep_state import save_sweep, schedule_sweep, start_sweep

logger = structlog.get_logger(__name__)

_METADATA_BATCH_SIZE = 25
_METADATA_BATCH_SECONDS = 120.0
_METADATA_SERIES_SECONDS = 900.0


def _metadata_refresh_days(settings: PullboxSettings) -> int:
    try:
        return int(settings.metadata_refresh_days)
    except (TypeError, ValueError):
        return 30


def _provider_pause_seconds(exc: Exception) -> float | None:
    if is_sqlite_locked_error(exc):
        return 60
    if isinstance(exc, TimeoutError):
        return 300
    if isinstance(exc, SeriesRefreshError):
        if exc.retry_after_seconds is not None:
            return float(max(60, exc.retry_after_seconds))
        delays = [
            max(
                60,
                outcome.retry_after_seconds
                or (3600 if outcome.status is SourceStatus.RATE_LIMITED else 300),
            )
            for outcome in exc.outcomes
            if outcome.status
            in {
                SourceStatus.RATE_LIMITED,
                SourceStatus.TIMEOUT,
                SourceStatus.UNAVAILABLE,
            }
        ]
        return float(min(delays)) if delays else None
    details = exc.details or {} if isinstance(exc, ProviderError) else {}
    status = exc.status_code if isinstance(exc, ComicVineError) else details.get("status_code")
    retryable = exc.retryable if isinstance(exc, ComicVineError) else details.get("retryable")
    if status in {100, 107, 401, 403, 420, 429}:
        retry = (
            exc.retry_after_seconds
            if isinstance(exc, ComicVineError)
            else details.get("retry_after_seconds")
        )
        return float(retry) if retry else 3600
    return 300 if retryable else None


async def _run_metadata_sweep(task_id: str) -> TaskExecutionResult:
    settings = get_settings()
    factory = get_session_factory()
    started = time.monotonic()
    async with factory() as session:
        eligible = await scheduled_series_eligibility(
            session, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
        state = await start_sweep(session, task_id, eligible=eligible)
        refresh_days = _metadata_refresh_days(settings)
        base = [eligible]
        if task_id == "refresh_metadata":
            base.append(Series.monitored.is_(True))
        await prune_retries(
            session,
            task_id,
            base[0]
            & (Series.monitored.is_(True) if task_id == "refresh_metadata" else Series.id > 0),
        )
        runtime = await retry_runtime(session, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled)
        predicates = [
            *base,
            Series.id > state.cursor,
            Series.id <= state.upper_bound,
            Series.issue_catalog_state != IssueCatalogState.HYDRATING,
        ]
        if task_id == "refresh_metadata":
            predicates.append(
                or_(
                    Series.metadata_last_refreshed.is_(None),
                    Series.metadata_last_refreshed
                    < datetime.now(UTC) - timedelta(days=refresh_days),
                )
            )
        ids = list(
            await session.scalars(
                select(Series.id)
                .where(*predicates)
                .order_by(Series.id)
                .limit(_METADATA_BATCH_SIZE + 1)
            )
        )
        pending = await retry_candidates(session, task_id, runtime, limit=_METADATA_BATCH_SIZE)
        due = (
            list(
                await session.scalars(
                    select(Series.id)
                    .where(
                        *base,
                        Series.id.in_(pending),
                        Series.issue_catalog_state != IssueCatalogState.HYDRATING,
                    )
                    .order_by(Series.id)
                )
            )
            if pending
            else []
        )
        if not ids and not due:
            deadline = await retry_deadline(session, task_id, runtime)
            state.active = deadline is not None
            state.retry_at = deadline.timestamp() if deadline else 0
            await save_sweep(session, task_id, state)
            await session.commit()
            schedule_sweep(task_id, state)
            return TaskExecutionResult(status="waiting" if state.active else "completed")

        schedule_sweep(task_id, state)
        if state.retry_at > datetime.now(UTC).timestamp() and not due:
            await session.commit()
            return TaskExecutionResult(status="waiting")
        await session.commit()
        # Reserve space for both fresh work and overdue retries, avoiding starvation.
        quota = max(1, _METADATA_BATCH_SIZE // 2) if due else _METADATA_BATCH_SIZE
        work = [(sid, False) for sid in ids[:quota]]
        selected = {sid for sid, _ in work}
        work.extend((sid, True) for sid in due if sid not in selected)
        processed = normal_done = failed = new_issues = 0
        paused = False
        for series_id, retry_only in work[:_METADATA_BATCH_SIZE]:
            if processed and time.monotonic() - started >= _METADATA_BATCH_SECONDS:
                break
            previous_cursor = state.cursor
            admission = await admit_retry(
                session,
                task_id,
                series_id,
                gcd_api_enabled=settings.metadata_gcd_api_v2_enabled,
                retry_only=retry_only,
            )
            refreshed: ScheduledSeriesRefresh | None = None
            committing = False
            try:
                async with asyncio.timeout(_METADATA_SERIES_SECONDS):
                    refreshed = (
                        await refresh_scheduled_series(
                            session, series_id, registry=admission.registry
                        )
                        if task_id == "refresh_metadata"
                        else await sync_scheduled_issue_catalog(
                            session,
                            series_id,
                            refresh_days=refresh_days,
                            registry=admission.registry,
                        )
                    )
                committing = True
                await settle_retry(
                    session, task_id, series_id, admission, outcomes=refreshed.outcomes
                )
                if not retry_only:
                    state.cursor = series_id
                state.retry_at = 0
                await save_sweep(session, task_id, state)
                await session.commit()
            except Exception as exc:
                refreshed = None
                await session.rollback()
                state.cursor = previous_cursor
                if committing:
                    # A failed atomic write must leave the previous cursor/retry intact.
                    raise
                if is_sqlite_locked_error(exc):
                    state.retry_at = (datetime.now(UTC) + timedelta(seconds=60)).timestamp()
                    await save_sweep(session, task_id, state)
                    await session.commit()
                    paused = True
                    logger.warning(
                        "metadata_sweep_paused",
                        task_id=task_id,
                        series_id=series_id,
                        retry_seconds=60,
                        failure_type=type(exc).__name__,
                    )
                    break
                outcomes = exc.outcomes if isinstance(exc, SeriesRefreshError) else ()
                await settle_retry(
                    session,
                    task_id,
                    series_id,
                    admission,
                    outcomes=outcomes,
                    retry_seconds=_provider_pause_seconds(exc) if not outcomes else None,
                )
                if not retry_only:
                    state.cursor = series_id
                state.retry_at = 0
                await save_sweep(session, task_id, state)
                await session.commit()
                failed += 1
                if isinstance(exc, SeriesRefreshError):
                    logger.warning(
                        f"{task_id}_series_deferred"
                        if _provider_pause_seconds(exc) or outcomes
                        else f"{task_id}_series_failed",
                        series_id=series_id,
                        reason=str(exc),
                        source_statuses={item.source.value: item.status.value for item in outcomes},
                    )
                else:
                    logger.exception(f"{task_id}_series_failed", series_id=series_id)
            processed += 1
            normal_done += int(not retry_only)
            # Issue writes, deferred source work and sweep progress precede search.
            if refreshed is not None:
                new_issues += refreshed.added
                if refreshed.search_wanted:
                    _schedule_new_issue_search(series_id)
                if refreshed.cover_url:
                    await refresh_series_artwork(
                        session, series_id, refreshed.cover_url, refreshed.covers
                    )

        deadline = await retry_deadline(session, task_id, runtime)
        more = normal_done < len(ids)
        state.active = paused or more or deadline is not None
        if not paused:
            state.retry_at = 0 if more or deadline is None else deadline.timestamp()
        await save_sweep(session, task_id, state)
        await session.commit()
    schedule_sweep(task_id, state)
    logger.info(
        f"{task_id}_batch_complete",
        series_checked=processed,
        new_issues=new_issues,
        failed=failed,
        cursor=state.cursor,
        upper_bound=state.upper_bound,
        waiting=state.active,
    )
    return TaskExecutionResult(status="waiting" if state.active else "completed")


def _schedule_new_issue_search(series_id: int) -> None:
    from pullbox.tasks.search_task import search_series_issues

    get_scheduler()._scheduler.add_job(
        search_series_issues,
        trigger="date",
        args=[series_id],
        id=f"search_new_{series_id}_{int(time.time())}",
        misfire_grace_time=300,
    )


async def sync_new_issues() -> TaskExecutionResult:
    """Resume a bounded all-series issue sweep without monopolizing the scheduler."""
    return await _run_metadata_sweep("sync_new_issues")


async def refresh_metadata() -> TaskExecutionResult:
    """Resume a bounded sweep of stale monitored-series metadata."""
    return await _run_metadata_sweep("refresh_metadata")
