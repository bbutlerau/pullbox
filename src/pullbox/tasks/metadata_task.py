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
                or (
                    3600
                    if outcome.status
                    in {SourceStatus.RATE_LIMITED, SourceStatus.AUTHENTICATION_FAILED}
                    else 300
                ),
            )
            for outcome in exc.outcomes
            if outcome.status
            in {
                SourceStatus.RATE_LIMITED,
                SourceStatus.AUTHENTICATION_FAILED,
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
        predicates = [
            eligible,
            Series.id > state.cursor,
            Series.id <= state.upper_bound,
            Series.issue_catalog_state != IssueCatalogState.HYDRATING,
        ]
        if task_id == "refresh_metadata":
            predicates.extend(
                [
                    Series.monitored.is_(True),
                    or_(
                        Series.metadata_last_refreshed.is_(None),
                        Series.metadata_last_refreshed
                        < datetime.now(UTC) - timedelta(days=refresh_days),
                    ),
                ]
            )
        ids = list(
            await session.scalars(
                select(Series.id)
                .where(*predicates)
                .order_by(Series.id)
                .limit(_METADATA_BATCH_SIZE + 1)
            )
        )
        if not ids:
            state.active = False
            state.retry_at = 0
            await save_sweep(session, task_id, state)
            await session.commit()
            schedule_sweep(task_id, state)
            return TaskExecutionResult(status="completed")

        schedule_sweep(task_id, state)
        if state.retry_at > datetime.now(UTC).timestamp():
            return TaskExecutionResult(status="waiting")
        await session.commit()
        processed = failed = new_issues = 0
        paused = False
        for series_id in ids[:_METADATA_BATCH_SIZE]:
            if processed and time.monotonic() - started >= _METADATA_BATCH_SECONDS:
                break
            series = await session.get(Series, series_id)
            if series is None:
                state.cursor = series_id
                await save_sweep(session, task_id, state)
                await session.commit()
                processed += 1
                continue
            previous_cursor = state.cursor
            refreshed: ScheduledSeriesRefresh | None = None
            try:
                async with asyncio.timeout(_METADATA_SERIES_SECONDS):
                    refreshed = (
                        await refresh_scheduled_series(session, series_id)
                        if task_id == "refresh_metadata"
                        else await sync_scheduled_issue_catalog(
                            session, series_id, refresh_days=refresh_days
                        )
                    )
                state.cursor = series_id
                state.retry_at = 0
                await save_sweep(session, task_id, state)
                await session.commit()
                processed += 1
            except Exception as exc:
                refreshed = None
                await session.rollback()
                state.cursor = previous_cursor
                pause_seconds = _provider_pause_seconds(exc)
                if pause_seconds is not None:
                    state.retry_at = (
                        datetime.now(UTC) + timedelta(seconds=pause_seconds)
                    ).timestamp()
                    await save_sweep(session, task_id, state)
                    await session.commit()
                    paused = True
                    logger.warning(
                        "metadata_sweep_paused",
                        task_id=task_id,
                        series_id=series_id,
                        retry_seconds=pause_seconds,
                        failure_type=type(exc).__name__,
                    )
                    break
                failed += 1
                processed += 1
                state.cursor = series_id
                await save_sweep(session, task_id, state)
                await session.commit()
                if isinstance(exc, SeriesRefreshError):
                    logger.warning(
                        f"{task_id}_series_failed",
                        series_id=series_id,
                        reason=str(exc),
                        source_statuses={
                            item.source.value: item.status.value for item in exc.outcomes
                        },
                    )
                else:
                    logger.exception(f"{task_id}_series_failed", series_id=series_id)

            # Search cannot precede the atomic issue/checkpoint/cursor commit.
            # Optional artwork never postpones wanted searches or rewinds progress.
            if refreshed is not None:
                new_issues += refreshed.added
                if refreshed.search_wanted:
                    _schedule_new_issue_search(series_id)
                if refreshed.cover_url:
                    await refresh_series_artwork(
                        session, series_id, refreshed.cover_url, refreshed.covers
                    )

        state.active = paused or processed < len(ids)
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
