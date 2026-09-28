"""Event subscribers — handlers wired to the application event bus.

Each handler receives an event dataclass and performs side effects such as
updating database records or logging summaries.  Handlers obtain their own
database sessions since they run outside of request scope.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select
from sqlalchemy.orm import aliased

from pullbox.database import get_session_factory
from pullbox.models.download import DownloadHistory
from pullbox.models.issue import Issue, IssueStatus
from pullbox.models.library import LibraryFile, MatchConfidence
from pullbox.models.search_log import SearchLog
from pullbox.models.series import IssueCatalogState, Series

if TYPE_CHECKING:
    from pullbox.core.events import (
        DownloadCompleted,
        DownloadFailed,
        FileMatched,
        SeriesAdded,
    )

logger = structlog.get_logger(__name__)

# Strong references to background tasks to prevent garbage collection
_background_tasks: set[asyncio.Task[None]] = set()
SEARCH_ON_ADD_INITIAL_DELAY_SECONDS = 0.5
SEARCH_ON_ADD_RETRY_DELAY_SECONDS = 5.0
SEARCH_ON_ADD_MAX_READINESS_ATTEMPTS = 24
SEARCH_ON_ADD_RECOVERY_WINDOW_HOURS = 24
SEARCH_ON_ADD_RECOVERY_LIMIT = 10


def _track_background_task(task: asyncio.Task[None]) -> None:
    """Keep a strong reference until a fire-and-forget task completes."""
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def on_download_completed(event: DownloadCompleted) -> None:
    """Record the downloaded path when a download completes.

    Note: issue status is NOT set to OWNED here. That only happens
    in register_library_file() after post-processing successfully
    transfers and registers the file. This prevents orphaned "owned"
    issues with no library file.
    """
    log = logger.bind(
        download_id=event.download_id,
        issue_id=event.issue_id,
        file_path=event.file_path,
    )
    log.info("subscriber_download_completed")

    factory = get_session_factory()
    async with factory() as session:
        try:
            download = await session.get(DownloadHistory, event.download_id)
            if download:
                download.downloaded_path = event.file_path or download.downloaded_path

            await session.commit()
        except Exception:
            await session.rollback()
            log.exception("subscriber_download_completed_failed")


async def on_download_failed(event: DownloadFailed) -> None:
    """Revert the issue status to wanted when a download fails."""
    log = logger.bind(
        download_id=event.download_id,
        issue_id=event.issue_id,
        error=event.error,
    )
    log.warning("subscriber_download_failed")

    factory = get_session_factory()
    async with factory() as session:
        try:
            issue = await session.get(Issue, event.issue_id)
            if issue and issue.status == IssueStatus.DOWNLOADING:
                issue.status = IssueStatus.WANTED

            await session.commit()
        except Exception:
            await session.rollback()
            log.exception("subscriber_download_failed_error")


async def on_file_matched(event: FileMatched) -> None:
    """Update issue status and link library file on a high-confidence match."""
    log = logger.bind(
        library_file_id=event.library_file_id,
        issue_id=event.issue_id,
        confidence=event.confidence,
    )
    log.info("subscriber_file_matched")

    factory = get_session_factory()
    async with factory() as session:
        try:
            library_file = await session.get(LibraryFile, event.library_file_id)
            if library_file:
                library_file.issue_id = event.issue_id

            if event.confidence in (MatchConfidence.HIGH, MatchConfidence.MANUAL):
                issue = await session.get(Issue, event.issue_id)
                if issue:
                    issue.status = IssueStatus.OWNED

            await session.commit()
        except Exception:
            await session.rollback()
            log.exception("subscriber_file_matched_failed")


async def on_series_added(event: SeriesAdded) -> None:
    """Schedule background tasks for a newly added series.

    Launches cover downloads and indexer searches as background tasks so
    the calling request can commit and respond immediately.
    """
    cover_task = asyncio.create_task(_download_covers_for_series(event))
    _track_background_task(cover_task)

    search_task = asyncio.create_task(_search_new_series(event))
    _track_background_task(search_task)


async def _download_covers_for_series(event: SeriesAdded) -> None:
    """Download cover images for a series and its issues in the background.

    Uses the CDN URLs persisted on each record to download local copies.
    Each download uses its own session to avoid expiry issues after commit.
    """
    from sqlalchemy import select

    from pullbox.models.series import Series
    from pullbox.services.provider_artwork import (
        ProviderArtworkClient,
        issue_cover_stem,
        pending_provider_cover,
    )

    log = logger.bind(series_id=event.series_id)
    log.info("subscriber_cover_download_start")

    # Small delay to let the triggering request commit first
    await asyncio.sleep(1.0)

    factory = get_session_factory()

    # Load series info and build a work list in one session, then close it
    try:
        async with factory() as session:
            series = await session.get(Series, event.series_id)
            if not series:
                log.warning("subscriber_cover_download_series_not_found")
                return

            series_id = series.id
            series_cover_url = series.cover_url
            series_has_cover = bool(series.cover_path)

            # Preserve exact designations: numeric compatibility values can collide.
            result = await session.execute(
                select(
                    Issue.id, Issue.cover_url, Issue.issue_number, Issue.issue_number_text
                ).where(
                    Issue.series_id == event.series_id,
                    Issue.cover_url.isnot(None),
                    Issue.cover_path.is_(None),
                )
            )
            issue_work: list[tuple[int, str, float, str | None]] = [
                (row.id, row.cover_url, row.issue_number, row.issue_number_text)
                for row in result.all()
            ]
    except Exception:
        log.exception("subscriber_cover_download_load_failed")
        return

    # Resolve the .covers/ directory (under comics_directory or legacy fallback)
    from pullbox.services.cover_resolver import resolve_covers_dir

    try:
        async with factory() as session:
            covers_base = await resolve_covers_dir(session)
    except Exception:
        log.exception("subscriber_cover_resolve_dir_failed")
        return

    covers_dir = covers_base / str(series_id)

    downloaded = 0
    async with ProviderArtworkClient() as artwork:
        if series_cover_url and not series_has_cover:
            try:
                series_cover_dest = covers_dir / "series.jpg"
                async with pending_provider_cover(
                    artwork, series_cover_url, series_cover_dest
                ) as pending:
                    if pending is not None:
                        async with factory() as session:
                            series_obj = await session.get(Series, series_id)
                            if (
                                series_obj
                                and not series_obj.cover_path
                                and series_obj.cover_url == series_cover_url
                            ):
                                pending.replace(series_cover_dest)
                                series_obj.cover_path = f"/api/v1/series/{series_id}/cover"
                                await session.commit()
                                log.info("subscriber_series_cover_downloaded")
            except Exception:
                log.exception("subscriber_series_cover_failed")

        for issue_id, cover_url, issue_number, exact in issue_work:
            try:
                stem = issue_cover_stem(issue_number, exact)
                cover_dest = covers_dir / f"{stem}.jpg"
                async with pending_provider_cover(artwork, cover_url, cover_dest) as pending:
                    if pending is not None:
                        async with factory() as session:
                            issue_obj = await session.get(Issue, issue_id)
                            if (
                                issue_obj
                                and not issue_obj.cover_path
                                and issue_obj.series_id == series_id
                                and issue_obj.cover_url == cover_url
                                and issue_cover_stem(
                                    issue_obj.issue_number, issue_obj.issue_number_text
                                )
                                == stem
                            ):
                                pending.replace(cover_dest)
                                issue_obj.cover_path = f"/api/v1/issues/{issue_id}/cover"
                                await session.commit()
                                downloaded += 1
            except Exception:
                log.exception("subscriber_issue_cover_failed", issue_id=issue_id)

            await asyncio.sleep(0.2)

    log.info(
        "subscriber_cover_download_complete",
        total_issues=len(issue_work),
        downloaded=downloaded,
    )


async def _search_new_series(event: SeriesAdded) -> None:
    """Search indexers for wanted issues if the series is monitored.

    Checks the series' ``monitored`` flag before triggering a search.
    Delegates the actual search to ``search_series_issues()`` in the
    search task module.
    """
    log = logger.bind(series_id=event.series_id)
    log.info("subscriber_series_added_search_start")

    ready = False
    last_catalog_state: str | None = None
    for attempt in range(1, SEARCH_ON_ADD_MAX_READINESS_ATTEMPTS + 1):
        delay = (
            SEARCH_ON_ADD_INITIAL_DELAY_SECONDS
            if attempt == 1
            else SEARCH_ON_ADD_RETRY_DELAY_SECONDS
        )
        await asyncio.sleep(delay)

        factory = get_session_factory()
        async with factory() as session:
            series = await session.get(Series, event.series_id)
            if not series:
                log.info(
                    "subscriber_series_added_search_waiting",
                    reason="series is not committed yet",
                    attempt=attempt,
                )
                continue

            if not series.monitored:
                log.info(
                    "subscriber_series_added_search_skipped",
                    reason="series is not monitored",
                )
                return

            last_catalog_state = series.issue_catalog_state.value
            if series.issue_catalog_state == IssueCatalogState.COMPLETE:
                ready = True
                break

            log.info(
                "subscriber_series_added_search_waiting",
                reason="issue catalog is not complete",
                issue_catalog_state=last_catalog_state,
                attempt=attempt,
            )

    if not ready:
        log.warning(
            "subscriber_series_added_search_skipped",
            reason="issue catalog did not become ready",
            issue_catalog_state=last_catalog_state,
            attempts=SEARCH_ON_ADD_MAX_READINESS_ATTEMPTS,
        )
        return

    # Series is monitored — delegate to reusable helper
    from pullbox.tasks.search_task import search_series_issues

    await search_series_issues(event.series_id)


async def load_recent_search_on_add_misses(
    *,
    limit: int = SEARCH_ON_ADD_RECOVERY_LIMIT,
    window_hours: int = SEARCH_ON_ADD_RECOVERY_WINDOW_HOURS,
) -> list[int]:
    """Find recent add-series rows that should have auto-searched but did not.

    The normal search-on-add trigger is intentionally asynchronous. If the app
    restarts or a transient subscriber race loses that fire-and-forget task,
    recent monitored series can be left with wanted issues and no search logs.
    This query limits recovery to fresh additions to avoid surprising users by
    sweeping old monitored libraries on startup.
    """
    cutoff = datetime.now(UTC) - timedelta(hours=window_hours)
    issue_for_log = aliased(Issue)
    existing_search = (
        select(SearchLog.id)
        .join(issue_for_log, SearchLog.issue_id == issue_for_log.id)
        .where(issue_for_log.series_id == Series.id)
        .exists()
    )
    stmt = (
        select(Series.id)
        .join(Issue, Issue.series_id == Series.id)
        .where(
            Series.monitored.is_(True),
            Series.issue_catalog_state == IssueCatalogState.COMPLETE,
            Series.created_at >= cutoff,
            Issue.status == IssueStatus.WANTED,
            ~existing_search,
        )
        .group_by(Series.id)
        .order_by(Series.created_at.asc())
        .limit(limit)
    )

    factory = get_session_factory()
    async with factory() as session:
        result = await session.execute(stmt)
        return [int(series_id) for series_id in result.scalars().all()]


async def recover_recent_search_on_add_misses() -> int:
    """Schedule searches for recent monitored series that missed search-on-add."""
    series_ids = await load_recent_search_on_add_misses()
    for series_id in series_ids:
        task = asyncio.create_task(_run_recovered_search_on_add(series_id))
        _track_background_task(task)
    if series_ids:
        logger.info(
            "subscriber_search_on_add_recovery_scheduled",
            series_count=len(series_ids),
            series_ids=series_ids,
        )
    return len(series_ids)


async def _run_recovered_search_on_add(series_id: int) -> None:
    from pullbox.tasks.search_task import search_series_issues

    await search_series_issues(series_id)
