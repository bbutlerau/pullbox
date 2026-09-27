"""Manual imported-series override helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol

from pullbox.core.exceptions import NotFoundError
from pullbox.models.import_job import ImportedSeries, ImportJob, ImportSeriesStatus
from pullbox.providers.base import SeriesMetadata
from pullbox.services.import_file_selection import sync_review_series_selection

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


SeriesMetadataFetcher = Callable[[int], Awaitable[SeriesMetadata]]
LogicalGroupKeyFunc = Callable[..., tuple[object, ...] | None]


class ReclassifyMatchedDuplicates(Protocol):
    """Callable contract for post-override duplicate reclassification."""

    def __call__(
        self,
        session: AsyncSession,
        job: ImportJob,
        *,
        series_ids: list[int] | None = None,
    ) -> Awaitable[int]: ...


class LogicalGroupSeriesIds(Protocol):
    """Callable contract for resolving import-series ids in a logical group."""

    def __call__(
        self,
        session: AsyncSession,
        job_id: int,
        group_key: tuple[object, ...],
        *,
        prefer_resolved_cv_only: bool = False,
    ) -> Awaitable[list[int]]: ...


class ResetSeriesGroupFiles(Protocol):
    """Callable contract for clearing file review state before rematching."""

    def __call__(
        self,
        session: AsyncSession,
        *,
        job_id: int,
        series_ids: list[int],
    ) -> Awaitable[None]: ...


class ConsolidateLogicalSeriesGroups(Protocol):
    """Callable contract for merging repeated logical import buckets."""

    def __call__(
        self,
        session: AsyncSession,
        job: ImportJob,
        *,
        series_ids: list[int] | None = None,
        prefer_resolved_cv_only: bool = False,
    ) -> Awaitable[dict[int, int]]: ...


class RunFileMatching(Protocol):
    """Callable contract for rerunning file matching after an override."""

    def __call__(
        self,
        session: AsyncSession,
        job: ImportJob,
        *,
        series_ids: list[int] | None = None,
    ) -> Awaitable[None]: ...


async def override_cv_id(
    session: AsyncSession,
    imported_series_id: int,
    cv_id: int,
    *,
    fetch_series_metadata: SeriesMetadataFetcher,
    reclassify_matched_series_duplicates: ReclassifyMatchedDuplicates,
    logical_series_group_key: LogicalGroupKeyFunc,
    logical_group_series_ids: LogicalGroupSeriesIds,
    reset_series_group_files: ResetSeriesGroupFiles,
    consolidate_logical_series_groups: ConsolidateLogicalSeriesGroups,
    run_file_matching: RunFileMatching,
    rematch_files: bool = True,
) -> ImportedSeries:
    """Apply a user-selected ComicVine id and rematch affected files."""
    item = await session.get(ImportedSeries, imported_series_id)
    if item is None:
        raise NotFoundError("ImportedSeries", imported_series_id)

    meta = await fetch_series_metadata(cv_id)

    item.user_selected_cv_id = cv_id
    item.cv_id = int(meta.provider_id)
    item.cv_title = meta.title
    item.cv_year = meta.year_start
    item.cv_publisher = meta.publisher
    item.cv_issue_count = meta.issue_count
    item.cv_url = meta.comicvine_url
    item.cv_match_score = 1.0
    item.cv_match_method = "user_override"
    item.status = ImportSeriesStatus.MATCHED
    item.diagnostics = {}

    await session.flush()
    job = await session.get(ImportJob, item.import_job_id)
    affected_series_ids = [item.id]
    canonical_series_id = item.id
    if job is not None:
        await reclassify_matched_series_duplicates(session, job, series_ids=[item.id])
        await session.flush()
        group_key = logical_series_group_key(item, prefer_resolved_cv_only=True)
        if group_key is not None:
            affected_series_ids = await logical_group_series_ids(
                session,
                job.id,
                group_key,
                prefer_resolved_cv_only=True,
            )
        await reset_series_group_files(
            session,
            job_id=job.id,
            series_ids=affected_series_ids,
        )
        consolidated = await consolidate_logical_series_groups(
            session,
            job,
            series_ids=affected_series_ids,
            prefer_resolved_cv_only=True,
        )
        canonical_series_id = consolidated.get(item.id, item.id)

    if job is not None and rematch_files:
        await run_file_matching(session, job, series_ids=[canonical_series_id])
        canonical_item = await session.get(ImportedSeries, canonical_series_id)
        if canonical_item is not None:
            await sync_review_series_selection(session, canonical_item)
            return canonical_item
    if job is not None:
        canonical_item = await session.get(ImportedSeries, canonical_series_id)
        if canonical_item is not None:
            diagnostics = dict(canonical_item.diagnostics or {})
            diagnostics["rematch_pending"] = True
            canonical_item.diagnostics = diagnostics
            return canonical_item
    return item
