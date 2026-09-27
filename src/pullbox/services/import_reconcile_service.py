"""Service helpers for Step 3 import issue reconciliation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from pullbox.core.exceptions import NotFoundError, ValidationError
from pullbox.models.import_job import (
    ImportedFile,
    ImportedFileStatus,
    ImportedSeries,
    ImportJob,
    ImportJobStatus,
    ImportSeriesStatus,
)
from pullbox.services.import_file_selection import sync_review_series_selection
from pullbox.services.import_reconcile_helpers import (
    apply_reconcile_decisions,
    build_reconcile_file_rows,
    issue_options_for_reconcile_series,
    provisional_issue_number_for_file,
    provisional_issue_type_for_file,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.schemas.import_job import ImportReconcileRequest
    from pullbox.services.metadata_service import MetadataService

    RecomputeFileCountersFunc = Callable[..., Awaitable[None]]
    RecomputeSeriesCountersFunc = Callable[[AsyncSession, ImportJob], Awaitable[None]]
    LogEventFunc = Callable[..., Awaitable[None]]


async def load_import_reconcile_item(
    session: AsyncSession,
    job_id: int,
    imported_series_id: int,
) -> tuple[ImportJob, ImportedSeries]:
    """Load a REVIEW-state import row that already has a known series target."""
    job = await session.get(ImportJob, job_id)
    if job is None:
        raise NotFoundError("ImportJob", job_id)
    if job.status != ImportJobStatus.REVIEW:
        raise ValidationError("Import reconciliation is only available during review.")

    item = await session.get(ImportedSeries, imported_series_id)
    if item is None or item.import_job_id != job_id:
        raise NotFoundError("ImportedSeries", imported_series_id)
    if item.cv_id is None and item.series_id is None and item.user_selected_cv_id is None:
        raise ValidationError("Choose a ComicVine match before reconciling files.")
    return job, item


async def build_import_reconcile_context(
    session: AsyncSession,
    job_id: int,
    imported_series_id: int,
    *,
    metadata_service: MetadataService,
) -> dict[str, Any]:
    """Build the Step 3 issue-reconciliation context for one review row."""
    _job, item = await load_import_reconcile_item(session, job_id, imported_series_id)

    files_result = await session.execute(
        select(ImportedFile)
        .where(ImportedFile.import_series_id == item.id)
        .order_by(ImportedFile.id.asc())
    )
    files = list(files_result.scalars().all())
    issue_options, _local_issue_by_cv_id = await issue_options_for_reconcile_series(
        session,
        item,
        metadata_service,
    )
    file_rows, files_remaining, files_completed = build_reconcile_file_rows(
        item,
        files,
        issue_options,
    )

    return {
        "imported_series": item,
        "issue_options": issue_options,
        "files": file_rows,
        "files_remaining": files_remaining,
        "files_completed": files_completed,
    }


async def reconcile_import_series_decisions(
    session: AsyncSession,
    job_id: int,
    imported_series_id: int,
    request: ImportReconcileRequest,
    *,
    metadata_service: MetadataService,
    recompute_file_counters: RecomputeFileCountersFunc,
    recompute_series_counters: RecomputeSeriesCountersFunc,
    log_event: LogEventFunc,
) -> ImportedSeries:
    """Save Step 3 issue decisions without importing or moving any files."""
    job, item = await load_import_reconcile_item(session, job_id, imported_series_id)
    if not request.decisions:
        raise ValidationError("Choose at least one file decision before saving.")
    keep_duplicate_row = item.status == ImportSeriesStatus.DUPLICATE and item.series_id is not None

    files_result = await session.execute(
        select(ImportedFile)
        .where(ImportedFile.import_series_id == item.id)
        .order_by(ImportedFile.id.asc())
    )
    files = list(files_result.scalars().all())
    issue_options, local_issue_by_cv_id = await issue_options_for_reconcile_series(
        session,
        item,
        metadata_service,
    )
    apply_reconcile_decisions(
        item=item,
        files=files,
        decisions=request.decisions,
        issue_options=issue_options,
        local_issue_by_cv_id=local_issue_by_cv_id,
        provisional_issue_number_for_file=provisional_issue_number_for_file,
        provisional_issue_type_for_file=provisional_issue_type_for_file,
    )

    await recompute_file_counters(session, job, series_ids=[item.id])
    pending_count = int(
        await session.scalar(
            select(func.count(ImportedFile.id)).where(
                ImportedFile.import_series_id == item.id,
                ImportedFile.status == ImportedFileStatus.PENDING,
            )
        )
        or 0
    )
    unresolved_count = int(item.files_no_match or 0) + pending_count
    if keep_duplicate_row:
        item.status = ImportSeriesStatus.DUPLICATE
    elif unresolved_count > 0:
        item.status = ImportSeriesStatus.NO_MATCH
    elif item.files_matched > 0:
        item.status = ImportSeriesStatus.MATCHED
    else:
        item.status = ImportSeriesStatus.SKIPPED
    await sync_review_series_selection(session, item)

    await recompute_series_counters(session, job)
    await session.flush()
    await log_event(
        session,
        job.id,
        "INFO",
        "import_series_reconciled",
        message=f"Reconciled import files for '{item.raw_series_name}'",
        imported_series_id=item.id,
        decisions=len(request.decisions),
        unresolved_count=unresolved_count,
    )
    return item
