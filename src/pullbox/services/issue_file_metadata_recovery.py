"""Settle only journaled existing-file metadata writes before utility recovery."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
from sqlalchemy import func, select

from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication, PublicationState
from pullbox.services.archive_metadata_publication import load_archive_publication
from pullbox.services.issue_file_metadata import file_metadata_error, recover_file_metadata
from pullbox.services.utility_operation_progress import project_utility_operation_progress
from pullbox.utilities.models import ItemState, JobState, JobType, UtilityJob, UtilityJobItem

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = structlog.get_logger(__name__)


async def recover_file_metadata_jobs(factory: async_sessionmaker[AsyncSession]) -> None:
    cursor = 0
    while True:
        async with factory() as session:
            rows = list(
                (
                    await session.execute(
                        select(
                            ArchiveMetadataPublication.id,
                            ArchiveMetadataPublication.operation_id,
                        )
                        .where(
                            ArchiveMetadataPublication.id > cursor,
                            ArchiveMetadataPublication.active_path_key.is_not(None),
                        )
                        .order_by(ArchiveMetadataPublication.id)
                        .limit(100)
                    )
                ).all()
            )
        if not rows:
            return
        for row_id, operation_id in rows:
            cursor = row_id
            async with factory() as session:
                receipt = await load_archive_publication(session, UUID(operation_id))
            if receipt is None or receipt.plan.metadata_job_id is None:
                continue
            try:
                state = await recover_file_metadata(factory, receipt.operation_id)
            except (ValueError, OSError) as exc:
                logger.warning(
                    "file_metadata_recovery_needs_review",
                    operation_id=operation_id,
                    reason=getattr(exc, "code", "unavailable"),
                )
                async with factory.begin() as session:
                    job = await session.get(UtilityJob, receipt.plan.metadata_job_id)
                    if job is not None and job.job_type == JobType.FILE_METADATA:
                        job.state = JobState.FAILED
                        job.error_message = file_metadata_error(exc)
                        await project_utility_operation_progress(session, job)
                continue
            if state is not PublicationState.FINALIZED:
                continue
            async with factory.begin() as session:
                item = await session.get(UtilityJobItem, receipt.operation_id.hex)
                job = await session.get(UtilityJob, receipt.plan.metadata_job_id)
                if (
                    item is None
                    or job is None
                    or item.job_id != job.id
                    or job.job_type != JobType.FILE_METADATA
                ):
                    continue
                item.state = ItemState.COMPLETED
                item.error_message = None
                item.completed_at = datetime.now(UTC).isoformat()
                item.after_state = '{"outcome":"recovered"}'
                job.completed_items = await session.scalar(
                    select(func.count())
                    .select_from(UtilityJobItem)
                    .where(
                        UtilityJobItem.job_id == job.id, UtilityJobItem.state == ItemState.COMPLETED
                    )
                )
                if job.completed_items == job.total_items and job.state not in {
                    JobState.CANCELLING,
                    JobState.CANCELLED,
                }:
                    job.state = JobState.COMPLETED
                    job.completed_at = item.completed_at
                    job.error_message = None
                await project_utility_operation_progress(session, job)
