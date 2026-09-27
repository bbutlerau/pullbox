"""Bounded, restart-safe application of a previewed mixed-folder recovery."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from pullbox.models.import_job import ImportJob, ImportJobLog, ImportJobStatus
from pullbox.schemas.import_job import ImportProgressEvent
from pullbox.services.import_completed_cleanup import (
    _apply_mixed_folder_resolutions,
    _load_mixed_folder_resolutions,
    _prepare_series_for_retry,
)
from pullbox.services.import_counters import recompute_file_counters, recompute_series_counters
from pullbox.services.import_recovery_checkpoint import compact_recovery_state
from pullbox.services.import_workflow_state import emit_progress, raise_if_job_cancelled

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession


MIXED_RECOVERY_BATCH_SIZE = 25


async def prepare_mixed_folder_recovery(
    session: AsyncSession,
    job: ImportJob,
    *,
    progress_callback: Callable[[ImportProgressEvent], Awaitable[None]] | None = None,
) -> None:
    """Persist each batch and its cursor atomically; never broaden the saved scope."""
    state = dict(job.progress_snapshot["deferred_recovery"])
    resolutions = state.get("mixed_resolutions", [])
    cursor = int(state.get("mixed_cursor", 0))
    while cursor < len(resolutions):
        await raise_if_job_cancelled(session, job.id)
        batch = resolutions[cursor : cursor + MIXED_RECOVERY_BATCH_SIZE]
        approved = {row["file_id"]: row for row in batch}
        current = await _load_mixed_folder_resolutions(session, job.id, file_ids=list(approved))
        unchanged = tuple(row for row in current if asdict(row) == approved.get(row.file_id))
        affected, retry = await _apply_mixed_folder_resolutions(
            session, job, resolutions=unchanged, isolate_targets=True
        )
        await _prepare_series_for_retry(session, job, retry)
        state["series_ids"] = sorted(set(state.get("series_ids", [])) | retry)
        state["mixed_applied"] = state.get("mixed_applied", 0) + len(unchanged)
        state["mixed_changed"] = state.get("mixed_changed", 0) + len(batch) - len(unchanged)
        cursor += len(batch)
        state["mixed_cursor"] = cursor
        await recompute_file_counters(session, job, series_ids=sorted(affected))
        await recompute_series_counters(session, job)
        job.progress_snapshot = {**dict(job.progress_snapshot or {}), "deferred_recovery": state}
        # emit_progress commits the mutations and cursor before notifying the UI.
        await emit_progress(
            session,
            job,
            ImportProgressEvent(
                job_id=job.id,
                status=ImportJobStatus.IMPORTING,
                mode="import",
                phase="deferred_recovery",
                progress=round(15 * cursor / max(len(resolutions), 1)),
                message=f"Reconciled mixed-folder files {cursor} of {len(resolutions)}...",
                progress_revision=int(job.progress_revision or 0) + 1,
                current_file_stage="deferred_recovery",
                current_file_progress_current=cursor,
                current_file_progress_total=len(resolutions),
                current_file_progress_pct=round(100 * cursor / max(len(resolutions), 1)),
                current_file_progress_unit="files",
            ),
            progress_callback,
        )

    await raise_if_job_cancelled(session, job.id)
    state["state"] = "prepared" if state.get("series_ids") else "completed"
    job.error_message = None
    job.progress_snapshot = {
        **dict(job.progress_snapshot or {}),
        "deferred_recovery": compact_recovery_state(state),
    }
    if state["state"] == "completed":
        job.status = ImportJobStatus.COMPLETED
        job.progress_snapshot = {
            **job.progress_snapshot,
            "status": "completed",
            "phase": "done",
            "progress": 100,
            "message": "Mixed-folder recovery completed. Changed files remain for review.",
        }
    session.add(
        ImportJobLog(
            import_job_id=job.id,
            level="INFO",
            event="import_mixed_folder_recovery_prepared",
            message="Completed background mixed-folder reconciliation.",
            data={
                "files_applied": state.get("mixed_applied", 0),
                "files_changed": state.get("mixed_changed", 0),
                "source_preserved": True,
            },
        )
    )
    await session.commit()
