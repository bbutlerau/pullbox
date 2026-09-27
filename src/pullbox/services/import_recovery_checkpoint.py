"""Keep recovery checkpoints compact and retry only uncommitted database work."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import OperationalError

from pullbox.core.exceptions import JobCancelledError, JobPausedError
from pullbox.core.sqlite_lock import (
    SQLITE_LOCK_RETRY_ATTEMPTS,
    is_sqlite_locked_error,
    sqlite_lock_retry_delay,
)
from pullbox.models.import_job import ImportJob

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession

_PREPARATION_CACHE_KEYS = frozenset(
    {
        "candidates",
        "completed",
        "matches",
        "title_candidates",
        "title_completed",
        "title_matches",
        "reference_candidates",
        "mixed_resolutions",
        "pending_files",
    }
)


def compact_recovery_state(state: dict[str, Any]) -> dict[str, Any]:
    """Discard caches only after their decisions are stored on import-file rows."""
    if state.get("state") not in {"prepared", "completed"}:
        return dict(state)
    return {key: value for key, value in state.items() if key not in _PREPARATION_CACHE_KEYS}


async def run_checkpointed_recovery(
    session: AsyncSession,
    job: ImportJob,
    prepare: Callable[[AsyncSession, ImportJob], Awaitable[None]],
) -> None:
    """Reload the durable cursor after lock failures, never replay committed batches."""
    job_id = job.id
    for attempt in range(1, SQLITE_LOCK_RETRY_ATTEMPTS + 1):
        try:
            await prepare(session, job)
            return
        except OperationalError as exc:
            if not is_sqlite_locked_error(exc):
                raise
            await session.rollback()
            if attempt == SQLITE_LOCK_RETRY_ATTEMPTS:
                raise JobPausedError(
                    "Recovery paused because the database is busy. Completed work and "
                    "confirmed matches are preserved; resume when database activity settles."
                ) from exc
            await asyncio.sleep(sqlite_lock_retry_delay(attempt))
            refreshed = await session.get(ImportJob, job_id, populate_existing=True)
            if refreshed is None:
                raise JobCancelledError(f"Import job {job_id} was cancelled.") from exc
            job = refreshed
