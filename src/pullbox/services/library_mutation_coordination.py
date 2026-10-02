"""Serialize short path changes with admission of durable archive publications."""

import asyncio
import hashlib
import os
from contextlib import suppress
from pathlib import Path

from sqlalchemy import case, false, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import ValidationError
from pullbox.models import Series
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication

# Two fixed PostgreSQL advisory-lock keys: PULL / FILE. Never derived from user input.
_LOCK_NAMESPACE = 0x50554C4C
_LOCK_RESOURCE = 0x46494C45
_MAX_PLAN_BYTES = 4 * 1024 * 1024
_PAGE_SIZE = 8


async def finish_short_mutation[T](task: asyncio.Task[T]) -> T:
    """Join short rename/commit/cleanup work before cancellation releases its mutex."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            with suppress(asyncio.CancelledError, Exception):
                await asyncio.shield(task)
        if not task.cancelled():
            task.exception()
        raise


async def lock_file_mutation_admission(session: AsyncSession) -> None:
    """Hold the admission mutex until the caller commits or rolls back.

    Acquire before entity locks. Only short path changes and publication admission
    belong here, never archive reads, conversion, hashing or provider requests.
    Durable publication reservations protect the longer interval between commits.
    """
    dialect = session.get_bind().dialect.name
    if dialect == "sqlite":
        await session.execute(update(Series).where(false()).values(path=Series.path))
    elif dialect == "postgresql":
        await session.execute(select(func.pg_advisory_xact_lock(_LOCK_NAMESPACE, _LOCK_RESOURCE)))
    else:
        raise ValidationError("This database cannot safely coordinate library file changes.")


async def require_no_archive_publication(
    session: AsyncSession, *paths: Path, include_descendants: bool
) -> None:
    """Check retained source/stage reservations under the admission mutex.

    Do not depend on live LibraryFile rows: their deletion deliberately leaves
    publication evidence behind. Bound each page and fail closed on corrupt plans.
    """
    from pullbox.services.archive_metadata_publication import ArchivePublicationPlan
    from pullbox.services.library_conversion_recovery import require_no_library_conversion
    from pullbox.services.library_removal import require_no_library_removal

    await require_no_library_removal(session, *paths, include_descendants=include_descendants)
    await require_no_library_conversion(session, *paths, include_descendants=include_descendants)

    targets = {variant for path in paths for variant in (path.absolute(), path.resolve())}
    last_id = 0
    while True:
        rows = (
            await session.execute(
                select(
                    ArchiveMetadataPublication.id,
                    ArchiveMetadataPublication.active_path_key,
                    case(
                        (
                            func.length(ArchiveMetadataPublication.plan_json) <= _MAX_PLAN_BYTES,
                            ArchiveMetadataPublication.plan_json,
                        ),
                        else_=None,
                    ),
                )
                .where(
                    ArchiveMetadataPublication.id > last_id,
                    ArchiveMetadataPublication.active_path_key.is_not(None),
                )
                .order_by(ArchiveMetadataPublication.id)
                .limit(_PAGE_SIZE)
            )
        ).all()
        if not rows:
            return
        for row_id, path_key, encoded in rows:
            last_id = row_id
            try:
                if encoded is None or len(encoded.encode("utf-8")) > _MAX_PLAN_BYTES:
                    raise ValueError("Invalid publication plan")
                plan = ArchivePublicationPlan.model_validate_json(encoded)
                if (
                    not plan.target.path.is_absolute()
                    or not plan.stage_path.is_absolute()
                    or hashlib.sha256(os.fsencode(plan.target.path)).hexdigest() != path_key
                ):
                    raise ValueError("Changed publication plan")
            except ValueError:
                raise ValidationError(
                    "A pending metadata update needs recovery before library files can be renamed."
                ) from None
            for reserved in (plan.target.path, plan.stage_path):
                if any(
                    reserved == target or (include_descendants and reserved.is_relative_to(target))
                    for target in targets
                ):
                    raise ValidationError(
                        "A metadata update is still pending for this file or folder. "
                        "Finish or recover that update, then retry the rename."
                    )
