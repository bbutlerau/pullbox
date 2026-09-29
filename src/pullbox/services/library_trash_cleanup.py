"""Coordinated trash cleanup with short, explicitly owned transactions."""

import asyncio
import stat
import time
from dataclasses import dataclass
from itertools import islice
from pathlib import Path

import structlog
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import ValidationError
from pullbox.models import LibraryFile, LibraryRoot
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services.library_mutation_coordination import (
    finish_short_mutation,
    lock_file_mutation_admission,
    require_no_archive_publication,
)
from pullbox.services.library_removal import decode_removal, trash_path_key
from pullbox.services.library_removal_cleanup import _decode_proof
from pullbox.services.library_trash_files import TrashEntry, remove_entry, walk_trash

logger = structlog.get_logger(__name__)
_PAGE_SIZE = 64


@dataclass
class TrashCleanupResult:
    deleted_entries: int = 0
    retained_entries: int = 0


async def _recent_trash_tree(session: AsyncSession, entry: TrashEntry, cutoff: int) -> bool:
    ancestors = (entry.path, *entry.path.parents)
    keys = [trash_path_key(path) for path in ancestors]
    last_id = 0
    while True:
        rows = (
            await session.execute(
                select(
                    LibraryRemoval.id,
                    LibraryRemoval.trash_path_key,
                    case(
                        (func.length(LibraryRemoval.plan_json) <= 65536, LibraryRemoval.plan_json),
                        else_=None,
                    ),
                    case(
                        (
                            func.length(LibraryRemoval.cleanup_json) <= 4096,
                            LibraryRemoval.cleanup_json,
                        ),
                        else_=None,
                    ),
                )
                .where(
                    LibraryRemoval.id > last_id,
                    LibraryRemoval.trash_path_key.in_(keys),
                    LibraryRemoval.state == "complete",
                    LibraryRemoval.active.is_(False),
                )
                .order_by(LibraryRemoval.id)
                .limit(8)
            )
        ).all()
        if not rows:
            return False
        for row_id, key, encoded, evidence in rows:
            last_id = row_id
            if encoded is None:
                raise ValidationError("Trash receipt needs review before retention cleanup.")
            plan = decode_removal(encoded)
            if plan.trash_path not in ancestors or trash_path_key(plan.trash_path) != key:
                raise ValidationError("Trash receipt path changed; review it before cleanup.")
            proof = _decode_proof(evidence, plan)
            if proof is None or proof.trash_mtime_ns is None:
                raise ValidationError("Trash receipt has no verified retention time.")
            if proof.trash_mtime_ns > cutoff:
                return True


async def cleanup_trash(
    session: AsyncSession, trash_dir: Path, *, retention_days: int | None = None
) -> TrashCleanupResult:
    """Own an idle session; commit each short deletion, never a whole tree walk.

    Callers must finish their read-only setup transaction first. Refuse pending
    transactions rather than accidentally committing unrelated request changes.
    """
    if session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValidationError("Trash cleanup requires an idle session with committed setup.")
    if retention_days is not None and not 1 <= retention_days <= 365:
        raise ValidationError("Trash retention must be between 1 and 365 days.")
    trash_dir = trash_dir.absolute()
    async with session.begin():
        if await session.scalar(select(LibraryRoot.id).where(LibraryRoot.path == str(trash_dir))):
            raise ValidationError(
                "A library root cannot be emptied as trash. Choose a separate trash folder."
            )
    cutoff = time.time_ns() - retention_days * 86400 * 1_000_000_000 if retention_days else None
    result = TrashCleanupResult()
    entries = walk_trash(trash_dir)
    try:
        while page := await finish_short_mutation(
            asyncio.create_task(asyncio.to_thread(lambda: list(islice(entries, _PAGE_SIZE))))
        ):
            for entry in page:
                if entry.protected:
                    result.retained_entries += 1
                    continue
                try:
                    async with session.begin():
                        await lock_file_mutation_admission(session)
                        await require_no_archive_publication(
                            session, entry.path, include_descendants=True
                        )
                        if await session.scalar(
                            select(LibraryRoot.id).where(
                                LibraryRoot.path.in_(
                                    [
                                        str(path)
                                        for path in (entry.path, *entry.path.parents)
                                        if path.is_relative_to(trash_dir)
                                    ]
                                )
                            )
                        ):
                            raise ValidationError(
                                "A library root inside this trash location was left untouched."
                            )
                        if await session.scalar(
                            select(LibraryFile.id).where(LibraryFile.file_path == str(entry.path))
                        ):
                            raise ValidationError(
                                "A registered library file is not disposable trash."
                            )
                        if cutoff is not None and (
                            await _recent_trash_tree(session, entry, cutoff)
                            or (
                                not stat.S_ISDIR(entry.fingerprint[5])
                                and entry.fingerprint[3] > cutoff
                            )
                        ):
                            continue
                        removed = await finish_short_mutation(
                            asyncio.create_task(asyncio.to_thread(remove_entry, entry))
                        )
                        result.deleted_entries += int(removed)
                except (OSError, ValueError, ValidationError) as exc:
                    result.retained_entries += 1
                    logger.warning(
                        "utility_trash_entry_retained", path=str(entry.path), error=str(exc)
                    )
    finally:
        await finish_short_mutation(asyncio.create_task(asyncio.to_thread(entries.close)))
    logger.info(
        "utility_trash_cleanup_complete",
        directory=str(trash_dir),
        retention_days=retention_days,
        deleted_entries=result.deleted_entries,
        retained_entries=result.retained_entries,
    )
    return result
