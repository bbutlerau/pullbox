"""Library browser rename helpers for immediate file and folder renames."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import or_, select

from pullbox.core.exceptions import ValidationError
from pullbox.core.file_publication import rename_path_without_overwrite
from pullbox.core.library_file_ownership import require_mutable_library_target
from pullbox.models.library import LibraryFile
from pullbox.models.series import Series
from pullbox.services.library_mutation_coordination import (
    finish_short_mutation,
    lock_file_mutation_admission,
    require_no_archive_publication,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class LibraryRenameOutcome:
    """Rename outcome returned to the API layer."""

    kind: str
    source_path: str
    target_path: str


def _rename_path(source: Path, target: Path) -> None:
    """Rename a file or folder, including case-only changes."""
    if not source.exists():
        raise ValidationError("Selected library item no longer exists on disk.")

    is_case_only = source.name.lower() == target.name.lower()
    if not is_case_only and target.exists():
        raise ValidationError("A file or folder with that name already exists.")

    try:
        if is_case_only and target.exists():
            if not source.samefile(target):
                raise FileExistsError
            temp_path = source.parent / f".rename_tmp_{os.urandom(8).hex()}"
            rename_path_without_overwrite(source, temp_path)
            try:
                rename_path_without_overwrite(temp_path, target)
            except OSError:
                rename_path_without_overwrite(temp_path, source)
                raise
        else:
            rename_path_without_overwrite(source, target)
    except FileExistsError:
        raise ValidationError("A file or folder with that name already exists.") from None


async def _sync_file_record(session: AsyncSession, *, before_path: str, after_path: str) -> None:
    result = await session.execute(select(LibraryFile).where(LibraryFile.file_path == before_path))
    library_file = result.scalar_one_or_none()
    if library_file is None:
        return

    updated_path = Path(after_path)
    library_file.file_path = after_path
    library_file.file_name = updated_path.name
    if updated_path.exists():
        stat = updated_path.stat()
        library_file.file_size = stat.st_size
        library_file.file_modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)


async def _sync_folder_records(session: AsyncSession, *, before_path: str, after_path: str) -> None:
    old_prefix = before_path.rstrip("/")
    new_prefix = after_path.rstrip("/")

    series_result = await session.execute(
        select(Series).where(
            or_(
                Series.path == old_prefix, Series.path.startswith(f"{old_prefix}/", autoescape=True)
            )
        )
    )
    for series in series_result.scalars().all():
        if not series.path:
            continue
        suffix = series.path[len(old_prefix) :]
        series.path = f"{new_prefix}{suffix}"

    file_result = await session.execute(
        select(LibraryFile).where(
            or_(
                LibraryFile.file_path == old_prefix,
                LibraryFile.file_path.startswith(f"{old_prefix}/", autoescape=True),
            )
        )
    )
    for library_file in file_result.scalars().all():
        if not library_file.file_path:
            continue
        suffix = library_file.file_path[len(old_prefix) :]
        next_path = f"{new_prefix}{suffix}"
        library_file.file_path = next_path
        library_file.file_name = Path(next_path).name


async def _destination_is_registered(session: AsyncSession, target: Path, kind: str) -> bool:
    prefix = str(target).rstrip("/")
    file_clause = LibraryFile.file_path == prefix
    if kind == "folder":
        file_clause |= LibraryFile.file_path.startswith(f"{prefix}/", autoescape=True)
    if await session.scalar(select(LibraryFile.id).where(file_clause).limit(1)) is not None:
        return True
    if kind == "folder":
        return (
            await session.scalar(
                select(Series.id)
                .where(
                    or_(
                        Series.path == prefix, Series.path.startswith(f"{prefix}/", autoescape=True)
                    )
                )
                .limit(1)
            )
            is not None
        )
    return False


async def rename_library_entry(
    session: AsyncSession,
    *,
    source: Path,
    target: Path,
    kind: str,
) -> LibraryRenameOutcome:
    """Rename a single Library browser target and sync tracked DB paths."""
    renamed_identity: tuple[int, int] | None = None
    committed = False

    def rename() -> None:
        nonlocal renamed_identity
        info = source.lstat()
        _rename_path(source, target)
        renamed_identity = info.st_dev, info.st_ino

    async def commit() -> None:
        nonlocal committed
        await session.commit()
        committed = True

    async def restore() -> bool:
        await session.rollback()
        if renamed_identity is None or committed:
            return False
        try:
            # Rollback may have released DB locks. Reacquire before compensation.
            await lock_file_mutation_admission(session)
            # A lost commit acknowledgment is not proof that the commit failed.
            if await _destination_is_registered(session, target, kind):
                return False
            await require_no_archive_publication(
                session, source, target, include_descendants=kind == "folder"
            )
            await require_mutable_library_target(
                session, target, include_descendants=kind == "folder", operation="renamed"
            )

            def restore_owned_path() -> bool:
                if source.exists() or not target.exists():
                    return False
                info = target.lstat()
                if (info.st_dev, info.st_ino) != renamed_identity:
                    return False
                _rename_path(target, source)
                return True

            return await finish_short_mutation(
                asyncio.create_task(asyncio.to_thread(restore_owned_path))
            )
        except Exception:
            logger.exception(
                "library_rename_rollback_failed", source_path=str(source), target_path=str(target)
            )
            return False
        finally:
            await session.rollback()

    try:
        await lock_file_mutation_admission(session)
        await require_no_archive_publication(
            session, source, target, include_descendants=kind == "folder"
        )
        await require_mutable_library_target(
            session,
            source,
            include_descendants=kind == "folder",
            operation="renamed",
        )
        await finish_short_mutation(asyncio.create_task(asyncio.to_thread(rename)))

        if kind == "file":
            await _sync_file_record(session, before_path=str(source), after_path=str(target))
        else:
            await _sync_folder_records(session, before_path=str(source), after_path=str(target))

        await finish_short_mutation(asyncio.create_task(commit()))
        return LibraryRenameOutcome(
            kind=kind,
            source_path=str(source),
            target_path=str(target),
        )
    except BaseException as exc:
        restored = await finish_short_mutation(asyncio.create_task(restore()))
        if isinstance(exc, ValidationError) or not isinstance(exc, Exception):
            raise
        message = "Rename could not be completed."
        if restored:
            message += " The original name was restored."
        raise ValidationError(message) from exc
