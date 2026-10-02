"""Prune proven empty terminal workspaces, never payloads or public paths."""

import asyncio
import os
import stat
from contextlib import ExitStack
from itertools import islice
from uuid import UUID

import structlog
from sqlalchemy import case, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import ValidationError
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services.library_mutation_coordination import (
    finish_short_mutation,
    lock_file_mutation_admission,
)
from pullbox.services.library_removal import RemovalPlan, decode_removal
from pullbox.services.library_trash_files import _open_checked

logger = structlog.get_logger(__name__)


def _identity(info: os.stat_result) -> tuple[int, int, int]:
    return info.st_dev, info.st_ino, info.st_mode


def prune_workspaces(plan: RemovalPlan) -> None:
    """Bounded descriptor-relative rmdir; refuse unknown contents and live locks."""
    if os.name != "posix":
        raise ValidationError("Safe workspace cleanup requires filesystem lock support.")
    import fcntl

    paths = [plan.stage.parent]
    if plan.trash_stage:
        paths.append(plan.trash_stage.parent)
    expected = {path: (device, inode, mode) for path, device, inode, mode in plan.directories}
    with ExitStack() as stack:
        opened = []
        for path in paths:
            parent = _open_checked(path.parent, plan.directories)
            stack.callback(os.close, parent)
            try:
                info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if _identity(info) != expected.get(path) or stat.S_IMODE(info.st_mode) != 0o700:
                raise ValidationError("Removal workspace changed; it was left untouched.")
            fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            stack.callback(os.close, fd)
            if _identity(os.fstat(fd)) != _identity(info):
                raise ValidationError("Removal workspace changed while opening it.")
            with os.scandir(fd) as entries:
                names = [entry.name for entry in islice(entries, 2)]
            lock = None
            if names:
                if path != plan.stage.parent or names != ["cleanup.lock"]:
                    raise ValidationError(
                        "Removal workspace contains unproven files; kept for review."
                    )
                lock = os.open("cleanup.lock", os.O_RDWR | os.O_NOFOLLOW, dir_fd=fd)
                stack.callback(os.close, lock)
                lock_info = os.fstat(lock)
                if (
                    not stat.S_ISREG(lock_info.st_mode)
                    or stat.S_IMODE(lock_info.st_mode) != 0o600
                    or lock_info.st_size != 0
                    or lock_info.st_nlink != 1
                ):
                    raise ValidationError("Removal workspace lock changed; kept for review.")
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            opened.append((path, parent, fd, info, lock))
        # Inspect both workspaces before pruning either. rmdir never removes contents.
        for path, parent, fd, info, lock in reversed(opened):
            if _identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != _identity(
                info
            ):
                raise ValidationError("Removal workspace changed before cleanup.")
            if lock is not None:
                actual = os.stat("cleanup.lock", dir_fd=fd, follow_symlinks=False)
                held = os.fstat(lock)
                if _identity(actual) != _identity(held) or actual.st_size or actual.st_nlink != 1:
                    raise ValidationError("Removal workspace lock changed before cleanup.")
                os.unlink("cleanup.lock", dir_fd=fd)
                os.fsync(fd)
            os.rmdir(path.name, dir_fd=parent)
            os.fsync(parent)


async def prune_terminal_removal(session: AsyncSession, operation_id: UUID) -> bool:
    """Own an idle session; cleanup failure does not undo a completed removal."""
    if session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValidationError("Workspace cleanup requires an idle, committed session.")
    try:
        async with session.begin():
            await lock_file_mutation_admission(session)
            row = (
                await session.execute(
                    select(
                        LibraryRemoval.id,
                        LibraryRemoval.state,
                        LibraryRemoval.active,
                        LibraryRemoval.workspace_cleaned,
                        case(
                            (
                                func.length(LibraryRemoval.plan_json) <= 65536,
                                LibraryRemoval.plan_json,
                            ),
                            else_=None,
                        ),
                    )
                    .where(LibraryRemoval.operation_id == str(operation_id))
                    .with_for_update()
                )
            ).one_or_none()
            if row is None or row.active or row.state not in {"complete", "abandoned"}:
                return False
            if row.workspace_cleaned:
                return True
            plan = decode_removal(row[4], operation_id=str(operation_id))
            await finish_short_mutation(
                asyncio.create_task(asyncio.to_thread(prune_workspaces, plan))
            )
            await session.execute(
                update(LibraryRemoval)
                .where(LibraryRemoval.id == row.id)
                .values(workspace_cleaned=True)
            )
        return True
    except (OSError, ValueError, TypeError, ValidationError):
        logger.warning(
            "library_removal_workspace_retained", operation_id=str(operation_id), exc_info=True
        )
        return False


async def prune_terminal_removals(session: AsyncSession) -> None:
    """Bounded startup repair; successful cleanup is not rescanned next startup."""
    if session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValidationError("Workspace recovery requires an idle, committed session.")
    last_id = 0
    while True:
        rows = (
            await session.execute(
                select(LibraryRemoval.id, LibraryRemoval.operation_id)
                .where(
                    LibraryRemoval.id > last_id,
                    LibraryRemoval.workspace_cleaned.is_(False),
                    LibraryRemoval.active.is_(False),
                    LibraryRemoval.state.in_(["complete", "abandoned"]),
                )
                .order_by(LibraryRemoval.id)
                .limit(8)
            )
        ).all()
        await session.commit()
        if not rows:
            return
        for row_id, operation in rows:
            last_id = row_id
            try:
                operation_id = UUID(operation)
            except ValueError:
                logger.warning("library_removal_workspace_invalid_operation", removal_id=row_id)
                continue
            await prune_terminal_removal(session, operation_id)
