"""Durable staging and recovery boundary for library removal owners."""

import asyncio
import stat
from contextlib import suppress
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, ConfigDict
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import ValidationError
from pullbox.core.file_publication import rename_path_without_overwrite
from pullbox.core.library_file_ownership import require_mutable_library_target
from pullbox.models import LibraryRoot
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services.archive_metadata_binding import FileFingerprint
from pullbox.services.archive_metadata_publication import _fingerprint
from pullbox.services.library_conversion_files import directories, matches, sync_directory
from pullbox.services.library_mutation_coordination import (
    finish_short_mutation,
    lock_file_mutation_admission,
)

logger = structlog.get_logger(__name__)


class RemovalPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    operation_id: UUID
    source: Path
    stage: Path
    root_id: int
    root_path: Path
    fingerprint: FileFingerprint
    directories: tuple[tuple[Path, int, int, int], ...]
    disposition: Literal["retain", "delete", "trash"] = "retain"
    trash_path: Path | None = None
    trash_stage: Path | None = None


def prepare_removal(
    source: Path,
    *,
    root_id: int,
    root_path: Path,
    disposition: Literal["retain", "delete", "trash"] = "retain",
    trash_path: Path | None = None,
) -> RemovalPlan:
    """Create an empty private sibling; caller owns recording or discarding intent."""
    source, root_path = source.absolute(), root_path.resolve()
    before = _fingerprint(source)
    if (
        before is None
        or source.resolve() != source
        or source == root_path
        or not source.is_relative_to(root_path)
        or not (stat.S_ISREG(before[5]) or stat.S_ISDIR(before[5]))
    ):
        raise ValidationError("Library removal requires a regular file or folder inside its root.")
    if (disposition == "trash") != (trash_path is not None):
        raise ValidationError("Trash removal requires an explicit destination.")
    if trash_path is not None and (
        not trash_path.is_absolute()
        or trash_path.resolve() != trash_path
        or trash_path.is_relative_to(source)
        or source.is_relative_to(trash_path)
    ):
        raise ValidationError("Trash must be a separate, resolved destination.")
    operation = uuid4()
    private = source.parent / f".pullbox-removal-{operation.hex}"
    private.mkdir(mode=0o700)
    created = private.lstat()
    trash_private: Path | None = None
    trash_created = None
    try:
        if trash_path is not None:
            trash_path.parent.mkdir(parents=True, exist_ok=True)
            trash_private = trash_path.parent / f".pullbox-removal-trash-{operation.hex}"
            trash_private.mkdir(mode=0o700)
            trash_created = trash_private.lstat()
        trash_stage = trash_private / "payload" if trash_private else None
        plan = RemovalPlan(
            operation_id=operation,
            source=source,
            stage=private / "payload",
            root_id=root_id,
            root_path=root_path,
            fingerprint=before,
            directories=directories(
                source, private / "payload", *((trash_path, trash_stage) if trash_stage else ())
            ),
            disposition=disposition,
            trash_path=trash_path,
            trash_stage=trash_stage,
        )
        _check_locations(plan)
        return decode_removal(plan.model_dump_json())
    except BaseException:
        if trash_private is not None and trash_created is not None:
            with suppress(OSError):
                current = trash_private.lstat()
                if (current.st_dev, current.st_ino) == (trash_created.st_dev, trash_created.st_ino):
                    trash_private.rmdir()
        with suppress(OSError):
            current = private.lstat()
            if (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino):
                private.rmdir()
        raise


async def recover_library_removals(session: AsyncSession) -> int:
    """Bounded startup pass restoring uncommitted intent, never deleting artifacts."""
    if session.new or session.dirty or session.deleted or session.in_nested_transaction():
        raise ValidationError("Library removal recovery requires a clean session.")
    last_id = restored = 0
    while True:
        rows = (
            await session.execute(
                select(LibraryRemoval.id, LibraryRemoval.operation_id)
                .where(
                    LibraryRemoval.id > last_id,
                    LibraryRemoval.active.is_(True),
                    LibraryRemoval.state == "intended",
                )
                .order_by(LibraryRemoval.id)
                .limit(8)
            )
        ).all()
        await session.commit()
        if not rows:
            return restored
        for row_id, operation in rows:
            last_id = row_id
            try:
                state = await recover_uncommitted_removal(session, UUID(operation))
                restored += state == "abandoned"
                logger.info("library_removal_recovered", operation_id=operation, state=state)
            except Exception:
                await session.rollback()
                logger.warning(
                    "library_removal_recovery_deferred", operation_id=operation, exc_info=True
                )


def decode_removal(encoded: str, *, operation_id: str | None = None) -> RemovalPlan:
    if not isinstance(encoded, str) or len(encoded.encode("utf-8")) > 65536:
        raise ValidationError("Library removal evidence exceeds its limit.")
    plan = RemovalPlan.model_validate_json(encoded)
    expected_dirs = set(plan.source.parents) | set(plan.stage.parents)
    if plan.trash_path is not None and plan.trash_stage is not None:
        expected_dirs |= set(plan.trash_path.parents) | set(plan.trash_stage.parents)
        if (
            plan.trash_path.resolve() != plan.trash_path
            or plan.trash_path.is_relative_to(plan.source)
            or plan.source.is_relative_to(plan.trash_path)
            or plan.trash_stage
            != plan.trash_path.parent
            / f".pullbox-removal-trash-{plan.operation_id.hex}"
            / "payload"
        ):
            raise ValidationError("Library removal trash evidence is invalid.")
    if (plan.disposition == "trash") != (
        plan.trash_path is not None and plan.trash_stage is not None
    ) or (plan.disposition != "trash" and (plan.trash_path or plan.trash_stage)):
        raise ValidationError("Library removal disposition is invalid.")
    if (
        (operation_id is not None and str(plan.operation_id) != operation_id)
        or any(
            not path.is_absolute() or ".." in path.parts
            for path in (plan.source, plan.stage, plan.root_path, plan.trash_path, plan.trash_stage)
            if path is not None
        )
        or plan.stage
        != plan.source.parent / f".pullbox-removal-{plan.operation_id.hex}" / "payload"
        or plan.source == plan.root_path
        or not plan.source.is_relative_to(plan.root_path)
        or len(plan.directories) != len(expected_dirs)
        or {entry[0] for entry in plan.directories} != expected_dirs
        or not (stat.S_ISREG(plan.fingerprint[5]) or stat.S_ISDIR(plan.fingerprint[5]))
        or any(not stat.S_ISDIR(mode) for _, _, _, mode in plan.directories)
    ):
        raise ValidationError("Library removal evidence is invalid.")
    return plan


def _check_locations(plan: RemovalPlan) -> None:
    # Removal and conversion retain the same immutable parent-directory proof.
    for path, device, inode, mode in plan.directories:
        info = path.lstat()
        if (info.st_dev, info.st_ino, info.st_mode) != (
            device,
            inode,
            mode,
        ) or path.resolve() != path:
            raise ValidationError("Library removal locations changed; review before retrying.")
    if stat.S_IMODE(plan.stage.parent.stat().st_mode) != 0o700:
        raise ValidationError("Library removal staging must remain private.")
    if plan.trash_stage and stat.S_IMODE(plan.trash_stage.parent.stat().st_mode) != 0o700:
        raise ValidationError("Library removal trash staging must remain private.")


async def _check_authority(session: AsyncSession, plan: RemovalPlan) -> None:
    root = await session.get(
        LibraryRoot, plan.root_id, populate_existing=True, with_for_update=True
    )
    if (
        root is None
        or not root.enabled
        or not root.allow_managed_writes
        or Path(root.path).resolve() != plan.root_path
    ):
        raise ValidationError("Library removal requires its unchanged managed root.")
    await require_mutable_library_target(
        session,
        plan.source,
        include_descendants=stat.S_ISDIR(plan.fingerprint[5]),
        operation="removed",
    )
    _check_locations(plan)


async def record_removal(session: AsyncSession, plan: RemovalPlan) -> None:
    """Reserve a caller-authorized target; caller commits before staging."""
    from pullbox.services.library_mutation_coordination import require_no_archive_publication

    decode_removal(plan.model_dump_json())
    await lock_file_mutation_admission(session)
    await require_no_archive_publication(
        session,
        plan.source,
        plan.stage.parent,
        *(
            (plan.trash_path, plan.trash_stage.parent)
            if plan.trash_path and plan.trash_stage
            else ()
        ),
        include_descendants=True,
    )
    await _check_authority(session, plan)
    if _fingerprint(plan.source) != plan.fingerprint or _fingerprint(plan.stage) is not None:
        raise ValidationError("Library removal source or staging changed.")
    session.add(
        LibraryRemoval(operation_id=str(plan.operation_id), plan_json=plan.model_dump_json())
    )
    await session.flush()
    session.info[f"removal_intent:{plan.operation_id}"] = session.sync_session.get_transaction()


async def _locked_removal(session: AsyncSession, operation_id: UUID) -> LibraryRemoval | None:
    await lock_file_mutation_admission(session)
    row: LibraryRemoval | None = await session.scalar(
        select(LibraryRemoval)
        .where(LibraryRemoval.operation_id == str(operation_id))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def stage_removal(session: AsyncSession, operation_id: UUID) -> None:
    """Short rename; owner detaches matching DB records in this same transaction."""
    if (
        session.in_transaction()
        and session.info.get(f"removal_intent:{operation_id}")
        is session.sync_session.get_transaction()
    ):
        raise ValidationError("Library removal intent must commit before staging.")
    row = await _locked_removal(session, operation_id)
    if row is None or not row.active or row.state != "intended":
        raise ValidationError("Library removal has already been recovered; retry the operation.")
    plan = decode_removal(row.plan_json, operation_id=row.operation_id)
    await _check_authority(session, plan)

    def stage() -> None:
        _check_locations(plan)
        if _fingerprint(plan.source) != plan.fingerprint:
            raise ValidationError("Library removal source changed.")
        rename_path_without_overwrite(plan.source, plan.stage)
        sync_directory(plan.source.parent)
        sync_directory(plan.stage.parent)

    await finish_short_mutation(asyncio.create_task(asyncio.to_thread(stage)))
    row.state = "detached"
    await session.flush()


async def recover_uncommitted_removal(session: AsyncSession, operation_id: UUID) -> str:
    """Own a clean session; restore an uncommitted removal, never resume deletion."""
    if session.new or session.dirty or session.deleted or session.in_nested_transaction():
        raise ValidationError("Library removal recovery requires a clean session.")
    row = await _locked_removal(session, operation_id)
    if row is None or not row.active or row.state != "intended":
        state = row.state if row else "missing"
        await session.rollback()
        return state
    plan = decode_removal(row.plan_json, operation_id=row.operation_id)

    def restore() -> bool:
        _check_locations(plan)
        source, stage = _fingerprint(plan.source), _fingerprint(plan.stage)
        if matches(source, plan.fingerprint) and stage is None:
            return True
        if source is None and matches(stage, plan.fingerprint):
            rename_path_without_overwrite(plan.stage, plan.source)
            sync_directory(plan.source.parent)
            sync_directory(plan.stage.parent)
            return True
        return False

    restored = await finish_short_mutation(asyncio.create_task(asyncio.to_thread(restore)))
    row.state, row.active = ("abandoned", False) if restored else ("review", True)
    state = row.state
    await finish_short_mutation(asyncio.create_task(session.commit()))
    return state


async def require_no_library_removal(
    session: AsyncSession, *paths: Path, include_descendants: bool
) -> None:
    """Check durable path scopes while the caller holds mutation admission."""
    targets = {variant for path in paths for variant in (path.absolute(), path.resolve())}
    last_id = 0
    while True:
        rows = (
            await session.execute(
                select(
                    LibraryRemoval.id,
                    LibraryRemoval.operation_id,
                    case(
                        (func.length(LibraryRemoval.plan_json) <= 65536, LibraryRemoval.plan_json),
                        else_=None,
                    ),
                )
                .where(LibraryRemoval.active.is_(True), LibraryRemoval.id > last_id)
                .order_by(LibraryRemoval.id)
                .limit(8)
            )
        ).all()
        if not rows:
            return
        for row_id, operation_id, encoded in rows:
            last_id = row_id
            try:
                plan = decode_removal(encoded, operation_id=operation_id)
            except (ValueError, TypeError):
                raise ValidationError(
                    "A pending library removal needs recovery before files can change."
                ) from None
            for reserved, descendants in (
                (plan.source, stat.S_ISDIR(plan.fingerprint[5])),
                (plan.stage.parent, True),
                *(
                    (
                        (plan.trash_path, stat.S_ISDIR(plan.fingerprint[5])),
                        (plan.trash_stage.parent, True),
                    )
                    if plan.trash_path and plan.trash_stage
                    else ()
                ),
            ):
                if any(
                    target == reserved
                    or (descendants and target.is_relative_to(reserved))
                    or (include_descendants and reserved.is_relative_to(target))
                    for target in targets
                ):
                    raise ValidationError(
                        "A library removal is pending for this file or folder. "
                        "Recover it before retrying."
                    )
