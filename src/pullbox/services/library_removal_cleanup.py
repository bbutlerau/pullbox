"""Finish committed removal work outside database write transactions."""

import asyncio
import os
import time
from threading import Event
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import ValidationError
from pullbox.core.file_publication import rename_path_without_overwrite
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services.archive_metadata_binding import FileFingerprint
from pullbox.services.archive_metadata_publication import _file_work, _fingerprint
from pullbox.services.library_conversion_files import matches, sync_directory
from pullbox.services.library_mutation_coordination import (
    finish_short_mutation,
    lock_file_mutation_admission,
)
from pullbox.services.library_removal import RemovalPlan, _check_locations, decode_removal
from pullbox.services.library_removal_files import (
    copy_payload,
    create_backup_stage,
    payload_digest,
    removal_claim,
    remove_payload,
    require_payload,
)


class CleanupProof(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    phase: Literal["copying", "prepared", "deleting"]
    transfer: Literal["copy", "rename"] = "copy"
    backup_fingerprint: FileFingerprint | None = None
    backup_digest: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    trash_mtime_ns: int | None = Field(default=None, gt=0)


def _decode_proof(value: str | None, plan: RemovalPlan) -> CleanupProof | None:
    if value is None:
        return None
    if len(value.encode("utf-8")) > 4096:
        raise ValidationError("Removal cleanup evidence exceeds its limit.")
    proof = CleanupProof.model_validate_json(value)
    if plan.disposition == "trash":
        if (
            proof.backup_fingerprint is None
            or (
                proof.transfer == "copy"
                and proof.phase != "copying"
                and proof.backup_digest is None
            )
            or (proof.transfer == "rename" and proof.phase == "copying")
        ):
            raise ValidationError("Removal backup evidence is incomplete.")
        if proof.phase != "copying" and proof.trash_mtime_ns is None:
            raise ValidationError("Removal backup retention evidence is incomplete.")
    elif proof.phase != "deleting" or proof.backup_fingerprint or proof.backup_digest:
        raise ValidationError("Removal cleanup evidence is invalid.")
    return proof


def _same_filesystem(plan: RemovalPlan) -> bool:
    assert plan.trash_path is not None
    return plan.stage.parent.stat().st_dev == plan.trash_path.parent.stat().st_dev


async def _read(session: AsyncSession, operation_id: UUID) -> LibraryRemoval:
    row = await session.scalar(
        select(LibraryRemoval)
        .where(LibraryRemoval.operation_id == str(operation_id))
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise ValidationError("Removal journal was not found.")
    return row


async def _save(
    session: AsyncSession,
    plan: RemovalPlan,
    encoded: str,
    previous: str | None,
    proof: CleanupProof,
    *,
    complete: bool = False,
) -> str:
    await lock_file_mutation_admission(session)
    row = await _read(session, plan.operation_id)
    if (
        not row.active
        or row.state != "detached"
        or row.plan_json != encoded
        or row.cleanup_json != previous
    ):
        raise ValidationError("Removal cleanup evidence changed.")
    current = proof.model_dump_json()
    row.cleanup_json = current
    if complete:
        row.state, row.active = "complete", False
    await finish_short_mutation(asyncio.create_task(session.commit()))
    return current


async def _backup(
    session: AsyncSession,
    plan: RemovalPlan,
    encoded: str,
    previous: str | None,
    proof: CleanupProof | None,
) -> tuple[str, CleanupProof]:
    assert plan.trash_stage is not None and plan.trash_path is not None
    if proof is None and _same_filesystem(plan):
        proof = CleanupProof(
            phase="prepared",
            transfer="rename",
            backup_fingerprint=plan.fingerprint,
            trash_mtime_ns=int(time.time()) * 1_000_000_000,
        )
        previous = await _save(session, plan, encoded, previous, proof)
    if proof is None:
        created = await finish_short_mutation(
            asyncio.create_task(asyncio.to_thread(create_backup_stage, plan))
        )
        proof = CleanupProof(phase="copying", backup_fingerprint=created)
        previous = await _save(session, plan, encoded, previous, proof)

    if proof.phase == "copying":
        expected = proof.backup_fingerprint
        assert expected is not None

        def reset_partial(stop: Event) -> FileFingerprint:
            _check_locations(plan)
            assert plan.trash_stage is not None
            actual = _fingerprint(plan.trash_stage)
            if actual is None or actual[:2] != expected[:2] or actual[5] != expected[5]:
                raise ValidationError("Private removal backup changed.")
            if plan.trash_stage.is_dir():
                for child in plan.trash_stage.iterdir():
                    child_identity = _fingerprint(child)
                    if child_identity is None:
                        raise ValidationError("Private removal backup changed.")
                    remove_payload(child, child_identity, stop, partial=True)
            return actual

        # Reset only the inode we created, never an unexplained replacement.
        created = await _file_work(reset_partial)
        proof = CleanupProof(phase="copying", backup_fingerprint=created)
        previous = await _save(session, plan, encoded, previous, proof)
        fingerprint, digest = await _file_work(lambda stop: copy_payload(plan, stop))
        proof = CleanupProof(
            phase="prepared",
            backup_fingerprint=fingerprint,
            backup_digest=digest,
            trash_mtime_ns=int(time.time()) * 1_000_000_000,
        )
        previous = await _save(session, plan, encoded, previous, proof)

    if proof.phase == "prepared" and _fingerprint(plan.trash_path) is None:
        stamp = int(time.time()) * 1_000_000_000
        if proof.trash_mtime_ns != stamp:
            proof = proof.model_copy(update={"trash_mtime_ns": stamp})
            previous = await _save(session, plan, encoded, previous, proof)
    assert proof.backup_fingerprint is not None
    assert proof.trash_mtime_ns is not None
    expected_backup, expected_digest = proof.backup_fingerprint, proof.backup_digest
    published_backup: FileFingerprint = (
        *expected_backup[:3],
        proof.trash_mtime_ns,
        expected_backup[4],
        expected_backup[5],
    )

    def publish_and_check(stop: Event) -> None:
        _check_locations(plan)
        assert plan.trash_path is not None and plan.trash_stage is not None
        if _fingerprint(plan.trash_path) is None:
            if proof.phase == "deleting":
                raise ValidationError("Published removal backup is missing.")
            source = plan.stage if proof.transfer == "rename" else plan.trash_stage
            require_payload(source, expected_backup)
            if expected_digest and payload_digest(source, stop) != expected_digest:
                raise ValidationError("Prepared removal backup changed.")
            rename_path_without_overwrite(source, plan.trash_path)
            sync_directory(plan.trash_path.parent)
            sync_directory(source.parent)
        actual = _fingerprint(plan.trash_path)
        if matches(actual, expected_backup):
            info = plan.trash_path.lstat()
            os.utime(
                plan.trash_path, ns=(info.st_atime_ns, published_backup[3]), follow_symlinks=False
            )
            fd = os.open(plan.trash_path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        require_payload(plan.trash_path, published_backup)
        if expected_digest and payload_digest(plan.trash_path, stop) != expected_digest:
            raise ValidationError("Published removal backup changed.")
        _check_locations(plan)

    await _file_work(publish_and_check)
    assert previous is not None
    return previous, proof.model_copy(update={"backup_fingerprint": published_backup})


async def complete_removal(session: AsyncSession, operation_id: UUID) -> str:
    """Own clean-session transactions; caller must commit detachment first.

    Slow copying, verification and recursive cleanup run without a DB transaction.
    Durable proof plus a non-expiring filesystem claim makes retries repeatable.
    Retain-only historical intents never authorize destructive cleanup.
    """
    if session.new or session.dirty or session.deleted or session.in_nested_transaction():
        raise ValidationError("Removal cleanup requires a clean, committed session.")
    row = await _read(session, operation_id)
    state, active, encoded = row.state, row.active, row.plan_json
    await session.commit()
    if state == "complete" and not active:
        return state
    if state != "detached" or not active:
        raise ValidationError("Removal must have committed detachment before cleanup.")
    plan = decode_removal(encoded, operation_id=str(operation_id))
    if plan.disposition == "retain":
        raise ValidationError("This retained removal does not authorize cleanup.")
    async with removal_claim(plan):
        try:
            row = await _read(session, operation_id)
            if row.plan_json != encoded or row.state != "detached" or not row.active:
                raise ValidationError("Removal cleanup changed while acquiring its claim.")
            previous = row.cleanup_json
            proof = _decode_proof(previous, plan)
            await session.commit()
            if proof is None or proof.phase == "copying":
                _check_locations(plan)
                require_payload(plan.stage, plan.fingerprint)
            if plan.disposition == "trash":
                previous, proof = await _backup(session, plan, encoded, previous, proof)
            if proof is None or proof.phase != "deleting":
                if proof and proof.transfer == "copy":
                    require_payload(plan.stage, plan.fingerprint)
                proof = (
                    proof.model_copy(update={"phase": "deleting"})
                    if proof
                    else CleanupProof(phase="deleting")
                )
                previous = await _save(session, plan, encoded, previous, proof)

            def clean(stop: Event) -> None:
                _check_locations(plan)
                if plan.trash_path:
                    assert proof.backup_fingerprint is not None
                    require_payload(plan.trash_path, proof.backup_fingerprint)
                if _fingerprint(plan.stage) is not None:
                    remove_payload(plan.stage, plan.fingerprint, stop, partial=True)
                _check_locations(plan)

            await _file_work(clean)
            await _save(session, plan, encoded, previous, proof, complete=True)
            return "complete"
        except BaseException:
            await session.rollback()
            raise
