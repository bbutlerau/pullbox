"""Bind paired archive publication to retained import placement evidence."""

import hashlib
import json
import os
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication, PublicationState
from pullbox.models.import_job import (
    ImportControlRequest,
    ImportedFile,
    ImportedFileStatus,
    ImportJob,
    ImportJobAction,
    ImportJobActionStatus,
    ImportJobStatus,
)
from pullbox.models.library import LibraryFile, LibraryFileStorageMode
from pullbox.schemas.archive_publication_owner import ImportArchiveOwner
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    ArchivePublicationPlan,
    ArchivePublicationReceipt,
    _clean_session,
    _receipt,
)
from pullbox.services.metadata_writer_identity import metadata_write_scope

_ENRICHMENT = "comicinfo_enrichment"
_SUCCESSOR = "metadata_publication"


def _digest(value: object) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError):
        raise ArchivePublicationError("import_owner_invalid") from None
    if len(encoded) > 4 * 1024 * 1024:
        raise ArchivePublicationError("import_owner_too_large")
    return hashlib.sha256(encoded).hexdigest()


def _signature(plan: ArchivePublicationPlan, *, successor: bool = False) -> dict[str, int | str]:
    fingerprint = plan.stage_fingerprint if successor else plan.target.fingerprint
    return {
        "schema_version": 1,
        "resolved_path": str(plan.target.path),
        "size": fingerprint[2],
        "mtime_ns": fingerprint[3],
        "device": fingerprint[0],
        "inode": fingerprint[1],
        "content_digest_algorithm": "sha256",
        "content_digest": plan.output_digest if successor else plan.source_digest,
    }


async def lock_import_archive_owner(
    session: AsyncSession, owner: ImportArchiveOwner | None
) -> None:
    """Lock the owner before library parents; replay may outlive these rows."""
    if owner is None:
        return
    await _lock_owner_rows(session, owner.job_id, owner.imported_file_id, owner.action_id)


async def _lock_owner_rows(
    session: AsyncSession, job_id: int, imported_file_id: int, action_id: int
) -> None:
    for model, local_id in (
        (ImportJob, job_id),
        (ImportedFile, imported_file_id),
        (ImportJobAction, action_id),
    ):
        await session.execute(select(model.id).where(model.id == local_id).with_for_update())


async def _owner_snapshot(
    session: AsyncSession,
    plan: ArchivePublicationPlan,
    *,
    job_id: int,
    imported_file_id: int,
    action_id: int,
) -> ImportArchiveOwner:
    job = await session.get(ImportJob, job_id, populate_existing=True)
    file = await session.get(ImportedFile, imported_file_id, populate_existing=True)
    action = await session.get(ImportJobAction, action_id, populate_existing=True)
    bound = plan.target.binding
    library_file = await session.get(LibraryFile, bound.library_file_id, populate_existing=True)
    if (
        job is None
        or file is None
        or action is None
        or library_file is None
        or library_file.storage_mode is not LibraryFileStorageMode.MANAGED
        or _digest(library_file.source_signature) != _digest(_signature(plan))
        or job.status is not ImportJobStatus.COMPLETED
        or job.control_request is not ImportControlRequest.NONE
        or file.import_job_id != job.id
        or action.import_job_id != job.id
        or file.status is not ImportedFileStatus.IMPORTED
        or file.library_file_id != bound.library_file_id
        or file.matched_issue_id != bound.metadata.issues[0].local_id
        or action.status is not ImportJobActionStatus.COMPLETED
        or action.action_type != "library_file_registered"
        or action.phase != "import"
    ):
        raise ArchivePublicationError("import_owner_changed")
    payload = action.payload
    diagnostics = file.diagnostics
    details = diagnostics.get(_ENRICHMENT) if isinstance(diagnostics, dict) else None
    if (
        not isinstance(payload, dict)
        or not isinstance(details, dict)
        or any(type(payload.get(key)) is not int for key in ("imported_file_id", "library_file_id"))
        or any(type(details.get(key)) is not int for key in ("library_file_id", "issue_id"))
        or details.get("status") != "pending"
        or details.get("library_file_id") != bound.library_file_id
        or details.get("issue_id") != file.matched_issue_id
        or details.get("artifact_path", str(plan.target.path)) != str(plan.target.path)
        or payload.get("imported_file_id") != file.id
        or payload.get("library_file_id") != bound.library_file_id
        or payload.get("destination_path") != str(plan.target.path)
        or payload.get("storage_mode") != "managed"
        or payload.get("transfer_method") not in ("copy", "move", "hardlink")
        or _digest(payload.get("destination_signature")) != _digest(_signature(plan))
        or payload.get("embedded_comicinfo_enrichment_deferred") is not True
        or _SUCCESSOR in payload
    ):
        raise ArchivePublicationError("import_owner_changed")
    return ImportArchiveOwner(
        job_id=job.id,
        imported_file_id=file.id,
        action_id=action.id,
        action_digest=_digest(payload),
        pending_digest=_digest(
            {
                "details": details,
                "path": file.file_path,
                "series": file.import_series_id,
                "source_signature": file.source_signature,
            }
        ),
    )


async def bind_import_archive_publication(
    session: AsyncSession,
    plan: ArchivePublicationPlan,
    *,
    imported_file_id: int,
    action_id: int,
) -> ArchivePublicationPlan:
    """Bind a prepared publication to its completed import placement."""
    _clean_session(session)
    if any(type(value) is not int or value <= 0 for value in (imported_file_id, action_id)):
        raise ArchivePublicationError("import_owner_invalid")
    async with metadata_write_scope(session):
        file = await session.get(ImportedFile, imported_file_id)
        if file is None:
            raise ArchivePublicationError("import_owner_missing")
        job_id = file.import_job_id
        await _lock_owner_rows(session, job_id, imported_file_id, action_id)
        owner = await _owner_snapshot(
            session,
            plan,
            job_id=job_id,
            imported_file_id=imported_file_id,
            action_id=action_id,
        )
        if plan.import_owner is not None and plan.import_owner != owner:
            raise ArchivePublicationError("import_owner_changed")
        return plan.model_copy(update={"import_owner": owner})


async def require_import_archive_owner(session: AsyncSession, plan: ArchivePublicationPlan) -> None:
    owner = plan.import_owner
    if owner is None:
        return
    current = await _owner_snapshot(
        session,
        plan,
        job_id=owner.job_id,
        imported_file_id=owner.imported_file_id,
        action_id=owner.action_id,
    )
    if current != owner:
        raise ArchivePublicationError("import_owner_changed")


async def acknowledge_import_archive_publication(
    session: AsyncSession,
    receipt: ArchivePublicationReceipt,
) -> None:
    """Caller already holds owner locks and verified the immutable owner snapshot."""
    owner = receipt.plan.import_owner
    if owner is None:
        return
    action = await session.get(ImportJobAction, owner.action_id)
    file = await session.get(ImportedFile, owner.imported_file_id)
    assert action is not None and file is not None
    action.payload = {**action.payload, _SUCCESSOR: str(receipt.operation_id)}
    details = dict(file.diagnostics[_ENRICHMENT])
    details.update(
        status="complete",
        publication_id=str(receipt.operation_id),
        completed_at=datetime.now(UTC).isoformat(),
    )
    file.diagnostics = {**file.diagnostics, _ENRICHMENT: details}


async def import_rollback_signature(
    session: AsyncSession,
    action: ImportJobAction,
) -> object:
    """Require a settled successor bound to this original action before rollback.

    Locks remain in the caller's transaction through removal. The original
    signature is never replaced; a retained finalized publication proves only
    the successor to compare with the actual filesystem, not permission to skip
    that comparison.
    """
    original_payload = dict(action.payload)
    async with metadata_write_scope(session):
        await session.execute(
            select(ImportJob.id).where(ImportJob.id == action.import_job_id).with_for_update()
        )
        await session.refresh(action, with_for_update=True)
        if action.payload != original_payload:
            raise ArchivePublicationError("import_owner_changed")
        file_id = action.payload.get("library_file_id")
        path = action.payload.get("destination_path")
        if not isinstance(path, str):
            raise ArchivePublicationError("import_owner_changed")
        path_key = hashlib.sha256(os.fsencode(path)).hexdigest()
        active = await session.scalar(
            select(ArchiveMetadataPublication.id)
            .where(
                or_(
                    ArchiveMetadataPublication.active_file_id == file_id,
                    ArchiveMetadataPublication.active_path_key == path_key,
                )
            )
            .limit(1)
        )
        if active is not None:
            raise ArchivePublicationError("import_owner_publication_pending")
        if _SUCCESSOR not in action.payload:
            return action.payload.get("destination_signature")
        file = await session.scalar(
            select(LibraryFile)
            .where(LibraryFile.id == file_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        try:
            operation_id = UUID(action.payload[_SUCCESSOR])
        except (ValueError, TypeError, AttributeError):
            raise ArchivePublicationError("import_owner_successor_invalid") from None
        row = await session.scalar(
            select(ArchiveMetadataPublication)
            .where(ArchiveMetadataPublication.operation_id == str(operation_id))
            .with_for_update()
        )
        if row is None or row.state is not PublicationState.FINALIZED:
            raise ArchivePublicationError("import_owner_successor_invalid")
        receipt = _receipt(row)
        owner = receipt.plan.import_owner
        binding = receipt.plan.target.binding
        original = {key: value for key, value in action.payload.items() if key != _SUCCESSOR}
        if (
            owner is None
            or file is None
            or file.storage_mode is not LibraryFileStorageMode.MANAGED
            or file.issue_id != binding.metadata.issues[0].local_id
            or file.library_root_id != binding.library_root_id
            or file.file_path != str(receipt.plan.target.path)
            or file.file_name != receipt.plan.target.path.name
            or _digest(file.source_signature) != _digest(_signature(receipt.plan))
            or owner.action_id != action.id
            or owner.job_id != action.import_job_id
            or owner.imported_file_id != action.payload.get("imported_file_id")
            or owner.action_digest != _digest(original)
            or receipt.plan.target.binding.library_file_id != file_id
            or str(receipt.plan.target.path) != path
            or action.payload.get("destination_signature") != _signature(receipt.plan)
        ):
            raise ArchivePublicationError("import_owner_successor_invalid")
        return _signature(receipt.plan, successor=True)
