"""Settle retained import publication without repeating filesystem mutation."""

from dataclasses import replace

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models import LibraryFile
from pullbox.models.archive_metadata_publication import PublicationState
from pullbox.models.import_job import ImportJob
from pullbox.services.archive_metadata_finalization import (
    _check_output,
    finalize_archive_publication,
)
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    ArchivePublicationInspection,
    ArchivePublicationReceipt,
    _clean_session,
    _file_work,
    _lock_binding,
    _locked_row,
    _receipt,
    load_archive_publication,
    reconcile_archive_publication,
)
from pullbox.services.import_archive_publication import (
    _owner_snapshot,
    acknowledge_import_archive_publication,
    import_archive_owner_stopped,
)
from pullbox.services.metadata_writer_identity import metadata_write_scope


async def recover_import_archive_publication(
    session: AsyncSession,
    receipt: ArchivePublicationReceipt,
    inspection: ArchivePublicationInspection,
) -> ArchivePublicationReceipt:
    """Classify and settle one inspected import-owned publication.

    Never republish or restore an archive. A stopped owner receives file accounting
    and successor proof only, not a stale canonical metadata update. All writes
    belong to the caller's transaction; inspection/hashing happens before it.
    """
    _clean_session(session)
    owner = receipt.plan.import_owner
    if owner is None:
        raise ArchivePublicationError("import_owner_missing")
    if (inspection.operation_id, inspection.revision) != (receipt.operation_id, receipt.revision):
        raise ArchivePublicationError("publication_changed")
    async with metadata_write_scope(session):
        known = await load_archive_publication(session, receipt.operation_id)
        if known is None or known.plan != receipt.plan:
            raise ArchivePublicationError("publication_changed")
        await _lock_binding(session, known.plan)
        row = await _locked_row(session, receipt.operation_id)
        current = _receipt(row)
        if current.state in {
            PublicationState.FINALIZED,
            PublicationState.SETTLED,
            PublicationState.ABANDONED,
        }:
            return current
        if current != receipt:
            raise ArchivePublicationError("publication_changed")
        current = await reconcile_archive_publication(session, receipt, inspection)
        if current.state is not PublicationState.PUBLISHED:
            return current
        inspection = replace(inspection, revision=current.revision)
        job = await session.get(ImportJob, owner.job_id, populate_existing=True)
        if job is None or not import_archive_owner_stopped(job):
            return await finalize_archive_publication(session, current, inspection)
        snapshot = await _owner_snapshot(
            session,
            current.plan,
            job_id=owner.job_id,
            imported_file_id=owner.imported_file_id,
            action_id=owner.action_id,
            stopped=True,
        )
        if snapshot != owner:
            raise ArchivePublicationError("import_owner_changed")
        modified_at = await _file_work(lambda stop: _check_output(current, inspection))
        file = await session.get(LibraryFile, current.plan.target.binding.library_file_id)
        assert file is not None and inspection.fingerprint is not None
        file.file_size = inspection.fingerprint[2]
        file.file_modified_at = modified_at
        file.file_hash = current.plan.output_digest
        file.has_comicinfo = True
        await acknowledge_import_archive_publication(session, current, cancelled=True)
        await session.flush()
        await _file_work(lambda stop: _check_output(current, inspection))
        row.state = PublicationState.SETTLED
        row.revision += 1
        row.active_file_id = None
        row.active_path_key = None
        await session.flush()
        return _receipt(row)
