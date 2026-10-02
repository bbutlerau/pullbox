"""Import-owned enrichment retains the original proof and a verified successor."""

import asyncio
import threading
from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import delete, update
from sqlalchemy.orm.attributes import flag_modified

from pullbox.core.library_file_ownership import build_managed_placement_signature
from pullbox.models import LibraryFile, LibraryRoot
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication
from pullbox.models.import_job import (
    ImportControlRequest,
    ImportedFile,
    ImportedFileStatus,
    ImportedSeries,
    ImportJob,
    ImportJobAction,
    ImportJobActionStatus,
    ImportJobStatus,
    ImportSourceType,
)
from pullbox.services.archive_metadata_finalization import finalize_archive_publication
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    inspect_archive_publication,
    load_archive_publication,
    publish_archive_publication,
    reconcile_archive_publication,
)
from pullbox.services.import_archive_publication import bind_import_archive_publication
from pullbox.services.import_job_actions import rollback_action
from tests.integration.metadata_identity.test_archive_metadata_publication import (
    load,
    prepared,
    record,
)


@asynccontextmanager
async def owned(factory, tmp_path):
    async with prepared(factory, tmp_path) as (path, _, plan):
        source = tmp_path / "source.cbz"
        source.write_bytes(path.read_bytes())
        signature = build_managed_placement_signature(path)
        bound = plan.target.binding
        async with factory.begin() as session:
            job = ImportJob(
                source_path=str(tmp_path),
                source_type=ImportSourceType.FILESYSTEM,
                status=ImportJobStatus.COMPLETED,
            )
            session.add(job)
            await session.flush()
            series = ImportedSeries(
                import_job_id=job.id,
                raw_series_name="Example",
                series_id=bound.metadata.series.local_id,
            )
            session.add(series)
            await session.flush()
            file = ImportedFile(
                import_job_id=job.id,
                import_series_id=series.id,
                file_path=str(source),
                file_name=source.name,
                file_format="cbz",
                status=ImportedFileStatus.IMPORTED,
                matched_issue_id=bound.metadata.issues[0].local_id,
                library_file_id=bound.library_file_id,
                diagnostics={
                    "keep": {"evidence": True},
                    "comicinfo_enrichment": {
                        "status": "pending",
                        "reason": "deferred_during_import",
                        "library_file_id": bound.library_file_id,
                        "issue_id": bound.metadata.issues[0].local_id,
                        "artifact_path": str(path),
                    },
                },
            )
            session.add(file)
            await session.flush()
            action = ImportJobAction(
                import_job_id=job.id,
                sequence_no=1,
                phase="import",
                action_type="library_file_registered",
                payload={
                    "imported_file_id": file.id,
                    "library_file_id": bound.library_file_id,
                    "destination_path": str(path),
                    "destination_signature": signature,
                    "original_source_path": str(source),
                    "transfer_method": "copy",
                    "storage_mode": "managed",
                    "embedded_comicinfo_enrichment_deferred": True,
                },
            )
            session.add(action)
            library = await session.get(LibraryFile, bound.library_file_id)
            library.source_signature = signature
            await session.flush()
            ids = job.id, file.id, action.id
        async with factory() as session:
            plan = await bind_import_archive_publication(
                session, plan, imported_file_id=ids[1], action_id=ids[2]
            )
        yield path, source, plan, ids, signature


async def publish(factory, plan):
    saved = await record(factory, plan)
    async with factory.begin() as session:
        return await publish_archive_publication(session, saved.operation_id)


async def finish(factory, receipt):
    inspection = await inspect_archive_publication(receipt)
    async with factory.begin() as session:
        return await finalize_archive_publication(session, receipt, inspection)


async def rollback(factory, action_id):
    async with factory.begin() as session:
        action = await session.get(ImportJobAction, action_id)
        await rollback_action(
            session,
            action_id=action.id,
            action_type=action.action_type,
            payload=deepcopy(action.payload),
            delete_series=AsyncMock(),
        )
    async with factory() as session:
        return (await session.get(ImportJobAction, action_id)).status


async def test_finalization_acknowledges_owner_and_rollback_accepts_only_successor(
    identity_probe_db,
    tmp_path,
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, signature):
        original = source.read_bytes()
        saved = await publish(factory, plan)
        completed = await finish(factory, saved)
        async with factory() as session:
            action = await session.get(ImportJobAction, ids[2])
            assert action.payload["destination_signature"] == signature
            assert action.payload.get("metadata_publication") == str(saved.operation_id)
            file = await session.get(ImportedFile, ids[1])
            assert file.diagnostics["keep"] == {"evidence": True}
            assert file.diagnostics["comicinfo_enrichment"]["status"] == "complete"
            assert file.diagnostics["comicinfo_enrichment"]["publication_id"] == str(
                saved.operation_id
            )
            library = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert library.source_signature == signature
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK
        assert not path.exists()
        assert source.read_bytes() == original
        assert await load(factory, saved.operation_id) == completed
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK


async def test_pre_owner_journal_json_remains_readable(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(
                update(ArchiveMetadataPublication).values(
                    plan_json=plan.model_dump_json(exclude={"import_owner"})
                )
            )
        async with factory() as session:
            assert await load_archive_publication(session, saved.operation_id) == saved


async def test_owner_acknowledgment_rolls_back_with_canonical_finalization(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        saved = await publish(factory, plan)
        inspection = await inspect_archive_publication(saved)
        async with factory() as session:
            await finalize_archive_publication(session, saved, inspection)
            await session.rollback()
        async with factory() as session:
            assert (
                "metadata_publication" not in (await session.get(ImportJobAction, ids[2])).payload
            )
            assert (await session.get(ImportedFile, ids[1])).diagnostics["comicinfo_enrichment"][
                "status"
            ] == "pending"
        assert await load(factory, saved.operation_id) == saved
        await finish(factory, saved)
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK


@pytest.mark.parametrize("boundary", ["record", "publish", "finalize"])
@pytest.mark.parametrize(
    "change", ["cancel", "rollback", "action", "imported_file", "registration", "delete"]
)
async def test_changed_owner_never_authorizes_mutation(
    identity_probe_db, tmp_path, boundary, change
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        saved = None
        if boundary != "record":
            saved = await record(factory, plan)
        if boundary == "finalize":
            async with factory.begin() as session:
                saved = await publish_archive_publication(session, saved.operation_id)
        async with factory.begin() as session:
            if change == "cancel":
                await session.execute(
                    update(ImportJob).values(control_request=ImportControlRequest.CANCEL)
                )
            elif change == "rollback":
                await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))
            elif change == "action":
                action = await session.get(ImportJobAction, ids[2])
                action.payload = {**action.payload, "original_source_path": "changed"}
            elif change == "imported_file":
                await session.execute(
                    update(ImportedFile).values(status=ImportedFileStatus.SKIPPED)
                )
            elif change == "registration":
                await session.execute(
                    update(LibraryFile).values(source_signature={"later": "owner"})
                )
            else:
                await session.execute(delete(ImportJob).where(ImportJob.id == ids[0]))
        before = path.read_bytes()
        with pytest.raises(ArchivePublicationError, match="import_owner"):
            if boundary == "record":
                await record(factory, plan)
            elif boundary == "publish":
                async with factory.begin() as session:
                    await publish_archive_publication(session, saved.operation_id)
            else:
                await finish(factory, saved)
        assert path.read_bytes() == before
        if saved:
            assert await load(factory, saved.operation_id) == saved


@pytest.mark.parametrize(
    "change", ["reference", "signature", "action_type", "target", "not_pending"]
)
async def test_owner_binding_rejects_unowned_placement(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, ids[2])
            payload = dict(action.payload)
            if change == "reference":
                payload["storage_mode"] = "referenced"
                payload["transfer_method"] = "leave_in_place"
            elif change == "signature":
                payload["destination_signature"] = {"content_digest": "a" * 64}
            elif change == "action_type":
                action.action_type = "series_created"
            elif change == "target":
                payload["library_file_id"] = 999
            else:
                file = await session.get(ImportedFile, ids[1])
                file.diagnostics = {"comicinfo_enrichment": {"status": "complete"}}
            action.payload = payload
        with pytest.raises(ArchivePublicationError, match="import_owner"):
            async with factory() as session:
                await bind_import_archive_publication(
                    session,
                    plan.model_copy(update={"import_owner": None}),
                    imported_file_id=ids[1],
                    action_id=ids[2],
                )


async def test_rename_commit_gap_retains_owner_and_recovers_once(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, _):
        saved = await record(factory, plan)
        async with factory() as session:
            await publish_archive_publication(session, saved.operation_id)
            await session.rollback()
        before = path.read_bytes(), path.stat().st_ino
        saved = await load(factory, saved.operation_id)
        assert getattr(saved.plan, "import_owner", None) is not None
        async with factory.begin() as session:
            saved = await reconcile_archive_publication(
                session, saved, await inspect_archive_publication(saved)
            )
        completed = await finish(factory, saved)
        assert await finish(factory, saved) == completed
        assert (path.read_bytes(), path.stat().st_ino) == before
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK
        assert source.exists()


@pytest.mark.parametrize("state", ["intended", "published", "finalized_changed"])
async def test_rollback_preserves_unsettled_or_user_changed_archive(
    identity_probe_db, tmp_path, state
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, _):
        saved = await record(factory, plan)
        if state != "intended":
            async with factory.begin() as session:
                saved = await publish_archive_publication(session, saved.operation_id)
        if state == "finalized_changed":
            await finish(factory, saved)
            path.write_bytes(b"user replacement")
        before = path.read_bytes()
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLBACK_FAILED
        assert path.read_bytes() == before and source.exists()


async def test_unverified_successor_pointer_cannot_authorize_deletion(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, _, ids, _):
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, ids[2])
            action.payload = {**action.payload, "metadata_publication": str(uuid4())}
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLBACK_FAILED
        assert path.exists()


async def test_concurrent_finalizers_acknowledge_import_once(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        saved = await publish(factory, plan)
        results = await asyncio.gather(finish(factory, saved), finish(factory, saved))
        assert results[0] == results[1]
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK


async def test_completed_replay_survives_deleted_import_without_recreating_it(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        saved = await publish(factory, plan)
        completed = await finish(factory, saved)
        async with factory.begin() as session:
            await session.execute(delete(ImportJob).where(ImportJob.id == ids[0]))
        path.write_bytes(b"later edit")
        assert await finish(factory, saved) == completed
        async with factory() as session:
            assert await session.get(ImportJob, ids[0]) is None
        assert path.read_bytes() == b"later edit"


@pytest.mark.parametrize("change", ["different_action", "original_proof", "source_path"])
async def test_finalized_receipt_cannot_be_repurposed(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        saved = await publish(factory, plan)
        await finish(factory, saved)
        action_id = ids[2]
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, action_id)
            payload = dict(action.payload)
            if change == "different_action":
                other = ImportJobAction(
                    import_job_id=ids[0],
                    sequence_no=2,
                    phase="import",
                    action_type=action.action_type,
                    payload=payload,
                )
                session.add(other)
                await session.flush()
                action_id = other.id
            elif change == "original_proof":
                payload["destination_signature"] = build_managed_placement_signature(path)
                action.payload = payload
            else:
                action.payload = {**payload, "original_source_path": "different/source.cbz"}
        assert await rollback(factory, action_id) is ImportJobActionStatus.ROLLBACK_FAILED
        assert path.exists()


@pytest.mark.parametrize("change", ["boolean_id", "diagnostics_shape", "transfer_shape"])
async def test_malformed_owner_evidence_has_bounded_error(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, ids[2])
            if change == "boolean_id":
                action.payload = {**action.payload, "imported_file_id": True}
                flag_modified(action, "payload")
            elif change == "diagnostics_shape":
                file = await session.get(ImportedFile, ids[1])
                file.diagnostics = ["legacy malformed data"]
            else:
                action.payload = {**action.payload, "transfer_method": ["copy"]}
        with pytest.raises(ArchivePublicationError, match="import_owner"):
            async with factory() as session:
                await bind_import_archive_publication(
                    session,
                    plan.model_copy(update={"import_owner": None}),
                    imported_file_id=ids[1],
                    action_id=ids[2],
                )


async def test_cancellation_cannot_cross_an_active_publication_lock(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import archive_metadata_publication as publication

    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        saved = await record(factory, plan)
        entered, release = threading.Event(), threading.Event()
        real_publish = publication._publish_files

        def held(plan, stop):
            entered.set()
            assert release.wait(10)
            return real_publish(plan, stop)

        async def run_publish():
            async with factory.begin() as session:
                return await publish_archive_publication(session, saved.operation_id)

        async def cancel():
            async with factory.begin() as session:
                await session.execute(
                    update(ImportJob)
                    .where(ImportJob.id == ids[0])
                    .values(control_request=ImportControlRequest.CANCEL)
                )

        monkeypatch.setattr(publication, "_publish_files", held)
        publishing = asyncio.create_task(run_publish())
        cancelling = None
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            cancelling = asyncio.create_task(cancel())
            await asyncio.sleep(0.05)
            assert not cancelling.done()
        finally:
            release.set()
            published = await publishing
            if cancelling:
                await cancelling
        before = path.read_bytes()
        with pytest.raises(ArchivePublicationError, match="import_owner"):
            await finish(factory, published)
        assert path.read_bytes() == before


@pytest.mark.parametrize("change", ["issue", "root", "source_evidence"])
async def test_reassigned_registration_is_not_removed_by_old_import(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        saved = await publish(factory, plan)
        await finish(factory, saved)
        async with factory.begin() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            if change == "issue":
                file.issue_id = None
            elif change == "root":
                root = LibraryRoot(name="Reassigned", path=str(tmp_path / "other-root"))
                session.add(root)
                await session.flush()
                file.library_root_id = root.id
            else:
                file.source_signature = {"later": "registration evidence"}
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLBACK_FAILED
        assert path.exists()
