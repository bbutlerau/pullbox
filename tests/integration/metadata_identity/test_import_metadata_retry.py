"""Metadata retry never reimports or replaces an import's original evidence."""

from copy import deepcopy

import pytest
from sqlalchemy import select

from pullbox.core.exceptions import NotFoundError, ValidationError
from pullbox.models import LibraryRoot
from pullbox.models.import_job import ImportControlRequest, ImportedFile, ImportJob, ImportJobAction
from pullbox.services.import_metadata_follow_up import retry_import_metadata_write
from tests.integration.metadata_identity.test_import_archive_publication import owned, publish
from tests.integration.metadata_identity.test_import_metadata_enrichment import import_service

pytestmark = pytest.mark.usefixtures("paired_import_writer_setting")


async def fail_for_root(factory, plan, ids):
    async with factory.begin() as session:
        root = await session.get(LibraryRoot, plan.target.binding.library_root_id)
        root.allow_managed_writes = False
    await import_service().recover_pending_comicinfo_enrichment(factory)
    async with factory() as session:
        file = await session.get(ImportedFile, ids[1])
        assert file.diagnostics["comicinfo_enrichment"]["status"] == "failed"


@pytest.mark.parametrize("changed", [False, True])
async def test_retry_only_requeues_metadata_and_revalidates_archive(
    identity_probe_db, tmp_path, changed
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, _):
        await fail_for_root(factory, plan, ids)
        if changed:
            path.write_bytes(b"Replacement that this import does not own")
        before, original = path.read_bytes(), source.read_bytes()
        async with factory.begin() as session:
            root = await session.get(LibraryRoot, plan.target.binding.library_root_id)
            root.allow_managed_writes = True
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, ids[2])
            evidence = deepcopy(action.payload)
            assert await retry_import_metadata_write(session, ids[0], ids[1], actor="test")
            assert not await retry_import_metadata_write(session, ids[0], ids[1], actor="test")
            assert action.payload == evidence
        assert path.read_bytes() == before
        await import_service().recover_pending_comicinfo_enrichment(factory)
        async with factory() as session:
            file = await session.get(ImportedFile, ids[1])
            assert file.diagnostics["comicinfo_enrichment"]["status"] == (
                "failed" if changed else "complete"
            )
            action = await session.get(ImportJobAction, ids[2])
            assert {
                key: value for key, value in action.payload.items() if key != "metadata_publication"
            } == evidence
        if changed:
            assert path.read_bytes() == before
        assert source.read_bytes() == original


async def test_retry_cannot_change_live_publication_owner(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        await publish(factory, plan)
        async with factory.begin() as session:
            file = await session.get(ImportedFile, ids[1])
            file.diagnostics = {
                **file.diagnostics,
                "comicinfo_enrichment": {
                    **file.diagnostics["comicinfo_enrichment"],
                    "status": "failed",
                },
            }
        async with factory.begin() as session:
            with pytest.raises(ValidationError, match="needs recovery"):
                await retry_import_metadata_write(session, ids[0], ids[1], actor="test")
            file = await session.get(ImportedFile, ids[1])
            assert file.diagnostics["comicinfo_enrichment"]["status"] == "failed"


async def test_retry_rejects_stopped_job_and_wrong_import(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        await fail_for_root(factory, plan, ids)
        async with factory.begin() as session:
            with pytest.raises(NotFoundError):
                await retry_import_metadata_write(session, ids[0], ids[1] + 1, actor="test")
            job = await session.get(ImportJob, ids[0])
            job.control_request = ImportControlRequest.CANCEL
        async with factory.begin() as session:
            with pytest.raises(ValidationError, match="no longer ready"):
                await retry_import_metadata_write(session, ids[0], ids[1], actor="test")
            file = await session.scalar(select(ImportedFile))
            assert file.diagnostics["comicinfo_enrichment"]["status"] == "failed"
