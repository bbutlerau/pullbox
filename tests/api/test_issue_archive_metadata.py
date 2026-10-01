"""Existing-file preview is read-only and approval queues only managed CBZs."""

import os
import sys
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update

from pullbox.models import LibraryFile
from pullbox.models.library import LibraryFileStorageMode
from pullbox.services.archive_metadata_rendering import ArchiveMetadataRenderError
from pullbox.services.issue_file_metadata import file_metadata_error
from pullbox.utilities.executors.file_metadata import FileMetadataExecutor
from pullbox.utilities.job_queue import JobQueueManager
from pullbox.utilities.models import JobType
from tests.api.test_metadata_sources_api import csrf
from tests.integration.metadata_identity.test_archive_metadata_publication import prepared

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


@pytest.mark.parametrize("field", ["title", "issue_count", "credits"])
def test_metadata_disagreement_names_the_field_without_exposing_values(field):
    message = file_metadata_error(ArchiveMetadataRenderError("unreconciled_field", field))
    assert f"disagree about {field.replace('_', ' ')}" in message
    assert "left unchanged" in message


async def test_managed_issue_can_preview_pair_without_writing(
    authenticated_client, sec_db, tmp_path
):
    async with prepared(sec_db, tmp_path) as (path, _, plan):
        issue_id = plan.target.binding.metadata.issues[0].local_id
        before = path.read_bytes(), path.stat()
        response = await authenticated_client.post(
            f"/api/v1/issues/{issue_id}/file-metadata/preview",
            headers=csrf(authenticated_client),
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["file_name"] == "example.cbz"
        assert data["documents"] == ["ComicInfo.xml", "MetronInfo.xml"]
        assert data["changes"] and len(data["review_key"]) == 64
        assert (path.read_bytes(), path.stat()) == before


async def test_referenced_issue_cannot_preview_a_write(authenticated_client, sec_db, tmp_path):
    async with prepared(sec_db, tmp_path) as (path, _, plan):
        async with sec_db.begin() as session:
            await session.execute(
                update(LibraryFile).values(storage_mode=LibraryFileStorageMode.REFERENCED)
            )
        before = path.read_bytes()
        response = await authenticated_client.post(
            f"/api/v1/issues/{plan.target.binding.metadata.issues[0].local_id}/file-metadata/preview",
            headers=csrf(authenticated_client),
        )
        assert response.status_code == 409, response.text
        assert "kept in place" in response.text
        assert path.read_bytes() == before
        async with sec_db() as session:
            assert (
                await session.scalar(select(LibraryFile))
            ).storage_mode is LibraryFileStorageMode.REFERENCED


async def test_approval_queues_background_work_and_exposes_result(
    authenticated_client, sec_db, tmp_path, monkeypatch
):
    import pullbox.api.v1.issue_metadata_links as api

    manager = JobQueueManager(sec_db)
    manager.register_executor(JobType.FILE_METADATA, FileMetadataExecutor)
    monkeypatch.setattr(api, "_get_manager", lambda: manager)
    monkeypatch.setattr(api, "_schedule_dispatch", lambda manager: None)
    async with prepared(sec_db, tmp_path) as (path, _, plan):
        issue_id = plan.target.binding.metadata.issues[0].local_id
        endpoint = f"/api/v1/issues/{issue_id}/file-metadata"
        preview = await authenticated_client.post(
            endpoint + "/preview", headers=csrf(authenticated_client)
        )
        before = path.read_bytes()
        response = await authenticated_client.post(
            endpoint + "/write",
            json={"review_key": preview.json()["review_key"]},
            headers=csrf(authenticated_client),
        )
        assert response.status_code == 202, response.text
        assert path.read_bytes() == before
        await manager.dispatch_next()
        report = await authenticated_client.get(endpoint + "/job")
        assert report.status_code == 200
        assert report.json()["job"]["state"] == "COMPLETED"
        assert report.json()["job"]["percent"] == 100


async def test_stale_approval_and_missing_csrf_do_not_queue(authenticated_client, sec_db, tmp_path):
    async with prepared(sec_db, tmp_path) as (path, _, plan):
        endpoint = f"/api/v1/issues/{plan.target.binding.metadata.issues[0].local_id}/file-metadata"
        response = await authenticated_client.post(endpoint + "/preview")
        assert response.status_code == 403
        before = path.read_bytes()
        response = await authenticated_client.post(
            endpoint + "/write", json={"review_key": "0" * 64}, headers=csrf(authenticated_client)
        )
        assert response.status_code == 409 and "Preview again" in response.text
        assert path.read_bytes() == before


@pytest.mark.parametrize("action", ["preview", "write"])
async def test_unreadable_archive_has_actionable_error_without_queuing(
    authenticated_client, sec_db, tmp_path, action
):
    async with prepared(sec_db, tmp_path) as (path, _, plan):
        path.write_bytes(b"Damaged comic archive")
        async with sec_db.begin() as session:
            await session.execute(
                update(LibraryFile)
                .where(LibraryFile.id == plan.target.binding.library_file_id)
                .values(
                    file_size=path.stat().st_size,
                    file_modified_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
                )
            )
        response = await authenticated_client.post(
            f"/api/v1/issues/{plan.target.binding.metadata.issues[0].local_id}"
            f"/file-metadata/{action}",
            json={"review_key": "a" * 64} if action == "write" else {},
            headers=csrf(authenticated_client),
        )
        assert response.status_code == 409, response.text
        assert "Replace or repair" in response.text
        assert path.read_bytes() == b"Damaged comic archive"
