"""Authenticated, scoped follow-up uses the existing metadata background job."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from pullbox.models.import_job import ImportedFile, ImportJobAction
from tests.api.test_import_completed_cleanup_api import _csrf_header_for
from tests.integration.metadata_identity.test_import_archive_publication import owned
from tests.integration.metadata_identity.test_import_metadata_retry import fail_for_root

pytest_plugins = ["conftest_security"]
pytestmark = pytest.mark.usefixtures("paired_import_writer_setting")


async def test_followup_retry_commits_before_scheduling_and_never_reimports(
    authenticated_client,
    sec_db,
    tmp_path,
    monkeypatch,
):
    async with owned(sec_db, tmp_path) as (path, _, plan, ids, _):
        await fail_for_root(sec_db, plan, ids)
        service = MagicMock()
        monkeypatch.setattr(
            "pullbox.composition.services.build_import_service", AsyncMock(return_value=service)
        )
        monkeypatch.setattr("pullbox.database.get_session_factory", lambda: sec_db)
        before = path.read_bytes()
        async with sec_db() as session:
            original = (await session.get(ImportJobAction, ids[2])).payload
        detail = await authenticated_client.get(f"/import/{ids[0]}/metadata-writes")
        assert detail.status_code == 200
        assert 'class="btn-primary btn-sm"' in detail.text and "Retry metadata" in detail.text
        assert "write permissions" in detail.text
        response = await authenticated_client.post(
            f"/import/{ids[0]}/files/{ids[1]}/retry-metadata",
            headers=_csrf_header_for(authenticated_client),
        )
        assert response.status_code == 200 and "Metadata retry queued" in response.text
        service.schedule_comicinfo_enrichment.assert_called_once_with(sec_db, job_id=ids[0])
        async with sec_db() as session:
            file = await session.get(ImportedFile, ids[1])
            assert file.diagnostics["comicinfo_enrichment"]["status"] == "pending"
            assert (await session.get(ImportJobAction, ids[2])).payload == original
        assert path.read_bytes() == before
        response = await authenticated_client.post(
            f"/import/{ids[0]}/files/{ids[1]}/retry-metadata",
            headers=_csrf_header_for(authenticated_client),
        )
        assert response.status_code == 200 and "already queued" in response.text
        assert service.schedule_comicinfo_enrichment.call_count == 1


async def test_metadata_retry_requires_csrf_and_operator(
    authenticated_client,
    sec_db,
    sec_api_key,
    tmp_path,
):
    async with owned(sec_db, tmp_path) as (_, _, plan, ids, _):
        await fail_for_root(sec_db, plan, ids)
        url = f"/import/{ids[0]}/files/{ids[1]}/retry-metadata"
        response = await authenticated_client.post(url)
        assert response.status_code == 403
        authenticated_client.cookies.clear()
        response = await authenticated_client.post(url, headers={"X-API-Key": sec_api_key})
        assert response.status_code == 401
        async with sec_db() as session:
            file = await session.get(ImportedFile, ids[1])
            assert file.diagnostics["comicinfo_enrichment"]["status"] == "failed"


async def test_metadata_follow_up_is_bounded_and_escapes_names(
    authenticated_client,
    sec_db,
    tmp_path,
):
    async with owned(sec_db, tmp_path) as (_, _, plan, ids, _):
        await fail_for_root(sec_db, plan, ids)
        async with sec_db.begin() as session:
            file = await session.get(ImportedFile, ids[1])
            file.file_name = "<script>not executable</script>.cbz"
            for number in range(25):
                session.add(
                    ImportedFile(
                        import_job_id=file.import_job_id,
                        import_series_id=file.import_series_id,
                        file_path=f"/imports/{number}.cbz",
                        file_name=f"Extra {number}.cbz",
                        file_format="cbz",
                        status=file.status,
                        diagnostics=file.diagnostics,
                    )
                )
        response = await authenticated_client.get(f"/import/{ids[0]}/metadata-writes")
        assert response.status_code == 200
        assert response.text.count('data-testid="metadata-write-') == 25
        assert "26 failed metadata writes remaining" in response.text
        assert "&lt;script&gt;not executable&lt;/script&gt;.cbz" in response.text
        assert "page=2" in response.text
        response = await authenticated_client.get(f"/import/{ids[0]}/metadata-writes?page=2")
        assert response.status_code == 200
        assert response.text.count('data-testid="metadata-write-') == 1
