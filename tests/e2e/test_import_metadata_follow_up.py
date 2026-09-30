"""Archive metadata follow-up keeps the import page stable during review/retry."""

import re

import pytest
from playwright.sync_api import expect
from sqlalchemy import delete, select

from pullbox.models import Issue, LibraryFile, LibraryRoot
from pullbox.models.import_job import (
    ImportedFile,
    ImportedFileStatus,
    ImportedSeries,
    ImportJob,
    ImportJobStatus,
    ImportSeriesStatus,
    ImportSourceType,
)
from pullbox.models.library import FileFormat
from pullbox.services.import_service import ImportService
from tests.e2e.accessibility import assert_no_axe_violations
from tests.e2e.conftest import _run_async_blocking, wait_for_htmx

pytestmark = pytest.mark.e2e


@pytest.fixture
def metadata_follow_up_job(seeded_server, tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from pullbox.database import get_session_factory

    async def seed():
        async with get_session_factory().begin() as session:
            issue = await session.scalar(select(Issue).order_by(Issue.id).limit(1))
            root = await session.scalar(select(LibraryRoot).order_by(LibraryRoot.id).limit(1))
            job = ImportJob(
                source_path="/tmp/metadata-follow-up-test",
                source_type=ImportSourceType.FILESYSTEM,
                status=ImportJobStatus.COMPLETED,
            )
            session.add(job)
            await session.flush()
            series = ImportedSeries(
                import_job_id=job.id,
                raw_series_name="Metadata follow-up test",
                status=ImportSeriesStatus.IMPORTED,
                series_id=issue.series_id,
            )
            session.add(series)
            await session.flush()
            library = LibraryFile(
                file_path=str(tmp_path / "test.cbz"),
                file_name="test.cbz",
                file_size=1,
                file_format=FileFormat.CBZ,
                file_modified_at=datetime.now(UTC),
                issue_id=issue.id,
                library_root_id=root.id,
            )
            session.add(library)
            await session.flush()
            for number in range(26):
                session.add(
                    ImportedFile(
                        import_job_id=job.id,
                        import_series_id=series.id,
                        file_path=f"/imports/metadata-{number}.cbz",
                        file_name=f"Metadata {number}.cbz",
                        file_format="cbz",
                        status=ImportedFileStatus.IMPORTED,
                        matched_issue_id=issue.id,
                        library_file_id=library.id,
                        diagnostics={
                            "comicinfo_enrichment": {"status": "failed", "error": "readonly_source"}
                        },
                    )
                )
            return job.id, library.id

    # Real retry persistence is exercised; don't launch provider work from this UI-only fixture.
    calls = []
    monkeypatch.setattr(
        ImportService,
        "schedule_comicinfo_enrichment",
        lambda self, factory, *, job_id: calls.append(job_id),
    )
    job_id, library_id = _run_async_blocking(seed())
    yield job_id, calls

    async def cleanup():
        async with get_session_factory().begin() as session:
            await session.execute(delete(ImportJob).where(ImportJob.id == job_id))
            await session.execute(delete(LibraryFile).where(LibraryFile.id == library_id))

    _run_async_blocking(cleanup())


def test_metadata_follow_up_paginates_and_retries_without_navigation(
    authed_page,
    seeded_server,
    metadata_follow_up_job,
):
    job_id, calls = metadata_follow_up_job
    page = authed_page
    page.goto(f"{seeded_server}/import?tab=follow-up&job_id={job_id}")
    card = page.get_by_test_id("import-follow-up-archive-metadata")
    expect(card).to_be_visible()
    page.evaluate("window.metadataFollowUpMarker = 'same document'")
    card.get_by_role("button", name="Review files", exact=True).click()
    panel = page.locator("#import-metadata-write-files")
    expect(panel.locator("article")).to_have_count(25)
    panel.locator("#metadata-writes-pagination-next").click()
    expect(panel.locator("article")).to_have_count(1)
    wait_for_htmx(page)
    expect(panel).not_to_have_class(re.compile("htmx-settling"))
    panel.get_by_role("button", name="Retry metadata", exact=True).focus()
    expect(panel.get_by_role("button", name="Retry metadata", exact=True)).to_be_focused()
    with page.expect_response(lambda response: "/retry-metadata" in response.url) as response:
        page.keyboard.press("Enter")
    assert response.value.status == 200, response.value.text()
    expect(panel.get_by_role("status")).to_contain_text("Metadata retry queued")
    expect(panel.locator("article")).to_have_count(25)
    expect(card.locator("#import-metadata-write-heading")).to_have_text(
        "25 files need a metadata retry"
    )
    assert calls == [job_id]
    assert page.evaluate("window.metadataFollowUpMarker") == "same document"
    expect(page.get_by_test_id("import-header")).to_have_count(1)
    expect(page.locator("#import-orphaned-results")).to_have_count(1)
    page.set_viewport_size({"width": 360, "height": 800})
    expect(panel.get_by_role("button", name="Retry metadata", exact=True).first).to_be_visible()


@pytest.mark.accessibility
def test_metadata_follow_up_has_no_wcag_aa_violations(
    authed_page, seeded_server, metadata_follow_up_job
):
    job_id, _ = metadata_follow_up_job
    page = authed_page
    page.goto(f"{seeded_server}/import?tab=follow-up&job_id={job_id}")
    card = page.get_by_test_id("import-follow-up-archive-metadata")
    card.get_by_role("button", name="Review files", exact=True).click()
    expect(card.locator("article")).to_have_count(25)
    assert_no_axe_violations(
        page,
        name="import archive metadata follow-up",
        include=["[data-testid='import-follow-up-archive-metadata']"],
    )
    page.screenshot(path="test-results/import-metadata-follow-up.png")
