"""Preview budgets preserve partial results without inventing complete catalogs."""

import asyncio

import pytest

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import MetadataFetch, MetadataPage, SourceStatus
from pullbox.services.metadata_series_artwork import (
    representative_series_cover,
    with_representative_cover,
)
from pullbox.services.metadata_series_preview import preview_source_series
from tests.unit.test_metadata_discovery import row
from tests.unit.test_metadata_source_reads import ReadAdapter, issue_row, registry


def test_representative_cover_is_source_bound_and_preserves_existing_artwork():
    cover = "https://static.metron.cloud/media/issue/first.jpg"
    issue = issue_row(image_url=cover)
    profile = row(Source.METRON_API)
    assert representative_series_cover(Source.METRON_API, "42", []) is None
    assert representative_series_cover(Source.COMICVINE_API, "42", [issue]) is None
    assert representative_series_cover(Source.METRON_API, "99", [issue]) is None
    assert representative_series_cover(Source.METRON_API, "42", [issue] * 101) is None
    assert with_representative_cover(profile, [issue]).image_url == cover
    assert profile.image_url is None
    profile.image_url = "https://static.metron.cloud/media/series/existing.jpg"
    assert with_representative_cover(profile, [issue]) is profile
    assert profile.image_url.endswith("existing.jpg")


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://static.metron.cloud/media/issue/first.jpg", True),
        ("https://127.0.0.1/private", False),
        ("javascript:alert(1)", False),
        (None, False),
    ],
)
async def test_preview_reuses_issue_cover_without_extra_requests(monkeypatch, url, expected):
    adapter = ReadAdapter()

    async def issues(external_id, *, page=1, validator=None):
        adapter.calls.append(("issues", external_id, page))
        return MetadataFetch(
            status=SourceStatus.OK,
            data=MetadataPage(results=[issue_row(image_url=url)], total=1),
        )

    monkeypatch.setattr(adapter, "issues", issues)
    preview = await preview_source_series(registry(adapter), adapter.source, "42")
    assert preview.series.data.image_url == (url if expected else None)
    assert adapter.calls == [("series", "42", None), ("issues", "42", 1)]


async def test_missing_series_does_not_request_issues():
    adapter = ReadAdapter(result=MetadataFetch(status=SourceStatus.NOT_FOUND))
    preview = await preview_source_series(registry(adapter), adapter.source, "42")
    assert preview.series.status is SourceStatus.NOT_FOUND
    assert preview.issues.status is SourceStatus.NOT_QUERIED
    assert adapter.calls == [("series", "42", None)]


async def test_preview_has_one_total_deadline_and_preserves_completed_profile(monkeypatch):
    adapter = ReadAdapter()
    entered = asyncio.Event()
    instance = registry(adapter, total_timeout=0.02)

    async def issues(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(instance, "issues", issues)
    preview = await asyncio.wait_for(preview_source_series(instance, adapter.source, "42"), 1)
    assert entered.is_set()
    assert preview.series.status is SourceStatus.OK
    assert preview.issues.status is SourceStatus.TIMEOUT


async def test_preview_cancellation_does_not_become_success_or_failure_result():
    adapter = ReadAdapter(wait=asyncio.Event())
    task = asyncio.create_task(preview_source_series(registry(adapter), adapter.source, "42"))
    await adapter.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.closed == 1
