"""Preview budgets preserve partial results without inventing complete catalogs."""

import asyncio

import pytest

from pullbox.schemas.metadata_sources import MetadataFetch, SourceStatus
from pullbox.services.metadata_series_preview import preview_source_series
from tests.unit.test_metadata_source_reads import ReadAdapter, registry


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
