"""Actual ComicVine wire/SQLite adapters, without external provider traffic."""

import asyncio
import threading
from unittest.mock import AsyncMock

import httpx
import pytest

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.providers.metadata.comicvine import ComicVineProvider
from pullbox.providers.metadata.sources import (
    ComicVineApiSource,
    ComicVineLocalSource,
    comicvine_sources,
)
from pullbox.schemas.metadata_sources import SeriesDiscoveryQuery, SourceCapability, SourceStatus
from pullbox.services.catalog.reader import CatalogReader
from pullbox.services.metadata_discovery import (
    MetadataSourceError,
    MetadataSourceRegistry,
    SourceRegistration,
)
from pullbox.services.metadata_sources import SourceRuntime, default_policy
from tests.unit.test_catalog_reader import installed_reader


async def api_adapter(handler):
    provider = ComicVineProvider("synthetic-source-contract", rate_limit=999999, burst_limit=10)
    await provider.close()
    provider._client = httpx.AsyncClient(
        base_url="https://comicvine.gamespot.com/api",
        timeout=1,
        transport=httpx.MockTransport(handler),
    )
    return ComicVineApiSource(provider)


async def test_api_adapter_uses_real_page_and_normalizes_identity_not_urls():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "status_code": 1,
                "number_of_total_results": 100,
                "results": [
                    {
                        "id": "0042",
                        "name": "Batman",
                        "start_year": "2024",
                        "publisher": {"name": "DC"},
                        "image": {"medium_url": "javascript:bad"},
                        "site_detail_url": "https://unrelated.example/4050-999/",
                    }
                ],
            },
        )

    adapter = await api_adapter(handle)
    try:
        result = await adapter.search(SeriesDiscoveryQuery(query="Batman", limit_per_source=10), 20)
        assert result.results[0].external_id == "42"
        assert result.results[0].resource_url == "https://comicvine.gamespot.com/volume/4050-42/"
        assert result.results[0].image_url is None
        assert result.total == 100 and result.next_offset == 30
        assert calls[0].url.params["page"] == "3"
        assert calls[0].url.params["limit"] == "10"
    finally:
        await adapter.close()
    assert adapter.provider._client.is_closed


async def test_full_api_search_keeps_volumes_filter_and_offset_semantics():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "status_code": 1,
                "number_of_total_results": 31,
                "results": [{"id": 42, "name": "The Punisher", "start_year": "2004"}],
            },
        )

    adapter = await api_adapter(handle)
    try:
        result = await adapter.search(
            SeriesDiscoveryQuery(
                query="The Punisher", year=2004, limit_per_source=10, search_mode="full"
            ),
            20,
        )
        assert calls[0].url.path == "/api/volumes/"
        assert calls[0].url.params["filter"] == "name:The,name:Punisher"
        assert calls[0].url.params["sort"] == "date_last_updated:desc"
        assert calls[0].url.params["limit"] == "10"
        assert calls[0].url.params["offset"] == "20"
        assert "page" not in calls[0].url.params
        assert "query" not in calls[0].url.params
        assert result.total == 31 and result.next_offset == 30
        assert result.results[0].external_id == "42"
    finally:
        await adapter.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"status_code": 1, "results": []},
        {"status_code": 1, "results": [], "number_of_total_results": True},
        {"status_code": 1, "results": [{"id": 42, "name": "Good"}], "number_of_total_results": 0},
    ],
)
async def test_full_search_rejects_invalid_success_envelopes(payload):
    adapter = await api_adapter(lambda request: httpx.Response(200, json=payload))
    try:
        with pytest.raises(MetadataSourceError) as raised:
            await adapter.search(SeriesDiscoveryQuery(query="test", search_mode="full"), 0)
        assert raised.value.status is SourceStatus.INCOMPATIBLE_RESPONSE
    finally:
        await adapter.close()


async def test_full_search_preserves_good_siblings_and_reports_rejected_identities():
    adapter = await api_adapter(
        lambda request: httpx.Response(
            200,
            json={
                "status_code": 1,
                "number_of_total_results": 2,
                "results": [{"id": 0, "name": "Bad"}, {"id": 42, "name": "Good"}],
            },
        )
    )
    try:
        result = await adapter.search(SeriesDiscoveryQuery(query="test", search_mode="full"), 0)
        assert [item.external_id for item in result.results] == ["42"]
        assert result.rejected_results == 1
    finally:
        await adapter.close()


async def test_full_collection_walks_actual_volume_pages_and_closes_each_transport():
    calls = []

    def handle(request):
        offset = int(request.url.params["offset"])
        calls.append((request.url.path, offset))
        return httpx.Response(
            200,
            json={
                "status_code": 1,
                "number_of_total_results": 103,
                "results": [
                    {"id": identifier, "name": f"Fixture {identifier}"}
                    for identifier in range(offset + 1, min(offset + 101, 104))
                ],
            },
        )

    adapters = [await api_adapter(handle)]
    available = iter(adapters)
    source = Source.COMICVINE_API
    registry = MetadataSourceRegistry(
        [SourceRuntime(default_policy(source))],
        factories={
            source: SourceRegistration(
                frozenset([SourceCapability.SERIES_SEARCH]), lambda _: next(available)
            )
        },
    )
    try:
        result = await registry.discover_all(
            SeriesDiscoveryQuery(query="Fixture", limit_per_source=100)
        )
        assert [int(item.external_id) for item in result.results] == list(range(1, 104))
        assert calls == [("/api/volumes/", 0), ("/api/volumes/", 100)]
        assert result.sources[0].total == 103
        assert result.sources[0].status is SourceStatus.OK
        assert result.sources[0].next_offset is None and not result.sources[0].truncated
        assert all(adapter.provider._client.is_closed for adapter in adapters)
    finally:
        for adapter in adapters:
            await adapter.close()


@pytest.mark.parametrize(
    "code,status", [(100, SourceStatus.AUTHENTICATION_FAILED), (107, SourceStatus.RATE_LIMITED)]
)
async def test_api_error_is_typed_not_empty(code, status):
    adapter = await api_adapter(lambda request: httpx.Response(200, json={"status_code": code}))
    # Each case needs independent cooldown state, as the real provider intentionally shares it.
    adapter.provider._api_key = f"synthetic-error-{code}"
    try:
        with pytest.raises(MetadataSourceError) as raised:
            await adapter.search(SeriesDiscoveryQuery(query="Batman"), 0)
        assert raised.value.status == status
        if code == 107:
            assert raised.value.retry_after_seconds > 0
    finally:
        await adapter.close()


async def test_api_transport_timeout_has_a_typed_outcome():
    def handle(request):
        raise httpx.ReadTimeout("synthetic-token", request=request)

    adapter = await api_adapter(handle)
    try:
        with pytest.raises(MetadataSourceError) as raised:
            await adapter.search(SeriesDiscoveryQuery(query="Batman"), 0)
        assert raised.value.status == SourceStatus.TIMEOUT
        assert "synthetic-token" not in str(raised.value)
    finally:
        await adapter.close()


async def test_malformed_response_is_not_reported_as_empty():
    adapter = await api_adapter(
        lambda request: httpx.Response(200, json={"status_code": 1, "results": "not rows"})
    )
    try:
        with pytest.raises(MetadataSourceError) as raised:
            await adapter.search(SeriesDiscoveryQuery(query="Batman"), 0)
        assert raised.value.status == SourceStatus.INCOMPATIBLE_RESPONSE
    finally:
        await adapter.close()


@pytest.mark.parametrize(
    "payload",
    [
        {"status_code": 1},
        {"status_code": 1, "results": []},
        {"status_code": 1, "results": [], "number_of_total_results": "invalid"},
        {"status_code": 1, "results": [], "number_of_total_results": True},
    ],
)
async def test_incomplete_success_envelope_is_not_an_empty_search(payload):
    adapter = await api_adapter(lambda request: httpx.Response(200, json=payload))
    try:
        with pytest.raises(MetadataSourceError) as raised:
            await adapter.search(SeriesDiscoveryQuery(query="Batman"), 0)
        assert raised.value.status == SourceStatus.INCOMPATIBLE_RESPONSE
    finally:
        await adapter.close()


async def test_bad_identity_does_not_drop_valid_sibling():
    adapter = await api_adapter(
        lambda request: httpx.Response(
            200,
            json={
                "status_code": 1,
                "number_of_total_results": 2,
                "results": [{"id": 0, "name": "Bad"}, {"id": 42, "name": "Good"}],
            },
        )
    )
    try:
        page = await adapter.search(SeriesDiscoveryQuery(query="test"), 0)
        assert [item.external_id for item in page.results] == ["42"]
        assert page.rejected_results == 1 and page.next_offset is None
    finally:
        await adapter.close()


async def test_local_adapter_reads_real_catalog_without_fake_total(tmp_path):
    adapter = ComicVineLocalSource(installed_reader(tmp_path))
    page = await adapter.search(SeriesDiscoveryQuery(query="Dark Knight", limit_per_source=1), 0)
    assert len(page.results) == 1 and page.results[0].external_id == "10"
    assert page.total is None and page.next_offset is None
    await adapter.check()


async def test_local_page_uses_lookahead_and_marks_offset_limit(tmp_path, monkeypatch):
    reader = installed_reader(tmp_path)
    found = await reader.search("Batman")
    mock = AsyncMock(return_value=found * 3)
    monkeypatch.setattr(reader, "search", mock)
    adapter = ComicVineLocalSource(reader)
    page = await adapter.search(SeriesDiscoveryQuery(query="Batman", limit_per_source=2), 10000)
    assert len(page.results) == 2 and page.truncated and page.next_offset is None
    mock.assert_awaited_once_with("Batman", None, 3, 10000)


@pytest.mark.parametrize("corrupt", [False, True])
async def test_local_health_does_not_treat_pointer_existence_as_healthy(tmp_path, corrupt):
    reader = installed_reader(tmp_path) if corrupt else CatalogReader(tmp_path / "missing")
    if corrupt:
        (reader.root / "bases/20260913T050000Z.db").write_bytes(b"not sqlite")
    with pytest.raises(MetadataSourceError) as raised:
        await ComicVineLocalSource(reader).check()
    assert raised.value.status == (
        SourceStatus.UNAVAILABLE if corrupt else SourceStatus.UNCONFIGURED
    )


def test_registry_declares_only_working_capabilities_and_requires_api_credentials():
    sources = comicvine_sources()
    assert set(sources) == {Source.COMICVINE_LOCAL, Source.COMICVINE_API}
    assert SourceCapability.SERIES_SEARCH in sources[Source.COMICVINE_API].capabilities
    assert SourceCapability.STORY_ARC_SEARCH not in sources[Source.COMICVINE_API].capabilities
    with pytest.raises(MetadataSourceError) as raised:
        sources[Source.COMICVINE_API].factory(SourceRuntime(default_policy(Source.COMICVINE_API)))
    assert raised.value.status == SourceStatus.UNCONFIGURED


async def test_registry_timeout_does_not_wait_for_owned_catalog_disk_read(tmp_path, monkeypatch):
    reader = installed_reader(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = reader._query

    def blocked(*args):
        entered.set()
        release.wait(3)
        return original(*args)

    monkeypatch.setattr(reader, "_query", blocked)
    registry = MetadataSourceRegistry(
        [SourceRuntime(default_policy(Source.COMICVINE_LOCAL))],
        factories={
            Source.COMICVINE_LOCAL: SourceRegistration(
                frozenset([SourceCapability.SERIES_SEARCH]), lambda _: ComicVineLocalSource(reader)
            )
        },
        per_source_timeout=0.25,
        total_timeout=2,
    )
    task = asyncio.create_task(registry.discover(SeriesDiscoveryQuery(query="Batman")))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        done, _ = await asyncio.wait({task}, timeout=0.75)
        assert task in done, "A catalog read must not hold the request past its deadline"
        assert task.result().sources[0].status == SourceStatus.TIMEOUT
    finally:
        release.set()
        await task
        await asyncio.gather(
            *(item for item in asyncio.all_tasks() if item.get_name() == "metadata-catalog-read"),
            return_exceptions=True,
        )


async def test_timed_out_catalog_reads_are_retained_and_globally_bounded(tmp_path, monkeypatch):
    reader = installed_reader(tmp_path)
    release = threading.Event()
    original = reader._query
    calls = []

    def blocked(*args):
        calls.append(args)
        release.wait(3)
        return original(*args)

    monkeypatch.setattr(reader, "_query", blocked)
    registry = MetadataSourceRegistry(
        [SourceRuntime(default_policy(Source.COMICVINE_LOCAL))],
        factories={
            Source.COMICVINE_LOCAL: SourceRegistration(
                frozenset([SourceCapability.SERIES_SEARCH]), lambda _: ComicVineLocalSource(reader)
            )
        },
        per_source_timeout=0.25,
        total_timeout=2,
    )
    tasks = [
        asyncio.create_task(registry.discover(SeriesDiscoveryQuery(query="Batman")))
        for _ in range(2)
    ]
    try:
        done, _ = await asyncio.wait(tasks, timeout=0.75)
        assert len(done) == 2
        assert all(task.result().sources[0].status == SourceStatus.TIMEOUT for task in tasks)
        again = await registry.discover(SeriesDiscoveryQuery(query="Batman"))
        assert again.sources[0].status == SourceStatus.UNAVAILABLE
        assert again.sources[0].retry_after_seconds == 1
        assert len(calls) == 2
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(
            *(item for item in asyncio.all_tasks() if item.get_name() == "metadata-catalog-read"),
            return_exceptions=True,
        )
    assert (await registry.discover(SeriesDiscoveryQuery(query="Batman"))).sources[
        0
    ].status == SourceStatus.OK
