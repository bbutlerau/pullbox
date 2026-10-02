"""Executable-source orchestration is bounded, honest, and identity-specific."""

import asyncio

import pytest
from pydantic import ValidationError

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import (
    ProviderSeriesRead,
    SeriesDiscoveryQuery,
    SourceCapability,
    SourceStatus,
)
from pullbox.services.metadata_discovery import (
    MetadataSourceError,
    MetadataSourceRegistry,
    SourcePage,
    SourceRegistration,
)
from pullbox.services.metadata_sources import SourceRuntime, default_policy


def runtime(source, **updates):
    return SourceRuntime(default_policy(source).model_copy(update={"enabled": True, **updates}))


def row(source, external_id="42", title="Same title"):
    return ProviderSeriesRead(
        source=source,
        identity_namespace=source.identity_namespace,
        external_id=external_id,
        title=title,
    )


class Adapter:
    def __init__(self, source, *, rows=None, error=None, wait=None):
        self.source, self.rows, self.error, self.wait = source, rows, error, wait
        self.calls = []
        self.closed = 0
        self.started = asyncio.Event()

    async def search(self, query, offset):
        self.calls.append(offset)
        self.started.set()
        if self.wait:
            await self.wait.wait()
        if self.error:
            raise self.error
        return SourcePage(
            self.rows if self.rows is not None else [row(self.source)],
            50,
            offset + query.limit_per_source,
        )

    async def check(self):
        if self.error:
            raise self.error

    async def close(self):
        self.closed += 1


def registration(adapter, calls=None, capabilities=None):
    def build(_):
        if calls is not None:
            calls.append(adapter.source)
        return adapter

    return SourceRegistration(
        frozenset(capabilities if capabilities is not None else [SourceCapability.SERIES_SEARCH]),
        build,
    )


async def test_interactive_fanout_groups_exact_identity_not_title_and_preserves_cursors():
    local, api, metron = Source.COMICVINE_LOCAL, Source.COMICVINE_API, Source.METRON_API
    adapters = {source: Adapter(source) for source in (local, api, metron)}
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api), runtime(metron)],
        factories={source: registration(adapter) for source, adapter in adapters.items()},
    )
    result = await registry.discover(SeriesDiscoveryQuery(query="same", offsets={api: 20}))
    assert [(item.source, item.external_id) for item in result.results] == [
        (local, "42"),
        (metron, "42"),
    ]
    assert result.results[0].also_from == [api]
    assert [item.next_offset for item in result.sources] == [20, 40, 20]
    assert all(adapter.closed == 1 for adapter in adapters.values())
    assert adapters[api].calls == [20]


async def test_automatic_cascade_is_lazy_and_uses_domain_priority():
    api, local = Source.COMICVINE_API, Source.COMICVINE_LOCAL
    calls = []
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api, domain_priorities={"core": 1})],
        factories={source: registration(Adapter(source), calls) for source in (local, api)},
    )
    result = await registry.discover(
        SeriesDiscoveryQuery(query="same", mode="automatic"),
        satisfied_by=lambda page: any(item.external_id == "42" for item in page.results),
    )
    assert calls == [api]
    assert result.results[0].source == api
    assert [(item.source, item.status.value) for item in result.sources] == [
        (api, "ok"),
        (local, "not_queried"),
    ]


@pytest.mark.parametrize(
    "error,status",
    [
        (MetadataSourceError(SourceStatus.RATE_LIMITED, 90), SourceStatus.RATE_LIMITED),
        (RuntimeError("synthetic-private-token"), SourceStatus.UNAVAILABLE),
    ],
)
async def test_partial_failure_is_not_empty_or_exposed_and_cascade_continues(error, status):
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    first, second = Adapter(local, error=error), Adapter(api)
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api)],
        factories={local: registration(first), api: registration(second)},
    )
    result = await registry.discover(SeriesDiscoveryQuery(query="same", mode="automatic"))
    assert result.sources[0].status == status
    assert result.sources[0].retry_after_seconds == (
        90 if status == SourceStatus.RATE_LIMITED else None
    )
    assert result.results[0].source == api
    assert "synthetic-private-token" not in result.model_dump_json()
    assert first.closed == second.closed == 1


async def test_disabled_flagged_and_incapable_sources_never_construct():
    calls = []
    registry = MetadataSourceRegistry(
        [
            runtime(Source.COMICVINE_API, enabled=False),
            runtime(Source.GCD_API_V2),
            runtime(Source.METRON_API),
            runtime(Source.GCD_LOCAL),
        ],
        factories={
            source: registration(Adapter(source), calls, capabilities=[]) for source in Source
        },
    )
    result = await registry.discover(SeriesDiscoveryQuery(query="same"))
    assert calls == []
    assert {item.source: item.status for item in result.sources} == {
        Source.COMICVINE_API: SourceStatus.DISABLED,
        Source.GCD_API_V2: SourceStatus.FEATURE_DISABLED,
        Source.METRON_API: SourceStatus.UNSUPPORTED,
        Source.GCD_LOCAL: SourceStatus.UNSUPPORTED,
    }


async def test_deadline_retains_fast_results_and_closes_slow_adapter():
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    slow = Adapter(api, wait=asyncio.Event())
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api)],
        factories={local: registration(Adapter(local)), api: registration(slow)},
        per_source_timeout=0.03,
        total_timeout=0.05,
    )
    result = await asyncio.wait_for(registry.discover(SeriesDiscoveryQuery(query="same")), 1)
    assert len(result.results) == 1
    assert result.sources[1].status == SourceStatus.TIMEOUT
    assert slow.closed == 1


async def test_cancellation_drains_children_and_closes_adapters():
    source = Source.COMICVINE_API
    adapter = Adapter(source, wait=asyncio.Event())
    registry = MetadataSourceRegistry([runtime(source)], factories={source: registration(adapter)})
    task = asyncio.create_task(registry.discover(SeriesDiscoveryQuery(query="same")))
    await asyncio.sleep(0.01)
    assert adapter.started.is_set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.closed == 1


async def test_factory_failure_does_not_hide_other_sources():
    def fail(_):
        raise ValueError("private settings")

    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api)],
        factories={
            local: SourceRegistration(frozenset([SourceCapability.SERIES_SEARCH]), fail),
            api: registration(Adapter(api)),
        },
    )
    result = await registry.discover(SeriesDiscoveryQuery(query="same"))
    assert result.sources[0].status == SourceStatus.UNAVAILABLE
    assert len(result.results) == 1


async def test_automatic_search_does_not_assume_candidates_satisfy_identity():
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    calls = []
    registry = MetadataSourceRegistry(
        [runtime(local), runtime(api)],
        factories={source: registration(Adapter(source), calls) for source in (local, api)},
    )
    result = await registry.discover(
        SeriesDiscoveryQuery(query="unrelated title", mode="automatic")
    )
    assert calls == [local, api]
    assert all(item.status == SourceStatus.OK for item in result.sources)


async def test_interactive_fanout_obeys_concurrency_and_total_deadline():
    calls = []
    source_list = [Source.COMICVINE_LOCAL, Source.COMICVINE_API, Source.METRON_API]
    adapters = {source: Adapter(source, wait=asyncio.Event()) for source in source_list}
    registry = MetadataSourceRegistry(
        [runtime(source) for source in source_list],
        factories={source: registration(adapter, calls) for source, adapter in adapters.items()},
        per_source_timeout=0.2,
        total_timeout=0.03,
        concurrency=1,
    )
    result = await asyncio.wait_for(registry.discover(SeriesDiscoveryQuery(query="same")), 1)
    assert calls == [Source.COMICVINE_LOCAL]
    assert all(item.status == SourceStatus.TIMEOUT for item in result.sources)
    assert adapters[Source.COMICVINE_LOCAL].closed == 1


@pytest.mark.parametrize(
    "data",
    [
        {"query": "   "},
        {"offsets": {"comicvine_api": -1}},
        {"offsets": {"comicvine_api": 10001}},
        {"offsets": {"comicvine_api": 1}},
        {"sources": ["comicvine_api", "comicvine_api"]},
        {"sources": ["comicvine_local"], "offsets": {"comicvine_api": 20}},
    ],
)
def test_discovery_rejects_ambiguous_or_unbounded_requests(data):
    with pytest.raises(ValidationError):
        SeriesDiscoveryQuery(**{"query": "test", **data})
