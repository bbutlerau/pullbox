"""Multi-source candidate collection preserves pages and partial outcomes."""

import asyncio

import pytest

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


def row(source, identifier, title="Same title"):
    return ProviderSeriesRead(
        source=source,
        identity_namespace=source.identity_namespace,
        external_id=str(identifier),
        title=title,
    )


class Pages:
    def __init__(self, source, pages, *, pause_at=None, gate=None):
        self.source = source
        self.pages = pages
        self.pause_at = pause_at
        self.gate = gate or asyncio.Event()
        self.started = asyncio.Event()
        self.calls = []
        self.search_modes = []
        self.closed = 0

    async def search(self, query, offset):
        self.calls.append(offset)
        self.search_modes.append(query.search_mode)
        if offset == self.pause_at:
            self.started.set()
            await self.gate.wait()
        response = self.pages[offset]
        if isinstance(response, Exception):
            raise response
        return response

    async def check(self):
        pass

    async def close(self):
        self.closed += 1


def registry_for(*adapters, **kwargs):
    runtime = [SourceRuntime(default_policy(adapter.source)) for adapter in adapters]
    for item in runtime:
        item.policy.enabled = True
    return MetadataSourceRegistry(
        runtime,
        factories={
            adapter.source: SourceRegistration(
                frozenset([SourceCapability.SERIES_SEARCH]),
                lambda _, adapter=adapter: adapter,
            )
            for adapter in adapters
        },
        **kwargs,
    )


def query(**updates):
    return SeriesDiscoveryQuery(query="Same", limit_per_source=2, **updates)


async def test_collects_each_sources_own_pages_before_exact_identity_grouping():
    local, api, metron = Source.COMICVINE_LOCAL, Source.COMICVINE_API, Source.METRON_API
    first = Pages(
        local,
        {
            0: SourcePage([row(local, 1), row(local, 2)], next_offset=2),
            2: SourcePage([row(local, 3)]),
        },
    )
    second = Pages(
        api,
        {
            0: SourcePage([row(api, 3), row(api, 4)], total=3, next_offset=2),
            2: SourcePage([row(api, 5)], total=3),
        },
    )
    third = Pages(metron, {0: SourcePage([row(metron, 1)], total=1)})
    result = await registry_for(first, second, third).discover_all(query())

    assert [(item.source, item.external_id) for item in result.results] == [
        (local, "1"),
        (local, "2"),
        (local, "3"),
        (api, "4"),
        (api, "5"),
        (metron, "1"),
    ]
    assert result.results[2].also_from == [api]
    assert first.calls == second.calls == [0, 2]
    assert third.calls == [0]
    assert [outcome.total for outcome in result.sources] == [None, 3, 1]
    assert all(outcome.next_offset is None and not outcome.truncated for outcome in result.sources)
    assert (first.closed, second.closed, third.closed) == (1, 1, 1)
    assert first.search_modes == second.search_modes == ["full", "full"]
    assert third.search_modes == ["full"]


async def test_later_empty_page_does_not_erase_earlier_candidates_or_claim_empty_search():
    source = Source.COMICVINE_LOCAL
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1)], next_offset=2),
            2: SourcePage([]),
        },
    )
    result = await registry_for(adapter).discover_all(query())
    assert adapter.calls == [0, 2]
    assert [item.external_id for item in result.results] == ["1"]
    assert result.sources[0].status is SourceStatus.OK
    assert result.sources[0].next_offset is None


@pytest.mark.parametrize("status", [SourceStatus.RATE_LIMITED, SourceStatus.AUTHENTICATION_FAILED])
async def test_later_failure_retains_good_candidates_and_retry_position(status):
    source = Source.COMICVINE_API
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1), row(source, 2)], total=7, next_offset=2),
            2: MetadataSourceError(status, retry_after_seconds=90),
        },
    )
    result = await registry_for(adapter).discover_all(query())
    assert [item.external_id for item in result.results] == ["1", "2"]
    outcome = result.sources[0]
    assert outcome.status is status
    assert outcome.retry_after_seconds == 90
    assert outcome.total == 7
    assert outcome.next_offset == 2
    assert outcome.truncated
    assert adapter.calls == [0, 2]
    assert adapter.closed == 1


async def test_rejected_siblings_on_early_page_remain_visible_in_final_outcome():
    source = Source.COMICVINE_API
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1)], total=5, next_offset=2, rejected_results=1),
            2: SourcePage([row(source, 3)], total=5, next_offset=4, rejected_results=1),
            4: SourcePage([row(source, 5)], total=5),
        },
    )
    result = await registry_for(adapter).discover_all(query())
    assert [item.external_id for item in result.results] == ["1", "3", "5"]
    assert result.sources[0].status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.sources[0].rejected_results == 2
    assert result.sources[0].next_offset is None


async def test_result_cap_counts_source_positions_not_unique_or_accepted_candidates():
    source = Source.COMICVINE_API
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1), row(source, 2)], total=1000, next_offset=2),
            2: SourcePage([row(source, 1)], total=1000, next_offset=4, rejected_results=1),
        },
    )
    result = await registry_for(adapter).discover_all(query(), max_results_per_source=4)
    assert [item.external_id for item in result.results] == ["1", "2"]
    assert adapter.calls == [0, 2]
    assert result.sources[0].total == 1000
    assert result.sources[0].next_offset == 4
    assert result.sources[0].truncated


@pytest.mark.parametrize("cursor", [True, -2, 0, 1, 4, 10002])
async def test_invalid_or_skipping_cursor_stops_without_discarding_valid_rows(cursor):
    source = Source.COMICVINE_API
    adapter = Pages(source, {0: SourcePage([row(source, 1)], next_offset=cursor)})
    result = await registry_for(adapter).discover_all(query())
    assert [item.external_id for item in result.results] == ["1"]
    assert result.sources[0].status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.sources[0].next_offset is None
    assert result.sources[0].truncated
    assert adapter.calls == [0]


async def test_timeout_keeps_completed_pages_and_other_sources():
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    fast = Pages(local, {0: SourcePage([row(local, 7)])})
    slow = Pages(
        api,
        {
            0: SourcePage([row(api, 1), row(api, 2)], next_offset=2),
            2: SourcePage([row(api, 3)]),
        },
        pause_at=2,
    )
    result = await asyncio.wait_for(
        registry_for(fast, slow, total_timeout=0.04, per_source_timeout=0.03).discover_all(query()),
        1,
    )
    assert [(item.source, item.external_id) for item in result.results] == [
        (local, "7"),
        (api, "1"),
        (api, "2"),
    ]
    assert result.sources[1].status is SourceStatus.TIMEOUT
    assert result.sources[1].next_offset == 2
    assert result.sources[1].truncated
    assert slow.closed == 1


async def test_total_deadline_is_shared_by_all_pages_not_restarted_per_page():
    source = Source.COMICVINE_LOCAL
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1), row(source, 2)], next_offset=2),
            2: SourcePage([row(source, 3)]),
        },
        pause_at=0,
    )
    registry = registry_for(adapter, total_timeout=0.2, per_source_timeout=1)
    original_search = adapter.search

    async def read_page(query, offset):
        result = await original_search(query, offset)
        await asyncio.sleep(0.12)
        return result

    adapter.search = read_page
    adapter.gate.set()
    result = await registry.discover_all(query())
    assert adapter.calls == [0, 2]
    assert [item.external_id for item in result.results] == ["1", "2"]
    assert result.sources[0].status is SourceStatus.TIMEOUT
    assert result.sources[0].next_offset == 2


async def test_cancel_during_second_page_drains_source_and_returns_no_partial_success():
    source = Source.COMICVINE_API
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1), row(source, 2)], next_offset=2),
            2: SourcePage([row(source, 3)]),
        },
        pause_at=2,
    )
    task = asyncio.create_task(registry_for(adapter).discover_all(query()))
    try:
        await asyncio.wait_for(adapter.started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert adapter.calls == [0, 2]
        assert adapter.closed == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_explicit_selection_does_not_query_other_sources():
    api, metron = Source.COMICVINE_API, Source.METRON_API
    first = Pages(api, {0: SourcePage([row(api, 1)])})
    second = Pages(
        metron,
        {
            0: SourcePage([row(metron, 1)], next_offset=2),
            2: SourcePage([row(metron, 2)]),
        },
    )
    result = await registry_for(first, second).discover_all(query(sources=[metron]))
    assert first.calls == []
    assert second.calls == [0, 2]
    assert len(result.sources) == 1


async def test_domain_priority_owns_grouped_row_even_when_default_priority_differs():
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    first = Pages(local, {0: SourcePage([row(local, 1, "Local title")])})
    second = Pages(
        api,
        {
            0: SourcePage([row(api, 2)], next_offset=2),
            2: SourcePage([row(api, 1, "API title")]),
        },
    )
    registry = registry_for(first, second)
    registry.runtime[api].policy.domain_priorities = {"core": 1}
    result = await registry.discover_all(query())
    assert [item.external_id for item in result.results] == ["2", "1"]
    assert result.results[1].source is api
    assert result.results[1].title == "API title"
    assert result.results[1].also_from == [local]
    assert first.pages[0].results[0].also_from == []
    assert second.pages[2].results[0].also_from == []


async def test_disabled_and_flagged_sources_never_construct_or_query_during_collection():
    sources = [Source.COMICVINE_API, Source.GCD_API_V2, Source.METRON_API]
    adapters = [Pages(source, {0: SourcePage([])}) for source in sources]
    registry = registry_for(*adapters)
    registry.runtime[Source.COMICVINE_API].policy.enabled = False
    registry.factories[Source.METRON_API] = SourceRegistration(
        frozenset(), lambda _: pytest.fail("Incapable sources must not construct")
    )
    result = await registry.discover_all(query())
    assert all(adapter.calls == [] and adapter.closed == 0 for adapter in adapters)
    assert {outcome.source: outcome.status for outcome in result.sources} == {
        Source.COMICVINE_API: SourceStatus.DISABLED,
        Source.GCD_API_V2: SourceStatus.FEATURE_DISABLED,
        Source.METRON_API: SourceStatus.UNSUPPORTED,
    }
    assert result.results == []


async def test_collection_shares_concurrency_limit_across_sources_and_subsequent_pages():
    sources = [Source.COMICVINE_LOCAL, Source.COMICVINE_API, Source.METRON_API]
    adapters = [
        Pages(
            source,
            {
                0: SourcePage([row(source, 1)], next_offset=2),
                2: SourcePage([row(source, 2)]),
            },
        )
        for source in sources
    ]
    active = maximum = 0

    def counted_read(original):
        async def read(query, offset):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                await asyncio.sleep(0.001)
                return await original(query, offset)
            finally:
                active -= 1

        return read

    for adapter in adapters:
        adapter.search = counted_read(adapter.search)
    await registry_for(*adapters, concurrency=2).discover_all(query())
    assert maximum == 2
    assert active == 0
    assert all(adapter.calls == [0, 2] and adapter.closed == 1 for adapter in adapters)


async def test_provider_truncation_remains_incomplete_even_without_a_usable_cursor():
    source = Source.COMICVINE_LOCAL
    adapter = Pages(source, {0: SourcePage([row(source, 1)], truncated=True)})
    result = await registry_for(adapter).discover_all(query())
    assert result.sources[0].truncated
    assert result.sources[0].next_offset is None
    assert adapter.calls == [0]


async def test_collection_reuses_one_source_transport_and_closes_it_after_all_pages():
    source = Source.COMICVINE_API
    adapter = Pages(
        source,
        {
            0: SourcePage([row(source, 1)], next_offset=2),
            2: SourcePage([row(source, 2)]),
        },
    )
    constructions = []

    def build(_):
        constructions.append(source)
        return adapter

    registry = registry_for(adapter)
    registry.factories[source] = SourceRegistration(
        frozenset([SourceCapability.SERIES_SEARCH]), build
    )
    result = await registry.discover_all(query())
    assert [item.external_id for item in result.results] == ["1", "2"]
    assert constructions == [source]
    assert adapter.closed == 1


async def test_empty_success_is_distinct_from_initial_provider_failure():
    local, api = Source.COMICVINE_LOCAL, Source.COMICVINE_API
    empty = Pages(local, {0: SourcePage([], total=0)})
    failed = Pages(api, {0: RuntimeError("synthetic-provider-secret")})
    result = await registry_for(empty, failed).discover_all(query())
    assert [(outcome.status, outcome.total) for outcome in result.sources] == [
        (SourceStatus.EMPTY, 0),
        (SourceStatus.UNAVAILABLE, None),
    ]
    assert "synthetic-provider-secret" not in result.model_dump_json()
    assert result.results == []


@pytest.mark.parametrize(
    "options",
    [
        {"mode": "automatic"},
        {"offsets": {Source.COMICVINE_API: 2}},
    ],
)
async def test_full_collection_rejects_automatic_or_midstream_queries_before_io(options):
    adapter = Pages(Source.COMICVINE_API, {0: SourcePage([])})
    with pytest.raises(ValueError):
        await registry_for(adapter).discover_all(query(**options))
    assert adapter.calls == []


@pytest.mark.parametrize("cap", [True, 0, -1, 3, 1002])
async def test_full_collection_rejects_unbounded_or_misaligned_caps_before_io(cap):
    adapter = Pages(Source.COMICVINE_API, {0: SourcePage([])})
    with pytest.raises(ValueError):
        await registry_for(adapter).discover_all(query(), max_results_per_source=cap)
    assert adapter.calls == []
