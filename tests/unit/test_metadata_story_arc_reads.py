"""Arc reads preserve exact source membership without adopting provider identities."""

import asyncio

import pytest

from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    MetadataFetch,
    MetadataPage,
    ProviderStoryArcRead,
    SourceCapability,
    SourceStatus,
    StoryArcDiscoveryQuery,
)
from pullbox.services.metadata_discovery import MetadataSourceError, MetadataSourceRegistry
from tests.unit.test_metadata_discovery import Adapter, registration, runtime
from tests.unit.test_metadata_source_reads import issue_row


def arc_row(source=Source.METRON_API, **updates):
    return ProviderStoryArcRead(
        source=source,
        identity_namespace=source.identity_namespace,
        external_id="42",
        title="Shared title",
    ).model_copy(update=updates)


class ArcAdapter(Adapter):
    def __init__(self, source=Source.METRON_API, *, result=None, page_result=None, **kwargs):
        super().__init__(source, **kwargs)
        self.result = result
        self.page_result = page_result

    async def _call(self, *args):
        self.calls.append(args)
        self.started.set()
        if self.wait:
            await self.wait.wait()
        if self.error:
            raise self.error

    async def story_arc(self, identifier, *, validator=None):
        await self._call("arc", identifier, validator)
        return self.result or MetadataFetch(status=SourceStatus.OK, data=arc_row(self.source))

    async def story_arc_issues(self, identifier, *, page=1, validator=None):
        await self._call("members", identifier, page, validator)
        return self.result or MetadataFetch(
            status=SourceStatus.OK,
            data=MetadataPage(
                results=[issue_row(), issue_row(external_id="124", series_external_id="43")],
                total=2,
            ),
        )

    async def story_arcs(self, query, *, page=1):
        await self._call("search", query, page)
        return self.page_result or MetadataPage(results=[arc_row(self.source)], total=1)


def registry(*adapters, **kwargs):
    return MetadataSourceRegistry(
        [runtime(adapter.source) for adapter in adapters],
        factories={
            a.source: registration(a, capabilities=list(SourceCapability)) for a in adapters
        },
        **kwargs,
    )


async def test_exact_arc_and_cross_series_members_preserve_source_and_order():
    adapter = ArcAdapter()
    instance = registry(adapter)
    detail = await instance.story_arc(adapter.source, "00042")
    assert detail.status is SourceStatus.OK
    assert detail.data.external_id == "42"
    members = await instance.story_arc_issues(adapter.source, "42")
    assert members.status is SourceStatus.OK
    assert [row.series_external_id for row in members.data.results] == ["42", "43"]
    assert members.data.results[0].issue_number_text == "50-x"
    assert not members.data.order_is_reading_order
    assert adapter.calls == [("arc", "42", None), ("members", "42", 1, None)]
    assert adapter.closed == 2


async def test_read_cleanup_failure_cannot_supply_metadata_for_a_catalog_write(monkeypatch):
    adapter = ArcAdapter()

    async def failed_close():
        adapter.closed += 1
        raise RuntimeError("secret-bearing cleanup failure")

    monkeypatch.setattr(adapter, "close", failed_close)
    result = await registry(adapter).story_arc(adapter.source, "42")
    assert result.status is SourceStatus.UNAVAILABLE and result.data is None
    assert adapter.closed == 1
    adapter.error = MetadataSourceError(SourceStatus.RATE_LIMITED, 60)
    limited = await registry(adapter).story_arc(adapter.source, "42")
    assert limited.status is SourceStatus.RATE_LIMITED and limited.retry_after_seconds == 60

    adapter.error, adapter.wait = None, asyncio.Event()
    adapter.started.clear()
    running = asyncio.create_task(registry(adapter).story_arc(adapter.source, "42"))
    await adapter.started.wait()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running


@pytest.mark.parametrize(
    "updates",
    [
        {"external_id": "43"},
        {"external_id": "042"},
        {"title": " "},
        {"source": Source.COMICVINE_API},
        {"identity_namespace": Source.GCD_LOCAL.identity_namespace},
        {"issue_external_ids": ["123", "123"], "membership_complete": True},
        {"issue_external_ids": ["0"]},
        {"membership_complete": True},
        {"issue_external_ids": ["123"], "declared_issue_count": 2, "membership_complete": True},
    ],
)
async def test_arc_detail_rejects_wrong_identity_or_false_complete_membership(updates):
    adapter = ArcAdapter(result=MetadataFetch(status=SourceStatus.OK, data=arc_row(**updates)))
    result = await registry(adapter).story_arc(adapter.source, "42")
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.data is None and adapter.closed == 1


@pytest.mark.parametrize(
    "page",
    [
        MetadataPage(results=[issue_row(), issue_row()], total=2),
        MetadataPage(results=[issue_row(series_external_id="0")], total=1),
        MetadataPage(results=[issue_row(source=Source.COMICVINE_API)], total=1),
        MetadataPage(results=[issue_row()], total=101, next_page=2),
        MetadataPage(results=[issue_row()], total=1, order_is_reading_order=True),
    ],
)
async def test_arc_members_reject_invalid_or_unqualified_pages(page):
    adapter = ArcAdapter(result=MetadataFetch(status=SourceStatus.OK, data=page))
    result = await registry(adapter).story_arc_issues(adapter.source, "42")
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE and result.data is None


@pytest.mark.parametrize("operation", ["story_arc", "story_arc_issues"])
@pytest.mark.parametrize("identifier", ["0", "../42", True, "42?token=x"])
async def test_invalid_arc_identity_does_not_construct_adapter(operation, identifier):
    adapter = ArcAdapter()
    with pytest.raises(ValueError):
        await getattr(registry(adapter), operation)(adapter.source, identifier)
    assert not adapter.calls and not adapter.closed


async def test_unavailable_arc_sources_never_construct_clients():
    adapter = ArcAdapter()
    constructed = []
    for enabled, caps, expected in (
        (False, list(SourceCapability), SourceStatus.DISABLED),
        (True, [], SourceStatus.UNSUPPORTED),
    ):
        instance = MetadataSourceRegistry(
            [runtime(adapter.source, enabled=enabled)],
            factories={adapter.source: registration(adapter, constructed, caps)},
        )
        assert (await instance.story_arc(adapter.source, "42")).status is expected
        assert (await instance.story_arc_issues(adapter.source, "42")).status is expected
    assert not constructed
    assert (
        await registry(adapter).story_arc(Source.GCD_API_V2, "42")
    ).status is SourceStatus.FEATURE_DISABLED


async def test_arc_timeout_cancellation_and_retry_after_use_owned_read_lifecycle():
    adapter = ArcAdapter(wait=asyncio.Event())
    result = await registry(adapter, per_source_timeout=0.01).story_arc(adapter.source, "42")
    assert result.status is SourceStatus.TIMEOUT and adapter.closed == 1
    adapter.started.clear()
    task = asyncio.create_task(registry(adapter).story_arc_issues(adapter.source, "42"))
    await adapter.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.closed == 2
    adapter.wait = None
    adapter.error = MetadataSourceError(SourceStatus.RATE_LIMITED, 60)
    result = await registry(adapter).story_arc(adapter.source, "42")
    assert result.status is SourceStatus.RATE_LIMITED and result.retry_after_seconds == 60


async def test_search_fans_out_by_arc_authority_without_title_or_raw_crosswalk_merges():
    cv, metron = ArcAdapter(Source.COMICVINE_API), ArcAdapter()
    metron.page_result = MetadataPage(
        results=[
            arc_row(
                cross_identities=[
                    ExternalIdentityRef(
                        Source.COMICVINE_API.identity_namespace, MetadataEntityKind.STORY_ARC, "42"
                    )
                ]
            )
        ],
        total=1,
    )
    instance = registry(cv, metron)
    instance.runtime[metron.source].policy.domain_priorities[MetadataDomain.STORY_ARCS] = 0
    instance.runtime[cv.source].policy.domain_priorities[MetadataDomain.STORY_ARCS] = 5
    result = await instance.discover_arcs(StoryArcDiscoveryQuery(query="Shared"))
    assert [r.source for r in result.results] == [metron.source, cv.source]
    assert [o.source for o in result.sources] == [metron.source, cv.source]
    assert all(o.status is SourceStatus.OK for o in result.sources)
    assert cv.closed == metron.closed == 1


async def test_arc_search_keeps_partial_success_and_independent_page_cursor():
    metron = ArcAdapter(page_result=MetadataPage(results=[arc_row()], total=101))
    cv = ArcAdapter(Source.COMICVINE_API, error=MetadataSourceError(SourceStatus.RATE_LIMITED, 30))
    result = await registry(cv, metron).discover_arcs(
        StoryArcDiscoveryQuery(query="Shared", pages={metron.source: 2})
    )
    assert [r.source for r in result.results] == [metron.source]
    outcomes = {o.source: o for o in result.sources}
    assert outcomes[cv.source].status is SourceStatus.RATE_LIMITED
    assert outcomes[cv.source].retry_after_seconds == 30
    assert outcomes[metron.source].total == 101
    assert metron.calls == [("search", "Shared", 2)]


async def test_automatic_arc_search_stops_only_after_caller_proves_satisfaction():
    cv, metron = ArcAdapter(Source.COMICVINE_API), ArcAdapter()
    instance = registry(cv, metron)
    result = await instance.discover_arcs(
        StoryArcDiscoveryQuery(query="Shared", mode="automatic"),
        satisfied_by=lambda page: bool(page.results),
    )
    assert len(result.results) == 1
    assert [o.status for o in result.sources] == [SourceStatus.OK, SourceStatus.NOT_QUERIED]
    assert sum(len(a.calls) for a in (cv, metron)) == 1


async def test_arc_search_cancellation_drains_all_started_sources():
    adapters = [
        ArcAdapter(Source.COMICVINE_API, wait=asyncio.Event()),
        ArcAdapter(wait=asyncio.Event()),
    ]
    task = asyncio.create_task(
        registry(*adapters).discover_arcs(StoryArcDiscoveryQuery(query="Shared"))
    )
    await asyncio.wait_for(asyncio.gather(*(a.started.wait() for a in adapters)), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(a.closed == 1 for a in adapters)


async def test_arc_search_groups_only_same_namespace_and_identity():
    local, remote = ArcAdapter(Source.GCD_LOCAL), ArcAdapter(Source.GCD_API_V2)
    result = await registry(local, remote, gcd_api_enabled=True).discover_arcs(
        StoryArcDiscoveryQuery(query="Shared")
    )
    assert len(result.results) == 1
    assert result.results[0].also_from == [remote.source]
    assert not local.page_result
    assert local.closed == remote.closed == 1


async def test_arc_search_total_deadline_bounds_waiting_and_later_automatic_sources():
    slow, later = ArcAdapter(Source.COMICVINE_API, wait=asyncio.Event()), ArcAdapter()
    instance = registry(slow, later, total_timeout=0.02, per_source_timeout=1, concurrency=1)
    result = await instance.discover_arcs(StoryArcDiscoveryQuery(query="Shared", mode="automatic"))
    assert [o.status for o in result.sources] == [SourceStatus.TIMEOUT, SourceStatus.TIMEOUT]
    assert not later.calls and not later.closed


async def test_arc_search_partial_timeout_keeps_fast_source_results():
    slow, fast = ArcAdapter(Source.COMICVINE_API, wait=asyncio.Event()), ArcAdapter()
    result = await registry(slow, fast, total_timeout=0.05, per_source_timeout=0.02).discover_arcs(
        StoryArcDiscoveryQuery(query="Shared")
    )
    assert [r.source for r in result.results] == [fast.source]
    assert [o.status for o in result.sources] == [SourceStatus.TIMEOUT, SourceStatus.OK]
    assert slow.closed == fast.closed == 1


@pytest.mark.parametrize(
    "page",
    [
        MetadataPage(results=[arc_row(external_id="0")], total=1),
        MetadataPage(results=[arc_row(), arc_row()], total=2),
        MetadataPage(results=[arc_row()], total=101),
        MetadataPage(results=[arc_row()], total=1, next_page=1),
        MetadataPage(results=[arc_row()], total=1, order_is_reading_order=True),
    ],
)
async def test_arc_search_invalid_pages_are_not_successful_candidates(page):
    adapter = ArcAdapter(page_result=page)
    result = await registry(adapter).discover_arcs(StoryArcDiscoveryQuery(query="Shared"))
    assert not result.results
    assert result.sources[0].status is SourceStatus.INCOMPATIBLE_RESPONSE


async def test_arc_preview_retains_detail_but_rejects_changed_explicit_membership():
    from pullbox.services.metadata_arc_preview import preview_source_arc

    adapter = ArcAdapter()
    original = adapter.story_arc

    async def detail(*args, **kwargs):
        result = await original(*args, **kwargs)
        result.data.issue_external_ids = ["123", "999"]
        result.data.membership_complete = True
        return result

    adapter.story_arc = detail
    result = await preview_source_arc(registry(adapter), adapter.source, "42")
    assert result.arc.status is SourceStatus.OK
    assert result.issues.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.issues.data is None


async def test_arc_preview_keeps_detail_when_member_source_is_temporarily_unavailable():
    from pullbox.services.metadata_arc_preview import preview_source_arc

    adapter = ArcAdapter()

    async def failed(*args, **kwargs):
        raise MetadataSourceError(SourceStatus.RATE_LIMITED, 60)

    adapter.story_arc_issues = failed
    result = await preview_source_arc(registry(adapter), adapter.source, "42")
    assert result.arc.status is SourceStatus.OK
    assert result.issues.status is SourceStatus.RATE_LIMITED
    assert result.issues.retry_after_seconds == 60


@pytest.mark.parametrize("page", [0, True, 51, 1.5])
async def test_arc_member_page_limits_reject_before_source_work(page):
    adapter = ArcAdapter()
    with pytest.raises(ValueError):
        await registry(adapter).story_arc_issues(adapter.source, "42", page=page)
    assert not adapter.calls
