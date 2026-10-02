"""Real provider transports feed one normalized arc contract."""

import httpx

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.providers.metadata.sources import metadata_sources
from pullbox.schemas.metadata_sources import SourceCapability, SourceStatus, StoryArcDiscoveryQuery
from pullbox.services.metadata_discovery import MetadataSourceRegistry, SourceRegistration
from tests.unit.test_comicvine_source_reads import issue_payload
from tests.unit.test_metadata_discovery import runtime
from tests.unit.test_metadata_source_adapters import api_adapter
from tests.unit.test_metron_source import envelope, issue_row
from tests.unit.test_metron_source import source as metron_source


def instance(source, adapter):
    return MetadataSourceRegistry(
        [runtime(source)],
        factories={
            source: SourceRegistration(metadata_sources()[source].capabilities, lambda _: adapter)
        },
    )


def cv_arc(**updates):
    return {
        "id": 42,
        "name": "Shared arc",
        "count_of_isssue_appearances": 0,
        "issues": [{"id": 124}, {"id": 123}],
        **updates,
    }


async def test_comicvine_arc_source_search_details_and_members_use_existing_transport():
    calls = []

    def handle(request):
        calls.append(request)
        if request.url.path.endswith("/story_arcs/"):
            payload = {"results": [cv_arc()], "number_of_total_results": 1}
        elif request.url.path.endswith("/issues/"):
            payload = {
                "results": [issue_payload(), issue_payload(124, volume={"id": 43})],
                "number_of_total_results": 2,
            }
        else:
            payload = {"results": cv_arc()}
        return httpx.Response(200, json={"status_code": 1, **payload})

    adapter = await api_adapter(handle)
    search = await instance(Source.COMICVINE_API, adapter).discover_arcs(
        StoryArcDiscoveryQuery(query="Shared")
    )
    assert search.sources[0].status is SourceStatus.OK
    assert search.results[0].external_id == "42"
    assert search.results[0].declared_issue_count is None
    assert calls[0].url.params["limit"] == "100"

    adapter = await api_adapter(handle)
    detail = await instance(Source.COMICVINE_API, adapter).story_arc(Source.COMICVINE_API, "42")
    assert detail.status is SourceStatus.OK
    assert detail.data.issue_external_ids == ["124", "123"]
    assert detail.data.membership_complete
    assert detail.data.warnings == ["unreliable_zero_issue_count"]
    adapter = await api_adapter(handle)
    members = await instance(Source.COMICVINE_API, adapter).story_arc_issues(
        Source.COMICVINE_API, "42"
    )
    assert members.status is SourceStatus.OK
    assert [r.external_id for r in members.data.results] == ["124", "123"]
    assert [r.series_external_id for r in members.data.results] == ["43", "42"]
    assert not members.data.order_is_reading_order
    assert len(calls) == 4


async def test_comicvine_arc_member_page_hydrates_only_requested_members():
    calls = []

    def handle(request):
        calls.append(request)
        payload = (
            {"results": [issue_payload(201)], "number_of_total_results": 1}
            if request.url.path.endswith("/issues/")
            else {"results": cv_arc(issues=[{"id": value} for value in range(101, 202)])}
        )
        return httpx.Response(200, json={"status_code": 1, **payload})

    adapter = await api_adapter(handle)
    result = await instance(Source.COMICVINE_API, adapter).story_arc_issues(
        Source.COMICVINE_API, "42", page=2
    )
    assert result.status is SourceStatus.OK
    assert result.data.total == 101 and result.data.next_page is None
    assert [r.external_id for r in result.data.results] == ["201"]
    assert calls[1].url.params["filter"] == "id:201"
    assert len(calls) == 2


async def test_comicvine_mismatched_arc_count_does_not_publish_members():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(
            200, json={"status_code": 1, "results": cv_arc(count_of_isssue_appearances=3)}
        )

    adapter = await api_adapter(handle)
    result = await instance(Source.COMICVINE_API, adapter).story_arc_issues(
        Source.COMICVINE_API, "42"
    )
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.data is None and len(calls) == 1


async def test_metron_arc_members_use_arc_not_series_endpoint():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json=envelope([issue_row()]))

    adapter = metron_source(handle)
    result = await instance(Source.METRON_API, adapter).story_arc_issues(Source.METRON_API, "4")
    assert result.status is SourceStatus.OK
    assert result.data.results[0].series_external_id == "8"
    assert not result.data.order_is_reading_order
    assert [r.url.path for r in calls] == ["/api/arc/4/issue_list/"]


def test_arc_capabilities_are_advertised_only_for_executable_sources():
    capabilities = {
        SourceCapability.STORY_ARC_SEARCH,
        SourceCapability.STORY_ARC_DETAILS,
        SourceCapability.STORY_ARC_ISSUES,
    }
    sources = metadata_sources()
    assert capabilities <= sources[Source.COMICVINE_API].capabilities
    assert capabilities <= sources[Source.METRON_API].capabilities
    assert not capabilities & sources[Source.COMICVINE_LOCAL].capabilities
