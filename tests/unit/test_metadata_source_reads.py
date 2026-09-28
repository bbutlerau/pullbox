"""Source-bound reads cannot turn an unrelated response into adopted metadata."""

import asyncio

import pytest

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    MetadataPage,
    ProviderIssueRead,
    SourceCapability,
    SourceStatus,
)
from pullbox.services.metadata_discovery import MetadataSourceError, MetadataSourceRegistry
from tests.unit.test_metadata_discovery import Adapter, registration, row, runtime


class ReadAdapter(Adapter):
    def __init__(self, *, result=None, **kwargs):
        super().__init__(Source.METRON_API, **kwargs)
        self.result = result

    async def series(self, external_id, *, validator=None):
        self.calls.append(("series", external_id, validator))
        self.started.set()
        if self.wait:
            await self.wait.wait()
        if self.error:
            raise self.error
        return self.result or MetadataFetch(status=SourceStatus.OK, data=row(self.source))

    async def issue(self, external_id, *, validator=None):
        self.calls.append(("issue", external_id, validator))
        return self.result or MetadataFetch(status=SourceStatus.OK, data=issue_row())

    async def issues(self, external_id, *, page=1, validator=None):
        self.calls.append(("issues", external_id, page, validator))
        return self.result or MetadataFetch(
            status=SourceStatus.OK, data=MetadataPage(results=[issue_row()], total=1)
        )


def issue_row(**changes):
    return ProviderIssueRead(
        source=Source.METRON_API,
        identity_namespace=Source.METRON_API.identity_namespace,
        external_id="123",
        series_external_id="42",
        issue_number_text="50-x",
        issue_number_key="50-X",
    ).model_copy(update=changes)


def registry(adapter, **kwargs):
    return MetadataSourceRegistry(
        [runtime(adapter.source)],
        factories={adapter.source: registration(adapter, capabilities=list(SourceCapability))},
        **kwargs,
    )


async def test_series_read_preserves_source_and_exact_identity_and_closes_adapter():
    adapter = ReadAdapter()
    result = await registry(adapter).series(adapter.source, "00042")
    assert result.status is SourceStatus.OK
    assert result.data.source is Source.METRON_API
    assert result.data.external_id == "42"
    assert adapter.calls == [("series", "42", None)]
    assert adapter.closed == 1


@pytest.mark.parametrize("operation", ["issue", "issues"])
async def test_issue_reads_preserve_lettered_designation_and_exact_parent(operation):
    adapter = ReadAdapter()
    result = await getattr(registry(adapter), operation)(
        adapter.source, "123" if operation == "issue" else "42"
    )
    assert result.status is SourceStatus.OK
    issue = result.data if operation == "issue" else result.data.results[0]
    assert issue.issue_number_text == "50-x"
    assert issue.issue_number_key == "50-X"
    assert issue.series_external_id == "42"
    assert adapter.closed == 1


@pytest.mark.parametrize(
    "updates",
    [
        {"external_id": "43"},
        {"external_id": "042"},
        {"identity_namespace": Source.GCD_LOCAL.identity_namespace},
        {"source": Source.COMICVINE_API},
        {"title": ""},
    ],
)
async def test_series_read_rejects_mismatched_or_invalid_identity(updates):
    adapter = ReadAdapter(
        result=MetadataFetch(
            status=SourceStatus.OK, data=row(Source.METRON_API).model_copy(update=updates)
        )
    )
    result = await registry(adapter).series(adapter.source, "42")
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.data is None
    assert adapter.closed == 1


@pytest.mark.parametrize(
    "page",
    [
        MetadataPage(results=[issue_row(series_external_id="43")], total=1),
        MetadataPage(results=[issue_row(external_id="0")], total=1),
        MetadataPage(results=[issue_row(), issue_row()], total=2),
        MetadataPage(results=[issue_row()], total=0),
        MetadataPage(results=[issue_row()], total=101, next_page=2),
        MetadataPage(results=[], total=100),
        MetadataPage(results=[issue_row()], total=1, next_page=1),
        MetadataPage(results=[issue_row()], total=1, order_is_reading_order=True),
    ],
)
async def test_issue_page_rejects_cross_parent_duplicates_and_incomplete_envelopes(page):
    adapter = ReadAdapter(result=MetadataFetch(status=SourceStatus.OK, data=page))
    result = await registry(adapter).issues(adapter.source, "42")
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert result.data is None


async def test_source_page_is_bounded_and_does_not_silently_fetch_other_pages():
    page = MetadataPage(results=[issue_row()], total=101)
    adapter = ReadAdapter(result=MetadataFetch(status=SourceStatus.OK, data=page))
    result = await registry(adapter).issues(adapter.source, "42", page=2)
    assert result.status is SourceStatus.OK
    assert result.data.total == 101
    assert adapter.calls == [("issues", "42", 2, None)]


@pytest.mark.parametrize("operation", ["series", "issue", "issues"])
@pytest.mark.parametrize("identifier", ["../42", "0", "-1", "42?apikey=secret", "4.2", True])
async def test_invalid_identifiers_never_construct_adapter(operation, identifier):
    calls = []
    adapter = ReadAdapter()
    instance = registry(adapter)
    instance.factories = {adapter.source: registration(adapter, calls, list(SourceCapability))}
    with pytest.raises(ValueError):
        await getattr(instance, operation)(adapter.source, identifier)
    assert calls == []


@pytest.mark.parametrize("page", [0, True, 10001, 1.1])
async def test_invalid_pages_do_not_start_reads(page):
    adapter = ReadAdapter()
    with pytest.raises(ValueError):
        await registry(adapter).issues(adapter.source, "42", page=page)
    assert not adapter.calls


@pytest.mark.parametrize("operation", ["series", "issue", "issues"])
async def test_disabled_and_unsupported_reads_never_create_clients(operation):
    adapter = ReadAdapter()
    calls = []
    for enabled, capabilities, expected in (
        (False, list(SourceCapability), SourceStatus.DISABLED),
        (True, [], SourceStatus.UNSUPPORTED),
    ):
        instance = MetadataSourceRegistry(
            [runtime(adapter.source, enabled=enabled)],
            factories={adapter.source: registration(adapter, calls, capabilities)},
        )
        assert (await getattr(instance, operation)(adapter.source, "42")).status is expected
    assert calls == []


async def test_gcd_release_flag_remains_enforced_for_reads():
    instance = MetadataSourceRegistry([runtime(Source.GCD_API_V2)])
    assert (await instance.series(Source.GCD_API_V2, "42")).status is SourceStatus.FEATURE_DISABLED


async def test_series_only_source_does_not_require_unadvertised_issue_operations():
    adapter = Adapter(Source.METRON_API)
    adapter.series = ReadAdapter().series
    instance = MetadataSourceRegistry(
        [runtime(adapter.source)],
        factories={
            adapter.source: registration(adapter, capabilities=[SourceCapability.SERIES_DETAILS])
        },
    )
    assert (await instance.series(adapter.source, "42")).status is SourceStatus.OK
    assert (await instance.issues(adapter.source, "42")).status is SourceStatus.UNSUPPORTED
    assert adapter.closed == 1


@pytest.mark.parametrize("status", [SourceStatus.NOT_FOUND, SourceStatus.NOT_MODIFIED])
async def test_empty_detail_outcomes_are_not_successful_metadata(status):
    adapter = ReadAdapter(result=MetadataFetch(status=status, validator="synthetic-validator"))
    result = await registry(adapter).series(adapter.source, "42", validator="synthetic-validator")
    assert result.status is status and result.data is None
    assert result.validator == "synthetic-validator"


async def test_unsolicited_not_modified_is_not_usable():
    adapter = ReadAdapter(result=MetadataFetch(status=SourceStatus.NOT_MODIFIED))
    assert (
        await registry(adapter).series(adapter.source, "42")
    ).status is SourceStatus.INCOMPATIBLE_RESPONSE


async def test_timeout_and_cancellation_close_owned_reads():
    adapter = ReadAdapter(wait=asyncio.Event())
    result = await registry(adapter, per_source_timeout=0.01).series(adapter.source, "42")
    assert result.status is SourceStatus.TIMEOUT and adapter.closed == 1
    task = asyncio.create_task(registry(adapter).series(adapter.source, "42"))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert adapter.closed == 2


async def test_typed_failure_keeps_retry_after_without_leaking_provider_text():
    adapter = ReadAdapter(error=MetadataSourceError(SourceStatus.RATE_LIMITED, 300))
    result = await registry(adapter).series(adapter.source, "42")
    assert result.status is SourceStatus.RATE_LIMITED
    assert result.retry_after_seconds == 300
    adapter.error = RuntimeError("synthetic-secret")
    result = await registry(adapter).series(adapter.source, "42")
    assert result.status is SourceStatus.UNAVAILABLE
    assert "synthetic-secret" not in result.model_dump_json()
