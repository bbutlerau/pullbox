"""Complete source-owned arc evidence, not a browser-supplied member list."""

import asyncio
from dataclasses import replace

import pytest

from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import MetadataFetch, MetadataPage, SourceStatus
from pullbox.services.metadata_arc_catalog import fetch_source_arc_catalog
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.services.story_arc_catalog_types import (
    StoryArcCatalogError,
    catalog_snapshot,
    snapshot_fingerprint,
)
from tests.unit.test_metadata_discovery import row
from tests.unit.test_metadata_source_reads import issue_row
from tests.unit.test_metadata_story_arc_reads import ArcAdapter, arc_row
from tests.unit.test_metadata_story_arc_reads import registry as read_registry


class CatalogAdapter(ArcAdapter):
    def __init__(self, count=103):
        super().__init__()
        self.members = [
            issue_row(
                external_id=str(1000 + i),
                series_external_id=str(42 + i % 2),
                issue_number_text="50-x" if i == 0 else str(i + 1),
            )
            for i in range(count)
        ]
        self.arc = arc_row(declared_issue_count=count)
        self.failure = None
        self.changed = None

    async def story_arc(self, identifier, *, validator=None):
        await self._call("arc", identifier)
        return MetadataFetch(status=SourceStatus.OK, data=self.arc)

    async def story_arc_issues(self, identifier, *, page=1, validator=None):
        await self._call("members", page)
        if self.failure and page == 2:
            raise MetadataSourceError(self.failure, 60)
        total = len(self.members)
        rows = self.members[(page - 1) * 100 : page * 100]
        if self.changed == "total" and page == 2:
            total -= 1
            rows = rows[:-1]
        if self.changed == "duplicate" and page == 2:
            rows = [self.members[0], *rows[1:]]
        return MetadataFetch(
            status=SourceStatus.OK,
            data=MetadataPage(
                results=rows, total=total, next_page=page + 1 if page * 100 < total else None
            ),
        )

    async def series(self, identifier, *, validator=None):
        await self._call("parent", identifier)
        profile = row(self.source).model_copy(
            update={
                "external_id": identifier,
                "title": f"Parent {identifier}",
                "series_type": "volume",
                "language": "en",
            }
        )
        return MetadataFetch(status=SourceStatus.OK, data=profile)


def registry(adapter):
    instance = read_registry(adapter)
    instance.runtime[adapter.source].policy.revision = 1
    return instance


async def fetch(adapter, **kwargs):
    return await fetch_source_arc_catalog(
        registry(adapter), Source.METRON_API, "42", source_revision=1, **kwargs
    )


async def test_complete_pages_and_parents_preserve_order_raw_designations_and_crosswalks():
    adapter = CatalogAdapter()
    cross = ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "99")
    adapter.members[0].cross_identities = [cross]
    preview = await fetch(adapter)
    assert len(preview.issues) == 103 and len(preview.series) == 2
    assert preview.metadata.issue_provider_ids == tuple(str(1000 + i) for i in range(103))
    assert preview.issues[0].issue_number_text == "50-x"
    assert preview.source is Source.METRON_API and preview.source_revision == 1
    assert preview.membership_complete and preview.order_basis == "response_order"
    assert preview.source_evidence.issues[0].cross_identities == [cross]
    assert preview.source_evidence.series[0].series_type == "volume"
    assert preview.source_evidence.series[0].language == "en"
    assert catalog_snapshot(preview)["source_evidence"]["issues"][0]["cross_identities"]
    assert snapshot_fingerprint(preview) == preview.fingerprint
    assert adapter.calls == [
        ("arc", "42"),
        ("members", 1),
        ("members", 2),
        ("parent", "42"),
        ("parent", "43"),
    ]
    assert adapter.closed == 5


@pytest.mark.parametrize(
    "change", ["total", "duplicate", "explicit_order", "incomplete", "count", "too_large"]
)
async def test_inconsistent_or_excessive_membership_never_fetches_parents(change):
    adapter = CatalogAdapter(2001 if change == "too_large" else 103)
    if change in {"total", "duplicate"}:
        adapter.changed = change
    elif change == "explicit_order":
        adapter.arc.issue_external_ids = [r.external_id for r in reversed(adapter.members)]
        adapter.arc.membership_complete = True
    elif change == "incomplete":
        adapter.arc.issue_external_ids = [r.external_id for r in adapter.members]
    elif change == "count":
        adapter.arc.declared_issue_count = 102
    with pytest.raises(StoryArcCatalogError):
        await fetch(adapter)
    assert adapter.calls
    assert not any(call[0] == "parent" for call in adapter.calls)


@pytest.mark.parametrize(
    "status", [SourceStatus.RATE_LIMITED, SourceStatus.UNAVAILABLE, SourceStatus.TIMEOUT]
)
async def test_failed_later_page_preserves_safe_failure_and_retry_after(status):
    adapter = CatalogAdapter()
    adapter.failure = status
    with pytest.raises(StoryArcCatalogError) as result:
        await fetch(adapter)
    assert result.value.source_status is status
    assert result.value.retry_after_seconds == 60
    assert not any(call[0] == "parent" for call in adapter.calls)


async def test_revision_is_checked_before_constructing_source_clients():
    adapter = CatalogAdapter()
    with pytest.raises(StoryArcCatalogError, match="settings changed"):
        await fetch_source_arc_catalog(
            registry(adapter), Source.METRON_API, "42", source_revision=2
        )
    assert not adapter.calls and adapter.closed == 0


async def test_timeout_and_cancellation_close_owned_clients():
    adapter = CatalogAdapter()
    adapter.wait = asyncio.Event()
    with pytest.raises(StoryArcCatalogError, match="timed out"):
        await fetch(adapter, timeout=0.01)
    assert adapter.closed == 1
    adapter.started.clear()
    task = asyncio.create_task(fetch(adapter))
    try:
        await asyncio.wait_for(adapter.started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert adapter.closed == 2
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_valid_empty_arc_is_complete_without_inventing_members():
    adapter = CatalogAdapter(0)
    preview = await fetch(adapter)
    assert preview.membership_complete and preview.metadata.issue_provider_ids == ()
    assert preview.issues == preview.series == ()
    assert adapter.calls == [("arc", "42"), ("members", 1)]


async def test_source_evidence_mutation_changes_snapshot_digest():
    preview = await fetch(CatalogAdapter(1))
    old = preview.fingerprint
    preview.source_evidence.series[0].language = "fr"
    assert snapshot_fingerprint(preview) != old
    assert snapshot_fingerprint(replace(preview, source_revision=2)) != old
