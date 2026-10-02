"""Registry-fetched evidence reaches the existing atomic arc saver intact."""

from dataclasses import replace

import pytest
from sqlalchemy import func, select

from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.models import Issue, Series, StoryArc
from pullbox.models.metadata_identity import IssueExternalIdentity, IssueIdentityEvent
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError, snapshot_fingerprint
from tests.integration.metadata_identity.test_source_arc_catalog import _add, _service, _setup
from tests.unit.test_source_arc_catalog_fetch import CatalogAdapter, fetch


async def test_complete_source_snapshot_preserves_format_and_unverified_crosswalks(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    adapter = CatalogAdapter(1)
    adapter.members[0].cross_identities = [
        ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "99")
    ]
    preview = await fetch(adapter)
    async with factory.begin() as session:
        arc = await _add(_service(), session, preview, root)
        parent = await session.scalar(select(Series))
        assert parent.series_type == "volume"
        assert parent.status == "unknown"
        issue = await session.scalar(select(Issue))
        assert issue.comicvine_id is None and issue.issue_number_text == "50-X"
        native = list(await session.scalars(select(IssueExternalIdentity)))
        assert len(native) == 1 and native[0].identity_namespace == "metron"
        claim = await session.scalar(
            select(IssueIdentityEvent).where(IssueIdentityEvent.identity_namespace == "comicvine")
        )
        assert claim is not None and claim.verification_state == "observed"
        snapshot = arc.diagnostics["provider_catalog"]["snapshot"]
        assert snapshot["source_evidence"]["series"][0]["language"] == "en"
    assert adapter.calls == [("arc", "42"), ("members", 1), ("parent", "42")]
    assert not list((tmp_path / "comics").iterdir())


@pytest.mark.parametrize("kind", ["arc", "series", "issue"])
async def test_unverified_crosswalk_cannot_create_a_second_owner(identity_probe_db, tmp_path, kind):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    adapter = CatalogAdapter(1)
    if kind == "arc":
        adapter.arc.cross_identities = [
            ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.STORY_ARC, "99")
        ]
    elif kind == "issue":
        adapter.members[0].cross_identities = [
            ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "99")
        ]
    preview = await fetch(adapter)
    if kind == "series":
        preview.source_evidence.series[0].cross_identities = [
            ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "99")
        ]
        preview = replace(preview, fingerprint=snapshot_fingerprint(preview))
    async with factory.begin() as session:
        parent = Series(title="Already here", sort_title="Already here", comicvine_id=99)
        session.add(parent)
        await session.flush()
        session.add(
            Issue(series_id=parent.id, issue_number=50, issue_number_text="50-X", comicvine_id=99)
        )
        session.add(StoryArc(name="Existing", comicvine_id=99))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError, match=r"cross.*review"):
            await _add(_service(), session, preview, root)
        for model in (Series, Issue, StoryArc):
            assert await session.scalar(select(func.count()).select_from(model)) == 1


@pytest.mark.parametrize("field", ["title", "issue", "source"])
async def test_legacy_projection_cannot_disagree_with_normalized_evidence(
    identity_probe_db, tmp_path, field
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await fetch(CatalogAdapter(1))
    if field == "title":
        preview = replace(preview, metadata=replace(preview.metadata, title="Changed"))
    elif field == "issue":
        preview = replace(preview, issues=(replace(preview.issues[0], issue_number_text="51"),))
    else:
        preview.source_evidence.arc.external_id = "99"
    preview = replace(preview, fingerprint=snapshot_fingerprint(preview))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError):
            await _add(_service(), session, preview, root)
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0
