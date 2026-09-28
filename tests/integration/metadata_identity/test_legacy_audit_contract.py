"""Legacy ownership preflight must give the same read-only answer on both databases."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity
from pullbox.services.metadata_identity_audit import (
    IdentityAuditStopReason,
    audit_legacy_identities,
)
from tests.fixtures.metadata_identity_persistence import drop_canonical_arc_index

if TYPE_CHECKING:
    from tests.fixtures.metadata_identity_persistence import IdentityProbeDatabase


async def test_normalized_ownership_crosses_pages_but_not_entity_kinds(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, _probe = identity_probe_db
    async with factory.begin() as session:
        series = Series(title="Same numeric key", sort_title="same numeric key", comicvine_id=42)
        session.add(series)
        await session.flush()
        session.add(Issue(series_id=series.id, issue_number=1, comicvine_id=42))
        arcs = [
            StoryArc(name=f"Arc {i}", comicvine_id=value) for i, value in enumerate([42, None, 99])
        ]
        session.add_all(arcs)
        await session.flush()
        session.add_all(
            [
                StoryArcExternalIdentity(
                    story_arc_id=arc.id,
                    source="comicvine",
                    namespace="story_arc",
                    external_id=value,
                )
                for arc, value in zip(arcs, ["00042", " 42 ", "98"], strict=True)
            ]
        )
    async with factory() as session:
        before = (await session.execute(select(StoryArcExternalIdentity.__table__))).all()
        for kind in (MetadataEntityKind.SERIES, MetadataEntityKind.ISSUE):
            report = await audit_legacy_identities(session, kind, page_size=1)
            assert report.targets_checked == report.observations_checked == 1
            assert report.complete and not report.has_blockers
        report = await audit_legacy_identities(session, MetadataEntityKind.STORY_ARC, page_size=1)
        assert report == await audit_legacy_identities(
            session, MetadataEntityKind.STORY_ARC, page_size=500
        )
        assert report.complete and report.has_blockers
        assert report.targets_checked == 3 and report.observations_checked == 5
        assert len(report.collisions) == len(report.disagreements) == 1
        assert report.collisions[0].identity.external_id == "42"
        assert {claim.local_id for claim in report.collisions[0].claims} == {arcs[0].id, arcs[1].id}
        assert report.disagreements[0].local_id == arcs[2].id
        assert (await session.execute(select(StoryArcExternalIdentity.__table__))).all() == before
        assert not session.identity_map


async def test_duplicate_normalized_legacy_rows_are_reported_not_deleted(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    engine, factory, probe = identity_probe_db
    # Reproduce pre-migration rows without either canonical-ownership index.
    assert probe.arc_index is not None
    async with engine.begin() as connection:
        await connection.run_sync(probe.arc_index.drop)
        await connection.run_sync(drop_canonical_arc_index)
    async with factory.begin() as session:
        arc = StoryArc(name="Duplicate legacy rows", comicvine_id=42)
        session.add(arc)
        await session.flush()
        relations = [
            StoryArcExternalIdentity(
                story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id=value
            )
            for value in ["42", "00042"]
        ]
        session.add_all(relations)
    async with factory() as session:
        report = await audit_legacy_identities(session, MetadataEntityKind.STORY_ARC)
        assert report.complete and report.has_blockers
        assert report.collisions == report.disagreements == ()
        assert report.duplicate_relations[0].storage_row_ids == tuple(row.id for row in relations)
        assert (
            await session.scalars(
                select(StoryArcExternalIdentity.external_id).order_by(StoryArcExternalIdentity.id)
            )
        ).all() == ["42", "00042"]


async def test_partial_audit_preserves_invalid_evidence_and_does_not_flush_pending_changes(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, _probe = identity_probe_db
    async with factory.begin() as session:
        arcs = [StoryArc(name=f"Arc {i}", comicvine_id=value) for i, value in enumerate([0, 42])]
        session.add_all(arcs)
    async with factory() as session:
        first = await session.get(StoryArc, arcs[0].id)
        assert first is not None
        first.comicvine_id = 7
        pending = StoryArc(name="Not persisted", comicvine_id=8)
        session.add(pending)
        report = await audit_legacy_identities(
            session, MetadataEntityKind.STORY_ARC, page_size=1, max_evidence_rows=1
        )
        assert not report.complete and report.has_blockers
        assert report.stop_reason == IdentityAuditStopReason.EVIDENCE_LIMIT
        assert report.targets_checked == report.evidence_rows_checked == 1
        assert report.problems[0].local_id == first.id
        assert pending.id is None
        await session.rollback()
    async with factory() as session:
        assert (
            await session.scalars(select(StoryArc.comicvine_id).order_by(StoryArc.id))
        ).all() == [0, 42]
