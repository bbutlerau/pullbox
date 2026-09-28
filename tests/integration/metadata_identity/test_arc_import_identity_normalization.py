"""Imports compare canonical arc claims without rewriting saved source evidence."""

import pytest
from sqlalchemy import func, select

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models import StoryArc, StoryArcExternalIdentity
from pullbox.models.story_arc import ImportedStoryArcStatus, StoryArcSourceKind
from pullbox.services.import_story_arc_materialization import materialize_confirmed_story_arcs
from pullbox.services.metadata_identity_audit import audit_legacy_identities
from tests.unit.test_import_story_arc_materialization import (
    _add_job,
    _add_staged_arc,
    _add_staged_entry,
)


@pytest.mark.parametrize("source", [StoryArcSourceKind.MYLAR3, StoryArcSourceKind.FOLDER])
@pytest.mark.parametrize("bad", [None, "4000-12", "4045-13", True, "4045-12/", " "])
async def test_import_compares_all_selected_evidence_before_any_arc_write(
    identity_probe_db, source, bad
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job = await _add_job(session)
        staged = await _add_staged_arc(
            session,
            job=job,
            name="Imported",
            source_key="source:one",
            source_arc_id="local-one",
            source_kind=source,
        )
        raw_values = ["4045-12", "00012", bad] if bad is not None else ["4045-12", "00012", " 12 "]
        entries = []
        for ordinal, value in enumerate(raw_values, 1):
            entries.append(
                await _add_staged_entry(
                    session,
                    staged_arc=staged,
                    source_ordinal=ordinal,
                    reading_order=ordinal,
                    issue_number_text=str(ordinal),
                    cv_arc_id=value,
                )
            )
        excluded = await _add_staged_entry(
            session,
            staged_arc=staged,
            source_ordinal=4,
            reading_order=4,
            issue_number_text="4",
            cv_arc_id="4045-999",
            selected=False,
        )
        other = await _add_staged_arc(
            session,
            job=job,
            name="Unrelated",
            source_key="source:two",
            source_arc_id="local-two",
            source_kind=source,
        )
        await _add_staged_entry(
            session,
            staged_arc=other,
            source_ordinal=1,
            reading_order=1,
            issue_number_text="1",
            cv_arc_id=None,
        )
        result = await materialize_confirmed_story_arcs(
            session, import_job_id=job.id, entry_checkpoint_size=1
        )
        assert other.status == ImportedStoryArcStatus.IMPORTED
        assert [row.evidence["cv_arc_id"] for row in entries] == raw_values
        assert excluded.evidence["cv_arc_id"] == "4045-999"
        if bad is None:
            assert result.arcs_created == 2 and result.arcs_failed == 0
            owner = await session.scalar(
                select(StoryArcExternalIdentity).where(
                    StoryArcExternalIdentity.source == "comicvine"
                )
            )
            assert owner.external_id == "12"
            assert staged.status == ImportedStoryArcStatus.IMPORTED
        else:
            assert result.arcs_created == 1 and result.arcs_failed == 1
            assert staged.status == ImportedStoryArcStatus.FAILED
            assert staged.materialized_story_arc_id is None
            expected = (
                "conflicting_external_identity_evidence"
                if bad == "4045-13"
                else "invalid_external_identity_evidence"
            )
            assert expected in {warning.code for warning in result.warnings}
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StoryArcExternalIdentity)
                    .where(StoryArcExternalIdentity.external_id == "local-one")
                )
                == 0
            )


@pytest.mark.parametrize("explicit", [False, True])
async def test_import_finds_existing_numeric_identity_instead_of_creating_an_alias(
    identity_probe_db, explicit
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job = await _add_job(session)
        arc = StoryArc(name="Existing", comicvine_id=12)
        session.add(arc)
        await session.flush()
        session.add(
            StoryArcExternalIdentity(
                story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id="12"
            )
        )
        staged = await _add_staged_arc(
            session,
            job=job,
            name="Imported",
            source_key="source:one",
            source_arc_id=None,
            proposed_story_arc_id=arc.id if explicit else None,
        )
        await _add_staged_entry(
            session,
            staged_arc=staged,
            source_ordinal=1,
            reading_order=1,
            issue_number_text="1",
            cv_arc_id="4045-12",
        )
        result = await materialize_confirmed_story_arcs(session, import_job_id=job.id)
        assert result.arcs_created == 0
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 1
        assert await session.scalar(select(func.count()).select_from(StoryArcExternalIdentity)) == 1
        assert result.arcs_merged == int(explicit)
        assert result.arcs_failed == int(not explicit)


async def test_legacy_audit_detects_resource_alias_collision_without_mutation(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        arcs = [StoryArc(name="First"), StoryArc(name="Second")]
        session.add_all(arcs)
        await session.flush()
        rows = [
            StoryArcExternalIdentity(
                story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id=value
            )
            for arc, value in zip(arcs, ["4045-12", "12"], strict=True)
        ]
        session.add_all(rows)
        await session.flush()
        audit = await audit_legacy_identities(session, MetadataEntityKind.STORY_ARC, page_size=1)
        assert audit.complete and audit.has_blockers
        assert not audit.problems
        assert len(audit.collisions) == 1
        assert audit.collisions[0].identity.external_id == "12"
        assert [row.external_id for row in rows] == ["4045-12", "12"]
