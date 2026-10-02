"""Canonical history follows real arc commands without taking ownership of local edits."""

from dataclasses import replace

import pytest
from sqlalchemy import func, select, update

from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.models import Issue, Series, StoryArc
from pullbox.models.metadata_baseline import (
    IssueMetadataBaseline,
    SeriesMetadataBaseline,
    StoryArcMetadataBaseline,
)
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import (
    IssueStoryArc,
    StoryArcPlacement,
    StoryArcPlacementMode,
    StoryArcPlacementOwnership,
    StoryArcResolutionState,
)
from pullbox.services.metadata_arc_catalog import project_source_arc_catalog
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.story_arc_catalog import StoryArcCatalogError, StoryArcCatalogService
from tests.integration.metadata_identity.test_source_arc_catalog import _add, _service, _setup
from tests.unit.test_source_arc_catalog_fetch import CatalogAdapter, fetch


async def _preview(count=2, **changes):
    adapter = CatalogAdapter(count)
    adapter.arc = adapter.arc.model_copy(
        update={
            "title": "Original arc",
            "description": "Provider description",
            "publisher": "Original publisher",
            "image_url": "https://static.metron.cloud/media/arc.jpg",
            **changes,
        }
    )
    return await fetch(adapter)


async def test_new_arc_member_credits_reach_library_relations(identity_probe_db, tmp_path):
    from pullbox.schemas.metadata_credits import parse_credits
    from pullbox.utilities.comicinfo_creators import load_comicinfo_creator_fields

    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview()
    evidence = preview.source_evidence
    preview = project_source_arc_catalog(
        replace(
            evidence,
            issues=tuple(
                row.model_copy(
                    update={
                        "credits": parse_credits(
                            [
                                {"name": "Arc Creator", "role": "writer"},
                            ]
                        )
                    }
                )
                for row in evidence.issues
            ),
        ),
        1,
    )
    async with factory.begin() as session:
        await _add(_service(), session, preview, root)
    async with factory() as session:
        issue_ids = list(await session.scalars(select(Issue.id)))
        assert len(issue_ids) == 2
        for issue_id in issue_ids:
            assert await load_comicinfo_creator_fields(session, issue_id) == {
                "Writer": "Arc Creator"
            }


@pytest.mark.parametrize("source", [MetadataSource.METRON_API, MetadataSource.COMICVINE_API])
async def test_arc_add_persists_graph_baselines_across_reconnect(
    identity_probe_db, tmp_path, source
):
    engine, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview()
    if source is MetadataSource.COMICVINE_API:
        async with factory.begin() as session:
            session.add(
                MetadataSourceConfig(source=source.value, enabled=True, priority=2, revision=1)
            )
        evidence = preview.source_evidence
        changes = {"source": source, "identity_namespace": source.identity_namespace}
        preview = project_source_arc_catalog(
            replace(
                evidence,
                arc=evidence.arc.model_copy(update=changes),
                series=tuple(row.model_copy(update=changes) for row in evidence.series),
                issues=tuple(row.model_copy(update=changes) for row in evidence.issues),
            ),
            1,
        )
    async with factory.begin() as session:
        arc = await _add(
            StoryArcCatalogService(source=source, source_revision=1), session, preview, root
        )
        arc_id = arc.id
    await engine.dispose()
    async with factory() as session:
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert saved is not None, "Add must retain canonical arc provenance"
        assert saved.revision == 1
        assert saved.snapshot.values.title == "Original arc"
        assert saved.snapshot.values.publisher == "Original publisher"
        assert all(origin.source == preview.source for origin in saved.snapshot.origins)
        for parent in await session.scalars(select(Series)):
            baseline = await load_metadata_baseline(session, MetadataEntityKind.SERIES, parent.id)
            assert baseline is not None, "Targeted parent seeding needs provenance too"
            assert baseline.snapshot.values.language == "en"
            assert baseline.snapshot.values.series_type == parent.series_type.value
            assert baseline.snapshot.values.sort_title == parent.sort_title
            assert parent.issue_catalog_state.value == "partial" and not parent.monitored
        for issue in await session.scalars(select(Issue)):
            baseline = await load_metadata_baseline(session, MetadataEntityKind.ISSUE, issue.id)
            assert baseline is not None
            assert baseline.snapshot.values.issue_number_text == issue.effective_issue_number_text
            assert baseline.snapshot.values.image_url == issue.cover_url
    assert not list((tmp_path / "comics").iterdir())


async def test_arc_refresh_updates_managed_fields_preserves_clears_and_member_order(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    original = await _preview()
    async with factory.begin() as session:
        arc = await _service().add(
            session,
            original,
            ordered_issue_provider_ids=["1001", "1000"],
            skipped_issue_provider_ids=["1000"],
            library_root_id=root,
        )
        arc.description = None
        arc.cover_url = None
        arc_id, revision = arc.id, arc.revision
    changed = await _preview(3, title="Updated provider arc", publisher="Updated publisher")
    async with factory.begin() as session:
        await _service().refresh(session, arc_id, changed, expected_revision=revision)
    await engine.dispose()
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.name == "Updated provider arc", "Refresh must update provider-managed names"
        assert arc.normalized_name == "updated provider arc"
        assert arc.description is None and arc.cover_url is None
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert saved is not None and saved.revision == 2
        assert saved.snapshot.values.publisher == "Updated publisher"
        origins = {origin.field: origin for origin in saved.snapshot.origins}
        assert origins["description"].user_override and origins["image_url"].user_override
        members = list(
            await session.scalars(
                select(IssueStoryArc)
                .where(IssueStoryArc.story_arc_id == arc_id)
                .order_by(IssueStoryArc.sequence_number)
            )
        )
        assert [member.source_issue_id for member in members] == ["1001", "1000", "1002"]
        assert members[1].resolution_state == StoryArcResolutionState.SKIPPED
        assert members[2].evidence["catalog_review_required"] is True
        assert not members[2].sync_eligible
    assert not list((tmp_path / "comics").iterdir())


async def test_arc_without_baseline_keeps_unknown_local_values(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview(1)
    async with factory.begin() as session:
        arc = await _add(_service(), session, preview, root)
        arc_id, revision = arc.id, arc.revision
        baseline = await session.scalar(select(StoryArcMetadataBaseline))
        if baseline is not None:
            await session.delete(baseline)
        arc.name = "My established title"
        arc.description = None
    changed = await _preview(1, title="Different upstream title")
    async with factory.begin() as session:
        await _service().refresh(session, arc_id, changed, expected_revision=revision)
        arc = await session.get(StoryArc, arc_id)
        assert arc.name == "My established title"
        assert arc.description == "Provider description", "Unknown history may fill empty fields"
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert saved is not None
        title_origin = next(item for item in saved.snapshot.origins if item.field == "title")
        assert title_origin.source is None


async def test_reused_members_keep_their_values_and_baseline_history(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    original = await _preview(1)
    async with factory.begin() as session:
        await _add(_service(), session, original, root)
        parent = await session.scalar(select(Series))
        issue = await session.scalar(select(Issue))
        parent.title, parent.path, parent.monitored = "My parent", "/unchanged", True
        issue.title, issue.status, issue.manual_skip = "My issue", "owned", True
        parent_id, issue_id = parent.id, issue.id
        before = await load_metadata_baseline(session, MetadataEntityKind.ISSUE, issue_id)
    fresh = await _preview(2)
    evidence = replace(
        fresh.source_evidence,
        arc=fresh.source_evidence.arc.model_copy(update={"external_id": "99"}),
    )
    fresh = project_source_arc_catalog(evidence, fresh.source_revision)
    async with factory.begin() as session:
        await _add(_service(), session, fresh, root)
        parent, issue = await session.get(Series, parent_id), await session.get(Issue, issue_id)
        assert (parent.title, parent.path, parent.monitored) == ("My parent", "/unchanged", True)
        assert (issue.title, issue.status, issue.manual_skip) == ("My issue", "owned", True)
        assert before is not None, "New members need baselines before another arc reuses them"
        assert await load_metadata_baseline(session, MetadataEntityKind.ISSUE, issue_id) == before
        assert await session.scalar(select(func.count()).select_from(IssueMetadataBaseline)) == 2


async def test_corrupt_arc_baseline_rejects_refresh_atomically(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    original = await _preview(1)
    async with factory.begin() as session:
        arc = await _add(_service(), session, original, root)
        arc_id, revision = arc.id, arc.revision
        await session.execute(update(StoryArcMetadataBaseline).values(snapshot_json="{}"))
    changed = await _preview(2)
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError, match="baseline"):
            await _service().refresh(session, arc_id, changed, expected_revision=revision)
        assert (await session.get(StoryArc, arc_id)).revision == revision
        assert await session.scalar(select(func.count()).select_from(Issue)) == 1


async def test_arc_caller_rollback_discards_all_baselines(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview()
    async with factory() as session:
        await _add(_service(), session, preview, root)
        assert await session.scalar(select(func.count()).select_from(StoryArcMetadataBaseline)) == 1
        await session.rollback()
    async with factory() as session:
        for model in (StoryArcMetadataBaseline, SeriesMetadataBaseline, IssueMetadataBaseline):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_refresh_retains_placed_arc_name_without_blocking_safe_updates(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    original = await _preview(1)
    async with factory.begin() as session:
        arc = await _add(_service(), session, original, root)
        member = await session.scalar(select(IssueStoryArc))
        session.add(
            StoryArcPlacement(
                issue_story_arc_id=member.id,
                placement_path="/arcs/Original arc/001.cbz",
                mode=StoryArcPlacementMode.COPY,
                ownership=StoryArcPlacementOwnership.MANAGED,
            )
        )
        arc_id, revision = arc.id, arc.revision
    changed = await _preview(2, title="Changed upstream name", description="Updated description")
    async with factory.begin() as session:
        await _service().refresh(session, arc_id, changed, expected_revision=revision)
        arc = await session.get(StoryArc, arc_id)
        assert arc.name == "Original arc", "Refresh cannot bypass the managed-placement name guard"
        assert arc.description == "Updated description"
        assert await session.scalar(select(func.count()).select_from(IssueStoryArc)) == 2
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert "title_retained_for_managed_placements" in saved.snapshot.diagnostics
        origin = next(item for item in saved.snapshot.origins if item.field == "title")
        assert not origin.user_override, "A placement constraint is not a user edit"
        placement = await session.scalar(select(StoryArcPlacement))
        assert placement.placement_path == "/arcs/Original arc/001.cbz"


async def test_large_arc_seeding_uses_bounded_baseline_batches(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import metadata_arc_baselines

    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview(205)
    actual = metadata_arc_baselines.save_metadata_baselines
    batches = []

    async def record_batch(session, writes):
        batches.append(len(writes))
        return await actual(session, writes)

    monkeypatch.setattr(metadata_arc_baselines, "save_metadata_baselines", record_batch)
    async with factory.begin() as session:
        await _add(_service(), session, preview, root)
        assert await session.scalar(select(func.count()).select_from(IssueMetadataBaseline)) == 205
        assert await session.scalar(select(func.count()).select_from(SeriesMetadataBaseline)) == 2
    assert batches == [1, 200, 5]


async def test_late_baseline_failure_rolls_back_graph_without_losing_callers_work(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import metadata_arc_baselines
    from pullbox.services.metadata_baselines import MetadataBaselineConflictError

    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    preview = await _preview(2)
    actual = metadata_arc_baselines.save_metadata_baselines

    async def reject_issue_batch(session, writes):
        if any(item.snapshot.entity_kind is MetadataEntityKind.ISSUE for item in writes):
            raise MetadataBaselineConflictError("Synthetic concurrent baseline change")
        return await actual(session, writes)

    monkeypatch.setattr(metadata_arc_baselines, "save_metadata_baselines", reject_issue_batch)
    async with factory.begin() as session:
        unrelated = Series(title="Unrelated work", sort_title="Unrelated work")
        session.add(unrelated)
        await session.flush()
        with pytest.raises(StoryArcCatalogError, match="baseline"):
            await _add(_service(), session, preview, root)
        assert await session.scalar(select(Series.title)) == "Unrelated work"
        for model in (
            StoryArc,
            Issue,
            StoryArcMetadataBaseline,
            SeriesMetadataBaseline,
            IssueMetadataBaseline,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
