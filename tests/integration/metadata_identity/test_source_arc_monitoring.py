"""Scheduled source-backed arcs retain the same ownership and review boundaries."""

import asyncio

import pytest
from sqlalchemy import select, update

from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import IssueStoryArc, StoryArcLifecycle
from pullbox.providers.metadata import sources
from pullbox.schemas.metadata_sources import SourceCapability, SourceStatus
from pullbox.services.metadata_arc_commands import source_arc_add_transaction
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.tasks import story_arc_metadata_task as task
from tests.integration.metadata_identity.test_source_arc_commands import prepared
from tests.unit.test_metadata_discovery import registration
from tests.unit.test_source_arc_catalog_fetch import CatalogAdapter


async def setup(factory, tmp_path, monkeypatch):
    preview, decision = await prepared(factory, tmp_path)
    decision.monitored = True
    decision.skipped_issue_ids = ["1000"]
    async with (
        factory() as session,
        source_arc_add_transaction(session, preview, decision) as result,
    ):
        arc_id = result.arc.id
    adapter = CatalogAdapter(2)
    monkeypatch.setattr(task, "get_session_factory", lambda: factory)
    monkeypatch.setattr(
        sources,
        "metadata_sources",
        lambda: {adapter.source: registration(adapter, capabilities=list(SourceCapability))},
    )
    return arc_id, adapter


async def test_scheduled_metron_arc_needs_no_comicvine_and_retains_skips(
    identity_probe_db, tmp_path, monkeypatch
):
    engine, factory, _ = identity_probe_db
    arc_id, adapter = await setup(factory, tmp_path, monkeypatch)
    original_call = adapter._call

    async def outside_transaction(*args):
        assert engine.pool.checkedout() == 0
        await original_call(*args)

    adapter._call = outside_transaction
    await task.sync_story_arc_metadata()
    await task.sync_story_arc_metadata()
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.comicvine_id is None
        rows = list(
            await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.sequence_number))
        )
        assert [row.source_issue_id for row in rows] == ["1000", "1001"]
        assert rows[0].resolution_state.value == "skipped"
        assert not any(row.sync_eligible for row in rows)
        assert rows[1].evidence["catalog_review_required"] is True
        assert not any(await session.scalars(select(Series.monitored)))
        assert set(await session.scalars(select(Issue.issue_number_text))) == {"50-X", "2"}
        assert arc.diagnostics["provider_refresh_error"] is None
    assert len(adapter.calls) == 8 and adapter.closed == 8
    assert not list((tmp_path / "comics").iterdir())


@pytest.mark.parametrize("change", ["paused", "archived", "disabled", "stale_identity"])
async def test_unavailable_arc_or_source_performs_no_reads(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    _, adapter = await setup(factory, tmp_path, monkeypatch)
    async with factory.begin() as session:
        if change == "paused":
            await session.execute(update(StoryArc).values(monitored=False))
        elif change == "archived":
            await session.execute(update(StoryArc).values(lifecycle=StoryArcLifecycle.ARCHIVED))
        elif change == "disabled":
            await session.execute(update(MetadataSourceConfig).values(enabled=False))
        else:
            await session.execute(
                update(StoryArcExternalIdentity).values(verification_state="stale")
            )
    await task.sync_story_arc_metadata()
    assert adapter.calls == []
    async with factory() as session:
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


@pytest.mark.parametrize("change", ["paused", "revision", "source", "identity", "import"])
async def test_network_race_never_saves_stale_membership(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    _, adapter = await setup(factory, tmp_path, monkeypatch)
    original_call = adapter._call
    changed = False

    async def mutate(*args):
        nonlocal changed
        await original_call(*args)
        if changed:
            return
        changed = True
        async with factory.begin() as session:
            if change == "source":
                await session.execute(update(MetadataSourceConfig).values(revision=2))
            elif change == "identity":
                await session.execute(
                    update(StoryArcExternalIdentity).values(verification_state="stale")
                )
            elif change == "import":
                from pullbox.models.import_job import ImportJob, ImportJobStatus, ImportSourceType

                session.add(
                    ImportJob(
                        source_type=ImportSourceType.FOLDER,
                        source_path="/unused",
                        status=ImportJobStatus.SCANNING,
                    )
                )
            else:
                await session.execute(
                    update(StoryArc).values(
                        **(
                            {"monitored": False}
                            if change == "paused"
                            else {"revision": StoryArc.revision + 1}
                        )
                    )
                )

    adapter._call = mutate
    await task.sync_story_arc_metadata()
    assert changed, "The scheduled task must include a native-only Story Arc"
    async with factory() as session:
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_cancellation_closes_source_without_mutation(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    _, adapter = await setup(factory, tmp_path, monkeypatch)
    adapter.wait = asyncio.Event()
    running = asyncio.create_task(task.sync_story_arc_metadata())
    try:
        await asyncio.wait_for(adapter.started.wait(), timeout=2)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
    assert adapter.closed == 1
    async with factory() as session:
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_provider_failure_is_recorded_without_mutation(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, adapter = await setup(factory, tmp_path, monkeypatch)
    adapter.error = MetadataSourceError(SourceStatus.RATE_LIMITED, 60)
    await task.sync_story_arc_metadata()
    assert adapter.closed == 1
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.diagnostics["provider_refresh_error"]["code"] == "source_unavailable"
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_scheduled_arc_enrichment_fills_gaps_without_replacing_managed_values(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.core.metadata_identity import MetadataEntityKind
    from pullbox.services.metadata_baselines import load_metadata_baseline

    _, factory, _ = identity_probe_db
    arc_id, adapter = await setup(factory, tmp_path, monkeypatch)
    async with factory() as session:
        before = await session.get(StoryArc, arc_id)
        title = before.name
        assert before.description is None
    adapter.arc = adapter.arc.model_copy(
        update={
            "title": "Different upstream title",
            "description": "A newly supplied description",
        }
    )
    await task.sync_story_arc_metadata()
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.name == title, "Background enrichment must not act like explicit refresh"
        assert arc.description == "A newly supplied description"
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert saved is not None and saved.snapshot.values.title == title
        assert saved.snapshot.values.description == arc.description
        assert not any(origin.user_override for origin in saved.snapshot.origins)
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 2
