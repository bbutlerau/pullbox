"""Monitoring and reading-order approval are independent of issue identity."""

from dataclasses import replace
from unittest.mock import AsyncMock, Mock

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.issue import Issue
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArcResolutionState
from pullbox.providers.metadata import sources
from pullbox.services.metadata_discovery import SourceRegistration
from pullbox.services.story_arc_service import StoryArcService


async def _configure_source(session, monkeypatch, provider):
    from tests.unit.test_story_arc_catalog import _series

    provider.close = AsyncMock()
    provider.get_series = AsyncMock(side_effect=lambda value, **_: _series(value))
    session.add(MetadataSourceConfig(source="comicvine_api", enabled=True, priority=1, revision=1))
    await session.commit()
    capabilities = sources.comicvine_sources()[MetadataSource.COMICVINE_API].capabilities
    monkeypatch.setattr(
        sources,
        "metadata_sources",
        lambda: {
            MetadataSource.COMICVINE_API: SourceRegistration(
                capabilities, lambda _: sources.ComicVineApiSource(provider)
            )
        },
    )


async def test_order_review_survives_sync_toggle_and_skip_until_explicit_confirmation(db_session):
    service = StoryArcService()
    issue = Issue(series=Series(title="Parent", sort_title="parent"), issue_number=1)
    db_session.add(issue)
    await db_session.flush()
    arc = await service.create(db_session, name="Arc", monitored=True)
    member = await service.add_membership(db_session, arc.id, issue_id=issue.id, sequence_number=1)
    member.evidence = {"catalog_review_required": True}
    await service.update(db_session, arc.id, expected_revision=arc.revision, sync_enabled=True)
    assert member.resolution_state is StoryArcResolutionState.RESOLVED
    assert member.sync_eligible is False
    await service.update_membership(db_session, member.id, intentionally_skipped=True)
    await service.update_membership(db_session, member.id, intentionally_skipped=False)
    assert member.sync_eligible is False
    await service.resolve_membership(db_session, member.id, issue_id=issue.id)
    assert member.evidence["catalog_review_required"] is False
    assert member.sync_eligible is True


async def test_monitoring_task_defers_during_import(monkeypatch):
    from pullbox.tasks import story_arc_metadata_task as task

    monkeypatch.setattr(
        task, "has_active_import_scheduler_protection", AsyncMock(return_value=True)
    )
    provider = Mock()
    monkeypatch.setattr(task, "MetadataSourceRegistry", provider)
    await task.sync_story_arc_metadata()
    provider.assert_not_called()


async def test_scheduled_discovery_is_idempotent_and_keeps_paused_arcs_and_parent_series_paused(
    db_session, tmp_path, monkeypatch
):
    from pullbox.models.story_arc import IssueStoryArc, StoryArc, StoryArcLifecycle
    from pullbox.services.search_targets import load_wanted_issue_search_targets
    from pullbox.services.story_arc_catalog import StoryArcCatalogService
    from pullbox.tasks import story_arc_metadata_task as task
    from tests.unit.test_story_arc_catalog import _issue, _provider, _root

    root = await _root(db_session, tmp_path)
    service = StoryArcCatalogService(_provider([_issue()]))
    arc = await service.add(
        db_session,
        await service.preview("31"),
        ordered_issue_provider_ids=["11"],
        library_root_id=root.id,
        monitored=True,
    )
    paused = StoryArc(name="Paused", comicvine_id=32, monitored=False, search_missing=True)
    archived = StoryArc(
        name="Archived", comicvine_id=33, monitored=True, lifecycle=StoryArcLifecycle.ARCHIVED
    )
    db_session.add_all([paused, archived])
    await db_session.commit()
    provider = _provider([_issue(), _issue("12", "22", "1000000")])
    await _configure_source(db_session, monkeypatch, provider)
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    monkeypatch.setattr(task, "get_session_factory", lambda: factory)
    await task.sync_story_arc_metadata()
    await task.sync_story_arc_metadata()
    assert [call.args[0] for call in provider.get_story_arc.await_args_list] == ["31"] * 4
    assert provider.close.await_count == 8
    async with factory() as session:
        members = list(
            await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.sequence_number))
        )
        assert len(members) == 2
        assert members[1].resolution_state is StoryArcResolutionState.RESOLVED
        assert members[1].sync_eligible is False
        assert members[1].evidence["catalog_review_required"] is True
        assert len(await load_wanted_issue_search_targets(session, limit=10)) == 2
        assert not any(await session.scalars(select(Series.monitored)))
        assert (await session.get(StoryArc, paused.id)).monitored is False
        assert (await session.get(StoryArc, arc.id)).diagnostics["provider_refresh_error"] is None


async def test_pause_during_network_fetch_prevents_membership_write(
    db_session, tmp_path, monkeypatch
):
    from pullbox.models.story_arc import IssueStoryArc, StoryArc
    from pullbox.services.story_arc_catalog import StoryArcCatalogService
    from pullbox.tasks import story_arc_metadata_task as task
    from tests.unit.test_story_arc_catalog import _issue, _provider, _root

    root = await _root(db_session, tmp_path)
    service = StoryArcCatalogService(_provider([_issue()]))
    arc = await service.add(
        db_session,
        await service.preview("31"),
        ordered_issue_provider_ids=["11"],
        library_root_id=root.id,
        monitored=True,
    )
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    provider = _provider()
    await _configure_source(db_session, monkeypatch, provider)
    metadata = provider.get_story_arc.return_value

    async def pause_before_returning(_):
        async with factory() as session:
            current = await session.get(StoryArc, arc.id)
            await StoryArcService().update(
                session, arc.id, expected_revision=current.revision, monitored=False
            )
            await session.commit()
        return metadata

    provider.get_story_arc.side_effect = pause_before_returning
    assert await task._refresh_arc(factory, arc.id) is None
    provider.get_story_arc.assert_awaited()
    async with factory() as session:
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_failed_arc_refresh_does_not_stop_later_arcs(db_session, tmp_path, monkeypatch):
    from pullbox.models.story_arc import IssueStoryArc, StoryArc
    from pullbox.providers.metadata.comicvine import ComicVineError
    from pullbox.tasks import story_arc_metadata_task as task
    from tests.unit.test_story_arc_catalog import _provider, _root

    root = await _root(db_session, tmp_path)
    root.is_default_managed_destination = True
    db_session.add_all(
        [StoryArc(name=f"Arc {n}", monitored=True, comicvine_id=n) for n in (31, 32)]
    )
    await db_session.commit()
    factory = async_sessionmaker(db_session.bind, expire_on_commit=False)
    provider = _provider()
    metadata = provider.get_story_arc.return_value

    async def get_arc(external_id):
        if external_id == "31":
            raise ComicVineError(503, "unavailable")
        return replace(metadata, provider_id=external_id)

    provider.get_story_arc.side_effect = get_arc
    await _configure_source(db_session, monkeypatch, provider)
    monkeypatch.setattr(task, "get_session_factory", lambda: factory)
    await task.sync_story_arc_metadata()
    assert [call.args[0] for call in provider.get_story_arc.await_args_list] == ["31", "32", "32"]
    assert provider.close.await_count == 5
    async with factory() as session:
        first = await session.scalar(select(StoryArc).where(StoryArc.comicvine_id == 31))
        second = await session.scalar(select(StoryArc).where(StoryArc.comicvine_id == 32))
        assert first.diagnostics["provider_refresh_error"]["code"] == "source_unavailable"
        assert second.diagnostics["provider_refresh_error"] is None
        assert list(await session.scalars(select(IssueStoryArc.story_arc_id))) == [second.id] * 2
