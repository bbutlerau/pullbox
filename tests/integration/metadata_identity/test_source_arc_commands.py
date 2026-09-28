"""Owned arc commands commit or roll back identity history and the whole graph."""

import asyncio

import pytest
from sqlalchemy import event, func, select

from pullbox.models import Issue, Series, StoryArc
from pullbox.models.metadata_identity import (
    IssueIdentityEvent,
    SeriesIdentityEvent,
    StoryArcIdentityEvent,
)
from pullbox.schemas.metadata_arc_catalog import ArcCatalogAdd, ArcCatalogRefresh
from pullbox.services.metadata_arc_commands import (
    describe_arc_catalog,
    source_arc_add_transaction,
    source_arc_refresh_transaction,
)
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from tests.integration.metadata_identity.test_source_arc_catalog import _setup
from tests.unit.test_source_arc_catalog_fetch import CatalogAdapter, fetch


async def prepared(factory, tmp_path):
    root = await _setup(factory, tmp_path)
    preview = await fetch(CatalogAdapter(1))
    async with factory() as session:
        summary = await describe_arc_catalog(session, preview)
    return preview, ArcCatalogAdd(
        source=preview.source,
        external_id=preview.metadata.provider_id,
        source_revision=preview.source_revision,
        fingerprint=preview.fingerprint,
        file_defaults_fingerprint=summary.file_defaults_fingerprint,
        ordered_issue_ids=list(preview.metadata.issue_provider_ids),
        library_root_id=root,
    )


@pytest.mark.parametrize("failure", ["none", "response", "cancel", "commit"])
async def test_add_is_one_owned_transaction_including_response_and_commit(
    identity_probe_db, tmp_path, failure
):
    _, factory, _ = identity_probe_db
    preview, decision = await prepared(factory, tmp_path)
    async with factory() as session:
        if failure == "commit":

            def reject_commit(_session):
                raise RuntimeError("Synthetic commit rejection")

            event.listen(session.sync_session, "before_commit", reject_commit)
        try:
            async with source_arc_add_transaction(session, preview, decision) as result:
                assert result.arc.id is not None
                assert session.in_transaction()
                if failure == "response":
                    raise RuntimeError("Synthetic response failure")
                if failure == "cancel":
                    raise asyncio.CancelledError
        except (RuntimeError, asyncio.CancelledError):
            assert failure != "none"
        else:
            assert failure == "none"
        assert not session.in_transaction()
    async with factory() as session:
        for model in (
            StoryArc,
            Series,
            Issue,
            StoryArcIdentityEvent,
            SeriesIdentityEvent,
            IssueIdentityEvent,
        ):
            count = await session.scalar(select(func.count()).select_from(model))
            assert bool(count) is (failure == "none")
    assert not list((tmp_path / "comics").iterdir())


async def test_command_does_not_commit_a_callers_transaction(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    preview, decision = await prepared(factory, tmp_path)
    async with factory.begin() as session:
        other = Series(title="Unrelated work", sort_title="Unrelated work")
        session.add(other)
        with pytest.raises(ValueError, match="active transaction"):
            async with source_arc_add_transaction(session, preview, decision):
                pytest.fail("Caller-owned transactions must not be consumed")
        assert other in session.new
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Series)) == 1
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0


async def test_refresh_cancel_preserves_revision_and_membership(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    preview, decision = await prepared(factory, tmp_path)
    async with (
        factory() as session,
        source_arc_add_transaction(session, preview, decision) as result,
    ):
        arc_id, revision = result.arc.id, result.arc.revision
    refreshed = await fetch(CatalogAdapter(2))
    refresh = ArcCatalogRefresh(
        source=refreshed.source,
        external_id=refreshed.metadata.provider_id,
        source_revision=refreshed.source_revision,
        fingerprint=refreshed.fingerprint,
        expected_revision=revision,
    )
    async with factory() as session:
        with pytest.raises(asyncio.CancelledError):
            async with source_arc_refresh_transaction(session, arc_id, refreshed, refresh):
                raise asyncio.CancelledError
        assert not session.in_transaction()
    async with factory() as session:
        assert (await session.get(StoryArc, arc_id)).revision == revision
        assert await session.scalar(select(func.count()).select_from(Issue)) == 1


async def test_command_rejects_selection_swap_even_with_valid_snapshot(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    preview, decision = await prepared(factory, tmp_path)
    decision.external_id = "99"
    async with factory() as session:
        with pytest.raises(StoryArcCatalogError, match="metadata changed"):
            async with source_arc_add_transaction(session, preview, decision):
                pytest.fail("A decision cannot be rebound to another snapshot")
        assert not session.in_transaction()
