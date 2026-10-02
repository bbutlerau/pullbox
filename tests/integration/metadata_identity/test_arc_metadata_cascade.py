"""Real Arc commands enrich descriptions without a second membership authority."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import IdentityVerificationAction
from pullbox.models import Issue, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_baseline import StoryArcMetadataBaseline
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import IssueStoryArc
from pullbox.providers.metadata import sources
from pullbox.schemas.metadata_arc_catalog import ArcCatalogRefresh
from pullbox.schemas.metadata_sources import MetadataFetch, SourceCapability, SourceStatus
from pullbox.services.metadata_arc_commands import (
    source_arc_add_transaction,
    source_arc_refresh_transaction,
)
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.services.metadata_identity_attachment import attach_verified_identities
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from pullbox.tasks import story_arc_metadata_task as task
from tests.integration.metadata_identity.test_source_arc_commands import prepared
from tests.unit.test_metadata_discovery import registration
from tests.unit.test_metadata_story_arc_reads import ArcAdapter, arc_row
from tests.unit.test_source_arc_catalog_fetch import CatalogAdapter, fetch

CV = MetadataSource.COMICVINE_API
KIND = MetadataEntityKind.STORY_ARC


async def setup(factory, tmp_path, monkeypatch, *, verified=True):
    preview, decision = await prepared(factory, tmp_path)
    decision.monitored = True
    decision.skipped_issue_ids = ["1000"]
    async with (
        factory() as session,
        source_arc_add_transaction(session, preview, decision) as result,
    ):
        arc_id, revision = result.arc.id, result.arc.revision
    async with factory.begin() as session:
        session.add(MetadataSourceConfig(source=CV.value, enabled=True, priority=1, revision=1))
        if verified:
            identity = ExternalIdentityRef(CV.identity_namespace, KIND, "98")
            await attach_verified_identities(
                session,
                [
                    IdentityEventRequest(
                        uuid4(),
                        arc_id,
                        IdentityVerificationAction.VERIFY,
                        IdentityEventEvidence(
                            ExactIdentityEvidence(
                                identity, IdentityEvidenceKind.PROVIDER_RESULT, CV
                            ),
                            "a" * 64,
                            source_identity=identity,
                        ),
                    )
                ],
            )
    adapter = ArcAdapter(
        CV,
        result=MetadataFetch(
            status=SourceStatus.OK,
            data=arc_row(
                CV,
                external_id="98",
                title="Preferred title",
                description="CV description",
                issue_external_ids=["901"],
                declared_issue_count=1,
                membership_complete=True,
            ),
        ),
    )
    native = CatalogAdapter(2)
    monkeypatch.setattr(task, "get_session_factory", lambda: factory)
    monkeypatch.setattr(
        "pullbox.services.metadata_sources.get_comicvine_api_key",
        AsyncMock(return_value="arc-cascade-test"),
    )
    monkeypatch.setattr(
        sources,
        "metadata_sources",
        lambda: {
            CV: registration(adapter, capabilities=[SourceCapability.STORY_ARC_DETAILS]),
            native.source: registration(native, capabilities=list(SourceCapability)),
        },
    )
    refreshed = await fetch(native)
    native.calls.clear()
    native.closed = 0
    refresh = ArcCatalogRefresh(
        source=refreshed.source,
        external_id=refreshed.metadata.provider_id,
        source_revision=refreshed.source_revision,
        fingerprint=refreshed.fingerprint,
        expected_revision=revision,
    )
    return arc_id, refreshed, refresh, adapter, native


async def test_manual_refresh_uses_attached_priority_without_changing_membership_source(
    identity_probe_db, tmp_path, monkeypatch
):
    engine, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, native = await setup(factory, tmp_path, monkeypatch)
    original = adapter._call

    async def unlocked(*args):
        assert engine.pool.checkedout() == 0, "Provider I/O must not retain a database transaction"
        await original(*args)

    adapter._call = unlocked
    async with (
        factory() as session,
        source_arc_refresh_transaction(session, arc_id, preview, decision) as result,
    ):
        assert result.arc.description == "CV description"
        assert result.arc.name == "Preferred title"
    async with factory() as session:
        saved = await load_metadata_baseline(session, KIND, arc_id)
        assert saved.snapshot.values.description == "CV description"
        assert next(o.source for o in saved.snapshot.origins if o.field == "description") is CV
        members = list(await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.id)))
        assert [row.source_issue_id for row in members] == ["1000", "1001"]
        assert members[0].resolution_state.value == "skipped"
        assert members[1].evidence["catalog_review_required"]
        assert not any(row.sync_eligible for row in members)
        assert set(await session.scalars(select(Issue.comicvine_id))) == {None}
    assert len(adapter.calls) == 1 and adapter.closed == 1
    assert native.calls == [], "Reuse catalog evidence instead of refetching it"


async def test_scheduled_refresh_fills_descriptive_gaps_through_same_cascade(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, _, _, adapter, native = await setup(factory, tmp_path, monkeypatch)
    await task.sync_story_arc_metadata()
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.description == "CV description"
        assert arc.name == "Shared title", "Scheduled work is gap-fill, not explicit replacement"
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 2
    assert len(adapter.calls) == 1
    assert sum(call[0] == "arc" for call in native.calls) == 1


async def test_unverified_source_never_supplies_metadata(identity_probe_db, tmp_path, monkeypatch):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(
        factory, tmp_path, monkeypatch, verified=False
    )
    async with (
        factory() as session,
        source_arc_refresh_transaction(session, arc_id, preview, decision) as result,
    ):
        assert result.arc.description is None
    assert adapter.calls == []


@pytest.mark.parametrize("change", ["edit", "clear", "identity", "policy", "baseline"])
async def test_network_race_rejects_all_metadata_and_membership_writes(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(factory, tmp_path, monkeypatch)
    original = adapter._call

    async def mutate(*args):
        await original(*args)
        async with factory.begin() as session:
            if change in {"edit", "clear"}:
                await session.execute(
                    update(StoryArc)
                    .where(StoryArc.id == arc_id)
                    .values(
                        name="My title" if change == "edit" else "Shared title",
                        cover_url=None,
                        description="Keep my edit" if change == "edit" else "",
                    )
                )
            elif change == "identity":
                await session.execute(
                    update(StoryArcExternalIdentity)
                    .where(StoryArcExternalIdentity.source == "comicvine")
                    .values(verification_state="stale", revision=2)
                )
            elif change == "policy":
                await session.execute(
                    update(MetadataSourceConfig)
                    .where(MetadataSourceConfig.source == CV.value)
                    .values(priority=9, revision=2)
                )
            else:
                await session.execute(update(StoryArcMetadataBaseline).values(revision=2))

    adapter._call = mutate
    async with factory() as session:
        with pytest.raises(StoryArcCatalogError, match="changed"):
            async with source_arc_refresh_transaction(session, arc_id, preview, decision):
                pytest.fail("Stale descriptive reads must not update membership either")
        assert not session.in_transaction()
    async with factory() as session:
        assert (await session.get(StoryArc, arc_id)).revision == decision.expected_revision
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_cancelled_secondary_read_closes_client_without_library_write(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(factory, tmp_path, monkeypatch)
    adapter.wait = asyncio.Event()

    async def refresh():
        async with (
            factory() as session,
            source_arc_refresh_transaction(session, arc_id, preview, decision),
        ):
            pytest.fail("A cancelled read cannot commit")

    running = asyncio.create_task(refresh())
    try:
        await asyncio.wait_for(adapter.started.wait(), 2)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
    assert adapter.closed == 1
    async with factory() as session:
        assert (await session.get(StoryArc, arc_id)).revision == decision.expected_revision
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1


async def test_secondary_failure_keeps_safe_catalog_progress_and_records_outcome(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(factory, tmp_path, monkeypatch)
    adapter.error = MetadataSourceError(SourceStatus.RATE_LIMITED, 60)
    async with (
        factory() as session,
        source_arc_refresh_transaction(session, arc_id, preview, decision) as result,
    ):
        assert result.arc.description is None
        outcomes = result.arc.diagnostics.get("metadata_refresh", {}).get("outcomes", [])
        assert {"source": CV.value, "status": "rate_limited", "retry_after_seconds": 60} in outcomes
    async with factory() as session:
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 2


async def test_corrupt_baseline_returns_safe_domain_error_without_network(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(factory, tmp_path, monkeypatch)
    async with factory.begin() as session:
        await session.execute(update(StoryArcMetadataBaseline).values(snapshot_json="{}"))
    async with factory() as session:
        with pytest.raises(StoryArcCatalogError, match="baseline"):
            async with source_arc_refresh_transaction(session, arc_id, preview, decision):
                pytest.fail("Invalid baseline must stop the entire refresh")
        assert not session.in_transaction()
    assert not adapter.calls


async def test_conflicting_secondary_crosswalk_stops_membership_and_metadata(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    arc_id, preview, decision, adapter, _ = await setup(factory, tmp_path, monkeypatch)
    adapter.result.data.cross_identities = [
        ExternalIdentityRef(MetadataSource.METRON_API.identity_namespace, KIND, "999")
    ]
    async with factory() as session:
        with pytest.raises(StoryArcCatalogError, match="needs review"):
            async with source_arc_refresh_transaction(session, arc_id, preview, decision):
                pytest.fail("Priority cannot conceal exact disagreement")
    async with factory() as session:
        assert (await session.get(StoryArc, arc_id)).revision == decision.expected_revision
        assert len(list(await session.scalars(select(IssueStoryArc)))) == 1
