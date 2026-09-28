"""Canonical arc attachment shares ownership/history without claiming import scopes."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import delete, event, func, select, update

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import (
    IdentityEventEvidence,
    IdentityEventRequest,
    prepare_identity_event,
)
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
)
from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesIdentityEvent,
    StoryArcIdentityEvent,
)
from pullbox.services.metadata_identity_attachment import (
    IdentityAttachmentConflictError,
    attach_verified_identities,
)
from pullbox.services.story_arc_catalog import StoryArcCatalogService
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from tests.integration.metadata_identity.test_attachment_service import _request
from tests.unit.test_story_arc_catalog import _provider, _root


def _arc_request(target, external="31", namespace=IdentityNamespace.COMICVINE):
    identity = ExternalIdentityRef(namespace, MetadataEntityKind.STORY_ARC, external)
    if namespace is IdentityNamespace.LOCG:
        from pullbox.core.metadata_identity_events import (
            IdentityEvidenceLocator,
            IdentityEvidenceRecordKind,
        )

        evidence = IdentityEventEvidence(
            ExactIdentityEvidence(identity, IdentityEvidenceKind.MIGRATION),
            "a" * 64,
            locator=IdentityEvidenceLocator(IdentityEvidenceRecordKind.STORY_ARC, target),
        )
    else:
        source = {
            IdentityNamespace.COMICVINE: MetadataSource.COMICVINE_API,
            IdentityNamespace.METRON: MetadataSource.METRON_API,
            IdentityNamespace.GCD: MetadataSource.GCD_API_V2,
        }[namespace]
        evidence = IdentityEventEvidence(
            ExactIdentityEvidence(identity, IdentityEvidenceKind.PROVIDER_RESULT, source),
            "a" * 64,
            source_identity=identity,
        )
    return IdentityEventRequest(uuid4(), target, IdentityVerificationAction.VERIFY, evidence)


async def _arcs(factory, count=2):
    async with factory.begin() as session:
        rows = [StoryArc(name=f"Arc {i}") for i in range(count)]
        session.add_all(rows)
        await session.flush()
        return [row.id for row in rows]


@pytest.mark.parametrize("namespace", list(IdentityNamespace))
async def test_arc_attachment_preserves_scoped_keys_and_legacy_evidence(
    identity_probe_db, namespace
):
    _, factory, _ = identity_probe_db
    first, second = await _arcs(factory)
    async with factory.begin() as session:
        session.add_all(
            [
                StoryArcExternalIdentity(
                    story_arc_id=first,
                    source=namespace.value,
                    namespace="story_arc",
                    external_id="31",
                    source_url="https://example.invalid/31",
                    evidence={"imported_story_arc_id": 100},
                ),
                StoryArcExternalIdentity(
                    story_arc_id=second,
                    source=namespace.value,
                    namespace="import-scope",
                    external_id="31",
                ),
                StoryArcExternalIdentity(
                    story_arc_id=first,
                    source="mylar3",
                    namespace="story_arc",
                    external_id="not-a-provider-id",
                ),
            ]
        )
    request = _arc_request(first, namespace=namespace)
    async with factory.begin() as session:
        receipt = (await attach_verified_identities(session, [request]))[0]
        assert not receipt.replayed
    async with factory.begin() as session:
        replay = (
            await attach_verified_identities(session, [request], require_current_ownership=True)
        )[0]
        assert replay.replayed and replay.event_id == receipt.event_id
        rows = list(
            await session.scalars(
                select(StoryArcExternalIdentity).order_by(StoryArcExternalIdentity.id)
            )
        )
        assert len(rows) == 3
        assert rows[0].verification_state == "verified" and rows[0].revision == 2
        assert rows[0].source_url == "https://example.invalid/31"
        assert rows[0].evidence == {"imported_story_arc_id": 100}
        assert rows[1].verification_state is rows[2].verification_state is None
        assert rows[1].revision == rows[2].revision == 1
        assert (await session.get(StoryArc, first)).comicvine_id == (
            31 if namespace is IdentityNamespace.COMICVINE else None
        )
        history = await session.scalar(select(StoryArcIdentityEvent))
        assert history.request_json == prepare_identity_event(request).request_json
        assert history.identity_namespace == namespace


async def test_arc_attachment_never_commits_and_rolls_back_mixed_graph(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _arcs(factory)
    async with factory.begin() as session:
        series = Series(title="Parent", sort_title="parent")
        session.add(series)
        await session.flush()
        issue = Issue(series_id=series.id, issue_number=1)
        session.add(issue)
        await session.flush()
        parent_id, issue_id = series.id, issue.id
    requests = [_arc_request(first), _request(parent_id)]
    async with factory() as session:
        await attach_verified_identities(session, requests)
        await session.rollback()
    async with factory.begin() as session:
        wrong_parent = ExternalIdentityRef(
            IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "99"
        )
        with pytest.raises(IdentityAttachmentConflictError):
            await attach_verified_identities(
                session,
                [
                    *requests,
                    _request(issue_id, "91", kind=MetadataEntityKind.ISSUE, parent=wrong_parent),
                ],
            )
    async with factory() as session:
        for model in (
            StoryArcExternalIdentity,
            StoryArcIdentityEvent,
            SeriesIdentityEvent,
            IssueIdentityEvent,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert (await session.get(StoryArc, first)).comicvine_id is None
        assert (await session.get(Series, parent_id)).comicvine_id is None


@pytest.mark.parametrize(
    "collision", ["legacy_owner", "legacy_target", "active_owner", "active_target"]
)
async def test_arc_attachment_refuses_competing_ownership(identity_probe_db, collision):
    _, factory, _ = identity_probe_db
    first, second = await _arcs(factory)
    async with factory.begin() as session:
        target = second if collision.endswith("owner") else first
        value = "31" if collision.endswith("owner") else "99"
        if collision.startswith("legacy"):
            await session.execute(
                update(StoryArc).where(StoryArc.id == target).values(comicvine_id=int(value))
            )
        else:
            session.add(
                StoryArcExternalIdentity(
                    story_arc_id=target,
                    source="comicvine",
                    namespace="story_arc",
                    external_id=value,
                )
            )
    async with factory.begin() as session:
        with pytest.raises(IdentityAttachmentConflictError):
            await attach_verified_identities(session, [_arc_request(first)])
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 0


@pytest.mark.parametrize("mode", ["retry", "observation", "competing_owner"])
async def test_arc_concurrent_writers_serialize(identity_probe_db, mode):
    _, factory, _ = identity_probe_db
    first, second = await _arcs(factory)
    original = _arc_request(first)
    other = (
        original
        if mode == "retry"
        else _arc_request(second if mode == "competing_owner" else first)
    )

    async def attach(request):
        try:
            async with factory.begin() as session:
                return (await attach_verified_identities(session, [request]))[0]
        except IdentityAttachmentConflictError:
            return "conflict"

    receipts = await asyncio.wait_for(asyncio.gather(attach(original), attach(other)), timeout=20)
    assert receipts.count("conflict") == (1 if mode == "competing_owner" else 0)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArcExternalIdentity)) == 1
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == (
            2 if mode == "observation" else 1
        )


async def test_arc_bulk_attachment_is_bounded_and_locks_before_series(identity_probe_db):
    engine, factory, _ = identity_probe_db
    arcs = await _arcs(factory, 210)
    async with factory.begin() as session:
        series = Series(title="Parent", sort_title="parent")
        session.add(series)
        await session.flush()
        series_id = series.id
    statements, binds = [], []

    def track(_conn, _cursor, statement, parameters, _context, many):
        statements.append(statement)
        binds.append(
            len(parameters[0]) if many and isinstance(parameters[0], tuple) else len(parameters)
        )

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            receipts = await attach_verified_identities(
                session,
                [_request(series_id), *[_arc_request(i, str(i + 100)) for i in reversed(arcs)]],
            )
            assert len(receipts) == 211
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)
    assert len(statements) < 65
    if engine.dialect.name == "sqlite":
        assert max(binds) <= 999
    else:
        locks = [statement for statement in statements if "FOR UPDATE" in statement]
        assert "story_arcs" in locks[0] and "story_arcs" in locks[1]
        assert "FROM series" in locks[2]


async def _adopt(factory, tmp_path):
    provider = _provider()
    service = StoryArcCatalogService(provider)
    preview = await service.preview("31")
    library = tmp_path / "comics"
    library.mkdir()
    async with factory.begin() as session:
        root = await _root(session, library)
        arc = await service.add(
            session, preview, ordered_issue_provider_ids=["11", "12"], library_root_id=root.id
        )
        return service, provider, preview, arc.id, arc.revision


async def test_catalog_arc_add_and_refresh_record_exact_evidence_without_more_fetches(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, provider, preview, arc_id, revision = await _adopt(factory, tmp_path)
    async with factory.begin() as session:
        owner = await session.scalar(select(StoryArcExternalIdentity))
        assert owner.verification_state == "verified" and owner.evidence_kind == "provider_result"
        history = await session.scalar(select(StoryArcIdentityEvent))
        assert json.loads(history.request_json)["origin"]["source_instance"] == "comicvine_api"
        assert owner.evidence["snapshot_fingerprint"] == preview.fingerprint
        assert (await session.get(StoryArc, arc_id)).comicvine_id == 31
        await service.refresh(session, arc_id, preview, expected_revision=revision)
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 1
    provider.get_story_arc.assert_awaited_once()
    provider.get_story_arc_issues.assert_awaited_once()
    assert provider.get_series.await_count == 2
    assert not list((tmp_path / "comics").iterdir())


@pytest.mark.parametrize("state", ["released", "stale", "conflicted", "rejected"])
async def test_catalog_arc_refresh_revalidates_its_own_identity(identity_probe_db, tmp_path, state):
    _, factory, _ = identity_probe_db
    service, _, preview, arc_id, revision = await _adopt(factory, tmp_path)
    async with factory.begin() as session:
        if state == "released":
            await session.execute(delete(StoryArcExternalIdentity))
        elif state == "rejected":
            original = await session.scalar(select(StoryArcIdentityEvent))
            assert original is not None, "Arc adoption must leave durable evidence"
            request = prepare_identity_event(_arc_request(arc_id))
            session.add(
                StoryArcIdentityEvent(
                    story_arc_id=arc_id,
                    identity_namespace="comicvine",
                    external_id="31",
                    verification_state="rejected",
                    evidence_kind="provider_result",
                    event_key=request.event_key,
                    request_fingerprint=request.request_fingerprint,
                    request_json=request.request_json,
                )
            )
        else:
            await session.execute(update(StoryArcExternalIdentity).values(verification_state=state))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError, match=r"identity.*review"):
            await service.refresh(session, arc_id, preview, expected_revision=revision)
        assert (await session.get(StoryArc, arc_id)).revision == revision
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 2


async def test_fresh_arc_evidence_cannot_override_retained_rejection(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _arcs(factory)
    request = _arc_request(first)
    async with factory.begin() as session:
        await attach_verified_identities(session, [request])
        await session.execute(update(StoryArcIdentityEvent).values(verification_state="rejected"))
    async with factory.begin() as session:
        with pytest.raises(IdentityReviewRequiredError):
            await attach_verified_identities(session, [replace(request, operation_id=uuid4())])


async def test_refresh_legacy_arc_keeps_import_provenance_and_missing_provider_fields(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service = StoryArcCatalogService(_provider())
    preview = await service.preview("31")
    library = tmp_path / "comics"
    library.mkdir()
    async with factory.begin() as session:
        root = await _root(session, library)
        arc = StoryArc(name="Local arc name", comicvine_id=31, source_kind="mylar3")
        session.add(arc)
        await session.flush()
        owner = StoryArcExternalIdentity(
            story_arc_id=arc.id,
            source="comicvine",
            namespace="story_arc",
            external_id="31",
            source_url="https://comicvine.gamespot.com/story-arc/4045-31/",
            evidence={"import_job_id": 7},
        )
        session.add(owner)
        await session.flush()
        arc_id, root_id, revision = arc.id, root.id, arc.revision
    async with factory.begin() as session:
        await service.refresh(
            session, arc_id, preview, expected_revision=revision, library_root_id=root_id
        )
    async with factory() as session:
        owner = await session.scalar(select(StoryArcExternalIdentity))
        assert owner.verification_state == "verified" and owner.revision == 2
        assert owner.source_url == "https://comicvine.gamespot.com/story-arc/4045-31/"
        assert owner.evidence == {"import_job_id": 7, "snapshot_fingerprint": preview.fingerprint}
        assert (await session.get(StoryArc, arc_id)).name == "Local arc name"
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 1


@pytest.mark.parametrize("member_conflict", [False, True])
async def test_arc_new_observation_and_members_are_one_rollback_boundary(
    identity_probe_db, tmp_path, member_conflict
):
    _, factory, _ = identity_probe_db
    service, provider, _, arc_id, revision = await _adopt(factory, tmp_path)
    provider.get_story_arc.return_value = replace(
        provider.get_story_arc.return_value, cover_url="https://example.invalid/new-cover.jpg"
    )
    preview = await service.preview("31")
    if member_conflict:
        async with factory.begin() as session:
            await session.execute(
                update(IssueExternalIdentity).values(verification_state="conflicted")
            )
    async with factory.begin() as session:
        if member_conflict:
            with pytest.raises(StoryArcCatalogError):
                await service.refresh(session, arc_id, preview, expected_revision=revision)
        else:
            await service.refresh(session, arc_id, preview, expected_revision=revision)
    async with factory() as session:
        arc = await session.get(StoryArc, arc_id)
        assert arc.revision == revision + (0 if member_conflict else 1)
        assert arc.cover_url == (None if member_conflict else preview.metadata.cover_url)
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == (
            1 if member_conflict else 2
        )
        assert (await session.scalar(select(StoryArcExternalIdentity))).revision == (
            1 if member_conflict else 2
        )


async def test_arc_catalog_caller_rollback_removes_whole_identity_graph(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service = StoryArcCatalogService(_provider())
    preview = await service.preview("31")
    library = tmp_path / "comics"
    library.mkdir()
    async with factory.begin() as session:
        root_id = (await _root(session, library)).id
    async with factory() as session:
        await service.add(
            session, preview, ordered_issue_provider_ids=["11", "12"], library_root_id=root_id
        )
        await session.rollback()
    async with factory() as session:
        for model in (
            StoryArc,
            StoryArcExternalIdentity,
            StoryArcIdentityEvent,
            Series,
            SeriesIdentityEvent,
            Issue,
            IssueIdentityEvent,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    assert not list(library.iterdir())
