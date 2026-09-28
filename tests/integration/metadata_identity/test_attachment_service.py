"""Runtime attachment, replay, ownership and compatibility on both databases."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

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
    IdentityEventActor,
    IdentityEventEvidence,
    IdentityEventReplayConflictError,
    IdentityEventRequest,
    prepare_identity_event,
)
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
    IdentityVerificationState,
)
from pullbox.models import Issue, Series
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesExternalIdentity,
    SeriesIdentityEvent,
)
from pullbox.services.metadata_identity_attachment import (
    IdentityAttachmentConflictError,
    attach_verified_identities,
)


def _request(target, external="42", *, kind=MetadataEntityKind.SERIES, parent=None):
    identity = ExternalIdentityRef(IdentityNamespace.COMICVINE, kind, external)
    return IdentityEventRequest(
        UUID(int=42),
        target,
        IdentityVerificationAction.VERIFY,
        IdentityEventEvidence(
            ExactIdentityEvidence(
                identity, IdentityEvidenceKind.PROVIDER_RESULT, MetadataSource.COMICVINE_API
            ),
            "a" * 64,
            source_identity=identity,
            parent_identity=parent,
        ),
    )


async def _series(factory, count=2, legacy=None):
    async with factory.begin() as session:
        rows = [Series(title=f"Series {i}", sort_title=f"series {i}") for i in range(count)]
        if legacy is not None:
            rows[0].comicvine_id = legacy
        session.add_all(rows)
        await session.flush()
        return [row.id for row in rows]


async def _counts(factory):
    async with factory() as session:
        return tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (SeriesExternalIdentity, SeriesIdentityEvent)
            ]
        )


async def test_attachment_dual_writes_and_replay_does_not_mutate(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    request = _request(first)
    async with factory.begin() as session:
        receipts = await attach_verified_identities(session, [request])
        assert len(receipts) == 1 and not receipts[0].replayed
    async with factory.begin() as session:
        replay = await attach_verified_identities(session, [request])
        assert replay[0].replayed and replay[0].event_id == receipts[0].event_id
        owner = await session.scalar(select(SeriesExternalIdentity))
        assert owner.series_id == first and owner.external_id == "42" and owner.revision == 1
        assert owner.verified_at.tzinfo is not None and owner.last_seen_at == owner.verified_at
        assert (await session.get(Series, first)).comicvine_id == 42
        history = await session.scalar(select(SeriesIdentityEvent))
        assert history.request_json == prepare_identity_event(request).request_json
    assert await _counts(factory) == (1, 1)


async def test_attachment_never_commits_even_when_sqlite_starts_with_savepoint(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory() as session:
        await attach_verified_identities(session, [_request(first)])
        await session.rollback()
    assert await _counts(factory) == (0, 0)
    async with factory() as session:
        assert (await session.get(Series, first)).comicvine_id is None


@pytest.mark.parametrize(
    "collision", ["legacy_owner", "legacy_target", "active_owner", "active_target"]
)
async def test_attachment_conflicts_roll_back_entire_batch_even_if_caller_catches(
    identity_probe_db, collision
):
    _, factory, _ = identity_probe_db
    first, second, other = await _series(factory, 3)
    async with factory.begin() as session:
        if collision.startswith("legacy"):
            target = other if collision == "legacy_owner" else second
            value = 43 if collision == "legacy_owner" else 99
            await session.execute(
                update(Series).where(Series.id == target).values(comicvine_id=value)
            )
        else:
            session.add(
                SeriesExternalIdentity(
                    series_id=other if collision == "active_owner" else second,
                    identity_namespace=IdentityNamespace.COMICVINE,
                    external_id="43" if collision == "active_owner" else "99",
                    verification_state=IdentityVerificationState.STALE,
                    evidence_kind=IdentityEvidenceKind.LEGACY_BACKFILL,
                )
            )
    async with factory.begin() as session:
        with pytest.raises(IdentityAttachmentConflictError):
            await attach_verified_identities(session, [_request(first), _request(second, "43")])
    assert (await _counts(factory))[1] == 0
    async with factory() as session:
        assert (await session.get(Series, first)).comicvine_id is None


@pytest.mark.parametrize("state", ["rejected", "conflicted"])
async def test_attachment_preserves_retained_decisions_without_active_owner(
    identity_probe_db, state
):
    _, factory, _ = identity_probe_db
    first, second = await _series(factory)
    request = _request(first)
    prior = prepare_identity_event(replace(request, operation_id=uuid4()))
    async with factory.begin() as session:
        session.add(
            SeriesIdentityEvent(
                series_id=first,
                identity_namespace=IdentityNamespace.COMICVINE,
                external_id="42",
                verification_state=IdentityVerificationState(state),
                evidence_kind=IdentityEvidenceKind.COMICINFO_XML,
                event_key=prior.event_key,
                request_fingerprint=prior.request_fingerprint,
                request_json=prior.request_json,
            )
        )
    async with factory.begin() as session:
        with pytest.raises(IdentityReviewRequiredError):
            await attach_verified_identities(session, [request])
        # Rejected candidates never reserve ownership on other local entities.
        await attach_verified_identities(session, [_request(second)])
    assert await _counts(factory) == (1, 2)


async def test_replay_does_not_restore_released_ownership(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first)])
    async with factory.begin() as session:
        await session.execute(delete(SeriesExternalIdentity))
        await session.execute(update(Series).values(comicvine_id=None))
    async with factory.begin() as session:
        receipt = await attach_verified_identities(session, [_request(first)])
        assert len(receipt) == 1 and receipt[0].replayed
    assert await _counts(factory) == (0, 1)


async def test_replay_rejects_changed_parent_or_corrupt_payload(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first)])
    async with factory.begin() as session:
        await session.execute(update(SeriesIdentityEvent).values(request_json='{"changed":true}'))
    async with factory.begin() as session:
        with pytest.raises(IdentityEventReplayConflictError):
            await attach_verified_identities(session, [_request(first)])


@pytest.mark.parametrize("mode", ["same_retry", "different_observation", "competing_owner"])
async def test_concurrent_attachments_serialize_without_duplicate_events(identity_probe_db, mode):
    _, factory, _ = identity_probe_db
    first, second = await _series(factory)
    requests = [_request(first), _request(second if mode == "competing_owner" else first)]
    if mode == "different_observation":
        requests[1] = replace(requests[1], operation_id=uuid4())

    async def attach(request):
        try:
            async with factory.begin() as session:
                return (await attach_verified_identities(session, [request]))[0]
        except IdentityAttachmentConflictError:
            return "conflict"

    receipts = await asyncio.gather(*(attach(request) for request in requests))
    assert receipts.count("conflict") == (1 if mode == "competing_owner" else 0)
    assert await _counts(factory) == (1, 2 if mode == "different_observation" else 1)


@pytest.mark.parametrize(
    "problem", [None, "wrong_parent", "missing_parent", "stale_parent", "legacy_parent_drift"]
)
async def test_issue_attachment_requires_verified_parent_agreement(identity_probe_db, problem):
    _, factory, _ = identity_probe_db
    first, second = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first), _request(second, "43")])
        issue = Issue(series_id=first, issue_number=1)
        session.add(issue)
        await session.flush()
        issue_id = issue.id
        if problem == "stale_parent":
            await session.execute(update(SeriesExternalIdentity).values(verification_state="stale"))
        if problem == "legacy_parent_drift":
            await session.execute(update(Series).where(Series.id == first).values(comicvine_id=99))
    parent = ExternalIdentityRef(
        IdentityNamespace.COMICVINE,
        MetadataEntityKind.SERIES,
        "43" if problem == "wrong_parent" else "42",
    )
    request = _request(
        issue_id,
        "91",
        kind=MetadataEntityKind.ISSUE,
        parent=None if problem == "missing_parent" else parent,
    )
    async with factory.begin() as session:
        if problem:
            with pytest.raises(IdentityAttachmentConflictError):
                await attach_verified_identities(session, [request])
        else:
            receipts = await attach_verified_identities(session, [request])
            assert len(receipts) == 1
            assert (await session.get(Issue, issue_id)).comicvine_id == 91
            assert (await session.scalar(select(IssueExternalIdentity))).external_id == "91"
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == (
            0 if problem else 1
        )


async def test_attachment_batches_queries_and_rolls_back_across_batch_boundary(identity_probe_db):
    engine, factory, _ = identity_probe_db
    ids = await _series(factory, 450)
    statements = []

    def track(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            receipts = await attach_verified_identities(
                session, [_request(i, str(i + 100)) for i in ids]
            )
            assert len(receipts) == 450
        assert len(statements) < 60, "Identity attachment must not query per issue"
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)
    assert await _counts(factory) == (450, 450)


async def test_rejects_unsupported_actions_and_missing_targets(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        with pytest.raises(IdentityAttachmentConflictError):
            await attach_verified_identities(session, [_request(999)])
        request = replace(
            _request(999),
            action=IdentityVerificationAction.CONFIRM,
            actor=IdentityEventActor.USER,
            actor_user_id=1,
            review_revision=1,
        )
        with pytest.raises(ValueError):
            await attach_verified_identities(session, [request])


@pytest.mark.parametrize("preexisting", [False, True])
async def test_multiple_observations_of_same_claim_in_batch_share_one_owner(
    identity_probe_db, preexisting
):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    original = _request(first)
    if preexisting:
        async with factory.begin() as session:
            await attach_verified_identities(session, [original])
    requests = [replace(original, operation_id=uuid4()) for _ in range(2)]
    async with factory.begin() as session:
        receipts = await attach_verified_identities(session, [*requests, requests[0]])
        assert len(receipts) == 3 and receipts[0].event_id == receipts[-1].event_id
    assert await _counts(factory) == (1, 3 if preexisting else 2)
    async with factory() as session:
        assert (await session.scalar(select(SeriesExternalIdentity))).revision == (
            3 if preexisting else 2
        )


async def test_batch_limit_keeps_sqlite_bind_counts_below_999(identity_probe_db):
    engine, factory, _ = identity_probe_db
    if engine.dialect.name != "sqlite":
        return
    ids = await _series(factory, 210)
    bind_counts = []

    def track(_conn, _cursor, _statement, parameters, _context, executemany):
        # executemany binds one parameter row at a time, unlike insertmanyvalues.
        bind_counts.append(
            len(parameters[0])
            if executemany and isinstance(parameters[0], tuple)
            else len(parameters)
        )

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            await attach_verified_identities(session, [_request(i, str(i + 100)) for i in ids])
        assert max(bind_counts) <= 999
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)


async def test_late_batch_conflict_rolls_back_earlier_batches(identity_probe_db):
    _, factory, _ = identity_probe_db
    ids = await _series(factory, 210)
    requests = [_request(i, str(i + 100)) for i in ids]
    async with factory.begin() as session:
        await session.execute(update(Series).where(Series.id == ids[-1]).values(comicvine_id=9999))
    async with factory.begin() as session:
        with pytest.raises(IdentityAttachmentConflictError):
            await attach_verified_identities(session, requests)
    assert await _counts(factory) == (0, 0)
    async with factory() as session:
        assert (await session.get(Series, ids[0])).comicvine_id is None


@pytest.mark.parametrize("state", ["stale", "conflicted"])
async def test_active_state_cannot_be_cleared_without_review(identity_probe_db, state):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first)])
    async with factory.begin() as session:
        await session.execute(update(SeriesExternalIdentity).values(verification_state=state))
    request = replace(_request(first), operation_id=uuid4())
    async with factory.begin() as session:
        if state == "conflicted":
            with pytest.raises(IdentityReviewRequiredError):
                await attach_verified_identities(session, [request])
        else:
            await attach_verified_identities(session, [request])
        owner = await session.scalar(select(SeriesExternalIdentity))
        assert owner.verification_state == ("conflicted" if state == "conflicted" else "verified")


async def test_other_namespace_does_not_overwrite_comicvine_compatibility(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory, legacy=42)
    request = _request(first)
    identity = ExternalIdentityRef(IdentityNamespace.METRON, MetadataEntityKind.SERIES, "77")
    metron = replace(
        request,
        evidence=IdentityEventEvidence(
            ExactIdentityEvidence(
                identity, IdentityEvidenceKind.PROVIDER_RESULT, MetadataSource.METRON_API
            ),
            "b" * 64,
            source_identity=identity,
        ),
    )
    async with factory.begin() as session:
        await attach_verified_identities(session, [request, metron])
        assert (await session.get(Series, first)).comicvine_id == 42
    assert await _counts(factory) == (2, 2)


async def test_issue_replay_rejects_different_parent_proof(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first)])
        issue = Issue(series_id=first, issue_number=1)
        session.add(issue)
        await session.flush()
        target = issue.id
    parent = ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "42")
    request = _request(target, "91", kind=MetadataEntityKind.ISSUE, parent=parent)
    async with factory.begin() as session:
        await attach_verified_identities(session, [request])
    changed = replace(
        request,
        evidence=replace(request.evidence, parent_identity=replace(parent, external_id="43")),
    )
    async with factory.begin() as session:
        with pytest.raises(IdentityEventReplayConflictError):
            await attach_verified_identities(session, [changed])


async def test_loaded_owner_stays_consistent_with_batched_update(identity_probe_db):
    _, factory, _ = identity_probe_db
    first, _ = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first)])
    async with factory.begin() as session:
        owner = await session.scalar(select(SeriesExternalIdentity))
        assert owner.revision == 1
        await attach_verified_identities(session, [replace(_request(first), operation_id=uuid4())])
        assert owner.revision == 2


async def test_mixed_entity_batches_lock_all_series_before_issues(identity_probe_db, monkeypatch):
    from pullbox.services import metadata_identity_attachment as attachment

    _, factory, _ = identity_probe_db
    first, second = await _series(factory)
    async with factory.begin() as session:
        await attach_verified_identities(session, [_request(first), _request(second, "43")])
        issues = [Issue(series_id=parent, issue_number=1) for parent in (first, second)]
        session.add_all(issues)
        await session.flush()
        issue_ids = [issue.id for issue in issues]
    original = attachment._locked_targets

    async def interleave(session, kind, ids):
        rows = await original(session, kind, ids)
        await asyncio.sleep(0.02)
        return rows

    monkeypatch.setattr(attachment, "_locked_targets", interleave)

    async def attach(index):
        own, other = (first, second)[index], (first, second)[1 - index]
        parent = ExternalIdentityRef(
            IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "42" if other == first else "43"
        )
        requests = [
            replace(_request(own, "42" if own == first else "43"), operation_id=uuid4()),
            _request(
                issue_ids[1 - index], str(91 + index), kind=MetadataEntityKind.ISSUE, parent=parent
            ),
        ]
        async with factory.begin() as session:
            return await attach_verified_identities(session, requests)

    receipts = await asyncio.wait_for(asyncio.gather(attach(0), attach(1)), timeout=10)
    assert [len(batch) for batch in receipts] == [2, 2]
