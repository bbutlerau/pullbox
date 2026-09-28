"""Durable decisions, stale reviews, and ownership on both database backends."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationAction as Action,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationState as State,
)
from pullbox.models import Base, Issue, Series, StoryArc, User
from pullbox.services.metadata_identity_attachment import (
    IdentityAttachmentConflictError,
    attach_verified_identities,
)
from pullbox.services.metadata_identity_review import (
    apply_identity_review,
    list_identity_claims,
    preview_identity_review,
    record_identity_observation,
)


def claim(local_id, kind, external="42", action=Action.OBSERVE, parent=None):
    identity = ExternalIdentityRef(IdentityNamespace.COMICVINE, kind, external)
    return IdentityEventRequest(
        uuid4(),
        local_id,
        action,
        IdentityEventEvidence(
            ExactIdentityEvidence(
                identity, IdentityEvidenceKind.PROVIDER_RESULT, MetadataSource.COMICVINE_API
            ),
            "a" * 64,
            source_identity=identity,
            parent_identity=parent,
        ),
    )


async def seed(factory, kind):
    async with factory.begin() as session:
        user = User(username="identity-reviewer", password_hash="unused")
        series = Series(title="Saved series", sort_title="saved series")
        session.add_all([user, series])
        await session.flush()
        target = series
        parent = None
        if kind is MetadataEntityKind.ISSUE:
            await attach_verified_identities(
                session, [claim(series.id, MetadataEntityKind.SERIES, "90", Action.VERIFY)]
            )
            parent = ExternalIdentityRef(
                IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "90"
            )
            target = Issue(series_id=series.id, issue_number=13, issue_number_text="13A")
            session.add(target)
        elif kind is MetadataEntityKind.STORY_ARC:
            target = StoryArc(name="Saved arc")
            session.add(target)
        await session.flush()
        return target.id, user.id, parent


async def decide(session, kind, target, event_id, user_id, action=Action.CONFIRM, preview=None):
    snapshot = preview or await preview_identity_review(session, kind, target, event_id)
    return await apply_identity_review(
        session,
        kind,
        target,
        event_id,
        action=action,
        fingerprint=snapshot["fingerprint"],
        review_revision=snapshot["review_revision"],
        actor_user_id=user_id,
    )


async def rows(session, kind, suffix):
    table = Base.metadata.tables[f"{kind.value}_{suffix}"]
    return (await session.execute(select(table).order_by(table.c.id))).mappings().all()


async def test_observation_persists_once_without_claiming_ownership(identity_probe_db):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, _, _ = await seed(factory, kind)
    request = claim(target, kind)
    async with factory.begin() as session:
        saved = await record_identity_observation(session, request)
        history = await rows(session, kind, "identity_events")
        assert len(history) == 1
        assert history[0].id == saved.event_id
        assert history[0].verification_state == State.OBSERVED
        assert not await rows(session, kind, "external_identities")
        replay = await record_identity_observation(session, request)
        assert replay.replayed and replay.event_id == saved.event_id
        assert len(await rows(session, kind, "identity_events")) == 1


@pytest.mark.parametrize("kind", list(MetadataEntityKind))
async def test_review_confirm_reject_reobserve_and_reconfirm(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    target, user, parent = await seed(factory, kind)
    request = claim(target, kind, parent=parent)
    async with factory.begin() as session:
        saved = await record_identity_observation(session, request)
        assert not await rows(session, kind, "external_identities")
        snapshot = await preview_identity_review(session, kind, target, saved.event_id)
        confirmed = await decide(session, kind, target, saved.event_id, user, preview=snapshot)
        replay = await decide(session, kind, target, saved.event_id, user, preview=snapshot)
        assert replay.replayed and replay.event_id == confirmed.event_id
        owner = (await rows(session, kind, "external_identities"))[0]
        assert owner.external_id == "42" and owner.verification_state == State.VERIFIED
        rejected = await decide(session, kind, target, confirmed.event_id, user, Action.REJECT)
        assert not await rows(session, kind, "external_identities")
        history = await rows(session, kind, "identity_events")
        assert [row.verification_state for row in history] == [
            State.OBSERVED,
            State.VERIFIED,
            State.REJECTED,
        ]
        again = await record_identity_observation(session, replace(request, operation_id=uuid4()))
        assert (await rows(session, kind, "identity_events"))[
            -1
        ].verification_state == State.REJECTED
        with pytest.raises(IdentityReviewRequiredError):
            await attach_verified_identities(
                session, [replace(request, operation_id=uuid4(), action=Action.VERIFY)]
            )
        await decide(session, kind, target, again.event_id, user)
        assert (await rows(session, kind, "external_identities"))[0].external_id == "42"
        assert rejected.event_id != confirmed.event_id


@pytest.mark.parametrize("kind", list(MetadataEntityKind))
async def test_lifecycle_retains_owner_and_blocks_automatic_conflict_clear(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    target, user, parent = await seed(factory, kind)
    request = claim(target, kind, action=Action.VERIFY, parent=parent)
    async with factory.begin() as session:
        original = (await attach_verified_identities(session, [request]))[0]
        stale = replace(request, operation_id=uuid4(), action=Action.MARK_STALE)
        await record_identity_observation(session, stale)
        owner = (await rows(session, kind, "external_identities"))[0]
        assert owner.verification_state == State.STALE and owner.revision == 2
        assert owner.last_seen_at == owner.verified_at
        conflict = replace(request, operation_id=uuid4(), action=Action.REPORT_CONFLICT)
        await record_identity_observation(session, conflict)
        await record_identity_observation(session, replace(stale, operation_id=uuid4()))
        owner = (await rows(session, kind, "external_identities"))[0]
        assert owner.verification_state == State.CONFLICTED
        with pytest.raises(IdentityReviewRequiredError):
            await attach_verified_identities(session, [replace(request, operation_id=uuid4())])
        await decide(session, kind, target, original.event_id, user)
        assert (await rows(session, kind, "external_identities"))[
            0
        ].verification_state == State.VERIFIED


@pytest.mark.parametrize("drift", ["observation", "owner", "legacy"])
async def test_stale_review_is_rejected_without_partial_writes(identity_probe_db, drift):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    request = claim(target, kind)
    async with factory.begin() as session:
        saved = await record_identity_observation(session, request)
        snapshot = await preview_identity_review(session, kind, target, saved.event_id)
    async with factory.begin() as session:
        if drift == "observation":
            await record_identity_observation(session, replace(request, operation_id=uuid4()))
        elif drift == "owner":
            await attach_verified_identities(session, [replace(request, action=Action.VERIFY)])
        else:
            await session.execute(update(Series).where(Series.id == target).values(comicvine_id=42))
    async with factory.begin() as session:
        before = await rows(session, kind, "identity_events")
        with pytest.raises(IdentityReviewRequiredError):
            await decide(session, kind, target, saved.event_id, user, preview=snapshot)
        assert await rows(session, kind, "identity_events") == before


async def test_candidate_rejection_does_not_release_other_owned_identity(identity_probe_db):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    async with factory.begin() as session:
        await attach_verified_identities(session, [claim(target, kind, action=Action.VERIFY)])
        other = await record_identity_observation(
            session, claim(target, kind, "99", Action.REPORT_CONFLICT)
        )
        assert (await rows(session, kind, "external_identities"))[
            0
        ].verification_state == State.CONFLICTED
        with pytest.raises(IdentityAttachmentConflictError):
            await decide(session, kind, target, other.event_id, user)
        await decide(session, kind, target, other.event_id, user, Action.REJECT)
        assert (await rows(session, kind, "external_identities"))[0].external_id == "42"


async def test_cannot_steal_identity_or_detach_series_with_owned_issues(identity_probe_db):
    _, factory, _ = identity_probe_db
    issue, user, parent = await seed(factory, MetadataEntityKind.ISSUE)
    async with factory.begin() as session:
        await attach_verified_identities(
            session, [claim(issue, MetadataEntityKind.ISSUE, action=Action.VERIFY, parent=parent)]
        )
        series_id = (await session.get(Issue, issue)).series_id
        original = (await rows(session, MetadataEntityKind.SERIES, "identity_events"))[0]
        with pytest.raises(IdentityAttachmentConflictError, match="issue"):
            await decide(
                session, MetadataEntityKind.SERIES, series_id, original.id, user, Action.REJECT
            )
        other = Series(title="Other", sort_title="other")
        session.add(other)
        await session.flush()
        saved = await record_identity_observation(
            session, claim(other.id, MetadataEntityKind.SERIES, "90")
        )
        with pytest.raises(IdentityAttachmentConflictError):
            await decide(session, MetadataEntityKind.SERIES, other.id, saved.event_id, user)


async def test_review_and_observation_are_rollback_safe(identity_probe_db):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    request = claim(target, kind)
    async with factory() as session:
        await record_identity_observation(session, request)
        await session.rollback()
    async with factory.begin() as session:
        assert not await rows(session, kind, "identity_events")
        saved = await record_identity_observation(session, request)
    async with factory() as session:
        await decide(session, kind, target, saved.event_id, user)
        await session.rollback()
    async with factory() as session:
        assert not await rows(session, kind, "external_identities")
        assert len(await rows(session, kind, "identity_events")) == 1


@pytest.mark.parametrize("kind", list(MetadataEntityKind))
async def test_claim_display_reflects_active_conflict_not_old_verified_event(
    identity_probe_db, kind
):
    _, factory, _ = identity_probe_db
    target, _, parent = await seed(factory, kind)
    async with factory.begin() as session:
        original = (
            await attach_verified_identities(
                session, [claim(target, kind, action=Action.VERIFY, parent=parent)]
            )
        )[0]
        await record_identity_observation(
            session, claim(target, kind, "99", Action.REPORT_CONFLICT, parent)
        )
        preview = await preview_identity_review(session, kind, target, original.event_id)
        assert preview["verification_state"] == State.CONFLICTED
        items, total = await list_identity_claims(session, kind, target, limit=1, offset=1)
        assert total == 2 and len(items) == 1
        assert items[0]["external_id"] == "42"
        assert items[0]["verification_state"] == State.CONFLICTED


@pytest.mark.parametrize("problem", ["missing", "wrong", "malformed", "drift"])
async def test_issue_parent_proof_blocks_confirmation_but_can_be_rejected(
    identity_probe_db, problem
):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.ISSUE
    target, user, parent = await seed(factory, kind)
    if problem == "missing":
        parent = None
    elif problem == "wrong":
        parent = ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "999")
    async with factory.begin() as session:
        saved = await record_identity_observation(session, claim(target, kind, parent=parent))
        if problem == "malformed":
            table = Base.metadata.tables["issue_identity_events"]
            await session.execute(
                update(table)
                .where(table.c.id == saved.event_id)
                .values(request_json='{"invalid":true}')
            )
        preview = await preview_identity_review(session, kind, target, saved.event_id)
        if problem == "drift":
            table = Base.metadata.tables["series_external_identities"]
            await session.execute(
                update(table).values(verification_state=State.CONFLICTED, revision=2)
            )
        with pytest.raises((IdentityAttachmentConflictError, IdentityReviewRequiredError)):
            await decide(session, kind, target, saved.event_id, user, preview=preview)
        await decide(session, kind, target, saved.event_id, user, Action.REJECT)
        assert not await rows(session, kind, "external_identities")
        assert (await rows(session, kind, "identity_events"))[
            -1
        ].verification_state == State.REJECTED


@pytest.mark.parametrize("namespace", list(IdentityNamespace))
async def test_review_namespaces_remain_independent(identity_probe_db, namespace):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    from pullbox.core.metadata_identity_events import (
        IdentityEvidenceLocator,
        IdentityEvidenceRecordKind,
    )

    identity = ExternalIdentityRef(namespace, kind, "12")
    request = IdentityEventRequest(
        uuid4(),
        target,
        Action.OBSERVE,
        IdentityEventEvidence(
            ExactIdentityEvidence(identity, IdentityEvidenceKind.COMICINFO_XML),
            "a" * 64,
            locator=IdentityEvidenceLocator(IdentityEvidenceRecordKind.SERIES, target),
        ),
    )
    async with factory.begin() as session:
        saved = await record_identity_observation(session, request)
        confirmed = await decide(session, kind, target, saved.event_id, user)
        legacy = (await session.get(Series, target)).comicvine_id
        assert legacy == (12 if namespace is IdentityNamespace.COMICVINE else None)
        event = (await rows(session, kind, "identity_events"))[-1]
        assert json.loads(event.request_json)["origin"] == {
            "record_kind": "series_identity_event",
            "record_id": saved.event_id,
        }
        await decide(session, kind, target, confirmed.event_id, user, Action.REJECT)
        assert not await rows(session, kind, "external_identities")
        await session.refresh(await session.get(Series, target))
        assert (await session.get(Series, target)).comicvine_id is None


@pytest.mark.parametrize(
    "actions", [(Action.CONFIRM, Action.CONFIRM), (Action.CONFIRM, Action.REJECT)]
)
async def test_concurrent_reviews_serialize_or_replay_without_lost_decisions(
    identity_probe_db, actions
):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    async with factory.begin() as session:
        saved = await record_identity_observation(session, claim(target, kind))
        snapshot = await preview_identity_review(session, kind, target, saved.event_id)

    async def run(action):
        async with factory.begin() as session:
            try:
                return await decide(
                    session, kind, target, saved.event_id, user, action, preview=snapshot
                )
            except IdentityReviewRequiredError:
                return None

    receipts = await asyncio.wait_for(asyncio.gather(*(run(action) for action in actions)), 15)
    assert (
        len([receipt for receipt in receipts if receipt is not None and not receipt.replayed]) == 1
    )
    if actions[0] is actions[1]:
        assert all(receipt is not None for receipt in receipts)
    else:
        assert receipts.count(None) == 1
    async with factory() as session:
        assert len(await rows(session, kind, "identity_events")) == 2


async def test_old_successful_confirmation_replay_never_revives_rejected_owner(identity_probe_db):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    target, user, _ = await seed(factory, kind)
    async with factory.begin() as session:
        saved = await record_identity_observation(session, claim(target, kind))
        snapshot = await preview_identity_review(session, kind, target, saved.event_id)
        confirmed = await decide(session, kind, target, saved.event_id, user, preview=snapshot)
        await decide(session, kind, target, confirmed.event_id, user, Action.REJECT)
        replay = await decide(session, kind, target, saved.event_id, user, preview=snapshot)
        assert replay.replayed
        assert not await rows(session, kind, "external_identities")


async def test_two_targets_cannot_confirm_the_same_unowned_identity(identity_probe_db):
    _, factory, _ = identity_probe_db
    kind = MetadataEntityKind.SERIES
    first, user, _ = await seed(factory, kind)
    async with factory.begin() as session:
        other = Series(title="Another series", sort_title="another series")
        session.add(other)
        await session.flush()
        targets = [first, other.id]
        snapshots = []
        for target in targets:
            saved = await record_identity_observation(session, claim(target, kind))
            snapshots.append(
                (
                    saved.event_id,
                    await preview_identity_review(session, kind, target, saved.event_id),
                )
            )

    async def run(target, snapshot):
        async with factory.begin() as session:
            try:
                return await decide(session, kind, target, snapshot[0], user, preview=snapshot[1])
            except (IdentityAttachmentConflictError, IdentityReviewRequiredError):
                return None

    receipts = await asyncio.wait_for(
        asyncio.gather(
            *(run(target, snapshot) for target, snapshot in zip(targets, snapshots, strict=True))
        ),
        15,
    )
    assert receipts.count(None) == 1
    async with factory() as session:
        assert len(await rows(session, kind, "external_identities")) == 1
        assert len(await rows(session, kind, "identity_events")) == 3


@pytest.mark.parametrize("action", [Action.VERIFY, Action.CONFIRM, Action.REJECT])
async def test_observation_cannot_be_used_as_review_or_automatic_verification(
    identity_probe_db, action
):
    _, factory, _ = identity_probe_db
    target, user, _ = await seed(factory, MetadataEntityKind.SERIES)
    from pullbox.core.metadata_identity_events import IdentityEventActor

    request = claim(target, MetadataEntityKind.SERIES, action=Action.VERIFY)
    # Confirm/reject requests need a typed actor, but that alone is not authorization.
    if action is not Action.VERIFY:
        request = replace(
            claim(target, MetadataEntityKind.SERIES),
            action=action,
            actor=IdentityEventActor.USER,
            actor_user_id=user,
            review_revision=1,
        )
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="non-verifying"):
            await record_identity_observation(session, request)
