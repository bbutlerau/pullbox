"""Verified import arc identities and guarded, history-preserving journal undo."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import false, select, update

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_events import (
    IdentityEventActor,
    IdentityEventEvidence,
    IdentityEventRequest,
    IdentityEvidenceLocator,
    IdentityEvidenceRecordKind,
    prepare_identity_event,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationAction,
    IdentityVerificationState,
)
from pullbox.models.import_job import ImportJobAction
from pullbox.models.metadata_identity import StoryArcIdentityEvent
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity, StoryArcSourceKind
from pullbox.services.metadata_identity_attachment import attach_verified_identities

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.models.import_job import ImportJob
    from pullbox.models.story_arc_import import ImportedStoryArc
    from pullbox.services.import_job_execution_types import RecordActionFunc

IDENTITY_VERIFIED_ACTION = "story_arc_identity_verified"


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_json(value).encode("ascii")).hexdigest()


def identity_state(identity: StoryArcExternalIdentity) -> dict[str, Any]:
    """Persist a complete proof snapshot, including the monotonic revision."""
    return {
        "story_arc_id": identity.story_arc_id,
        "source": identity.source,
        "namespace": identity.namespace,
        "external_id": identity.external_id,
        "source_url": identity.source_url,
        "evidence": identity.evidence,
        "verification_state": identity.verification_state,
        "evidence_kind": identity.evidence_kind,
        "evidence_locator": identity.evidence_locator,
        "verified_at": identity.verified_at.isoformat() if identity.verified_at else None,
        "last_seen_at": identity.last_seen_at.isoformat() if identity.last_seen_at else None,
        "created_at": identity.created_at.isoformat(),
        "revision": identity.revision,
    }


async def _latest_event(
    session: AsyncSession, arc_id: int, source: str
) -> StoryArcIdentityEvent | None:
    result: StoryArcIdentityEvent | None = await session.scalar(
        select(StoryArcIdentityEvent)
        .where(
            StoryArcIdentityEvent.story_arc_id == arc_id,
            StoryArcIdentityEvent.identity_namespace == source,
        )
        .order_by(StoryArcIdentityEvent.id.desc())
        .limit(1)
        .execution_options(populate_existing=True)
    )
    return result


async def attach_import_arc_identity(
    session: AsyncSession,
    *,
    arc: StoryArc,
    staged: ImportedStoryArc,
    external_id: str,
    evidence_revision: str,
    job: ImportJob,
    record_action: RecordActionFunc | None,
) -> str:
    """Attach exact saved evidence without treating a source key as a provider ID."""
    # The materializer owns an outer per-arc savepoint and locks existing arcs.
    await session.flush()
    await session.refresh(arc)
    owner = await session.scalar(
        select(StoryArcExternalIdentity)
        .where(
            StoryArcExternalIdentity.story_arc_id == arc.id,
            StoryArcExternalIdentity.source == "comicvine",
            StoryArcExternalIdentity.namespace == "story_arc",
        )
        .execution_options(populate_existing=True)
    )
    before = identity_state(owner) if owner is not None else None
    cv_before = arc.comicvine_id
    prior_event = await _latest_event(session, arc.id, "comicvine")
    request = IdentityEventRequest(
        uuid5(NAMESPACE_URL, f"pullbox:import-story-arc:{job.id}:{staged.id}"),
        arc.id,
        IdentityVerificationAction.VERIFY,
        IdentityEventEvidence(
            ExactIdentityEvidence(
                ExternalIdentityRef(
                    IdentityNamespace.COMICVINE, MetadataEntityKind.STORY_ARC, external_id
                ),
                IdentityEvidenceKind.MYLAR_DATABASE
                if staged.source_kind is StoryArcSourceKind.MYLAR3
                else IdentityEvidenceKind.MIGRATION,
            ),
            evidence_revision,
            locator=IdentityEvidenceLocator(
                IdentityEvidenceRecordKind.IMPORTED_STORY_ARC, staged.id
            ),
        ),
    )
    receipt = (
        await attach_verified_identities(session, [request], require_current_ownership=True)
    )[0]
    await session.refresh(arc)
    owner = await session.scalar(
        select(StoryArcExternalIdentity)
        .where(
            StoryArcExternalIdentity.story_arc_id == arc.id,
            StoryArcExternalIdentity.source == "comicvine",
            StoryArcExternalIdentity.namespace == "story_arc",
        )
        .execution_options(populate_existing=True)
    )
    assert owner is not None
    if not receipt.replayed and record_action is not None:
        await record_action(
            session,
            job,
            phase="story_arcs",
            action_type=IDENTITY_VERIFIED_ACTION,
            payload={
                "schema_version": 1,
                "story_arc_id": arc.id,
                "imported_story_arc_id": staged.id,
                "external_identity_id": owner.id,
                "identity_event_id": receipt.event_id,
                "previous_event_id": prior_event.id if prior_event else None,
                "restore_before": before,
                "expected_after": identity_state(owner),
                "comicvine_id_before": cv_before,
                "comicvine_id_after": arc.comicvine_id,
            },
        )
    return "created" if before is None else "reused"


def _rollback_request(
    action: ImportJobAction, restored_revision: int | None
) -> tuple[str, str, str]:
    payload = action.payload
    expected = payload["expected_after"]
    prepared = prepare_identity_event(
        IdentityEventRequest(
            uuid5(
                NAMESPACE_URL,
                f"pullbox:undo-arc-identity:{action.id}:{payload['identity_event_id']}",
            ),
            payload["story_arc_id"],
            IdentityVerificationAction.OBSERVE,
            IdentityEventEvidence(
                ExactIdentityEvidence(
                    ExternalIdentityRef(
                        IdentityNamespace(expected["source"]),
                        MetadataEntityKind.STORY_ARC,
                        expected["external_id"],
                    ),
                    IdentityEvidenceKind.MIGRATION,
                ),
                _digest(payload),
                locator=IdentityEvidenceLocator(
                    IdentityEvidenceRecordKind.IMPORT_JOB_ACTION, action.id
                ),
            ),
            actor=IdentityEventActor.MIGRATION,
        )
    )
    # Rollback observes the restored state; it is not a new verification or a
    # rejection. This adapter extension records the revision without rewinding it.
    request = json.loads(prepared.request_json)
    request["import_rollback"] = {
        "schema_version": 1,
        "restored_revision": restored_revision,
        "restored_event_id": payload["previous_event_id"],
    }
    return prepared.event_key, _digest(request), _json(request)


async def _rollback_chain_revision(
    session: AsyncSession, action: ImportJobAction, latest: StoryArcIdentityEvent | None
) -> int | None:
    """Recognize only a later undo from this job which restored this exact proof."""
    if latest is None:
        return None
    request = json.loads(latest.request_json)
    origin = request.get("origin", {})
    restored = request.get("import_rollback", {})
    if (
        origin.get("record_kind") != "import_job_action"
        or restored.get("restored_event_id") != action.payload["identity_event_id"]
        or type(restored.get("restored_revision")) is not int
    ):
        return None
    previous = await session.get(ImportJobAction, origin.get("record_id"))
    if (
        previous is None
        or previous.action_type != IDENTITY_VERIFIED_ACTION
        or previous.import_job_id != action.import_job_id
        or previous.sequence_no <= action.sequence_no
        or previous.payload["story_arc_id"] != action.payload["story_arc_id"]
        or latest.identity_namespace != action.payload["expected_after"]["source"]
        or latest.external_id != action.payload["expected_after"]["external_id"]
        or latest.verification_state != action.payload["expected_after"]["verification_state"]
    ):
        return None
    revision = int(restored["restored_revision"])
    if (latest.event_key, latest.request_fingerprint, latest.request_json) != _rollback_request(
        previous, revision
    ):
        return None
    return revision


async def rollback_import_arc_identity(session: AsyncSession, action: ImportJobAction) -> None:
    """Reverse only the persisted action's still-owned mutation; never erase history.

    The journal dispatcher validates the saved payload and staging/job ownership.
    Parent locking serializes this path with attachment/catalog mutations.
    """
    payload = action.payload
    if session.get_bind().dialect.name == "sqlite":
        await session.execute(update(StoryArc).where(false()).values(revision=StoryArc.revision))
    arc = await session.scalar(
        select(StoryArc)
        .where(StoryArc.id == payload["story_arc_id"])
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if arc is None:
        return
    event_key, _, _ = _rollback_request(action, None)
    previous_undo = await session.scalar(
        select(StoryArcIdentityEvent).where(
            StoryArcIdentityEvent.story_arc_id == arc.id,
            StoryArcIdentityEvent.event_key == event_key,
        )
    )
    if previous_undo is not None:
        revision = (
            json.loads(previous_undo.request_json)
            .get("import_rollback", {})
            .get("restored_revision")
        )
        if (
            previous_undo.event_key,
            previous_undo.request_fingerprint,
            previous_undo.request_json,
        ) != _rollback_request(action, revision):
            raise ValueError("Story-arc identity rollback receipt changed after import")
        return
    owner = await session.scalar(
        select(StoryArcExternalIdentity)
        .where(StoryArcExternalIdentity.id == payload["external_identity_id"])
        .execution_options(populate_existing=True)
    )
    expected = dict(payload["expected_after"])
    original = await session.get(
        StoryArcIdentityEvent, payload["identity_event_id"], populate_existing=True
    )
    if (
        original is None
        or original.story_arc_id != arc.id
        or original.identity_namespace != expected["source"]
        or original.external_id != expected["external_id"]
        or original.verification_state is not IdentityVerificationState.VERIFIED
        or original.request_json != expected["evidence_locator"]
        or original.request_fingerprint
        != hashlib.sha256(original.request_json.encode("ascii")).hexdigest()
    ):
        raise ValueError("Story-arc identity history changed after import; rollback refused")
    latest = await _latest_event(session, arc.id, expected["source"])
    if latest is None or latest.id != payload["identity_event_id"]:
        restored_revision = await _rollback_chain_revision(session, action, latest)
        if restored_revision is None:
            raise ValueError("Story-arc identity changed after import; rollback refused")
        expected["revision"] = restored_revision
    if (
        owner is None
        or identity_state(owner) != expected
        or arc.comicvine_id != payload["comicvine_id_after"]
    ):
        raise ValueError("Story-arc identity changed after import; rollback refused")
    before = payload["restore_before"]
    revision = owner.revision + 1 if before is not None else None
    state = IdentityVerificationState.OBSERVED
    if before is None:
        await session.delete(owner)
    else:
        for field in ("verification_state", "evidence_kind", "evidence_locator"):
            setattr(owner, field, before[field])
        for field in ("verified_at", "last_seen_at"):
            setattr(owner, field, datetime.fromisoformat(before[field]) if before[field] else None)
        assert revision is not None
        owner.revision = revision
        state = (
            IdentityVerificationState(before["verification_state"])
            if before["verification_state"]
            else state
        )
    arc.comicvine_id = payload["comicvine_id_before"]
    key, fingerprint, request_json = _rollback_request(action, revision)
    session.add(
        StoryArcIdentityEvent(
            story_arc_id=arc.id,
            identity_namespace=IdentityNamespace(expected["source"]),
            external_id=expected["external_id"],
            verification_state=state,
            evidence_kind=IdentityEvidenceKind.MIGRATION,
            event_key=key,
            request_fingerprint=fingerprint,
            request_json=request_json,
        )
    )
    await session.flush()


async def require_owned_arc_identity_history(
    session: AsyncSession, *, arc_id: int, import_job_id: int
) -> None:
    """Deleting an import-created parent must not cascade another writer's evidence."""
    actions = list(
        await session.scalars(
            select(ImportJobAction).where(
                ImportJobAction.import_job_id == import_job_id,
                ImportJobAction.action_type == IDENTITY_VERIFIED_ACTION,
                ImportJobAction.payload["story_arc_id"].as_integer() == arc_id,
            )
        )
    )
    by_event = {action.payload["identity_event_id"]: action for action in actions}
    by_undo = {_rollback_request(action, None)[0]: action for action in actions}
    result = await session.stream_scalars(
        select(StoryArcIdentityEvent)
        .where(StoryArcIdentityEvent.story_arc_id == arc_id)
        .execution_options(yield_per=200, populate_existing=True)
    )
    try:
        async for event in result:
            original = by_event.get(event.id)
            if original is not None:
                expected = original.payload["expected_after"]
                if (
                    event.identity_namespace == expected["source"]
                    and event.external_id == expected["external_id"]
                    and event.verification_state is IdentityVerificationState.VERIFIED
                    and event.request_json == expected["evidence_locator"]
                    and event.request_fingerprint
                    == hashlib.sha256(event.request_json.encode("ascii")).hexdigest()
                ):
                    continue
            undone = by_undo.get(event.event_key)
            if undone is not None:
                revision = (
                    json.loads(event.request_json)
                    .get("import_rollback", {})
                    .get("restored_revision")
                )
                if (
                    event.event_key,
                    event.request_fingerprint,
                    event.request_json,
                ) == _rollback_request(undone, revision):
                    continue
            raise ValueError("Story arc has later identity history; rollback refused")
    finally:
        await result.close()
