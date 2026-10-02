"""Disposable event persistence rehearsal; not a runtime attachment service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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
    IdentityEventReplayConflictError,
    IdentityEventRequest,
    IdentityEvidenceLocator,
    IdentityEvidenceRecordKind,
    prepare_identity_event,
    validate_identity_event_replay,
)
from pullbox.core.metadata_identity_state import (
    IdentityVerificationAction,
    IdentityVerificationState,
)

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncSession

_DEFAULT_OPERATION_ID = UUID(int=1)


def identity_event_request(
    kind: MetadataEntityKind,
    local_id: int,
    action: IdentityVerificationAction = IdentityVerificationAction.OBSERVE,
    *,
    external_id: str = "100",
    operation_id: UUID = _DEFAULT_OPERATION_ID,
) -> IdentityEventRequest:
    review = action in {IdentityVerificationAction.REJECT, IdentityVerificationAction.CONFIRM}
    return IdentityEventRequest(
        operation_id,
        local_id,
        action,
        IdentityEventEvidence(
            ExactIdentityEvidence(
                ExternalIdentityRef(IdentityNamespace.COMICVINE, kind, external_id),
                IdentityEvidenceKind.COMICINFO_XML,
            ),
            "a" * 64,
            IdentityEvidenceLocator(IdentityEvidenceRecordKind.IMPORTED_FILE, 5),
        ),
        actor=IdentityEventActor.USER if review else IdentityEventActor.AUTOMATION,
        actor_user_id=1 if review else None,
        review_revision=1 if review else None,
    )


@dataclass(frozen=True)
class IdentityEventReceipt:
    id: int
    state: IdentityVerificationState
    created_at: datetime
    request_json: str


async def record_identity_event_probe(
    session: AsyncSession,
    table: Table,
    request: IdentityEventRequest,
    state: IdentityVerificationState,
) -> IdentityEventReceipt:
    """Rehearse atomic append/replay only; caller owns transaction and authorization."""
    prepared = prepare_identity_event(request)
    claim = request.evidence.claim
    identity = claim.identity
    target = table.c[f"{identity.entity_kind.value}_id"]
    backend = session.get_bind().dialect.name
    if backend == "postgresql":
        statement = pg_insert(table)
    elif backend == "sqlite":
        statement = sqlite_insert(table)
    else:
        raise ValueError("Unsupported identity rehearsal database")
    # Attempt the unique write first so concurrent retries have one durable result.
    await session.execute(
        statement.values(
            {
                target.name: request.local_id,
                "identity_namespace": identity.namespace.value,
                "external_id": identity.external_id,
                "event_key": prepared.event_key,
                "request_fingerprint": prepared.request_fingerprint,
                "request_json": prepared.request_json,
                "verification_state": state.value,
                "evidence_kind": claim.evidence_kind.value,
            }
        ).on_conflict_do_nothing(index_elements=[target, table.c.event_key])
    )
    row = (
        await session.execute(
            select(table).where(target == request.local_id, table.c.event_key == prepared.event_key)
        )
    ).one()
    validate_identity_event_replay(
        prepared, event_key=row.event_key, request_fingerprint=row.request_fingerprint
    )
    if row.request_json != prepared.request_json:
        raise IdentityEventReplayConflictError("Stored identity request payload is inconsistent")
    return IdentityEventReceipt(row.id, row.verification_state, row.created_at, row.request_json)
