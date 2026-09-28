"""Transactional verified-identity attachment for validated internal adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import groupby
from typing import TYPE_CHECKING

from sqlalchemy import false, func, insert, or_, select, tuple_, update
from sqlalchemy.exc import IntegrityError

from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind
from pullbox.core.metadata_identity_events import (
    IdentityEventActor,
    IdentityEventReplayConflictError,
    prepare_identity_event,
    validate_identity_event_replay,
)
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
    IdentityVerificationState,
)
from pullbox.models import Base, Issue, Series
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import RowMapping
    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.core.metadata_identity_events import IdentityEventRequest

_BATCH_SIZE = 200


class IdentityAttachmentConflictError(ValueError):
    """Identity ownership or parent evidence disagrees with the local target."""


@dataclass(frozen=True)
class IdentityAttachmentReceipt:
    event_id: int
    replayed: bool


async def attach_verified_identities(
    session: AsyncSession, requests: Sequence[IdentityEventRequest]
) -> list[IdentityAttachmentReceipt]:
    """Attach adapter-validated exact evidence in the caller's transaction.

    This is not a transport API or an authorization boundary. Adapters must
    establish evidence trust before calling it. Explicit user decisions use a
    separate review path; this path must never override their retained history.
    """
    if not requests:
        return []
    unique: dict[str, IdentityEventRequest] = {}
    for request in requests:
        identity = request.evidence.claim.identity
        if (
            request.action is not IdentityVerificationAction.VERIFY
            or request.actor is not IdentityEventActor.AUTOMATION
            or identity.entity_kind not in {MetadataEntityKind.SERIES, MetadataEntityKind.ISSUE}
        ):
            raise ValueError("This attachment path accepts validated automatic verification only")
        if (
            identity.entity_kind is MetadataEntityKind.ISSUE
            and request.evidence.parent_identity is None
        ):
            raise IdentityAttachmentConflictError(
                "Verified issue attachment requires exact parent evidence"
            )
        if identity.namespace is IdentityNamespace.COMICVINE and int(identity.external_id) >= 2**63:
            raise IdentityAttachmentConflictError(
                "ComicVine identity exceeds the compatibility ID range"
            )
        prepared = prepare_identity_event(request)
        previous = unique.setdefault(prepared.event_key, request)
        if prepare_identity_event(previous).request_json != prepared.request_json:
            raise IdentityEventReplayConflictError(
                "One batch reuses an identity operation inconsistently"
            )

    # SQLite's legacy deferred BEGIN must enclose SAVEPOINT, even for existing
    # parents with no pending ORM writes. This also serializes SQLite writers.
    if session.get_bind().dialect.name == "sqlite":
        await session.execute(
            update(Series).where(false()).values(comicvine_id=Series.comicvine_id)
        )
    receipts: dict[str, IdentityAttachmentReceipt] = {}
    ordered = sorted(unique.values(), key=_sort_key)
    try:
        async with session.begin_nested():
            await _lock_parent_graph(session, ordered)
            for kind, members in groupby(
                ordered, key=lambda item: item.evidence.claim.identity.entity_kind
            ):
                group = list(members)
                for offset in range(0, len(group), _BATCH_SIZE):
                    receipts.update(
                        await _attach_batch(session, kind, group[offset : offset + _BATCH_SIZE])
                    )
    except IntegrityError as exc:
        raise IdentityAttachmentConflictError(
            "Identity ownership changed; reload before retrying"
        ) from exc
    return [receipts[prepare_identity_event(request).event_key] for request in requests]


def _sort_key(request: IdentityEventRequest) -> tuple[int, int, str, str]:
    identity = request.evidence.claim.identity
    return (
        0 if identity.entity_kind is MetadataEntityKind.SERIES else 1,
        request.local_id,
        identity.namespace.value,
        identity.external_id,
    )


async def _lock_parent_graph(session: AsyncSession, requests: list[IdentityEventRequest]) -> None:
    """Acquire every parent before any child, across the whole batch sequence."""
    series_ids = {
        request.local_id
        for request in requests
        if request.evidence.claim.identity.entity_kind is MetadataEntityKind.SERIES
    }
    issue_ids = sorted(
        {
            request.local_id
            for request in requests
            if request.evidence.claim.identity.entity_kind is MetadataEntityKind.ISSUE
        }
    )
    expected_parents: dict[int, int] = {}
    for offset in range(0, len(issue_ids), _BATCH_SIZE):
        expected_parents.update(
            (
                await session.execute(
                    select(Issue.id, Issue.series_id).where(
                        Issue.id.in_(issue_ids[offset : offset + _BATCH_SIZE])
                    )
                )
            )
            .tuples()
            .all()
        )
    series_ids.update(expected_parents.values())
    ordered_parents = sorted(series_ids)
    for offset in range(0, len(ordered_parents), _BATCH_SIZE):
        await session.execute(
            select(Series.id)
            .where(Series.id.in_(ordered_parents[offset : offset + _BATCH_SIZE]))
            .order_by(Series.id)
            .with_for_update()
        )
    for offset in range(0, len(issue_ids), _BATCH_SIZE):
        rows = await session.execute(
            select(Issue.id, Issue.series_id)
            .where(Issue.id.in_(issue_ids[offset : offset + _BATCH_SIZE]))
            .order_by(Issue.id)
            .with_for_update()
        )
        if any(expected_parents.get(local_id) != parent_id for local_id, parent_id in rows):
            raise IdentityAttachmentConflictError("Issue parent changed during verification")


async def _locked_targets(
    session: AsyncSession, kind: MetadataEntityKind, target_ids: list[int]
) -> dict[int, RowMapping]:
    model = Series if kind is MetadataEntityKind.SERIES else Issue
    table = Base.metadata.tables[model.__tablename__]
    columns = [table.c.id, table.c.comicvine_id]
    if kind is MetadataEntityKind.ISSUE:
        columns.append(table.c.series_id)
    rows = (
        (
            await session.execute(
                select(*columns).where(table.c.id.in_(target_ids)).order_by(table.c.id)
            )
        )
        .mappings()
        .all()
    )
    targets = {int(row.id): row for row in rows}
    if len(targets) != len(set(target_ids)):
        raise IdentityAttachmentConflictError("Identity target no longer exists")
    return targets


async def _attach_batch(
    session: AsyncSession, kind: MetadataEntityKind, requests: list[IdentityEventRequest]
) -> dict[str, IdentityAttachmentReceipt]:
    target_key = f"{kind.value}_id"
    active = Base.metadata.tables[f"{kind.value}_external_identities"]
    history = Base.metadata.tables[f"{kind.value}_identity_events"]
    targets = await _locked_targets(
        session, kind, sorted({request.local_id for request in requests})
    )
    prepared = {
        prepare_identity_event(request).event_key: prepare_identity_event(request)
        for request in requests
    }
    stored = (
        (
            await session.execute(
                select(history).where(
                    tuple_(history.c[target_key], history.c.event_key).in_(
                        [
                            (request.local_id, prepare_identity_event(request).event_key)
                            for request in requests
                        ]
                    )
                )
            )
        )
        .mappings()
        .all()
    )
    receipts: dict[str, IdentityAttachmentReceipt] = {}
    for row in stored:
        payload = prepared[row.event_key]
        validate_identity_event_replay(
            payload, event_key=row.event_key, request_fingerprint=row.request_fingerprint
        )
        if (
            row.request_json != payload.request_json
            or row.verification_state is not IdentityVerificationState.VERIFIED
        ):
            raise IdentityEventReplayConflictError("Stored identity request is inconsistent")
        receipts[row.event_key] = IdentityAttachmentReceipt(row.id, True)
    fresh = [
        request for request in requests if prepare_identity_event(request).event_key not in receipts
    ]
    if not fresh:
        return receipts

    active_rows = (
        (
            await session.execute(
                select(active).where(
                    or_(
                        active.c[target_key].in_([request.local_id for request in fresh]),
                        tuple_(active.c.identity_namespace, active.c.external_id).in_(
                            [
                                (
                                    request.evidence.claim.identity.namespace,
                                    request.evidence.claim.identity.external_id,
                                )
                                for request in fresh
                            ]
                        ),
                    )
                )
            )
        )
        .mappings()
        .all()
    )
    by_target = {(row[target_key], row.identity_namespace): row for row in active_rows}
    by_external = {(row.identity_namespace, row.external_id): row for row in active_rows}
    claim_columns = (history.c[target_key], history.c.identity_namespace, history.c.external_id)
    latest_ids = (
        select(func.max(history.c.id))
        .where(
            tuple_(*claim_columns).in_(
                [
                    (
                        request.local_id,
                        request.evidence.claim.identity.namespace,
                        request.evidence.claim.identity.external_id,
                    )
                    for request in fresh
                ]
            )
        )
        .group_by(*claim_columns)
    )
    decisions = {
        (row[target_key], row.identity_namespace, row.external_id): row.verification_state
        for row in (
            await session.execute(select(history).where(history.c.id.in_(latest_ids)))
        ).mappings()
    }
    await _validate_legacy_owners(session, kind, fresh, targets)
    if kind is MetadataEntityKind.ISSUE:
        await _validate_issue_parents(session, fresh, targets)

    now = datetime.now(UTC)
    creates: dict[tuple[int, IdentityNamespace], dict[str, object]] = {}
    changes: dict[tuple[int, IdentityNamespace], dict[str, object]] = {}
    revisions: dict[tuple[int, IdentityNamespace], int] = {}
    events: list[dict[str, object]] = []
    legacy_changes: dict[int, dict[str, int]] = {}
    seen_targets: dict[tuple[int, IdentityNamespace], str] = {}
    seen_owners: dict[tuple[IdentityNamespace, str], int] = {}
    for request in fresh:
        identity = request.evidence.claim.identity
        namespace, external_id = identity.namespace, identity.external_id
        target = (request.local_id, namespace)
        external = (namespace, external_id)
        if (
            seen_targets.setdefault(target, external_id) != external_id
            or seen_owners.setdefault(external, request.local_id) != request.local_id
        ):
            raise IdentityAttachmentConflictError("Batch contains conflicting identity ownership")
        existing, owner = by_target.get(target), by_external.get(external)
        if (existing is not None and existing.external_id != external_id) or (
            owner is not None and owner[target_key] != request.local_id
        ):
            raise IdentityAttachmentConflictError("Provider identity already has a different owner")
        if (
            existing is not None
            and existing.verification_state is IdentityVerificationState.CONFLICTED
        ) or decisions.get((*target, external_id)) in {
            IdentityVerificationState.CONFLICTED,
            IdentityVerificationState.REJECTED,
        }:
            raise IdentityReviewRequiredError("This identity requires explicit review")
        payload = prepare_identity_event(request)
        values: dict[str, object] = {
            "verification_state": IdentityVerificationState.VERIFIED,
            "evidence_kind": request.evidence.claim.evidence_kind,
            "evidence_locator": payload.request_json,
            "verified_at": now,
            "last_seen_at": now,
        }
        revisions[target] = (
            revisions.get(target, existing.revision if existing is not None else 0) + 1
        )
        values["revision"] = revisions[target]
        if existing is None:
            creates[target] = {
                **values,
                target_key: request.local_id,
                "identity_namespace": namespace,
                "external_id": external_id,
            }
        else:
            changes[target] = {**values, "id": existing.id}
        if (
            namespace is IdentityNamespace.COMICVINE
            and targets[request.local_id].comicvine_id is None
        ):
            legacy_changes[request.local_id] = {
                "id": request.local_id,
                "comicvine_id": int(external_id),
            }
        events.append(
            {
                target_key: request.local_id,
                "identity_namespace": namespace,
                "external_id": external_id,
                "verification_state": IdentityVerificationState.VERIFIED,
                "evidence_kind": request.evidence.claim.evidence_kind,
                "event_key": payload.event_key,
                "request_fingerprint": payload.request_fingerprint,
                "request_json": payload.request_json,
            }
        )
    if creates:
        await session.execute(insert(active), list(creates.values()))
    if changes:
        await session.execute(
            update(
                SeriesExternalIdentity
                if kind is MetadataEntityKind.SERIES
                else IssueExternalIdentity
            ),
            list(changes.values()),
        )
    if legacy_changes:
        await session.execute(
            update(Series if kind is MetadataEntityKind.SERIES else Issue),
            list(legacy_changes.values()),
        )
    rows = await session.execute(
        insert(history).returning(history.c.id, history.c.event_key),
        events,
        execution_options={"insertmanyvalues_page_size": 50},
    )
    for event_id, event_key in rows:
        receipts[event_key] = IdentityAttachmentReceipt(event_id, False)
    return receipts


async def _validate_legacy_owners(
    session: AsyncSession,
    kind: MetadataEntityKind,
    requests: list[IdentityEventRequest],
    targets: dict[int, RowMapping],
) -> None:
    model = Series if kind is MetadataEntityKind.SERIES else Issue
    legacy_claims = [
        (request.local_id, int(request.evidence.claim.identity.external_id))
        for request in requests
        if request.evidence.claim.identity.namespace is IdentityNamespace.COMICVINE
    ]
    if not legacy_claims:
        return
    owners = {
        external_id: local_id
        for local_id, external_id in (
            await session.execute(
                select(model.id, model.comicvine_id).where(
                    model.comicvine_id.in_([value for _, value in legacy_claims])
                )
            )
        ).all()
    }
    for local_id, external_id in legacy_claims:
        if (
            targets[local_id].comicvine_id not in (None, external_id)
            or owners.get(external_id, local_id) != local_id
        ):
            raise IdentityAttachmentConflictError(
                "ComicVine compatibility identity disagrees with attachment"
            )


async def _validate_issue_parents(
    session: AsyncSession, requests: list[IdentityEventRequest], targets: dict[int, RowMapping]
) -> None:
    table = Base.metadata.tables["series_external_identities"]
    rows = (
        (
            await session.execute(
                select(table, Series.comicvine_id)
                .join(Series, Series.id == table.c.series_id)
                .where(table.c.series_id.in_({row.series_id for row in targets.values()}))
            )
        )
        .mappings()
        .all()
    )
    parents = {(row.series_id, row.identity_namespace): row for row in rows}
    for request in requests:
        proof = request.evidence.parent_identity
        assert proof is not None
        parent = parents.get((targets[request.local_id].series_id, proof.namespace))
        if (
            parent is None
            or parent.external_id != proof.external_id
            or parent.verification_state is not IdentityVerificationState.VERIFIED
        ):
            raise IdentityAttachmentConflictError(
                "Issue evidence disagrees with its verified local series"
            )
        if proof.namespace is IdentityNamespace.COMICVINE and parent.comicvine_id != int(
            proof.external_id
        ):
            raise IdentityAttachmentConflictError(
                "Issue parent has inconsistent ComicVine ownership"
            )
