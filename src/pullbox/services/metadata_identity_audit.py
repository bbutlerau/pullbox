"""Bounded, read-only ownership preflight for legacy identity migration rehearsals."""

from __future__ import annotations

import enum
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.services.metadata_identity_inventory import (
    MAX_INVENTORY_PAGE_SIZE,
    IdentityInventoryLimitError,
    LegacyIdentityDisagreement,
    LegacyIdentityObservation,
    LegacyIdentityProblem,
    LegacyIdentityStorage,
    read_legacy_identity_page,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.core.metadata_identity import ExternalIdentityRef

MAX_AUDIT_TARGETS = 1_000_000
MAX_AUDIT_EVIDENCE_ROWS = 2_000_000


class IdentityAuditStopReason(enum.StrEnum):
    TARGET_LIMIT = "target_limit"
    EVIDENCE_LIMIT = "evidence_limit"
    PAGE_EVIDENCE_LIMIT = "page_evidence_limit"


@dataclass(frozen=True)
class LegacyIdentityOwnershipCollision:
    identity: ExternalIdentityRef
    claims: tuple[LegacyIdentityObservation, ...]


@dataclass(frozen=True)
class LegacyIdentityDuplicateRelations:
    identity: ExternalIdentityRef
    local_id: int
    storage_row_ids: tuple[int, ...]


@dataclass(frozen=True)
class LegacyIdentityAudit:
    """Checked-prefix evidence; completion is scoped to this one entity kind."""

    entity_kind: MetadataEntityKind
    targets_checked: int
    observations_checked: int
    collisions: tuple[LegacyIdentityOwnershipCollision, ...] = ()
    duplicate_relations: tuple[LegacyIdentityDuplicateRelations, ...] = ()
    problems: tuple[LegacyIdentityProblem, ...] = ()
    disagreements: tuple[LegacyIdentityDisagreement, ...] = ()
    stop_reason: IdentityAuditStopReason | None = None

    @property
    def complete(self) -> bool:
        return self.stop_reason is None

    @property
    def evidence_rows_checked(self) -> int:
        return self.observations_checked + len(self.problems)

    @property
    def has_blockers(self) -> bool:
        return bool(
            not self.complete
            or self.collisions
            or self.duplicate_relations
            or self.problems
            or self.disagreements
        )


async def audit_legacy_identities(
    session: AsyncSession,
    entity_kind: MetadataEntityKind,
    *,
    page_size: int = 100,
    max_targets: int = 250_000,
    max_evidence_rows: int = 500_000,
) -> LegacyIdentityAudit:
    """Audit one entity kind on a quiescent copy, not a live migration approval.

    No repair, attachment, provider call, commit, or snapshot isolation is implied.
    Scan every kind separately on the same stable copy before planning backfill.
    Findings are deterministic across page sizes when the traversal completes.
    An over-budget page is excluded entirely; retained counts/findings describe
    only fully checked pages. Incomplete reports must never authorize a backfill.
    """
    if not isinstance(entity_kind, MetadataEntityKind):
        raise ValueError("Unknown metadata entity kind")
    for value, maximum in (
        (page_size, MAX_INVENTORY_PAGE_SIZE),
        (max_targets, MAX_AUDIT_TARGETS),
        (max_evidence_rows, MAX_AUDIT_EVIDENCE_ROWS),
    ):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError("Invalid identity audit bounds")

    claims: dict[ExternalIdentityRef, list[LegacyIdentityObservation]] = defaultdict(list)
    problems: list[LegacyIdentityProblem] = []
    disagreements: list[LegacyIdentityDisagreement] = []
    targets_checked = observations_checked = after_id = 0
    stop_reason = None
    while True:
        try:
            page = await read_legacy_identity_page(
                session,
                entity_kind,
                after_id=after_id,
                limit=min(page_size, max_targets - targets_checked),
            )
        except IdentityInventoryLimitError:
            stop_reason = IdentityAuditStopReason.PAGE_EVIDENCE_LIMIT
            break
        evidence_rows = (
            observations_checked + len(problems) + len(page.observations) + len(page.problems)
        )
        if evidence_rows > max_evidence_rows:
            stop_reason = IdentityAuditStopReason.EVIDENCE_LIMIT
            break
        targets_checked += len(page.local_ids)
        observations_checked += len(page.observations)
        problems.extend(page.problems)
        disagreements.extend(page.disagreements)
        for observation in page.observations:
            claims[observation.identity].append(observation)
        if not page.has_more:
            break
        if targets_checked == max_targets:
            stop_reason = IdentityAuditStopReason.TARGET_LIMIT
            break
        if page.next_after_id is None or page.next_after_id <= after_id:
            raise ValueError("Identity inventory cursor did not advance")
        after_id = page.next_after_id

    collisions, duplicates = _ownership_findings(claims)
    return LegacyIdentityAudit(
        entity_kind=entity_kind,
        targets_checked=targets_checked,
        observations_checked=observations_checked,
        collisions=collisions,
        duplicate_relations=duplicates,
        problems=tuple(sorted(problems, key=lambda p: (p.local_id, p.storage, p.storage_row_id))),
        disagreements=tuple(disagreements),
        stop_reason=stop_reason,
    )


def _ownership_findings(
    claims: dict[ExternalIdentityRef, list[LegacyIdentityObservation]],
) -> tuple[
    tuple[LegacyIdentityOwnershipCollision, ...], tuple[LegacyIdentityDuplicateRelations, ...]
]:
    collisions = []
    duplicates = []
    for identity in sorted(claims, key=lambda key: (key.namespace, key.external_id)):
        observations = sorted(
            claims[identity],
            key=lambda claim: (claim.local_id, claim.storage, claim.storage_row_id),
        )
        if len({claim.local_id for claim in observations}) > 1:
            collisions.append(LegacyIdentityOwnershipCollision(identity, tuple(observations)))
        relations: dict[int, list[int]] = defaultdict(list)
        for claim in observations:
            if claim.storage == LegacyIdentityStorage.STORY_ARC_RELATION:
                relations[claim.local_id].append(claim.storage_row_id)
        for local_id, row_ids in relations.items():
            if len(row_ids) > 1:
                duplicates.append(
                    LegacyIdentityDuplicateRelations(identity, local_id, tuple(row_ids))
                )
    return tuple(collisions), tuple(duplicates)
