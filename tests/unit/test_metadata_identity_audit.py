"""Whole-kind preflight must not confuse partial or normalized evidence with safety."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, select

from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity
from pullbox.services import metadata_identity_audit as audit_module
from pullbox.services.metadata_identity_audit import (
    IdentityAuditStopReason,
    audit_legacy_identities,
)
from pullbox.services.metadata_identity_inventory import LegacyIdentityStorage
from tests.fixtures.metadata_identity_persistence import drop_canonical_arc_index

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def _arcs(session: AsyncSession, values: list[int | None]) -> list[StoryArc]:
    arcs = [StoryArc(name=f"Arc {index}", comicvine_id=value) for index, value in enumerate(values)]
    session.add_all(arcs)
    await session.commit()
    return arcs


def _relation(
    arc: StoryArc, value: str, source: str = "comicvine", namespace: str = "story_arc"
) -> StoryArcExternalIdentity:
    return StoryArcExternalIdentity(
        story_arc_id=arc.id, source=source, namespace=namespace, external_id=value
    )


@pytest.mark.parametrize("kind", list(MetadataEntityKind))
async def test_audit_empty_kind_is_complete_without_invented_issues(
    db_session: AsyncSession, kind: MetadataEntityKind
) -> None:
    report = await audit_legacy_identities(db_session, kind)
    assert report.entity_kind == kind
    assert report.targets_checked == report.observations_checked == 0
    assert report.complete and not report.has_blockers


async def test_cross_page_normalized_collision_retains_all_claims_and_storage_locators(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [42, None, None])
    relations = [_relation(arcs[0], "00042"), _relation(arcs[2], " 42 ")]
    db_session.add_all(relations)
    await db_session.commit()

    small = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=1)
    large = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=500)

    assert small == large
    assert small.complete and small.has_blockers
    assert small.targets_checked == 3
    assert small.observations_checked == 3
    assert len(small.collisions) == 1
    collision = small.collisions[0]
    assert collision.identity.external_id == "42"
    assert collision.identity.namespace == IdentityNamespace.COMICVINE
    assert [
        (claim.local_id, claim.storage, claim.storage_row_id) for claim in collision.claims
    ] == [
        (arcs[0].id, LegacyIdentityStorage.COMICVINE_COLUMN, arcs[0].id),
        (arcs[0].id, LegacyIdentityStorage.STORY_ARC_RELATION, relations[0].id),
        (arcs[2].id, LegacyIdentityStorage.STORY_ARC_RELATION, relations[1].id),
    ]
    assert small.duplicate_relations == small.disagreements == small.problems == ()
    assert arcs[0].comicvine_id == 42
    assert [row.external_id for row in relations] == ["00042", " 42 "]


async def test_agreement_is_not_collision_and_provider_namespaces_stay_independent(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [42, None])
    db_session.add_all([_relation(arcs[0], "00042"), _relation(arcs[1], "42", source="metron")])
    await db_session.commit()
    report = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=1)
    assert report.targets_checked == 2
    assert report.observations_checked == 3
    assert report.complete and not report.has_blockers


async def test_equivalent_duplicate_relations_need_explicit_consolidation_not_an_owner_winner(
    db_session: AsyncSession,
) -> None:
    await (await db_session.connection()).run_sync(drop_canonical_arc_index)
    [arc] = await _arcs(db_session, [42])
    relations = [_relation(arc, "42"), _relation(arc, "00042")]
    db_session.add_all(relations)
    await db_session.commit()
    report = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC)
    assert report.complete and report.has_blockers
    assert report.collisions == report.disagreements == ()
    assert len(report.duplicate_relations) == 1
    duplicate = report.duplicate_relations[0]
    assert duplicate.local_id == arc.id
    assert duplicate.identity.external_id == "42"
    assert duplicate.storage_row_ids == tuple(row.id for row in relations)
    assert len((await db_session.scalars(select(StoryArcExternalIdentity))).all()) == 2


async def test_disagreement_and_invalid_values_survive_page_boundaries_without_raw_payloads(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [42, 0, None, None])
    raw = "https://example.invalid/?token=private-fixture-value"
    relations = [_relation(arcs[0], "43"), _relation(arcs[3], raw)]
    db_session.add_all(relations)
    await db_session.commit()
    report = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=1)
    assert report.complete and report.has_blockers
    assert report.targets_checked == 4
    assert report.observations_checked == 2
    assert [(p.local_id, p.storage_row_id, p.code) for p in report.problems] == [
        (arcs[1].id, arcs[1].id, "invalid_external_id"),
        (arcs[3].id, relations[1].id, "invalid_external_id"),
    ]
    assert len(report.disagreements) == 1
    assert report.disagreements[0].local_id == arcs[0].id
    assert "private-fixture-value" not in repr(report)


async def test_import_scopes_never_become_canonical_ownership(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [42, None])
    db_session.add_all(
        [
            _relation(arcs[1], "42", source="mylar3", namespace="database-a"),
            _relation(arcs[1], "42", namespace="import:private"),
            _relation(arcs[1], "42", source="folder"),
        ]
    )
    await db_session.commit()
    report = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=1)
    assert report.observations_checked == 1
    assert report.targets_checked == 2
    assert report.complete and not report.has_blockers


async def test_target_budget_cannot_report_a_clear_audit_when_later_owner_is_unchecked(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [42, None, None])
    db_session.add(_relation(arcs[2], "42"))
    await db_session.commit()
    report = await audit_legacy_identities(
        db_session, MetadataEntityKind.STORY_ARC, page_size=500, max_targets=2
    )
    assert report.targets_checked == 2
    assert report.collisions == ()
    assert not report.complete and report.has_blockers
    assert report.stop_reason == IdentityAuditStopReason.TARGET_LIMIT


async def test_exact_target_and_evidence_limits_can_finish_without_false_incompleteness(
    db_session: AsyncSession,
) -> None:
    await _arcs(db_session, [42, None, 43])
    report = await audit_legacy_identities(
        db_session, MetadataEntityKind.STORY_ARC, page_size=2, max_targets=3, max_evidence_rows=2
    )
    assert report.targets_checked == 3
    assert report.observations_checked == 2
    assert report.complete and not report.has_blockers


async def test_evidence_budget_keeps_prior_findings_but_does_not_count_partial_pages(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, [0, 42, 43])
    report = await audit_legacy_identities(
        db_session, MetadataEntityKind.STORY_ARC, page_size=1, max_evidence_rows=2
    )
    assert not report.complete and report.has_blockers
    assert report.stop_reason == IdentityAuditStopReason.EVIDENCE_LIMIT
    assert report.targets_checked == 2
    assert report.observations_checked == 1
    assert report.evidence_rows_checked == 2
    assert report.problems[0].local_id == arcs[0].id


async def test_invalid_evidence_uses_the_same_retained_memory_budget(
    db_session: AsyncSession,
) -> None:
    await (await db_session.connection()).run_sync(drop_canonical_arc_index)
    arcs = await _arcs(db_session, [0, None, None])
    db_session.add_all([_relation(arcs[1], "bad-1"), _relation(arcs[1], "bad-2")])
    await db_session.commit()
    report = await audit_legacy_identities(
        db_session, MetadataEntityKind.STORY_ARC, page_size=1, max_evidence_rows=1
    )
    assert not report.complete and report.has_blockers
    assert report.stop_reason == IdentityAuditStopReason.EVIDENCE_LIMIT
    assert report.targets_checked == 1
    assert len(report.problems) == 1
    assert report.evidence_rows_checked == 1


async def test_relation_overflow_is_explicit_incomplete_and_keeps_prior_findings(
    db_session: AsyncSession,
) -> None:
    await (await db_session.connection()).run_sync(drop_canonical_arc_index)
    arcs = await _arcs(db_session, [0, None])
    db_session.add_all([_relation(arcs[1], str(index + 1)) for index in range(2001)])
    await db_session.commit()
    report = await audit_legacy_identities(db_session, MetadataEntityKind.STORY_ARC, page_size=1)
    assert not report.complete and report.has_blockers
    assert report.stop_reason == IdentityAuditStopReason.PAGE_EVIDENCE_LIMIT
    assert report.targets_checked == 1
    assert report.problems[0].local_id == arcs[0].id


async def test_audit_does_not_flush_commit_hydrate_or_per_target_query(
    db_session: AsyncSession,
) -> None:
    arcs = await _arcs(db_session, list(range(100, 125)))
    arcs[0].comicvine_id = 900
    pending = StoryArc(name="Pending", comicvine_id=999)
    db_session.add(pending)
    before_map = set(db_session.identity_map)
    statements = []

    def record(
        _conn: object,
        _cursor: object,
        statement: str,
        _params: object,
        _context: object,
        _many: bool,
    ) -> None:
        statements.append(statement)

    event.listen(db_session.bind.sync_engine, "before_cursor_execute", record)
    try:
        report = await audit_legacy_identities(
            db_session, MetadataEntityKind.STORY_ARC, page_size=10
        )
    finally:
        event.remove(db_session.bind.sync_engine, "before_cursor_execute", record)
    assert report.targets_checked == report.observations_checked == 25
    assert len(statements) == 6
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in statements)
    assert set(db_session.identity_map) == before_map
    assert pending.id is None
    await db_session.rollback()
    assert await db_session.scalar(select(StoryArc.comicvine_id).order_by(StoryArc.id)) == 100


@pytest.mark.parametrize("kind", [MetadataEntityKind.SERIES, MetadataEntityKind.ISSUE])
async def test_series_and_issue_audits_do_not_mix_local_entity_id_spaces(
    db_session: AsyncSession, kind: MetadataEntityKind
) -> None:
    series = Series(title="Parent", sort_title="parent", comicvine_id=42)
    db_session.add(series)
    await db_session.flush()
    db_session.add_all(
        [
            Issue(series_id=series.id, issue_number=i, comicvine_id=value)
            for i, value in enumerate([42, None, 43], 1)
        ]
    )
    await db_session.commit()
    report = await audit_legacy_identities(db_session, kind, page_size=1)
    assert report.targets_checked == (1 if kind == MetadataEntityKind.SERIES else 3)
    assert report.observations_checked == (1 if kind == MetadataEntityKind.SERIES else 2)
    assert report.complete and not report.has_blockers


@pytest.mark.parametrize(
    "kwargs",
    [
        {"page_size": 0},
        {"page_size": 501},
        {"page_size": True},
        {"page_size": "1"},
        {"max_targets": 0},
        {"max_targets": 1_000_001},
        {"max_targets": True},
        {"max_evidence_rows": 0},
        {"max_evidence_rows": 2_000_001},
        {"max_evidence_rows": 1.5},
    ],
)
async def test_invalid_bounds_fail_before_querying(kwargs: dict[str, object]) -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="bounds"):
        await audit_legacy_identities(session, MetadataEntityKind.SERIES, **kwargs)
    session.execute.assert_not_called()


async def test_raw_unknown_kind_is_rejected_before_querying() -> None:
    session = AsyncMock()
    with pytest.raises(ValueError, match="entity kind"):
        await audit_legacy_identities(session, cast("MetadataEntityKind", "series"))
    session.execute.assert_not_called()


@pytest.mark.parametrize("error", [RuntimeError("read failed"), asyncio.CancelledError()])
async def test_query_errors_and_cancellation_never_become_a_complete_report(
    error: BaseException,
) -> None:
    session = AsyncMock()
    session.execute.side_effect = error
    with pytest.raises(type(error)):
        await audit_module.audit_legacy_identities(session, MetadataEntityKind.SERIES)
