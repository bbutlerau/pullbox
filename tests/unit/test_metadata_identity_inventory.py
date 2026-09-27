"""Legacy identity inventory must preserve evidence without mutating the library."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import event, select

from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity
from pullbox.services.metadata_identity_inventory import (
    IdentityInventoryLimitError,
    LegacyIdentityStorage,
    read_legacy_identity_page,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def _targets(
    session: AsyncSession, kind: MetadataEntityKind, values: list[int | None]
) -> list[Series | Issue | StoryArc]:
    if kind == MetadataEntityKind.ISSUE:
        series = Series(title="Parent", sort_title="parent")
        session.add(series)
        await session.flush()
    rows = []
    for index, value in enumerate(values):
        if kind == MetadataEntityKind.SERIES:
            row = Series(title=f"Series {index}", sort_title=f"series {index}", comicvine_id=value)
        elif kind == MetadataEntityKind.ISSUE:
            row = Issue(series_id=series.id, issue_number=index + 1, comicvine_id=value)
        else:
            row = StoryArc(name=f"Arc {index}", comicvine_id=value)
        rows.append(row)
    session.add_all(rows)
    await session.commit()
    return rows


@pytest.mark.parametrize("kind", list(MetadataEntityKind))
async def test_pages_include_null_and_invalid_targets_without_changing_legacy_ids(
    db_session: AsyncSession, kind: MetadataEntityKind
) -> None:
    targets = await _targets(db_session, kind, [41, None, 0, -3, 42])

    first = await read_legacy_identity_page(db_session, kind, limit=3)
    second = await read_legacy_identity_page(
        db_session, kind, after_id=first.next_after_id, limit=3
    )
    empty = await read_legacy_identity_page(db_session, kind, after_id=second.next_after_id)

    assert first.local_ids == tuple(row.id for row in targets[:3])
    assert first.next_after_id == targets[2].id
    assert first.has_more is True
    assert second.local_ids == tuple(row.id for row in targets[3:])
    assert second.has_more is False
    assert empty.local_ids == ()
    assert empty.next_after_id is None
    assert empty.has_more is False
    assert [item.identity.external_id for item in first.observations + second.observations] == [
        "41",
        "42",
    ]
    assert {item.identity.namespace for item in first.observations} == {IdentityNamespace.COMICVINE}
    assert {item.identity.entity_kind for item in first.observations} == {kind}
    problems = first.problems + second.problems
    assert [(item.local_id, item.code) for item in problems] == [
        (targets[2].id, "invalid_external_id"),
        (targets[3].id, "invalid_external_id"),
    ]
    assert all(item.storage == LegacyIdentityStorage.COMICVINE_COLUMN for item in problems)
    assert [row.comicvine_id for row in targets] == [41, None, 0, -3, 42]


async def test_arc_inventory_keeps_disagreement_and_origin_but_excludes_import_scopes(
    db_session: AsyncSession,
) -> None:
    [arc] = await _targets(db_session, MetadataEntityKind.STORY_ARC, [42])
    rows = [
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id="00043"
        ),
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="metron", namespace="story_arc", external_id="7"
        ),
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="gcd", namespace="story_arc", external_id="9"
        ),
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="mylar3", namespace="database-a", external_id="42"
        ),
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="comicvine", namespace="import:private", external_id="99"
        ),
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="folder", namespace="story_arc", external_id="123"
        ),
    ]
    db_session.add_all(rows)
    await db_session.commit()

    page = await read_legacy_identity_page(db_session, MetadataEntityKind.STORY_ARC)

    assert [
        (item.identity.namespace.value, item.identity.external_id) for item in page.observations
    ] == [("comicvine", "42"), ("comicvine", "43"), ("metron", "7"), ("gcd", "9")]
    assert page.observations[0].storage == LegacyIdentityStorage.COMICVINE_COLUMN
    assert page.observations[1].storage == LegacyIdentityStorage.STORY_ARC_RELATION
    assert page.observations[1].storage_row_id == rows[0].id
    assert len(page.disagreements) == 1
    assert page.disagreements[0].local_id == arc.id
    assert {item.identity.external_id for item in page.disagreements[0].conflict.evidence} == {
        "42",
        "43",
    }
    assert arc.comicvine_id == 42
    assert rows[0].external_id == "00043"
    assert len((await db_session.scalars(select(StoryArcExternalIdentity))).all()) == 6


async def test_arc_inventory_retains_agreeing_observations_from_both_storage_paths(
    db_session: AsyncSession,
) -> None:
    [arc] = await _targets(db_session, MetadataEntityKind.STORY_ARC, [42])
    db_session.add(
        StoryArcExternalIdentity(
            story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id="00042"
        )
    )
    await db_session.commit()

    page = await read_legacy_identity_page(db_session, MetadataEntityKind.STORY_ARC)

    assert len(page.observations) == 2
    assert page.observations[0].identity == page.observations[1].identity
    assert {item.storage for item in page.observations} == set(LegacyIdentityStorage)
    assert page.disagreements == ()


async def test_invalid_arc_identity_has_safe_diagnostic_without_raw_url_or_payload(
    db_session: AsyncSession,
) -> None:
    [arc] = await _targets(db_session, MetadataEntityKind.STORY_ARC, [None])
    raw = "https://example.invalid/?token=private-fixture-value"
    relation = StoryArcExternalIdentity(
        story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id=raw
    )
    db_session.add(relation)
    await db_session.commit()

    page = await read_legacy_identity_page(db_session, MetadataEntityKind.STORY_ARC)

    assert page.observations == ()
    assert len(page.problems) == 1
    assert page.problems[0].storage_row_id == relation.id
    assert page.problems[0].code == "invalid_external_id"
    assert "private-fixture-value" not in repr(page)


async def test_inventory_reads_persisted_values_without_flush_commit_or_orm_hydration(
    db_session: AsyncSession,
) -> None:
    [series] = await _targets(db_session, MetadataEntityKind.SERIES, [42])
    series.comicvine_id = 43
    pending = Series(title="Pending", sort_title="pending", comicvine_id=44)
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
        page = await read_legacy_identity_page(db_session, MetadataEntityKind.SERIES)
    finally:
        event.remove(db_session.bind.sync_engine, "before_cursor_execute", record)

    assert [item.identity.external_id for item in page.observations] == ["42"]
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith("SELECT")
    assert set(db_session.identity_map) == before_map
    assert pending.id is None
    await db_session.rollback()
    assert (await db_session.scalar(select(Series.comicvine_id))) == 42


async def test_arc_inventory_is_bounded_bulk_read_and_only_loads_the_requested_page(
    db_session: AsyncSession,
) -> None:
    arcs = await _targets(db_session, MetadataEntityKind.STORY_ARC, [None] * 30)
    db_session.add_all(
        [
            StoryArcExternalIdentity(
                story_arc_id=arc.id,
                source="comicvine",
                namespace="story_arc",
                external_id=str(1000 + arc.id),
            )
            for arc in arcs
        ]
    )
    await db_session.commit()
    db_session.expunge_all()
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
        page = await read_legacy_identity_page(
            db_session, MetadataEntityKind.STORY_ARC, after_id=arcs[9].id, limit=10
        )
    finally:
        event.remove(db_session.bind.sync_engine, "before_cursor_execute", record)

    assert page.local_ids == tuple(arc.id for arc in arcs[10:20])
    assert {item.local_id for item in page.observations} == set(page.local_ids)
    assert len(page.observations) == 10
    assert len(statements) == 2
    assert not db_session.identity_map


async def test_arc_inventory_refuses_silent_relation_truncation(db_session: AsyncSession) -> None:
    [arc] = await _targets(db_session, MetadataEntityKind.STORY_ARC, [None])
    db_session.add_all(
        [
            StoryArcExternalIdentity(
                story_arc_id=arc.id,
                source="comicvine",
                namespace="story_arc",
                external_id=str(index + 1),
            )
            for index in range(2001)
        ]
    )
    await db_session.commit()

    with pytest.raises(IdentityInventoryLimitError, match="Smaller pages"):
        await read_legacy_identity_page(db_session, MetadataEntityKind.STORY_ARC, limit=1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": 501},
        {"limit": True},
        {"limit": 2.5},
        {"after_id": -1},
        {"after_id": True},
        {"after_id": "1"},
    ],
)
async def test_invalid_inventory_paging_is_rejected(
    db_session: AsyncSession, kwargs: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="page"):
        await read_legacy_identity_page(db_session, MetadataEntityKind.SERIES, **kwargs)


async def test_unknown_entity_kind_is_rejected(db_session: AsyncSession) -> None:
    with pytest.raises(ValueError, match="entity kind"):
        await read_legacy_identity_page(db_session, cast("MetadataEntityKind", "publisher"))
