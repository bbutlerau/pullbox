"""Rejected evidence is durable but does not reserve canonical ownership."""

from __future__ import annotations

from datetime import UTC
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError

from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity_state import IdentityVerificationState as State
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity

if TYPE_CHECKING:
    from sqlalchemy import Insert, Table
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from tests.fixtures.metadata_identity_persistence import (
        IdentityProbeDatabase,
        IdentitySchemaProbe,
    )


async def _targets(factory: async_sessionmaker[AsyncSession], kind: Kind) -> list[int]:
    async with factory.begin() as session:
        series = [
            Series(title=f"History {index}", sort_title=f"history {index}") for index in range(2)
        ]
        session.add_all(series)
        await session.flush()
        if kind is Kind.SERIES:
            return [row.id for row in series]
        rows = (
            [Issue(series_id=row.id, issue_number=1) for row in series]
            if kind is Kind.ISSUE
            else [StoryArc(name=f"History arc {index}") for index in range(2)]
        )
        session.add_all(rows)
        await session.flush()
        return [row.id for row in rows]


def _event(
    table: Table,
    kind: Kind,
    target: int,
    state: State = State.OBSERVED,
    *,
    key: str = "a" * 64,
    external_id: str = "100",
) -> Insert:
    return insert(table).values(
        **{f"{kind.value}_id": target},
        identity_namespace="comicvine",
        external_id=external_id,
        event_key=key,
        verification_state=state.value,
        evidence_kind="user_selection" if state is State.REJECTED else "comicinfo_xml",
    )


def _ownership(probe: IdentitySchemaProbe, kind: Kind) -> Table:
    if kind is Kind.STORY_ARC:
        return StoryArcExternalIdentity.__table__
    return probe.series if kind is Kind.SERIES else probe.issue


def _attach(table: Table, kind: Kind, target: int, state: State = State.VERIFIED) -> Insert:
    if kind is Kind.STORY_ARC:
        return insert(table).values(
            story_arc_id=target, source="comicvine", namespace="story_arc", external_id="100"
        )
    return insert(table).values(
        **{f"{kind.value}_id": target},
        identity_namespace="comicvine",
        external_id="100",
        verification_state=state.value,
    )


@pytest.mark.parametrize("kind", list(Kind))
@pytest.mark.parametrize("state", [State.OBSERVED, State.CONFLICTED, State.REJECTED])
async def test_unattached_evidence_never_reserves_an_external_identity(
    identity_probe_db: IdentityProbeDatabase, kind: Kind, state: State
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    events, ownership = probe.events[kind], _ownership(probe, kind)
    async with factory.begin() as session:
        for target in targets:
            await session.execute(_event(events, kind, target, state))
        await session.execute(_attach(ownership, kind, targets[1]))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(events)) == 2
        assert await session.scalar(select(func.count()).select_from(ownership)) == 1
        assert (await session.execute(select(events.c.verification_state))).scalars().all() == [
            state,
            state,
        ]


@pytest.mark.parametrize("kind", [Kind.SERIES, Kind.ISSUE])
@pytest.mark.parametrize("state", [State.OBSERVED, State.REJECTED])
async def test_observed_or_rejected_claims_cannot_be_active_attachments(
    identity_probe_db: IdentityProbeDatabase, kind: Kind, state: State
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_attach(_ownership(probe, kind), kind, targets[0], state))


@pytest.mark.parametrize("kind", [Kind.SERIES, Kind.ISSUE])
@pytest.mark.parametrize("state", [State.STALE, State.CONFLICTED])
async def test_stale_or_disputed_attachment_retains_ownership_until_explicit_resolution(
    identity_probe_db: IdentityProbeDatabase, kind: Kind, state: State
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    ownership = _ownership(probe, kind)
    async with factory.begin() as session:
        await session.execute(_attach(ownership, kind, targets[0]))
        await session.execute(update(ownership).values(verification_state=state.value))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_attach(ownership, kind, targets[1]))
    async with factory() as session:
        assert await session.scalar(select(ownership.c.verification_state)) == state
        assert await session.scalar(select(ownership.c[f"{kind.value}_id"])) == targets[0]


@pytest.mark.parametrize("kind", list(Kind))
async def test_detaching_does_not_erase_rejection_history_or_block_correct_owner(
    identity_probe_db: IdentityProbeDatabase, kind: Kind
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    events, ownership = probe.events[kind], _ownership(probe, kind)
    async with factory.begin() as session:
        await session.execute(_attach(ownership, kind, targets[0]))
        await session.execute(_event(events, kind, targets[0], State.REJECTED))
        await session.execute(
            delete(ownership).where(ownership.c[f"{kind.value}_id"] == targets[0])
        )
        await session.execute(_attach(ownership, kind, targets[1]))
    async with factory() as session:
        assert await session.scalar(select(events.c.verification_state)) == State.REJECTED
        assert await session.scalar(select(events.c[f"{kind.value}_id"])) == targets[0]
        assert await session.scalar(select(ownership.c[f"{kind.value}_id"])) == targets[1]


@pytest.mark.parametrize("kind", list(Kind))
async def test_history_requires_a_real_parent_and_cascades_only_with_that_parent(
    identity_probe_db: IdentityProbeDatabase, kind: Kind
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    table = probe.events[kind]
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_event(table, kind, 999999))
    parent = {Kind.SERIES: Series, Kind.ISSUE: Issue, Kind.STORY_ARC: StoryArc}[kind]
    async with factory.begin() as session:
        for target in targets:
            await session.execute(_event(table, kind, target))
        await session.execute(delete(parent).where(parent.id == targets[0]))
    async with factory() as session:
        assert (await session.execute(select(table.c[f"{kind.value}_id"]))).scalars().all() == [
            targets[1]
        ]
    assert {fk.target_fullname for fk in table.foreign_keys} == {f"{parent.__tablename__}.id"}


@pytest.mark.parametrize("kind", list(Kind))
async def test_retry_key_is_unique_per_target_but_new_decision_preserves_old_history(
    identity_probe_db: IdentityProbeDatabase, kind: Kind
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    table = probe.events[kind]
    async with factory.begin() as session:
        await session.execute(_event(table, kind, targets[0], State.REJECTED))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_event(table, kind, targets[0], State.VERIFIED))
    async with factory.begin() as session:
        await session.execute(_event(table, kind, targets[0], State.VERIFIED, key="b" * 64))
    async with factory() as session:
        rows = (await session.execute(select(table).order_by(table.c.id))).all()
        assert [row.verification_state for row in rows] == [State.REJECTED, State.VERIFIED]
        assert all(row.created_at.tzinfo is UTC for row in rows)


@pytest.mark.parametrize("kind", list(Kind))
@pytest.mark.parametrize(
    "column,value", [("verification_state", "trusted"), ("evidence_kind", "anything")]
)
async def test_history_rejects_unrecognized_state_or_evidence_kind(
    identity_probe_db: IdentityProbeDatabase, kind: Kind, column: str, value: str
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(
                _event(probe.events[kind], kind, targets[0]).values(**{column: value})
            )


@pytest.mark.parametrize("kind", list(Kind))
async def test_failed_attachment_rolls_back_its_history_without_losing_prior_rejection(
    identity_probe_db: IdentityProbeDatabase, kind: Kind
) -> None:
    _engine, factory, probe = identity_probe_db
    targets = await _targets(factory, kind)
    events, ownership = probe.events[kind], _ownership(probe, kind)
    async with factory.begin() as session:
        await session.execute(_attach(ownership, kind, targets[0]))
        await session.execute(_event(events, kind, targets[1], State.REJECTED))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_event(events, kind, targets[1], State.VERIFIED, key="b" * 64))
            await session.execute(_attach(ownership, kind, targets[1]))
    async with factory() as session:
        assert (await session.execute(select(events.c.verification_state))).scalars().all() == [
            State.REJECTED
        ]
        assert await session.scalar(select(ownership.c[f"{kind.value}_id"])) == targets[0]
