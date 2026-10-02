"""Durable retries return the original result instead of reinterpreting history."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import IntegrityError

from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity_events import (
    IdentityEventReplayConflictError,
    prepare_identity_event,
)
from pullbox.core.metadata_identity_state import IdentityVerificationAction as Action
from pullbox.core.metadata_identity_state import IdentityVerificationState as State
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc
from tests.fixtures.metadata_identity_events import (
    identity_event_request,
    record_identity_event_probe,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from tests.fixtures.metadata_identity_events import IdentityEventReceipt
    from tests.fixtures.metadata_identity_persistence import IdentityProbeDatabase


async def _target(factory: async_sessionmaker[AsyncSession], kind: Kind) -> int:
    async with factory.begin() as session:
        series = Series(title="Evidence", sort_title="evidence")
        session.add(series)
        await session.flush()
        if kind is Kind.SERIES:
            return series.id
        target = (
            Issue(series_id=series.id, issue_number=1)
            if kind is Kind.ISSUE
            else StoryArc(name="Evidence")
        )
        session.add(target)
        await session.flush()
        return target.id


@pytest.mark.parametrize("kind", list(Kind))
async def test_retry_returns_original_receipt_without_appending_or_recomputing_result(
    identity_probe_db: IdentityProbeDatabase,
    kind: Kind,
) -> None:
    _engine, factory, probe = identity_probe_db
    target = await _target(factory, kind)
    request = identity_event_request(kind, target)
    table = probe.events[kind]
    async with factory.begin() as session:
        first = await record_identity_event_probe(session, table, request, State.OBSERVED)
    async with factory.begin() as session:
        second = await record_identity_event_probe(session, table, request, State.VERIFIED)
    assert first == second
    assert first.id > 0 and first.state is State.OBSERVED
    assert json.loads(first.request_json)["origin"] == {
        "record_kind": "imported_file",
        "record_id": 5,
    }
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(table)) == 1


@pytest.mark.parametrize("kind", list(Kind))
async def test_semantically_different_retry_cannot_overwrite_original_record(
    identity_probe_db: IdentityProbeDatabase,
    kind: Kind,
) -> None:
    _engine, factory, probe = identity_probe_db
    target = await _target(factory, kind)
    request = identity_event_request(kind, target)
    changed = replace(
        request,
        evidence=replace(
            request.evidence,
            claim=replace(
                request.evidence.claim,
                identity=replace(request.evidence.claim.identity, external_id="101"),
            ),
        ),
    )
    table = probe.events[kind]
    async with factory.begin() as session:
        first = await record_identity_event_probe(session, table, request, State.OBSERVED)
    with pytest.raises(IdentityEventReplayConflictError):
        async with factory.begin() as session:
            await record_identity_event_probe(session, table, changed, State.VERIFIED)
    async with factory() as session:
        rows = (await session.execute(select(table))).all()
        assert len(rows) == 1
        assert rows[0].request_json == first.request_json
        assert rows[0].external_id == "100"


@pytest.mark.parametrize("kind", list(Kind))
async def test_later_review_preserves_prior_rejection_as_a_separate_event(
    identity_probe_db: IdentityProbeDatabase,
    kind: Kind,
) -> None:
    _engine, factory, probe = identity_probe_db
    target = await _target(factory, kind)
    table = probe.events[kind]
    rejected = identity_event_request(kind, target, Action.REJECT)
    confirmed = replace(
        identity_event_request(kind, target, Action.CONFIRM, operation_id=UUID(int=2)),
        review_revision=2,
    )
    async with factory.begin() as session:
        first = await record_identity_event_probe(session, table, rejected, State.REJECTED)
        second = await record_identity_event_probe(session, table, confirmed, State.VERIFIED)
    assert first.id != second.id
    async with factory() as session:
        rows = (await session.execute(select(table).order_by(table.c.id))).all()
        assert [row.verification_state for row in rows] == [State.REJECTED, State.VERIFIED]
        assert [json.loads(row.request_json)["review_revision"] for row in rows] == [1, 2]


@pytest.mark.parametrize("kind", list(Kind))
async def test_concurrent_identical_retries_have_one_durable_receipt(
    identity_probe_db: IdentityProbeDatabase,
    kind: Kind,
) -> None:
    _engine, factory, probe = identity_probe_db
    target = await _target(factory, kind)
    request = identity_event_request(kind, target)
    table = probe.events[kind]
    barrier = asyncio.Barrier(2)

    async def record(state: State) -> IdentityEventReceipt:
        await barrier.wait()
        async with factory.begin() as session:
            return await record_identity_event_probe(session, table, request, state)

    first, second = await asyncio.wait_for(
        asyncio.gather(record(State.OBSERVED), record(State.VERIFIED)), timeout=15
    )
    assert first == second and first.id > 0
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(table)) == 1


@pytest.mark.parametrize("kind", list(Kind))
async def test_recording_does_not_commit_or_outlive_the_caller_transaction(
    identity_probe_db: IdentityProbeDatabase,
    kind: Kind,
) -> None:
    _engine, factory, probe = identity_probe_db
    target = await _target(factory, kind)
    table = probe.events[kind]
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with factory.begin() as session:
            await record_identity_event_probe(
                session, table, identity_event_request(kind, target), State.OBSERVED
            )
            assert await session.scalar(select(func.count()).select_from(table)) == 1
            raise RuntimeError("caller rollback")
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(table)) == 0


@pytest.mark.parametrize(
    "column,value",
    [("request_fingerprint", None), ("request_fingerprint", "short"), ("request_json", None)],
)
async def test_persisted_events_require_replay_material(
    identity_probe_db: IdentityProbeDatabase, column: str, value: str | None
) -> None:
    _engine, factory, probe = identity_probe_db
    kind = Kind.SERIES
    target = await _target(factory, kind)
    request = identity_event_request(kind, target)
    prepared = prepare_identity_event(request)
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(
                insert(probe.events[kind])
                .values(
                    series_id=target,
                    identity_namespace="comicvine",
                    external_id="100",
                    event_key=prepared.event_key,
                    request_fingerprint=prepared.request_fingerprint,
                    request_json=prepared.request_json,
                    verification_state="observed",
                    evidence_kind="comicinfo_xml",
                )
                .values(**{column: value})
            )


async def test_inconsistent_stored_payload_does_not_get_silently_replayed(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, probe = identity_probe_db
    kind = Kind.SERIES
    target = await _target(factory, kind)
    table = probe.events[kind]
    request = identity_event_request(kind, target)
    async with factory.begin() as session:
        receipt = await record_identity_event_probe(session, table, request, State.OBSERVED)
        await session.execute(
            update(table).where(table.c.id == receipt.id).values(request_json="{}")
        )
    with pytest.raises(IdentityEventReplayConflictError):
        async with factory.begin() as session:
            await record_identity_event_probe(session, table, request, State.OBSERVED)
