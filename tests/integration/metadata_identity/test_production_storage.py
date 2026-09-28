"""Enforce production identity storage on both supported database engines."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import delete, func, insert, select
from sqlalchemy.exc import IntegrityError

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.core.metadata_identity_events import prepare_identity_event
from pullbox.models import Base, Issue, Series, StoryArc
from tests.fixtures.metadata_identity_events import identity_event_request


def _table(kind, history=False):
    name = f"{kind}_{'identity_events' if history else 'external_identities'}"
    assert name in Base.metadata.tables, "Production identity storage must be registered"
    return Base.metadata.tables[name]


async def _parents(factory, kind):
    async with factory.begin() as session:
        series = [Series(title=f"Identity {n}", sort_title=f"identity {n}") for n in range(2)]
        session.add_all(series)
        await session.flush()
        if kind == "series":
            return [row.id for row in series]
        targets = (
            [Issue(series_id=row.id, issue_number=1) for row in series]
            if kind == "issue"
            else [StoryArc(name=f"Arc {n}") for n in range(2)]
        )
        session.add_all(targets)
        await session.flush()
        return [row.id for row in targets]


def _values(kind, target, **overrides):
    return {
        f"{kind}_id": target,
        "identity_namespace": "comicvine",
        "external_id": "42",
        "verification_state": "verified",
        "evidence_kind": "legacy_backfill",
        **overrides,
    }


@pytest.mark.parametrize("kind", ["series", "issue"])
async def test_production_all_namespaces_have_independent_ownership(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    table = _table(kind)
    target, _other = await _parents(factory, kind)
    async with factory.begin() as session:
        for namespace in ("comicvine", "metron", "gcd", "locg"):
            await session.execute(
                insert(table).values(_values(kind, target, identity_namespace=namespace))
            )
    async with factory() as session:
        rows = (await session.execute(select(table))).all()
        assert len(rows) == 4
        assert all(row.revision == 1 for row in rows)
        assert all(row.created_at.utcoffset().total_seconds() == 0 for row in rows)


@pytest.mark.parametrize("kind", ["series", "issue"])
@pytest.mark.parametrize(
    "case",
    [
        "other_owner",
        "other_id",
        "missing_parent",
        "observed",
        "rejected",
        "bad_state",
        "bad_namespace",
        "bad_evidence",
        "zero_revision",
        "empty_id",
        "padded_id",
        "invalid_id",
    ],
)
async def test_production_constraints_reject_invalid_active_ownership(
    identity_probe_db, kind, case
):
    _, factory, _ = identity_probe_db
    table = _table(kind)
    first, second = await _parents(factory, kind)
    async with factory.begin() as session:
        await session.execute(insert(table).values(_values(kind, first)))
    changes = {
        "other_owner": {f"{kind}_id": second},
        "other_id": {"external_id": "43"},
        "missing_parent": {f"{kind}_id": 999999, "external_id": "43"},
        "observed": {"verification_state": "observed"},
        "rejected": {"verification_state": "rejected"},
        "bad_state": {"verification_state": "mystery"},
        "bad_namespace": {"identity_namespace": "unknown"},
        "bad_evidence": {"evidence_kind": "unknown"},
        "zero_revision": {"revision": 0},
        "empty_id": {"external_id": ""},
        "padded_id": {"external_id": "0042"},
        "invalid_id": {"external_id": "42x"},
    }[case]
    # Remove the valid row for value checks so uniqueness cannot mask the intended constraint.
    if case not in {"other_owner", "other_id", "missing_parent"}:
        async with factory.begin() as session:
            await session.execute(delete(table))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(insert(table).values(_values(kind, first, **changes)))


@pytest.mark.parametrize("kind", ["series", "issue"])
async def test_production_active_owner_race_and_parent_cascade(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    table = _table(kind)
    targets = await _parents(factory, kind)

    async def attach(target):
        try:
            async with factory.begin() as session:
                await session.execute(
                    insert(table).values(_values(kind, target, verification_state="stale"))
                )
            return "attached"
        except IntegrityError:
            return "conflict"

    assert sorted(await asyncio.gather(*(attach(target) for target in targets))) == [
        "attached",
        "conflict",
    ]
    async with factory.begin() as session:
        model = Series if kind == "series" else Issue
        await session.execute(delete(model))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(table)) == 0


@pytest.mark.parametrize("kind", ["series", "issue", "story_arc"])
async def test_production_history_retains_rejections_and_replay_scope(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    table = _table(kind, history=True)
    first, second = await _parents(factory, kind)
    prepared = prepare_identity_event(identity_event_request(MetadataEntityKind(kind), first))
    payload = _values(
        kind,
        first,
        verification_state="rejected",
        event_key=prepared.event_key,
        request_fingerprint=prepared.request_fingerprint,
        request_json=prepared.request_json,
    )
    async with factory.begin() as session:
        await session.execute(insert(table).values(payload))
        await session.execute(insert(table).values({**payload, f"{kind}_id": second}))
        if kind != "story_arc":
            active = _table(kind)
            await session.execute(insert(active).values(_values(kind, first)))
            await session.execute(delete(active))
        assert await session.scalar(select(func.count()).select_from(table)) == 2
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(insert(table).values(payload))
    async with factory.begin() as session:
        model = {"series": Series, "issue": Issue, "story_arc": StoryArc}[kind]
        await session.execute(delete(model))
        assert await session.scalar(select(func.count()).select_from(table)) == 0
