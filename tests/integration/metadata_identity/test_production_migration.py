"""Run actual identity revisions against disposable predecessor schemas."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from uuid import UUID

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Integer, MetaData, Table, event, insert, inspect, select, update
from sqlalchemy.exc import IntegrityError

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
from pullbox.core.metadata_identity_state import IdentityVerificationAction
from pullbox.models import Base, Issue, Series, StoryArc, StoryArcExternalIdentity

_VERSIONS = Path(__file__).resolve().parents[3] / "alembic" / "versions"
_NAMES = (
    "series_external_identities",
    "issue_external_identities",
    "series_identity_events",
    "issue_identity_events",
    "story_arc_identity_events",
)


def _revision(name, connection):
    spec = importlib.util.spec_from_file_location(name, _VERSIONS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


def _schema(connection):
    return _revision("p7j8k9l0m123_add_metadata_identity_storage", connection)


def _data(connection):
    return _revision("q8k9l0m1n234_backfill_comicvine_identities", connection)


async def _predecessor(engine):
    async with engine.begin() as connection:
        for name in reversed(_NAMES):
            await connection.run_sync(Base.metadata.tables[name].drop)


async def _seed(factory):
    async with factory.begin() as session:
        series = [
            Series(title=f"Legacy {n}", sort_title=f"legacy {n}", comicvine_id=value)
            for n, value in enumerate([42, None, 0, -10])
        ]
        session.add_all(series)
        await session.flush()
        issues = [
            Issue(series_id=row.id, issue_number=1, comicvine_id=value)
            for row, value in zip(series, [142, None, 0, -20], strict=True)
        ]
        session.add_all(issues)
        arc = StoryArc(name="Preserved arc", comicvine_id=77)
        session.add(arc)
        await session.flush()
        session.add_all(
            [
                StoryArcExternalIdentity(
                    story_arc_id=arc.id, source="comicvine", namespace="story_arc", external_id="77"
                ),
                StoryArcExternalIdentity(
                    story_arc_id=arc.id,
                    source="mylar3",
                    namespace="source-a",
                    external_id="arbitrary-scope",
                ),
            ]
        )
        return series, issues


async def test_schema_migration_matches_production_models_and_is_retry_safe(identity_probe_db):
    engine, _, _ = identity_probe_db
    await _predecessor(engine)
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert set(_NAMES) <= set(names)
        await connection.run_sync(lambda conn: _schema(conn).upgrade())

        def compare(conn):
            expected = MetaData()
            for parent in ("series", "issues", "story_arcs"):
                Table(parent, expected, Column("id", Integer, primary_key=True))
            for name in _NAMES:
                Base.metadata.tables[name].to_metadata(expected)
            context = MigrationContext.configure(
                conn,
                opts={
                    "include_object": lambda obj, name, type_, reflected, compare_to: (
                        name in _NAMES if type_ == "table" else True
                    ),
                    "compare_server_default": True,
                },
            )
            return compare_metadata(context, expected)

        assert await connection.run_sync(compare) == []
        await connection.run_sync(lambda conn: _schema(conn).downgrade())
        names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert not set(_NAMES) & set(names)
        await connection.run_sync(lambda conn: _schema(conn).upgrade())


async def test_backfill_round_trip_preserves_legacy_values_and_scoped_arcs(identity_probe_db):
    engine, factory, _ = identity_probe_db
    await _seed(factory)
    await _predecessor(engine)
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        await connection.run_sync(lambda conn: _data(conn).upgrade())
        for kind, expected in (("series", "42"), ("issue", "142")):
            table = Base.metadata.tables[f"{kind}_external_identities"]
            rows = (await connection.execute(select(table))).all()
            assert len(rows) == 1
            assert rows[0].external_id == expected
            assert rows[0].verification_state == "verified"
            assert rows[0].evidence_kind == "legacy_backfill"
            assert rows[0].verified_at is None  # No provider re-verification was performed.
            assert rows[0].last_seen_at is None
            history = Base.metadata.tables[f"{kind}_identity_events"]
            event = (await connection.execute(select(history))).one()
            payload = json.loads(event.request_json)
            assert payload["actor"] == "migration"
            assert payload["origin"]["record_kind"] == kind
            # Frozen migration payload must match the versioned production retry contract.
            request = IdentityEventRequest(
                UUID(payload["operation_id"]),
                payload["local_id"],
                IdentityVerificationAction.VERIFY,
                IdentityEventEvidence(
                    ExactIdentityEvidence(
                        ExternalIdentityRef(
                            IdentityNamespace.COMICVINE, MetadataEntityKind(kind), expected
                        ),
                        IdentityEvidenceKind.LEGACY_BACKFILL,
                    ),
                    payload["evidence_revision"],
                    IdentityEvidenceLocator(IdentityEvidenceRecordKind(kind), payload["local_id"]),
                ),
                actor=IdentityEventActor.MIGRATION,
            )
            prepared = prepare_identity_event(request)
            assert (event.event_key, event.request_fingerprint, event.request_json) == (
                prepared.event_key,
                prepared.request_fingerprint,
                prepared.request_json,
            )
        await connection.run_sync(lambda conn: _data(conn).upgrade())
        assert (
            len(
                (
                    await connection.execute(select(Base.metadata.tables["series_identity_events"]))
                ).all()
            )
            == 1
        )
        await connection.run_sync(lambda conn: _data(conn).downgrade())
        await connection.run_sync(lambda conn: _schema(conn).downgrade())
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        await connection.run_sync(lambda conn: _data(conn).upgrade())
    async with factory() as session:
        assert list(await session.scalars(select(Series.comicvine_id).order_by(Series.id))) == [
            42,
            None,
            0,
            -10,
        ]
        assert list(await session.scalars(select(Issue.comicvine_id).order_by(Issue.id))) == [
            142,
            None,
            0,
            -20,
        ]
        rows = (
            await session.execute(
                select(
                    StoryArcExternalIdentity.source, StoryArcExternalIdentity.external_id
                ).order_by(StoryArcExternalIdentity.id)
            )
        ).all()
        assert rows == [("comicvine", "77"), ("mylar3", "arbitrary-scope")]


@pytest.mark.parametrize(
    "change", ["new_provider", "review_history", "changed_owner", "changed_state"]
)
async def test_downgrade_refuses_to_discard_nonreconstructible_identity_data(
    identity_probe_db, change
):
    engine, factory, _ = identity_probe_db
    series, _ = await _seed(factory)
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _data(conn).upgrade())
        active = Base.metadata.tables["series_external_identities"]
        history = Base.metadata.tables["series_identity_events"]
        if change == "new_provider":
            await connection.execute(
                insert(active).values(
                    series_id=series[0].id,
                    identity_namespace="metron",
                    external_id="90",
                    verification_state="verified",
                    evidence_kind="provider_result",
                )
            )
        elif change == "review_history":
            await connection.execute(update(history).values(evidence_kind="user_selection"))
        elif change == "changed_owner":
            await connection.execute(
                update(Series).where(Series.id == series[0].id).values(comicvine_id=45)
            )
        else:
            await connection.execute(update(active).values(verification_state="conflicted"))
    async with engine.begin() as connection:
        with pytest.raises(RuntimeError, match="identity"):
            await connection.run_sync(lambda conn: _data(conn).downgrade())
        assert len((await connection.execute(select(active))).all()) >= 1
        assert len((await connection.execute(select(history))).all()) == 1


async def test_backfill_is_bounded_and_rolls_back_without_changing_legacy(identity_probe_db):
    engine, factory, _ = identity_probe_db
    async with factory.begin() as session:
        session.add_all(
            [
                Series(title=f"Page {n}", sort_title=f"page {n}", comicvine_id=n + 1)
                for n in range(1003)
            ]
        )
    batches = []

    def record_batch(_conn, _cursor, statement, parameters, _context, executemany):
        if executemany and statement.startswith("INSERT INTO series_"):
            batches.append(len(parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", record_batch)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            await connection.run_sync(lambda conn: _data(conn).upgrade())
            table = Base.metadata.tables["series_external_identities"]
            assert len((await connection.execute(select(table))).all()) == 1003
            await transaction.rollback()
            assert (await connection.execute(select(table))).all() == []
            assert len((await connection.execute(select(Series.id))).all()) == 1003
        assert batches and max(batches) <= 400  # Two IN clauses stay below SQLite's 999-bind floor.
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record_batch)


@pytest.mark.parametrize("case", ["ownership", "parent", "state", "numeric", "retry"])
async def test_actual_migration_constraints_are_enforced(identity_probe_db, case):
    engine, factory, _ = identity_probe_db
    series, _ = await _seed(factory)
    await _predecessor(engine)
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        await connection.run_sync(lambda conn: _data(conn).upgrade())
    active = Base.metadata.tables["series_external_identities"]
    history = Base.metadata.tables["series_identity_events"]
    values = dict(
        series_id=series[1].id,
        identity_namespace="comicvine",
        external_id="43",
        verification_state="verified",
        evidence_kind="provider_result",
    )
    table = active
    if case == "ownership":
        values["external_id"] = "42"
    elif case == "parent":
        values["series_id"] = 999999
    elif case == "state":
        values["verification_state"] = "rejected"
    elif case == "numeric":
        values["external_id"] = "0043"
    else:
        async with engine.connect() as connection:
            values = dict((await connection.execute(select(history))).mappings().one())
        values.pop("id")
        table = history
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await connection.execute(insert(table).values(values))


async def test_schema_retry_recovers_missing_index_and_refuses_nonempty_downgrade(
    identity_probe_db,
):
    engine, factory, _ = identity_probe_db
    await _seed(factory)
    await _predecessor(engine)
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        await connection.run_sync(
            lambda conn: _schema(conn).op.drop_index(
                "ix_series_identity_events_claim", table_name="series_identity_events"
            )
        )
        await connection.run_sync(lambda conn: _schema(conn).upgrade())
        indexes = await connection.run_sync(
            lambda conn: inspect(conn).get_indexes("series_identity_events")
        )
        assert "ix_series_identity_events_claim" in {item["name"] for item in indexes}
        await connection.run_sync(lambda conn: _data(conn).upgrade())
        with pytest.raises(RuntimeError, match="identity"):
            await connection.run_sync(lambda conn: _schema(conn).downgrade())
        assert (
            len(
                (
                    await connection.execute(
                        select(Base.metadata.tables["series_external_identities"])
                    )
                ).all()
            )
            == 1
        )
