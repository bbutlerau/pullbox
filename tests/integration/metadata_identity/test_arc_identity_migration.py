"""Real arc lifecycle migration preserves scoped import keys and retained history."""

from __future__ import annotations

import json

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    event,
    func,
    insert,
    inspect,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError

from pullbox.models import Base, StoryArc
from tests.integration.metadata_identity.test_production_migration import _revision

_NAME = "story_arc_external_identities"
_NEW = {
    "verification_state",
    "evidence_kind",
    "evidence_locator",
    "verified_at",
    "last_seen_at",
    "revision",
}


def _migration(conn):
    return _revision("r9l0m1n2o345_story_arc_identity_lifecycle", conn)


async def _old_schema(engine):
    metadata = MetaData()
    Table("story_arcs", metadata, Column("id", Integer, primary_key=True))
    table = Table(
        _NAME,
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "story_arc_id", Integer, ForeignKey("story_arcs.id", ondelete="CASCADE"), nullable=False
        ),
        Column("source", String(50), nullable=False),
        Column("namespace", String(100), nullable=False),
        Column("external_id", String(255), nullable=False),
        Column("source_url", String(1000)),
        Column("evidence", JSON, nullable=False, server_default="{}"),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        UniqueConstraint(
            "source", "namespace", "external_id", name="uq_story_arc_external_identity"
        ),
        Index("ix_story_arc_external_identities_arc_id", "story_arc_id", "id"),
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.tables[_NAME].drop)
        await connection.run_sync(table.create)
    return table


async def _arcs(factory, values=(31, None)):
    async with factory.begin() as session:
        arcs = [StoryArc(name=f"Arc {i}", comicvine_id=value) for i, value in enumerate(values)]
        session.add_all(arcs)
        await session.flush()
        return [arc.id for arc in arcs]


async def test_arc_migration_backfills_proof_and_preserves_import_scopes(identity_probe_db):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, second = await _arcs(factory)
    async with engine.begin() as connection:
        await connection.execute(
            insert(old),
            [
                {
                    "story_arc_id": first,
                    "source": "mylar3",
                    "namespace": "db-a",
                    "external_id": "non-numeric",
                },
                {
                    "story_arc_id": first,
                    "source": "mylar3",
                    "namespace": "db-b",
                    "external_id": "non-numeric",
                },
                {
                    "story_arc_id": first,
                    "source": "comicvine",
                    "namespace": "import-scope",
                    "external_id": "arbitrary",
                },
                {
                    "story_arc_id": second,
                    "source": "gcd",
                    "namespace": "story_arc",
                    "external_id": "71",
                },
            ],
        )
        before = (await connection.execute(select(old).order_by(old.c.id))).all()
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        columns = await connection.run_sync(
            lambda conn: {c["name"] for c in inspect(conn).get_columns(_NAME)}
        )
        assert columns >= _NEW
        active = Base.metadata.tables[_NAME]
        events = Base.metadata.tables["story_arc_identity_events"]
        assert (
            await connection.execute(
                select(old).where(old.c.id <= before[-1].id).order_by(old.c.id)
            )
        ).all() == before
        rows = (await connection.execute(select(active).order_by(active.c.id))).mappings().all()
        assert len(rows) == 5
        assert [row.verification_state for row in rows] == [
            None,
            None,
            None,
            "verified",
            "verified",
        ]
        assert rows[-1].external_id == "31" and rows[-1].story_arc_id == first
        assert all(row.verified_at is None and row.last_seen_at is None for row in rows)
        history = (await connection.execute(select(events))).mappings().all()
        assert len(history) == 2
        for event in history:
            payload = json.loads(event.request_json)
            assert (
                payload["actor"] == "migration"
                and payload["identity"]["entity_kind"] == "story_arc"
            )
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert await connection.scalar(select(func.count()).select_from(events)) == 2
        await connection.run_sync(lambda conn: _migration(conn).downgrade())
        assert await connection.scalar(select(func.count()).select_from(events)) == 0
        assert len((await connection.execute(select(old))).all()) == 5
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert await connection.scalar(select(func.count()).select_from(events)) == 2


@pytest.mark.parametrize(
    "problem", ["duplicate_provider", "column_disagreement", "other_owner", "wrong_type", "invalid"]
)
async def test_arc_migration_refuses_ambiguous_legacy_evidence_before_ddl(
    identity_probe_db, problem
):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, second = await _arcs(factory)
    rows = [
        {
            "story_arc_id": first,
            "source": "comicvine",
            "namespace": "story_arc",
            "external_id": "31",
        }
    ]
    if problem == "duplicate_provider":
        rows.append({**rows[0], "external_id": "32"})
    elif problem == "column_disagreement":
        rows[0]["external_id"] = "32"
    elif problem == "other_owner":
        rows[0]["story_arc_id"] = second
    else:
        rows[0]["external_id"] = "4050-31" if problem == "wrong_type" else "unparseable"
    async with engine.begin() as connection:
        await connection.execute(insert(old), rows)
        before = (await connection.execute(select(old))).all()
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert (await connection.execute(select(old))).all() == before
        columns = await connection.run_sync(
            lambda conn: {c["name"] for c in inspect(conn).get_columns(_NAME)}
        )
        assert not _NEW & columns


@pytest.mark.parametrize("raw", ["4045-31", "0031", " 4045-0031 "])
async def test_arc_migration_normalizes_proven_legacy_keys_reversibly(identity_probe_db, raw):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, _ = await _arcs(factory)
    async with engine.begin() as connection:
        await connection.execute(
            insert(old).values(
                story_arc_id=first,
                source="comicvine",
                namespace="story_arc",
                external_id=raw,
                source_url="https://comicvine.gamespot.com/arc/4045-31/",
                evidence={"imported_story_arc_id": 17, "unknown": {"keep": True}},
            )
        )
        before = (await connection.execute(select(old))).mappings().one()
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        active = Base.metadata.tables[_NAME]
        events = Base.metadata.tables["story_arc_identity_events"]
        after = (await connection.execute(select(active))).mappings().one()
        assert after.external_id == "31"
        assert after.id == before.id
        # Use the same legacy projection, not different UTC result processors.
        assert await connection.scalar(select(old.c.created_at)) == before.created_at
        assert after.source_url == before.source_url
        assert after.evidence["unknown"] == before.evidence["unknown"]
        assert after.evidence["_pullbox_identity_normalization"] == {
            "schema_version": 1,
            "external_id": raw,
        }
        assert after.verification_state == "verified"
        event = (await connection.execute(select(events))).mappings().one()
        assert json.loads(event.request_json)["identity"]["external_id"] == "31"
        assert json.loads(event.request_json)["legacy_external_id"] == raw
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert await connection.scalar(select(func.count()).select_from(events)) == 1
        await connection.run_sync(lambda conn: _migration(conn).downgrade())
        assert (await connection.execute(select(old))).mappings().one() == before
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert await connection.scalar(select(active.c.external_id)) == "31"


@pytest.mark.parametrize(
    "problem", ["alias_owner", "alias_same_owner", "column_owner", "bad_late", "marker"]
)
async def test_arc_normalization_refuses_ambiguity_before_any_write(identity_probe_db, problem):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    ids = await _arcs(factory, (None,) * 205)
    rows = [
        {
            "story_arc_id": local_id,
            "source": "comicvine",
            "namespace": "story_arc",
            "external_id": f"4045-{index + 100}",
            "evidence": {},
        }
        for index, local_id in enumerate(ids)
    ]
    if problem in {"alias_owner", "alias_same_owner"}:
        rows[-1]["external_id"] = "00100"
        if problem == "alias_same_owner":
            rows[-1]["story_arc_id"] = ids[0]
    elif problem == "bad_late":
        rows[-1]["external_id"] = "4000-100"
    elif problem == "marker":
        rows[-1]["evidence"] = {"_pullbox_identity_normalization": {"user": "data"}}
    async with engine.begin() as connection:
        await connection.execute(insert(old), rows)
        if problem == "column_owner":
            await connection.execute(
                update(StoryArc).where(StoryArc.id == ids[-1]).values(comicvine_id=100)
            )
        before = (await connection.execute(select(old).order_by(old.c.id))).all()
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert (await connection.execute(select(old).order_by(old.c.id))).all() == before
        columns = await connection.run_sync(
            lambda conn: {c["name"] for c in inspect(conn).get_columns(_NAME)}
        )
        assert not _NEW & columns


async def test_frozen_arc_parser_matches_runtime_boundary(identity_probe_db):
    from pullbox.core.comicvine_arc_identity import normalize_comicvine_arc_id

    engine, _, _ = identity_probe_db
    valid = ["12", "00012", " 4045-00012 ", f"4045-{2**63 - 1}"]
    invalid = ["", " ", "4045-0", "4000-12", "4045- 12", "4045-12/", "\t12", "\uff11", str(2**63)]
    async with engine.connect() as connection:
        for value in valid:
            result = await connection.run_sync(
                lambda conn, value=value: _migration(conn)._canonical_id("comicvine", value)
            )
            assert result == normalize_comicvine_arc_id(value)
        for value in invalid:
            with pytest.raises(ValueError):
                normalize_comicvine_arc_id(value)
            with pytest.raises(RuntimeError, match="review"):
                await connection.run_sync(
                    lambda conn, value=value: _migration(conn)._canonical_id("comicvine", value)
                )


async def test_normalization_downgrade_preserves_later_decisions(identity_probe_db):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, _ = await _arcs(factory)
    async with engine.begin() as connection:
        await connection.execute(
            insert(old).values(
                story_arc_id=first,
                source="comicvine",
                namespace="story_arc",
                external_id="4045-31",
            )
        )
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        active = Base.metadata.tables[_NAME]
        await connection.execute(update(active).values(verification_state="conflicted", revision=2))
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).downgrade())
        assert await connection.scalar(select(active.c.external_id)) == "31"
        assert await connection.scalar(select(active.c.verification_state)) == "conflicted"


async def test_arc_migration_checks_all_current_proof_before_normalizing(identity_probe_db):
    engine, factory, _ = identity_probe_db
    first, second = await _arcs(factory, (None, None))
    active = Base.metadata.tables[_NAME]
    async with engine.begin() as connection:
        await connection.execute(
            insert(active),
            [
                {
                    "story_arc_id": first,
                    "source": "comicvine",
                    "namespace": "story_arc",
                    "external_id": "4045-31",
                },
                {
                    "story_arc_id": second,
                    "source": "comicvine",
                    "namespace": "story_arc",
                    "external_id": "32",
                },
            ],
        )
        await connection.execute(
            update(active)
            .where(active.c.story_arc_id == second)
            .values(verification_state="conflicted", revision=2)
        )
        before = (await connection.execute(select(active).order_by(active.c.id))).all()
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert (await connection.execute(select(active).order_by(active.c.id))).all() == before


async def test_arc_migration_preserves_existing_history_before_normalizing(identity_probe_db):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, _ = await _arcs(factory)
    events = Base.metadata.tables["story_arc_identity_events"]
    async with engine.begin() as connection:
        await connection.execute(
            insert(old).values(
                story_arc_id=first, source="comicvine", namespace="story_arc", external_id="4045-31"
            )
        )
        receipt = await connection.run_sync(
            lambda conn: _migration(conn)._expected(first, "comicvine", "31")[1]
        )
        await connection.execute(insert(events).values(**receipt))
        before = (await connection.execute(select(old))).all()
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).upgrade())
        assert (await connection.execute(select(old))).all() == before
        assert await connection.scalar(select(func.count()).select_from(events)) == 1


async def test_arc_normalization_pages_large_legacy_libraries(identity_probe_db):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    ids = await _arcs(factory, (None,) * 210)
    maximum_binds = 0
    query_count = 0

    def track(_conn, _cursor, _statement, parameters, _context, many):
        nonlocal maximum_binds, query_count
        query_count += 1
        maximum_binds = max(
            maximum_binds,
            max((len(row) for row in parameters), default=0) if many else len(parameters),
        )

    async with engine.begin() as connection:
        await connection.execute(
            insert(old),
            [
                {
                    "story_arc_id": local_id,
                    "source": "comicvine",
                    "namespace": "story_arc",
                    "external_id": f"4045-{i + 100}",
                }
                for i, local_id in enumerate(ids)
            ],
        )
        before = (await connection.execute(select(old).order_by(old.c.id))).all()
        event.listen(engine.sync_engine, "before_cursor_execute", track)
        try:
            await connection.run_sync(lambda conn: _migration(conn).upgrade())
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", track)
        assert maximum_binds <= 999
        assert query_count < 650
        events = Base.metadata.tables["story_arc_identity_events"]
        assert await connection.scalar(select(func.count()).select_from(events)) == 210
        await connection.run_sync(lambda conn: _migration(conn).downgrade())
        assert (await connection.execute(select(old).order_by(old.c.id))).all() == before


@pytest.mark.parametrize("change", ["state", "history", "revision", "scoped_lifecycle"])
async def test_arc_downgrade_refuses_to_discard_later_decisions(identity_probe_db, change):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, _ = await _arcs(factory)
    async with engine.begin() as connection:
        await connection.execute(
            insert(old).values(story_arc_id=first, source="folder", namespace="a", external_id="a")
        )
        await connection.run_sync(lambda conn: _migration(conn).upgrade())
        columns = await connection.run_sync(
            lambda conn: {c["name"] for c in inspect(conn).get_columns(_NAME)}
        )
        assert columns >= _NEW
        active = Base.metadata.tables[_NAME]
        events = Base.metadata.tables["story_arc_identity_events"]
        if change == "history":
            await connection.execute(update(events).values(verification_state="rejected"))
        elif change == "scoped_lifecycle":
            await connection.execute(
                update(active).where(active.c.source == "folder").values(evidence_kind="migration")
            )
        else:
            await connection.execute(
                update(active)
                .where(active.c.source == "comicvine")
                .values(
                    **({"verification_state": "stale"} if change == "state" else {"revision": 2})
                )
            )
        with pytest.raises(RuntimeError, match="review"):
            await connection.run_sync(lambda conn: _migration(conn).downgrade())
        assert await connection.scalar(select(func.count()).select_from(events)) == 1


async def test_arc_schema_matches_models_and_only_constrains_provider_scope(identity_probe_db):
    engine, factory, _ = identity_probe_db
    old = await _old_schema(engine)
    first, _ = await _arcs(factory, (None, None))
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: _migration(conn).upgrade())

        def compare(conn):
            expected = MetaData()
            Table("story_arcs", expected, Column("id", Integer, primary_key=True))
            Base.metadata.tables[_NAME].to_metadata(expected)

            def compare_defaults(context, inspected, modeled, rendered, default, modeled_text):
                # PostgreSQL JSON has no equality operator for Alembic's default comparison.
                if context.dialect.name == "postgresql" and isinstance(modeled.type, JSON):
                    actual = rendered.removesuffix("::json").removeprefix("'").removesuffix("'")
                    return json.loads(actual) != json.loads(default.arg)
                return None

            return compare_metadata(
                MigrationContext.configure(
                    conn,
                    opts={
                        "include_object": lambda obj, name, type_, reflected, compare_to: (
                            name == _NAME if type_ == "table" else True
                        ),
                        "compare_server_default": compare_defaults,
                    },
                ),
                expected,
            )

        assert await connection.run_sync(compare) == []
        await connection.execute(
            insert(old).values(
                story_arc_id=first, source="gcd", namespace="story_arc", external_id="70"
            )
        )
        with pytest.raises(IntegrityError):
            async with connection.begin_nested():
                await connection.execute(
                    insert(old).values(
                        story_arc_id=first, source="gcd", namespace="story_arc", external_id="71"
                    )
                )
        await connection.execute(
            insert(old),
            [
                {
                    "story_arc_id": first,
                    "source": "gcd",
                    "namespace": "import-a",
                    "external_id": "71",
                },
                {
                    "story_arc_id": first,
                    "source": "gcd",
                    "namespace": "import-b",
                    "external_id": "72",
                },
            ],
        )
