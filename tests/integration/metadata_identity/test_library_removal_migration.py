"""Removal journal migration must retain unresolved authorization and receipts."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select, update

from pullbox.models.library_removal import LibraryRemoval
from tests.integration.metadata_identity.test_library_removal import prepared, record


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/c0w1x2y3z456_add_library_removal_journal.py"
    )
    spec = importlib.util.spec_from_file_location("library_removal_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_removal_migration_roundtrip(identity_probe_db):
    engine, _, _ = identity_probe_db
    table = LibraryRemoval.__table__
    async with engine.begin() as connection:
        await connection.run_sync(table.drop)
        await connection.run_sync(lambda conn: revision(conn).upgrade())
        assert await connection.run_sync(lambda conn: inspect(conn).has_table(table.name))
        await connection.run_sync(lambda conn: revision(conn).downgrade())
        assert not await connection.run_sync(lambda conn: inspect(conn).has_table(table.name))
        await connection.run_sync(lambda conn: revision(conn).upgrade())


@pytest.mark.parametrize("state", ["intended", "detached", "complete", "review"])
async def test_removal_downgrade_refuses_retained_evidence(identity_probe_db, tmp_path, state):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(
                update(LibraryRemoval).values(state=state, active=state != "complete")
            )
        with pytest.raises(RuntimeError, match="removal evidence"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda conn: revision(conn).downgrade())
        async with factory() as session:
            assert await session.scalar(select(LibraryRemoval.plan_json)) == plan.model_dump_json()
