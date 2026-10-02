"""Conversion journal upgrade/downgrade and orphan preservation on both engines."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select, update

from pullbox.models.library_conversion import LibraryConversion
from pullbox.services.library_conversion_recovery import record_conversion
from tests.integration.metadata_identity.test_library_conversion_lifecycle import prepared


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/b9v0w1x2y345_add_library_conversion_journal.py"
    )
    spec = importlib.util.spec_from_file_location("library_conversion_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_conversion_migration_roundtrip(identity_probe_db):
    engine, _, _ = identity_probe_db
    table = LibraryConversion.__table__
    async with engine.begin() as connection:
        await connection.run_sync(table.drop)
        await connection.run_sync(lambda conn: revision(conn).upgrade())
        keys = await connection.run_sync(lambda conn: inspect(conn).get_foreign_keys(table.name))
        assert keys[0]["options"]["ondelete"] == "SET NULL"
        await connection.run_sync(lambda conn: revision(conn).downgrade())
        await connection.run_sync(lambda conn: revision(conn).upgrade())


@pytest.mark.parametrize("state", ["intended", "registered", "complete", "review"])
async def test_conversion_downgrade_retains_evidence(identity_probe_db, tmp_path, state):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan, operation):
        async with factory() as session:
            await record_conversion(session, plan, operation)
            await session.execute(
                update(LibraryConversion).values(state=state, active=state != "complete")
            )
            await session.commit()
        with pytest.raises(RuntimeError, match="conversion evidence"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda conn: revision(conn).downgrade())
        async with factory() as session:
            assert (
                await session.scalar(select(LibraryConversion.plan_json)) == plan.model_dump_json()
            )
