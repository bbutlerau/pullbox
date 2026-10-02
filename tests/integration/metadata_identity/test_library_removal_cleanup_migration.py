"""Retained cleanup evidence must survive upgrades and forbid unsafe downgrade."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select, update
from sqlalchemy.exc import IntegrityError

from pullbox.models.library_removal import LibraryRemoval
from tests.integration.metadata_identity.test_library_removal import prepared, record


def revision(connection):
    path = Path(__file__).resolve().parents[3] / (
        "alembic/versions/d1x2y3z4a567_add_removal_cleanup_evidence.py"
    )
    spec = importlib.util.spec_from_file_location("removal_cleanup_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_cleanup_migration_preserves_retained_intents(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).downgrade())
            columns = await connection.run_sync(
                lambda conn: {
                    item["name"] for item in inspect(conn).get_columns("library_removals")
                }
            )
            assert "cleanup_json" not in columns
            await connection.run_sync(lambda conn: revision(conn).upgrade())
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert row.plan_json == plan.model_dump_json() and row.cleanup_json is None
            assert row.active and row.state == "intended"


async def test_cleanup_evidence_prevents_downgrade(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(update(LibraryRemoval).values(cleanup_json="{}"))
        with pytest.raises(RuntimeError, match="cleanup evidence"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda conn: revision(conn).downgrade())
        async with factory() as session:
            assert await session.scalar(select(LibraryRemoval.cleanup_json)) == "{}"


async def test_cleanup_storage_rejects_unbounded_evidence(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        with pytest.raises(IntegrityError):
            async with factory.begin() as session:
                await session.execute(update(LibraryRemoval).values(cleanup_json="x" * 4097))
