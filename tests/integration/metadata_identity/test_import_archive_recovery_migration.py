"""Settlement migration preserves intent and forbids erasing successor evidence."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, update

from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication
from tests.integration.metadata_identity.test_archive_finalization_migration import (
    revision as previous,
)
from tests.integration.metadata_identity.test_archive_metadata_publication import (
    load,
    prepared,
    record,
)
from tests.integration.metadata_identity.test_archive_metadata_publication import (
    revision as original,
)


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/a8u9v0w1x234_settle_stopped_archive_publications.py"
    )
    spec = importlib.util.spec_from_file_location("archive_settlement_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_settlement_upgrade_preserves_pending_journal(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    table = ArchiveMetadataPublication.__table__
    async with engine.begin() as connection:
        await connection.run_sync(table.drop)
        await connection.run_sync(lambda conn: original(conn).upgrade())
        await connection.run_sync(lambda conn: previous(conn).upgrade())
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).upgrade())
            constraints = await connection.run_sync(
                lambda conn: inspect(conn).get_check_constraints(table.name)
            )
            states = next(
                item["sqltext"]
                for item in constraints
                if item["name"] == "archive_publication_state"
            )
            assert "settled" in states, "Stopped publications need retained completion evidence"
        assert await load(factory, saved.operation_id) == saved
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).downgrade())
            await connection.run_sync(lambda conn: revision(conn).upgrade())
        assert await load(factory, saved.operation_id) == saved


async def test_settlement_downgrade_refuses_to_erase_evidence(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(
                update(ArchiveMetadataPublication).values(
                    state="settled", active_file_id=None, active_path_key=None
                )
            )
        with pytest.raises(RuntimeError, match="settled archive publication"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda conn: revision(conn).downgrade())
        assert (await load(factory, saved.operation_id)).state.value == "settled"
