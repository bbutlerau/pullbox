"""Existing trash receipts remain indexed across upgrades without touching files."""

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select

from pullbox.models.library_removal import LibraryRemoval
from pullbox.services.library_removal import trash_path_key
from pullbox.services.library_removal_cleanup import complete_removal
from tests.integration.metadata_identity.test_library_removal_cleanup import detached


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/e2y3z4a5b678_index_removal_trash_receipts.py"
    )
    spec = importlib.util.spec_from_file_location("trash_index_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_upgrade_backfills_completed_receipt_without_changing_evidence(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=True) as plan:
        async with factory() as session:
            await complete_removal(session, plan.operation_id)
            before = await session.scalar(select(LibraryRemoval.cleanup_json))
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).downgrade())
            columns = await connection.run_sync(
                lambda conn: inspect(conn).get_columns("library_removals")
            )
            assert "trash_path_key" not in {item["name"] for item in columns}
            await connection.run_sync(lambda conn: revision(conn).upgrade())
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert row.trash_path_key == trash_path_key(plan.trash_path)
            assert row.cleanup_json == before and row.state == "complete" and not row.active
        assert (plan.trash_path / "issue.cbz").read_bytes() == b"original comic"
