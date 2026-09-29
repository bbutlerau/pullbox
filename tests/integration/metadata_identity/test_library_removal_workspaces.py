"""Terminal removal prunes only proven empty private workspaces."""

import asyncio
import importlib.util
import threading
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, inspect, select, update

from pullbox.models.library_removal import LibraryRemoval
from pullbox.services import library_removal_cleanup as cleanup
from pullbox.services.library_removal import recover_library_removals
from pullbox.services.library_removal_cleanup import complete_removal
from pullbox.services.library_removal_workspaces import prune_terminal_removal
from tests.integration.metadata_identity.test_library_removal_cleanup import detached


@pytest.mark.parametrize("folder", [False, True])
@pytest.mark.parametrize("mode", ["delete", "trash_rename", "trash_copy"])
async def test_completion_removes_private_workspaces_but_keeps_receipt_and_backup(
    identity_probe_db, tmp_path, monkeypatch, folder, mode
):
    _, factory, _ = identity_probe_db
    monkeypatch.setattr(cleanup, "_same_filesystem", lambda plan: mode == "trash_rename")
    async with detached(factory, tmp_path, folder=folder, trash=mode != "delete") as plan:
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert not plan.stage.parent.exists(), "Completed staging must not accumulate"
        if plan.trash_stage:
            assert not plan.trash_stage.parent.exists()
            backup = plan.trash_path / "issue.cbz" if folder else plan.trash_path
            assert backup.read_bytes() == b"original comic"
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert row.plan_json == plan.model_dump_json()
            assert row.cleanup_json and row.state == "complete" and not row.active
            assert row.workspace_cleaned
            assert await complete_removal(session, plan.operation_id) == "complete"


@pytest.mark.parametrize("folder", [False, True])
async def test_restart_prunes_abandoned_workspace_after_restoring_source(
    identity_probe_db, tmp_path, folder
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, folder=folder, trash=True, commit=False) as plan:
        async with factory() as session:
            assert await recover_library_removals(session) == 1
        file = plan.source / "issue.cbz" if folder else plan.source
        assert file.read_bytes() == b"original comic"
        assert not plan.stage.parent.exists()
        assert not plan.trash_stage.parent.exists()
        assert not plan.trash_path.exists()


async def complete_with_lost_response(factory, plan, monkeypatch):
    original = cleanup._save

    async def lose_response(*args, **kwargs):
        value = await original(*args, **kwargs)
        if kwargs.get("complete"):
            raise OSError("lost final commit response")
        return value

    with monkeypatch.context() as patch:
        patch.setattr(cleanup, "_save", lose_response)
        async with factory() as session:
            with pytest.raises(OSError, match="lost final"):
                await complete_removal(session, plan.operation_id)
    assert plan.stage.parent.exists()


async def test_restart_prunes_terminal_workspaces_after_lost_commit_response(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        async with factory() as session:
            assert await recover_library_removals(session) == 0
        assert not plan.stage.parent.exists(), "Restart must finish terminal workspace cleanup"
        assert not plan.trash_stage.parent.exists()
        assert plan.trash_path.read_bytes() == b"original comic"


@pytest.mark.parametrize("change", ["unknown_child", "replaced", "symlink", "lock_contents"])
async def test_terminal_cleanup_keeps_unproven_workspaces(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        workspace = plan.stage.parent
        if change in {"replaced", "symlink"}:
            saved = workspace.with_name("saved-workspace")
            workspace.rename(saved)
            if change == "symlink":
                workspace.symlink_to(saved, target_is_directory=True)
            else:
                workspace.mkdir(mode=0o700)
        evidence = workspace / ("cleanup.lock" if change == "lock_contents" else "unknown.cbz")
        evidence.write_bytes(b"must remain")
        async with factory() as session:
            assert await recover_library_removals(session) == 0
        assert evidence.read_bytes() == b"must remain"
        assert workspace.exists()
        assert plan.trash_path.read_bytes() == b"original comic"


async def test_restart_does_not_prune_workspace_while_cleanup_worker_still_holds_claim(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        entered, release = asyncio.Event(), asyncio.Event()
        original = cleanup._save

        async def pause_after_commit(*args, **kwargs):
            value = await original(*args, **kwargs)
            if kwargs.get("complete"):
                entered.set()
                await asyncio.wait_for(release.wait(), 10)
            return value

        monkeypatch.setattr(cleanup, "_save", pause_after_commit)
        async with factory() as session:
            task = asyncio.create_task(complete_removal(session, plan.operation_id))
            await asyncio.wait_for(entered.wait(), 10)
            try:
                async with factory() as other:
                    await recover_library_removals(other)
                assert plan.stage.parent.exists(), "Live cleanup still owns this workspace"
            finally:
                release.set()
                assert await asyncio.wait_for(task, 10) == "complete"
        assert not plan.stage.parent.exists()


async def test_restart_never_prunes_detached_payload(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        async with factory() as session:
            await recover_library_removals(session)
        assert plan.stage.read_bytes() == b"original comic"
        assert plan.trash_stage.parent.exists()


@pytest.mark.parametrize("change", ["empty_replacement", "parent_replaced", "terminal_payload"])
async def test_workspace_proof_not_just_filename_or_emptiness(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        workspace = plan.stage.parent
        if change == "parent_replaced":
            parent = workspace.parent
            parent.rename(parent.with_name("saved-parent"))
            parent.mkdir()
            workspace.mkdir(mode=0o700)
        elif change == "empty_replacement":
            workspace.rename(workspace.with_name("saved-workspace"))
            workspace.mkdir(mode=0o700)
        else:
            plan.stage.write_bytes(b"unproven private payload")
        async with factory() as session:
            await recover_library_removals(session)
            row = await session.scalar(select(LibraryRemoval))
            assert not row.workspace_cleaned
        assert workspace.exists()
        if change == "terminal_payload":
            assert plan.stage.read_bytes() == b"unproven private payload"


async def test_terminal_cleanup_never_commits_pending_caller_edits(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.core.exceptions import ValidationError

    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        async with factory() as session:
            await session.execute(update(LibraryRemoval).values(active=True))
            with pytest.raises(ValidationError, match="idle"):
                await prune_terminal_removal(session, plan.operation_id)
            await session.rollback()
        assert plan.stage.parent.exists()
        async with factory() as session:
            assert not (await session.scalar(select(LibraryRemoval))).active


async def test_restart_retry_after_partial_prune_and_lost_cleanup_receipt(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import library_removal_workspaces as workspaces

    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        # A prior attempt may have pruned the trash workspace before interruption.
        plan.trash_stage.parent.rmdir()
        real = workspaces.prune_workspaces

        def lose_acknowledgement(plan):
            real(plan)
            raise OSError("lost prune acknowledgement")

        with monkeypatch.context() as patch:
            patch.setattr(workspaces, "prune_workspaces", lose_acknowledgement)
            async with factory() as session:
                assert not await prune_terminal_removal(session, plan.operation_id)
        assert not plan.stage.parent.exists()
        async with factory() as session:
            await recover_library_removals(session)
            assert (await session.scalar(select(LibraryRemoval))).workspace_cleaned
        assert plan.trash_path.read_bytes() == b"original comic"


async def test_restart_cleanup_is_bounded_and_skips_recorded_successes(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import library_removal_workspaces as workspaces

    engine, factory, _ = identity_probe_db
    plans = []
    for index in range(10):
        home = tmp_path / str(index)
        home.mkdir()
        async with detached(factory, home) as plan:
            await complete_with_lost_response(factory, plan, monkeypatch)
            plans.append(plan)
    selects = []

    def capture(_conn, _cursor, statement, parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT") and "library_removals.id >" in statement:
            selects.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with factory() as session:
            await recover_library_removals(session)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert len(selects) >= 3
    assert all("LIMIT" in statement and 8 in parameters for statement, parameters in selects)
    assert all(not plan.stage.parent.exists() for plan in plans)

    def no_repeat(*args):
        pytest.fail("Recorded terminal workspace cleanup must not rescan the filesystem")

    monkeypatch.setattr(workspaces, "prune_workspaces", no_repeat)
    async with factory() as session:
        await recover_library_removals(session)


def migration(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/f3z4a5b6c789_mark_removal_workspace_cleanup.py"
    )
    spec = importlib.util.spec_from_file_location("removal_workspace_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_workspace_migration_preserves_receipts_and_defaults_to_unverified(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        async with factory() as session:
            await complete_removal(session, plan.operation_id)
            row = await session.scalar(select(LibraryRemoval))
            before = row.plan_json, row.cleanup_json, row.state, row.active
            assert row.workspace_cleaned
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: migration(conn).downgrade())
            columns = await connection.run_sync(
                lambda conn: inspect(conn).get_columns("library_removals")
            )
            assert "workspace_cleaned" not in {column["name"] for column in columns}
            await connection.run_sync(lambda conn: migration(conn).upgrade())
            indexes = await connection.run_sync(
                lambda conn: inspect(conn).get_indexes("library_removals")
            )
            assert any(
                index["column_names"] == ["workspace_cleaned", "active", "id"] for index in indexes
            )
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert (row.plan_json, row.cleanup_json, row.state, row.active) == before
            assert not row.workspace_cleaned
            await session.commit()
            await recover_library_removals(session)
        assert plan.trash_path.read_bytes() == b"original comic"


async def test_corrupt_terminal_operation_does_not_stop_startup_recovery(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        async with factory.begin() as session:
            await session.execute(update(LibraryRemoval).values(operation_id="invalid-operation"))
        async with factory() as session:
            assert await recover_library_removals(session) == 0
        assert plan.stage.parent.exists()


async def test_cancelled_workspace_prune_joins_worker_before_releasing_admission(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.services import library_removal_workspaces as workspaces
    from pullbox.services.library_mutation_coordination import lock_file_mutation_admission

    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        await complete_with_lost_response(factory, plan, monkeypatch)
        real = workspaces.prune_workspaces
        entered, release = threading.Event(), threading.Event()

        def pause(plan):
            entered.set()
            assert release.wait(10)
            real(plan)

        monkeypatch.setattr(workspaces, "prune_workspaces", pause)

        async def competing_writer():
            async with factory.begin() as session:
                await lock_file_mutation_admission(session)

        async with factory() as session:
            task = asyncio.create_task(prune_terminal_removal(session, plan.operation_id))
            assert await asyncio.to_thread(entered.wait, 10)
            writer = asyncio.create_task(competing_writer())
            try:
                for _ in range(3):
                    task.cancel()
                    await asyncio.sleep(0.02)
                assert not task.done() and not writer.done()
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 10)
                await asyncio.wait_for(writer, 10)
        assert not plan.stage.parent.exists()
        async with factory() as session:
            await recover_library_removals(session)
            assert (await session.scalar(select(LibraryRemoval))).workspace_cleaned
