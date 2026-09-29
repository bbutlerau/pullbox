"""Trash cleanup must never consume a recovery dependency or a fresh trash tree."""

import asyncio
import hashlib
import os
import threading
from uuid import uuid4

import pytest
from sqlalchemy import inspect, select

from pullbox.core.exceptions import ValidationError
from pullbox.models.library_conversion import LibraryConversion
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services import library_trash_cleanup as cleanup
from pullbox.services.archive_metadata_publication import _fingerprint
from pullbox.services.library_conversion_files import (
    ConversionBinding,
    ConversionFile,
    ConversionPlan,
    directories,
)
from pullbox.services.library_mutation_coordination import lock_file_mutation_admission
from pullbox.services.library_removal import prepare_removal, record_removal
from pullbox.services.library_removal_cleanup import complete_removal
from pullbox.services.library_trash_cleanup import cleanup_trash
from tests.integration.metadata_identity.test_library_removal_cleanup import detached
from tests.unit.test_library_delete_service import _seed_root


async def test_empty_trash_preserves_active_removal_backup(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        plan.trash_path.write_bytes(b"backup awaiting cleanup")
        async with factory() as session:
            result = await cleanup_trash(session, plan.trash_path.parent)
        assert plan.trash_path.exists(), "Active recovery backup must not be emptied"
        assert result.retained_entries > 0


async def test_fresh_completed_folder_keeps_old_children(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=True) as plan:
        for child in plan.stage.iterdir():
            os.utime(child, (1, 1))
        async with factory() as session:
            await complete_removal(session, plan.operation_id)
        async with factory() as session:
            result = await cleanup_trash(session, plan.trash_path.parent, retention_days=30)
        assert (plan.trash_path / "issue.cbz").exists(), "Retention starts when folder was trashed"
        assert result.deleted_entries == 0


async def test_empty_trash_preserves_conversion_backup(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = tmp_path.resolve() / "library"
    root.mkdir()
    trash = root / ".trash"
    trash.mkdir()
    source = root / "comic.cbr"
    backup = trash / source.name
    source.write_bytes(b"original")
    backup.write_bytes(b"original")
    output_stage = root / ".pullbox-conversion-output" / "output.cbz"
    backup_stage = trash / ".pullbox-conversion-backup" / "original"
    output_stage.parent.mkdir()
    backup_stage.parent.mkdir()
    plan = ConversionPlan(
        binding=ConversionBinding(
            root_id=1,
            root_path=str(root),
            file_id=None,
            issue_id=None,
            file_format=None,
            file_size=None,
            file_modified_at=None,
        ),
        original=ConversionFile(path=source, fingerprint=_fingerprint(source), digest="a" * 64),
        output=ConversionFile(
            path=source.with_suffix(".cbz"), fingerprint=_fingerprint(source), digest="b" * 64
        ),
        backup=ConversionFile(path=backup, fingerprint=_fingerprint(backup), digest="a" * 64),
        output_stage=output_stage,
        backup_stage=backup_stage,
        directories=directories(source, backup),
    )
    async with factory.begin() as session:
        session.add(LibraryConversion(operation_id=str(uuid4()), plan_json=plan.model_dump_json()))
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert backup.exists(), "Conversion backup is still required by its journal"
    assert result.retained_entries > 0


async def test_unrecorded_private_stage_is_not_trash(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    private = trash / ".pullbox-conversion-pending" / "original"
    private.parent.mkdir(parents=True)
    private.write_bytes(b"work before durable admission")
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert private.exists(), "Preparation work must survive before its journal is recorded"
    assert result.retained_entries > 0


async def test_trash_root_symlink_is_refused(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "keep.cbz"
    victim.write_bytes(b"not trash")
    trash = tmp_path / "trash"
    trash.symlink_to(outside, target_is_directory=True)
    async with factory() as session:
        with pytest.raises(ValidationError):
            await cleanup_trash(session, trash)
    assert victim.exists()


async def test_legacy_retention_and_empty_trash_keep_existing_contract(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    nested = trash / "nested"
    nested.mkdir(parents=True)
    old = nested / "old.cbz"
    old.write_bytes(b"old")
    os.utime(old, (1, 1))
    fresh = trash / "fresh.cbz"
    fresh.write_bytes(b"fresh")
    async with factory() as session:
        result = await cleanup_trash(session, trash, retention_days=30)
    assert result.deleted_entries == 2
    assert not nested.exists() and fresh.exists()
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert result.deleted_entries == 1
    assert trash.is_dir() and not list(trash.iterdir())


async def test_recorded_trash_path_has_indexed_receipt_lookup(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert (
                getattr(row, "trash_path_key", None)
                == hashlib.sha256(os.fsencode(plan.trash_path)).hexdigest()
            )
        async with engine.connect() as connection:
            indexes = await connection.run_sync(
                lambda conn: inspect(conn).get_indexes("library_removals")
            )
        assert any(index["column_names"] == ["trash_path_key"] for index in indexes)


async def test_admission_is_rechecked_after_file_discovery(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    root = tmp_path.resolve() / "library"
    trash = root / ".trash"
    trash.mkdir(parents=True)
    file = trash / "claimed.cbz"
    file.write_bytes(b"claimed after traversal")
    async with factory.begin() as session:
        model = await _seed_root(session, root)
        root_id = model.id
    plan = prepare_removal(file, root_id=root_id, root_path=root)
    injected = False

    async def reserve_then_lock(session):
        nonlocal injected
        if not injected:
            injected = True
            async with factory.begin() as owner:
                await record_removal(owner, plan)
        await lock_file_mutation_admission(session)

    monkeypatch.setattr(cleanup, "lock_file_mutation_admission", reserve_then_lock)
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert injected and file.exists() and result.retained_entries > 0


async def test_cancel_joins_unlink_before_releasing_admission(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    trash.mkdir()
    (trash / "old.cbz").write_bytes(b"old")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    real = cleanup.remove_entry

    def paused(entry):
        entered.set()
        assert release.wait(10)
        try:
            return real(entry)
        finally:
            finished.set()

    monkeypatch.setattr(cleanup, "remove_entry", paused)
    async with factory() as session:
        task = asyncio.create_task(cleanup_trash(session, trash))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()

        async def contender():
            async with factory.begin() as other:
                await lock_file_mutation_admission(other)
                assert finished.is_set(), "Cancellation released the lock while unlink was running"

        other = asyncio.create_task(contender())
        try:
            await asyncio.sleep(0.05)
            assert not task.done() and not other.done()
            task.cancel()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        await asyncio.wait_for(other, 5)
    assert finished.is_set() and not list(trash.iterdir())


async def test_changed_entry_and_symlink_parent_are_not_followed(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    nested = trash / "nested"
    nested.mkdir(parents=True)
    (nested / "comic.cbz").write_bytes(b"trash")
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "comic.cbz"
    victim.write_bytes(b"not trash")
    real = cleanup.remove_entry
    swapped = False

    def swap(entry):
        nonlocal swapped
        if not swapped:
            swapped = True
            nested.rename(tmp_path / "retained-original")
            nested.symlink_to(outside, target_is_directory=True)
        return real(entry)

    monkeypatch.setattr(cleanup, "remove_entry", swap)
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert victim.read_bytes() == b"not trash"
    assert (tmp_path / "retained-original" / "comic.cbz").exists()
    assert result.retained_entries >= 1


async def test_unknown_active_evidence_fails_closed(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    trash.mkdir()
    file = trash / "old.cbz"
    file.write_bytes(b"keep while recovery evidence is unknown")
    async with factory.begin() as session:
        session.add(LibraryRemoval(operation_id=str(uuid4()), plan_json="{}"))
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert file.exists() and result.retained_entries == 1


async def test_completed_trash_expires_from_receipt_and_preserves_journal(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=True) as plan:
        for child in plan.stage.iterdir():
            os.utime(child, (1, 1))
        async with factory() as session:
            await complete_removal(session, plan.operation_id)
        async with factory.begin() as session:
            row = await session.scalar(select(LibraryRemoval))
            import json

            proof = json.loads(row.cleanup_json)
            proof["trash_mtime_ns"] = 1
            row.cleanup_json = json.dumps(proof)
        async with factory() as session:
            result = await cleanup_trash(session, plan.trash_path.parent, retention_days=30)
        assert not plan.trash_path.exists() and result.deleted_entries == 3
        async with factory() as session:
            assert await session.scalar(select(LibraryRemoval.state)) == "complete"


async def test_open_transaction_is_not_committed_by_cleanup(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        session.add(LibraryRemoval(operation_id=str(uuid4()), plan_json="{}"))
        await session.flush()
        with pytest.raises(ValidationError, match="idle session"):
            await cleanup_trash(session, tmp_path / "trash")
        await session.rollback()
    async with factory() as session:
        assert await session.scalar(select(LibraryRemoval.id)) is None


async def test_nested_library_root_is_not_disposable_trash(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    trash = tmp_path.resolve() / "configured-trash"
    library = trash / "actual-library"
    library.mkdir(parents=True)
    file = library / "not-imported-yet.cbz"
    file.write_bytes(b"live library data")
    async with factory.begin() as session:
        await _seed_root(session, library)
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert file.exists(), "A nested library root must not become disposable trash"
    assert result.retained_entries >= 1


async def test_directory_discovery_does_not_hold_database_writer(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    trash.mkdir()
    (trash / "old.cbz").write_bytes(b"old")
    entered, release = threading.Event(), threading.Event()
    real = cleanup.walk_trash

    def paused(root):
        entered.set()
        assert release.wait(10)
        yield from real(root)

    monkeypatch.setattr(cleanup, "walk_trash", paused)
    async with factory() as session:
        task = asyncio.create_task(cleanup_trash(session, trash))
        assert await asyncio.to_thread(entered.wait, 5)
        try:

            async def write():
                async with factory.begin() as other:
                    await lock_file_mutation_admission(other)

            await asyncio.wait_for(write(), 2)
        finally:
            release.set()
        result = await asyncio.wait_for(task, 5)
    assert result.deleted_entries == 1


async def test_entry_discovery_is_bounded_and_trash_symlinks_do_not_follow_targets(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    trash = tmp_path / "trash"
    trash.mkdir()
    for number in range(140):
        (trash / f"{number}.cbz").write_bytes(b"old")
    external = tmp_path / "outside.cbz"
    external.write_bytes(b"keep")
    (trash / "link.cbz").symlink_to(external)
    walked = 0
    real_walk, real_remove = cleanup.walk_trash, cleanup.remove_entry

    def counted(root):
        nonlocal walked
        for entry in real_walk(root):
            walked += 1
            yield entry

    removed = 0

    def counted_remove(entry):
        nonlocal removed
        assert walked - removed <= 64, "Do not materialize the whole trash listing"
        result = real_remove(entry)
        removed += int(result)
        return result

    monkeypatch.setattr(cleanup, "walk_trash", counted)
    monkeypatch.setattr(cleanup, "remove_entry", counted_remove)
    async with factory() as session:
        result = await cleanup_trash(session, trash)
    assert result.deleted_entries == 141
    assert external.read_bytes() == b"keep"
