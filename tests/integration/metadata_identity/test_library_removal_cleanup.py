"""Committed cleanup uses retained private artifacts, never the old public path."""

import asyncio
import os
import sys
import threading
import time
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import select, update

from pullbox.core.exceptions import ValidationError
from pullbox.models import LibraryFile
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services import library_removal_cleanup as cleanup
from pullbox.services.library_removal import prepare_removal, record_removal, stage_removal
from pullbox.services.library_removal_cleanup import complete_removal
from pullbox.services.library_removal_files import check_stop
from tests.unit.test_library_delete_service import _seed_root, _seed_series_issue_file


@pytest.fixture(autouse=True)
def cross_device_branch(monkeypatch):
    # Exercise real copying and publication even when pytest's temp directories share a disk.
    monkeypatch.setattr(cleanup, "_same_filesystem", lambda plan: False)


@asynccontextmanager
async def detached(
    factory, tmp_path, *, folder=False, trash=False, commit=True, extra_link=None, old_mtime=False
):
    root_path = tmp_path.resolve() / "library"
    root_path.mkdir()
    parent = root_path / "series"
    parent.mkdir()
    file = parent / "issue.cbz"
    file.write_bytes(b"original comic")
    if folder:
        (parent / "second.cbz").write_bytes(b"second comic")
        if extra_link is not None:
            (parent / "external-link").symlink_to(extra_link)
    if old_mtime:
        os.utime(parent if folder else file, (1, 1))
    async with factory.begin() as session:
        root = await _seed_root(session, root_path)
        _, _, linked = await _seed_series_issue_file(session, root, file)
        root_id, file_id = root.id, linked.id
    plan = prepare_removal(
        parent if folder else file,
        root_id=root_id,
        root_path=root_path,
        disposition="trash" if trash else "delete",
        trash_path=tmp_path.resolve() / "trash" / parent.name if trash else None,
    )
    async with factory.begin() as session:
        await record_removal(session, plan)
    async with factory() as session:
        await stage_removal(session, plan.operation_id)
        await session.delete(await session.get(LibraryFile, file_id))
        if commit:
            await session.commit()
        else:
            await session.rollback()
    yield plan


@pytest.mark.parametrize("folder", [False, True])
@pytest.mark.parametrize("trash", [False, True])
async def test_committed_cleanup_finishes_and_preserves_replacement(
    identity_probe_db, tmp_path, folder, trash
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, folder=folder, trash=trash) as plan:
        if folder:
            plan.source.mkdir()
            replacement = plan.source / "issue.cbz"
        else:
            replacement = plan.source
        replacement.write_bytes(b"new public copy")
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert replacement.read_bytes() == b"new public copy"
        assert not plan.stage.exists()
        if trash:
            backup = plan.trash_path / "issue.cbz" if folder else plan.trash_path
            assert backup.read_bytes() == b"original comic"
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert row.state == "complete" and not row.active
            assert await complete_removal(session, plan.operation_id) == "complete"


async def test_uncommitted_removal_cannot_be_cleaned_up(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, commit=False) as plan:
        async with factory() as session:
            with pytest.raises(ValidationError, match="committed"):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.read_bytes() == b"original comic"


async def test_trash_collision_never_replaces_or_deletes_original(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        plan.trash_path.write_bytes(b"existing trash")
        async with factory() as session:
            with pytest.raises((FileExistsError, ValidationError)):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.read_bytes() == b"original comic"
        assert plan.trash_path.read_bytes() == b"existing trash"


async def test_changed_private_payload_never_deleted(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        plan.stage.rename(plan.stage.with_name("saved-original"))
        plan.stage.write_bytes(b"replacement payload")
        async with factory() as session:
            with pytest.raises(ValidationError):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.read_bytes() == b"replacement payload"


async def test_slow_copy_releases_database_and_serializes_workers(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        entered, release = threading.Event(), threading.Event()
        real = cleanup.copy_payload

        def pause(plan, stop):
            entered.set()
            assert release.wait(10)
            return real(plan, stop)

        monkeypatch.setattr(cleanup, "copy_payload", pause)
        async with factory() as session:
            task = asyncio.create_task(complete_removal(session, plan.operation_id))
            assert await asyncio.to_thread(entered.wait, 10)
            try:

                async def write():
                    async with factory.begin() as other:
                        await other.execute(update(LibraryRemoval).values(active=True))

                await asyncio.wait_for(write(), 2)
                async with factory() as other:
                    with pytest.raises(BlockingIOError):
                        await complete_removal(other, plan.operation_id)
            finally:
                release.set()
                assert await asyncio.wait_for(task, 10) == "complete"


@pytest.mark.parametrize("folder", [False, True])
async def test_cancelled_copy_retains_source_and_retries_owned_partial_backup(
    identity_probe_db, tmp_path, monkeypatch, folder
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=folder) as plan:
        entered, stopped = threading.Event(), threading.Event()
        real = cleanup.copy_payload

        def cancel_copy(plan, stop):
            partial = plan.trash_stage / "partial.cbz" if folder else plan.trash_stage
            partial.write_bytes(b"partial copy")
            entered.set()
            try:
                assert stop.wait(10)
                check_stop(stop)
            finally:
                stopped.set()

        monkeypatch.setattr(cleanup, "copy_payload", cancel_copy)
        async with factory() as session:
            task = asyncio.create_task(complete_removal(session, plan.operation_id))
            assert await asyncio.to_thread(entered.wait, 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 10)
        assert stopped.is_set() and plan.stage.exists()
        monkeypatch.setattr(cleanup, "copy_payload", real)
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        backup = plan.trash_path / "issue.cbz" if folder else plan.trash_path
        assert backup.read_bytes() == b"original comic"
        assert not (plan.trash_path / "partial.cbz").exists()


@pytest.mark.parametrize("trash", [False, True])
async def test_interrupted_recursive_cleanup_resumes_without_touching_public_replacement(
    identity_probe_db, tmp_path, monkeypatch, trash
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, folder=True, trash=trash) as plan:
        real = cleanup.remove_payload

        def interrupted(path, expected, stop, *, partial):
            if path == plan.stage:
                (path / "issue.cbz").unlink()
                raise OSError("injected interruption after first file")
            return real(path, expected, stop, partial=partial)

        monkeypatch.setattr(cleanup, "remove_payload", interrupted)
        async with factory() as session:
            with pytest.raises(OSError, match="injected"):
                await complete_removal(session, plan.operation_id)
        plan.source.mkdir()
        (plan.source / "replacement.cbz").write_bytes(b"replacement")
        monkeypatch.setattr(cleanup, "remove_payload", real)
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert (plan.source / "replacement.cbz").read_bytes() == b"replacement"
        if trash:
            assert (plan.trash_path / "issue.cbz").read_bytes() == b"original comic"
            assert (plan.trash_path / "second.cbz").read_bytes() == b"second comic"


@pytest.mark.parametrize("rename", [False, True])
async def test_published_trash_survives_lost_database_acknowledgement(
    identity_probe_db, tmp_path, monkeypatch, rename
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        monkeypatch.setattr(cleanup, "_same_filesystem", lambda plan: rename)
        real = cleanup._save

        async def lost_response(session, plan, encoded, previous, proof, **kwargs):
            if proof.phase == "deleting":
                raise OSError("injected commit connection loss")
            return await real(session, plan, encoded, previous, proof, **kwargs)

        monkeypatch.setattr(cleanup, "_save", lost_response)
        async with factory() as session:
            with pytest.raises(OSError, match="injected"):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.exists() is not rename
        assert plan.trash_path.read_bytes() == b"original comic"
        monkeypatch.setattr(cleanup, "_save", real)
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"


@pytest.mark.parametrize("change", ["contents", "missing", "replacement"])
async def test_changed_published_backup_prevents_source_cleanup(
    identity_probe_db, tmp_path, monkeypatch, change
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True) as plan:
        real = cleanup.remove_payload

        def fail(*args, **kwargs):
            raise OSError("injected pause before source cleanup")

        monkeypatch.setattr(cleanup, "remove_payload", fail)
        async with factory() as session:
            with pytest.raises(OSError):
                await complete_removal(session, plan.operation_id)
        if change in {"missing", "replacement"}:
            plan.trash_path.unlink()
        if change in {"contents", "replacement"}:
            plan.trash_path.write_bytes(b"changed backup")
        monkeypatch.setattr(cleanup, "remove_payload", real)
        async with factory() as session:
            with pytest.raises(ValidationError):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.read_bytes() == b"original comic"


@pytest.mark.parametrize("folder", [False, True])
async def test_same_filesystem_trash_uses_exclusive_rename_without_copying(
    identity_probe_db, tmp_path, monkeypatch, folder
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=folder) as plan:
        monkeypatch.setattr(cleanup, "_same_filesystem", lambda plan: True)

        def no_copy(*args):
            pytest.fail("Same-filesystem trash must not copy a potentially huge collection")

        monkeypatch.setattr(cleanup, "copy_payload", no_copy)
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert plan.trash_path.stat().st_ino == plan.fingerprint[1]


@pytest.mark.parametrize("trash", [False, True])
async def test_nested_symlinks_never_modify_external_files(identity_probe_db, tmp_path, trash):
    _, factory, _ = identity_probe_db
    outside = tmp_path / "external.cbz"
    outside.write_bytes(b"external data")
    async with detached(factory, tmp_path, trash=trash, folder=True, extra_link=outside) as plan:
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert outside.read_bytes() == b"external data"
        if trash:
            assert (plan.trash_path / "external-link").is_symlink()


async def test_cleanup_claim_fences_another_process_and_releases_after_exit(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        script = (
            "import sys; from pullbox.services.library_removal import decode_removal; "
            "from pullbox.services.library_removal_files import _claim; "
            "fd = _claim(decode_removal(sys.stdin.readline())); "
            "print('claimed', flush=True); sys.stdin.readline()"
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            script,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            process.stdin.write(plan.model_dump_json().encode() + b"\n")
            await process.stdin.drain()
            assert await asyncio.wait_for(process.stdout.readline(), 10) == b"claimed\n"
            async with factory() as session:
                with pytest.raises(BlockingIOError):
                    await complete_removal(session, plan.operation_id)
            assert plan.stage.exists()
        finally:
            process.kill()
            await process.communicate()
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"


async def test_old_retain_only_plans_never_authorize_cleanup(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path) as plan:
        historical = plan.model_dump(mode="json")
        for key in ("disposition", "trash_path", "trash_stage"):
            historical.pop(key)
        import json

        async with factory.begin() as session:
            await session.execute(update(LibraryRemoval).values(plan_json=json.dumps(historical)))
        async with factory() as session:
            with pytest.raises(ValidationError, match="does not authorize"):
                await complete_removal(session, plan.operation_id)
        assert plan.stage.read_bytes() == b"original comic"


@pytest.mark.parametrize("folder", [False, True])
async def test_trash_retention_starts_at_publication_not_original_mtime(
    identity_probe_db, tmp_path, monkeypatch, folder
):
    _, factory, _ = identity_probe_db
    async with detached(factory, tmp_path, trash=True, folder=folder, old_mtime=True) as plan:
        monkeypatch.setattr(cleanup, "_same_filesystem", lambda plan: True)
        before = int(time.time())
        async with factory() as session:
            assert await complete_removal(session, plan.operation_id) == "complete"
        assert plan.trash_path.stat().st_mtime >= before
