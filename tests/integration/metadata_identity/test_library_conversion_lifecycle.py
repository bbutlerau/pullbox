"""Real archive and database evidence for Library conversion ownership."""

import asyncio
import threading
import zipfile
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from pullbox.core.exceptions import ValidationError
from pullbox.models import LibraryFile, LibraryRoot
from pullbox.models.library import FileFormat
from pullbox.models.library_conversion import LibraryConversion
from pullbox.services import library_conversion_files as files
from pullbox.services import library_conversion_recovery as recovery
from pullbox.services import library_convert_service as service


async def seed(factory, directory, *, managed=True):
    source = directory / "Issue.cbr"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("001.jpg", b"page bytes")
    async with factory() as session:
        root = LibraryRoot(
            name="Comics", path=str(directory), enabled=True, allow_managed_writes=managed
        )
        session.add(root)
        await session.flush()
        file = LibraryFile(
            file_path=str(source),
            file_name=source.name,
            file_format=FileFormat.CBR,
            file_size=source.stat().st_size,
            file_modified_at=datetime.fromtimestamp(source.stat().st_mtime, UTC),
            library_root_id=root.id,
        )
        session.add(file)
        await session.commit()
        return source, file.id


async def convert(session, source, tmp_path):
    return await service.convert_library_file(
        session, source=source, trash_dir=tmp_path / "trash", trash_relative_path=source.name
    )


async def test_original_remains_until_registration_commits(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, file_id = await seed(factory, tmp_path)
    original = source.read_bytes()
    sync = service._sync_converted_file_record

    async def guarded_sync(*args, **kwargs):
        assert source.read_bytes() == original, "Original removed before registration committed"
        await sync(*args, **kwargs)

    monkeypatch.setattr(service, "_sync_converted_file_record", guarded_sync)
    async with factory() as session:
        result = await convert(session, source, tmp_path)
    assert not source.exists()
    assert Path(result.original_trash_path).read_bytes() == original
    with zipfile.ZipFile(result.target_path) as archive:
        assert archive.read("001.jpg") == b"page bytes"
    async with factory() as session:
        row = await session.get(LibraryFile, file_id)
        assert row.file_path == result.target_path
        assert row.file_format is FileFormat.CBZ


async def test_conversion_rechecks_root_write_policy(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path, managed=False)
    before = source.read_bytes()
    async with factory() as session:
        with pytest.raises(ValidationError, match="managed"):
            await convert(session, source, tmp_path)
    assert source.read_bytes() == before
    assert not source.with_suffix(".cbz").exists()


async def test_lost_registration_commit_ack_preserves_registered_output(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, file_id = await seed(factory, tmp_path)
    target = source.with_suffix(".cbz")
    async with factory() as session:
        commit = session.commit
        lost = False

        async def lose_ack():
            nonlocal lost
            converted = any(
                isinstance(row, LibraryFile) and row.file_path == str(target)
                for row in session.dirty
            )
            await commit()
            if converted and not lost:
                lost = True
                raise OSError("lost commit acknowledgement")

        monkeypatch.setattr(session, "commit", lose_ack)
        with suppress(ValidationError):
            await convert(session, source, tmp_path)
        assert lost
    async with factory() as session:
        path = await session.scalar(select(LibraryFile.file_path).where(LibraryFile.id == file_id))
    assert path == str(target)
    assert target.is_file(), "Compensation deleted the successfully registered output"
    async with factory() as session:
        assert await recovery.recover_library_conversions(session) == 1
    assert not source.exists()


@asynccontextmanager
async def prepared(factory, tmp_path):
    source, file_id = await seed(factory, tmp_path)
    async with factory() as session:
        binding = await recovery.read_conversion_binding(session, source)
    async with files.prepare_conversion(source, tmp_path / "trash" / source.name, binding) as plan:
        yield source, file_id, plan, uuid4()


@pytest.mark.parametrize("point", ["intent", "backup", "output"])
async def test_restart_classifies_each_publication_boundary(identity_probe_db, tmp_path, point):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (source, file_id, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            await session.commit()
        if point in {"backup", "output"}:
            files.publish_file_without_overwrite(plan.backup_stage, plan.backup.path)
        if point == "output":
            files.publish_file_without_overwrite(plan.output_stage, plan.output.path)
    # All private stages are gone, just as after interrupted preparation cleanup.
    async with factory() as session:
        state = await recovery.recover_conversion(session, operation)
        assert state == ("complete" if point == "output" else "abandoned")
        row = await session.get(LibraryFile, file_id)
        assert row.file_path == str(plan.output.path if point == "output" else source)
    assert source.exists() is (point != "output")
    if point == "output":
        assert plan.backup.path.is_file()
    async with factory() as session:
        assert await recovery.recover_conversion(session, operation) == state


async def test_intent_must_commit_before_publication(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (source, _, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            with pytest.raises(ValidationError, match="commit"):
                await recovery.publish_conversion(session, operation)
            await session.rollback()
        assert source.exists()
        assert not plan.output.path.exists()
        assert not plan.backup.path.exists()


@pytest.mark.parametrize("changed", ["original", "output", "backup", "registration"])
async def test_recovery_preserves_changed_evidence(identity_probe_db, tmp_path, changed):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (source, file_id, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            await session.commit()
            await recovery.publish_conversion(session, operation)
            await session.commit()
        if changed == "registration":
            async with factory() as session:
                await session.delete(await session.get(LibraryFile, file_id))
                await session.commit()
        else:
            getattr(plan, changed).path.write_bytes(b"external replacement")
    before = {path: path.read_bytes() for path in (source, plan.output.path, plan.backup.path)}
    async with factory() as session:
        assert await recovery.recover_conversion(session, operation) == "review"
        assert (await session.scalar(select(LibraryConversion))).active
    assert {path: path.read_bytes() for path in before} == before


@pytest.mark.parametrize("which", ["original", "output", "backup", "output_stage", "backup_stage"])
async def test_conversion_reservation_blocks_rename(identity_probe_db, tmp_path, which):
    from pullbox.services.library_rename_service import rename_library_entry

    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            await session.commit()
        path = getattr(plan, which)
        if not isinstance(path, Path):
            path = path.path
        async with factory() as session:
            with pytest.raises(ValidationError, match="conversion"):
                await rename_library_entry(
                    session, source=path, target=path.with_name("renamed"), kind="file"
                )
        async with factory() as session:
            with pytest.raises(ValidationError, match="conversion"):
                await rename_library_entry(
                    session,
                    source=path.parent,
                    target=path.parent.with_name("renamed"),
                    kind="folder",
                )


@pytest.mark.parametrize("stage", ["convert", "backup"])
async def test_cancel_private_preparation_preserves_source(
    identity_probe_db, tmp_path, monkeypatch, stage
):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    original = source.read_bytes()
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def slow(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            exited.set()

    monkeypatch.setattr(
        files,
        "convert_file_interruptible" if stage == "convert" else "transfer_file_interruptible",
        slow,
    )
    async with factory() as session:
        task = asyncio.create_task(convert(session, source, tmp_path))
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
    assert exited.is_set()
    assert source.read_bytes() == original
    assert not source.with_suffix(".cbz").exists()
    assert not list(tmp_path.rglob(".pullbox-conversion-*"))


async def test_concurrent_intents_serialize(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            await session.commit()
        async with factory() as session:
            with pytest.raises(ValidationError, match="conversion"):
                await recovery.record_conversion(session, plan, uuid4())


async def test_changed_source_during_preparation_never_publishes(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    transfer = files.transfer_file_interruptible

    async def change(*args, **kwargs):
        result = await transfer(*args, **kwargs)
        source.write_bytes(b"changed externally")
        return result

    monkeypatch.setattr(files, "transfer_file_interruptible", change)
    async with factory() as session:
        with pytest.raises(ValidationError, match="changed"):
            await convert(session, source, tmp_path)
    assert source.read_bytes() == b"changed externally"
    assert not source.with_suffix(".cbz").exists()


async def test_cancel_publication_joins_worker_and_leaves_recoverable_files(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    original = source.read_bytes()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    publish = recovery.publish

    def gated_publish(plan):
        entered.set()
        assert release.wait(5)
        publish(plan)
        finished.set()

    monkeypatch.setattr(recovery, "publish", gated_publish)
    async with factory() as session:
        task = asyncio.create_task(convert(session, source, tmp_path))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "Cancellation must join the publishing worker"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    assert finished.is_set()
    assert source.read_bytes() == original
    assert source.with_suffix(".cbz").is_file()
    async with factory() as session:
        assert await recovery.recover_library_conversions(session) == 1
    assert not source.exists()


@pytest.mark.parametrize("which", ["output", "backup"])
async def test_racing_destinations_are_never_replaced(identity_probe_db, tmp_path, which):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (source, _, plan, operation):
        async with factory() as session:
            await recovery.record_conversion(session, plan, operation)
            await session.commit()
            collision = getattr(plan, which).path
            collision.write_bytes(b"racing existing file")
            with pytest.raises(FileExistsError):
                await recovery.publish_conversion(session, operation)
            await session.rollback()
        assert collision.read_bytes() == b"racing existing file"
        assert source.is_file()


async def test_preparation_does_not_hold_database_writer(identity_probe_db, tmp_path, monkeypatch):
    from pullbox.services.library_mutation_coordination import lock_file_mutation_admission

    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    converter = files.convert_file_interruptible

    async def assert_unlocked(*args, **kwargs):
        async with factory() as session:
            await asyncio.wait_for(lock_file_mutation_admission(session), 2)
            await session.rollback()
        return await converter(*args, **kwargs)

    monkeypatch.setattr(files, "convert_file_interruptible", assert_unlocked)
    async with factory() as session:
        await convert(session, source, tmp_path)


async def test_conversion_retains_reader_registration_if_cleanup_fails(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, file_id = await seed(factory, tmp_path)
    remove = recovery.remove_original

    async def failed_cleanup(*args):
        raise OSError("storage unavailable")

    monkeypatch.setattr(recovery, "remove_original", failed_cleanup)
    async with factory() as session:
        with pytest.raises(ValidationError):
            await convert(session, source, tmp_path)
    assert source.is_file()
    async with factory() as session:
        assert (await session.get(LibraryFile, file_id)).file_path == str(
            source.with_suffix(".cbz")
        )
        assert (await session.scalar(select(LibraryConversion))).state == "registered"
    monkeypatch.setattr(recovery, "remove_original", remove)
    async with factory() as session:
        assert await recovery.recover_library_conversions(session) == 1
    assert not source.exists()


async def test_registered_nested_root_is_not_mistaken_for_ambiguity(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    child = tmp_path / "series"
    child.mkdir()
    source, _ = await seed(factory, child)
    async with factory() as session:
        session.add(LibraryRoot(name="Parent", path=str(tmp_path), enabled=True))
        await session.commit()
        result = await convert(session, source, tmp_path)
    assert Path(result.target_path).is_file()


async def test_changed_root_policy_during_preparation_rejects_publication(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    converter = files.convert_file_interruptible

    async def change_policy(*args, **kwargs):
        result = await converter(*args, **kwargs)
        async with factory() as session:
            root = await session.scalar(select(LibraryRoot))
            root.allow_managed_writes = False
            await session.commit()
        return result

    monkeypatch.setattr(files, "convert_file_interruptible", change_policy)
    async with factory() as session:
        with pytest.raises(ValidationError, match="managed"):
            await convert(session, source, tmp_path)
    assert source.is_file()
    assert not source.with_suffix(".cbz").exists()


async def test_success_hashes_recovery_evidence_only_once(identity_probe_db, tmp_path, monkeypatch):
    _, factory, _ = identity_probe_db
    source, _ = await seed(factory, tmp_path)
    inspect = recovery.inspect_conversion
    calls = 0

    async def count(plan):
        nonlocal calls
        calls += 1
        return await inspect(plan)

    monkeypatch.setattr(recovery, "inspect_conversion", count)
    async with factory() as session:
        await convert(session, source, tmp_path)
    assert calls == 1, "Cleanup must reuse unchanged evidence, not reread entire large archives"


async def test_long_source_name_keeps_a_recoverable_backup(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    source, file_id = await seed(factory, tmp_path)
    long_source = source.with_name("A" * 240 + ".cbr")
    source.rename(long_source)
    async with factory() as session:
        file = await session.get(LibraryFile, file_id)
        file.file_path = str(long_source)
        file.file_name = long_source.name
        await session.commit()
        result = await convert(session, long_source, tmp_path)
    assert Path(result.original_trash_path).name == long_source.name
    assert Path(result.original_trash_path).is_file()
    assert Path(result.target_path).is_file()
