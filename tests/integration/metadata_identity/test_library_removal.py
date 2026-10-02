"""Real file/database boundary for recoverable deletion staging."""

import asyncio
import threading
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from pullbox.core.exceptions import ValidationError
from pullbox.models import LibraryFile, LibraryRoot
from pullbox.models.library import LibraryFileStorageMode
from pullbox.models.library_removal import LibraryRemoval
from pullbox.services import library_removal as removals
from pullbox.services.archive_metadata_publication import _fingerprint
from pullbox.services.library_conversion_files import directories
from pullbox.services.library_removal import (
    RemovalPlan,
    prepare_removal,
    record_removal,
    recover_library_removals,
    recover_uncommitted_removal,
    require_no_library_removal,
    stage_removal,
)
from pullbox.services.library_rename_service import rename_library_entry
from tests.integration.metadata_identity import test_archive_metadata_publication as archive_cases
from tests.unit.test_library_delete_service import _seed_root, _seed_series_issue_file


@asynccontextmanager
async def prepared(factory, tmp_path, *, folder=False):
    root_path = tmp_path.resolve() / "library"
    root_path.mkdir()
    folder_path = root_path / "series"
    folder_path.mkdir()
    file = folder_path / "issue.cbz"
    file.write_bytes(b"original comic")
    source = folder_path if folder else file
    async with factory.begin() as session:
        root = await _seed_root(session, root_path)
        _, _, linked = await _seed_series_issue_file(session, root, file)
        root_id, file_id = root.id, linked.id
    plan = prepare_removal(source, root_id=root_id, root_path=root_path)
    yield plan, file_id


async def record(factory, plan):
    async with factory.begin() as session:
        await record_removal(session, plan)


@pytest.mark.parametrize("folder", [False, True])
async def test_removal_records_intent_before_touching_source(identity_probe_db, tmp_path, folder):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, folder=folder) as (plan, _):
        await record(factory, plan)
        assert plan.source.exists() and not plan.stage.exists()
        async with factory() as session:
            row = await session.scalar(select(LibraryRemoval))
            assert row is not None, "Removal must retain intent before changing disk"
            assert row.state == "intended" and row.active
            assert row.plan_json == plan.model_dump_json()


@pytest.mark.parametrize("folder", [False, True])
async def test_uncommitted_staging_restores_source_on_recovery(identity_probe_db, tmp_path, folder):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, folder=folder) as (plan, file_id):
        await record(factory, plan)
        async with factory() as session:
            await stage_removal(session, plan.operation_id)
            assert not plan.source.exists() and plan.stage.exists()
            await session.delete(await session.get(LibraryFile, file_id))
            await session.flush()
            await session.rollback()
        async with factory() as session:
            assert await recover_uncommitted_removal(session, plan.operation_id) == "abandoned"
        assert plan.source.exists() and not plan.stage.exists()
        async with factory() as session:
            assert await session.get(LibraryFile, file_id) is not None
            assert not (await session.scalar(select(LibraryRemoval))).active


async def test_staged_removal_commits_with_registration_detachment(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, file_id):
        await record(factory, plan)
        async with factory.begin() as session:
            await stage_removal(session, plan.operation_id)
            await session.delete(await session.get(LibraryFile, file_id))
        async with factory() as session:
            assert await session.get(LibraryFile, file_id) is None
            row = await session.scalar(select(LibraryRemoval))
            assert row is not None and row.state == "detached"
            assert await recover_uncommitted_removal(session, plan.operation_id) == "detached"
        assert not plan.source.exists() and plan.stage.read_bytes() == b"original comic"


@pytest.mark.parametrize(
    "change", ["reference", "disabled_root", "changed_source", "same_transaction"]
)
async def test_staging_revalidates_authority_and_committed_intent(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, file_id):
        if change != "same_transaction":
            await record(factory, plan)
        async with factory.begin() as session:
            if change == "reference":
                await session.execute(
                    update(LibraryFile)
                    .where(LibraryFile.id == file_id)
                    .values(storage_mode=LibraryFileStorageMode.REFERENCED)
                )
            elif change == "disabled_root":
                await session.execute(update(LibraryRoot).values(enabled=False))
        if change == "changed_source":
            plan.source.write_bytes(b"replacement")
        async with factory() as session:
            if change == "same_transaction":
                await record_removal(session, plan)
            with pytest.raises(ValidationError):
                await stage_removal(session, plan.operation_id)
        assert plan.source.exists() and not plan.stage.exists()


async def test_recovery_never_overwrites_replacement(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        async with factory() as session:
            await stage_removal(session, plan.operation_id)
            await session.rollback()
        plan.source.write_bytes(b"replacement")
        async with factory() as session:
            assert await recover_uncommitted_removal(session, plan.operation_id) == "review"
            assert (await session.scalar(select(LibraryRemoval))).active
        assert plan.source.read_bytes() == b"replacement"
        assert plan.stage.read_bytes() == b"original comic"


@pytest.mark.parametrize("relation", ["source", "ancestor", "descendant", "stage"])
async def test_removal_reservations_cover_whole_path_scope(identity_probe_db, tmp_path, relation):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, folder=True) as (plan, _):
        await record(factory, plan)
        path = {
            "source": plan.source,
            "ancestor": plan.source.parent,
            "descendant": plan.source / "new.cbz",
            "stage": plan.stage,
        }[relation]
        async with factory() as session:
            with pytest.raises(ValidationError, match="removal"):
                await require_no_library_removal(
                    session, path, include_descendants=relation == "ancestor"
                )
            await require_no_library_removal(
                session, plan.source.with_name("other"), include_descendants=True
            )


async def test_removal_blocks_new_archive_publication(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with archive_cases.prepared(factory, tmp_path) as (path, _, archive_plan):
        operation = uuid4()
        stage_parent = path.parent / f".pullbox-removal-{operation.hex}"
        stage_parent.mkdir(mode=0o700)
        async with factory() as session:
            root = await session.get(LibraryRoot, archive_plan.target.binding.library_root_id)
        plan = RemovalPlan(
            operation_id=operation,
            source=path,
            stage=stage_parent / "payload",
            root_id=root.id,
            root_path=root.path,
            fingerprint=_fingerprint(path),
            directories=directories(path, stage_parent / "payload"),
        )
        await record(factory, plan)
        with pytest.raises(ValidationError, match="removal"):
            await archive_cases.record(factory, archive_plan)
        assert path.exists()


@pytest.mark.parametrize("change", ["operation", "oversized", "invalid", "stage_parent"])
async def test_changed_removal_evidence_fails_closed(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        encoded = {
            "operation": plan.model_copy(update={"operation_id": uuid4()}).model_dump_json(),
            "oversized": '"' + "x" * 65534 + '"',
            "invalid": "{}",
            "stage_parent": plan.model_copy(
                update={"stage": tmp_path / "outside"}
            ).model_dump_json(),
        }[change]
        async with factory.begin() as session:
            await session.execute(update(LibraryRemoval).values(plan_json=encoded))
        async with factory() as session:
            with pytest.raises((ValueError, ValidationError)):
                await stage_removal(session, plan.operation_id)
        async with factory() as session:
            with pytest.raises((ValueError, ValidationError)):
                await require_no_library_removal(session, plan.source, include_descendants=False)
        assert plan.source.exists()


async def test_removal_row_must_match_its_plan_identity(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        changed = uuid4()
        async with factory.begin() as session:
            await session.execute(update(LibraryRemoval).values(operation_id=str(changed)))
        async with factory() as session:
            with pytest.raises(ValidationError):
                await stage_removal(session, changed)
        async with factory() as session:
            with pytest.raises(ValidationError):
                await require_no_library_removal(session, plan.source, include_descendants=False)
        assert plan.source.exists()


async def test_preparation_only_creates_private_sibling_staging(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root_path = tmp_path.resolve() / "library"
    root_path.mkdir()
    source = root_path / "issue.cbz"
    source.write_bytes(b"original")
    async with factory.begin() as session:
        root = await _seed_root(session, root_path)
        root_id = root.id
    plan = prepare_removal(source, root_id=root_id, root_path=root_path)
    assert isinstance(plan, RemovalPlan)
    assert plan.stage.parent.parent == source.parent
    assert plan.stage.parent.stat().st_mode & 0o777 == 0o700
    assert not plan.stage.exists()
    assert source.read_bytes() == b"original"
    await record(factory, plan)
    async with factory.begin() as session:
        await stage_removal(session, plan.operation_id)
    assert plan.stage.read_bytes() == b"original"


async def test_startup_restores_only_uncommitted_removals(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        async with factory() as session:
            await stage_removal(session, plan.operation_id)
            await session.rollback()
        async with factory() as session:
            assert await recover_library_removals(session) == 1
        assert plan.source.read_bytes() == b"original comic"
        async with factory() as session:
            assert await recover_library_removals(session) == 0


async def test_removal_prevents_rename_even_after_file_registration_deleted(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, file_id):
        await record(factory, plan)
        async with factory.begin() as session:
            await session.delete(await session.get(LibraryFile, file_id))
        async with factory() as session:
            with pytest.raises(ValidationError, match="removal"):
                await rename_library_entry(
                    session,
                    source=plan.source,
                    target=plan.source.with_name("new.cbz"),
                    kind="file",
                )
        assert plan.source.exists()


async def test_cancelled_staging_is_joined_before_rollback(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        await record(factory, plan)
        entered, release = threading.Event(), threading.Event()
        real = removals.rename_path_without_overwrite

        def pause_after_rename(source, destination):
            real(source, destination)
            entered.set()
            assert release.wait(10)

        monkeypatch.setattr(removals, "rename_path_without_overwrite", pause_after_rename)
        async with factory() as session:
            task = asyncio.create_task(stage_removal(session, plan.operation_id))
            assert await asyncio.to_thread(entered.wait, 10)
            task.cancel()
            await asyncio.sleep(0.05)
            try:
                assert not task.done(), "Cancelled staging must not outlive its transaction"
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 10)
            await session.rollback()
        async with factory() as session:
            assert await recover_uncommitted_removal(session, plan.operation_id) == "abandoned"
        assert plan.source.read_bytes() == b"original comic"


async def test_lost_commit_response_does_not_restore_committed_deletion(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, file_id):
        await record(factory, plan)
        async with factory() as session:
            await stage_removal(session, plan.operation_id)
            await session.delete(await session.get(LibraryFile, file_id))
            await session.commit()
            # Model the caller treating a lost acknowledgement as failure.
            await session.rollback()
        async with factory() as session:
            assert await recover_uncommitted_removal(session, plan.operation_id) == "detached"
            assert await session.get(LibraryFile, file_id) is None
        assert not plan.source.exists() and plan.stage.exists()


async def test_removal_admission_serializes_with_rename(identity_probe_db, tmp_path, monkeypatch):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (plan, _):
        entered, release = asyncio.Event(), asyncio.Event()
        real = removals._check_authority

        async def pause(*args):
            entered.set()
            await asyncio.wait_for(release.wait(), 10)
            await real(*args)

        monkeypatch.setattr(removals, "_check_authority", pause)
        pending = asyncio.create_task(record(factory, plan))
        await asyncio.wait_for(entered.wait(), 10)
        async with factory() as session:
            rename = asyncio.create_task(
                rename_library_entry(
                    session,
                    source=plan.source,
                    target=plan.source.with_name("new.cbz"),
                    kind="file",
                )
            )
            try:
                await asyncio.sleep(0.05)
                assert not rename.done()
            finally:
                release.set()
                await asyncio.wait_for(pending, 10)
                result = await asyncio.gather(rename, return_exceptions=True)
            assert isinstance(result[0], ValidationError)
        assert plan.source.exists()
