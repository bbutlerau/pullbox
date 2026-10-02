"""Real Library renames must coordinate with durable metadata publications."""

import asyncio
import threading
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, update

from pullbox.core.events import EventBus
from pullbox.core.exceptions import ValidationError
from pullbox.models import LibraryFile, Series
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication, PublicationState
from pullbox.services import archive_metadata_publication as publications
from pullbox.services import library_rename_service as renames
from pullbox.services.archive_metadata_binding import (
    ArchiveMetadataBindingError,
    read_archive_metadata_binding,
)
from pullbox.services.archive_metadata_publication import publish_archive_publication
from pullbox.services.series_service import SeriesService
from tests.integration.metadata_identity.test_archive_metadata_publication import prepared, record


@pytest.mark.parametrize("state", ["intended", "published", "review"])
@pytest.mark.parametrize("kind", ["file", "folder"])
async def test_active_publication_prevents_library_rename(identity_probe_db, tmp_path, state, kind):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        receipt = await record(factory, plan)
        async with factory.begin() as session:
            if state == "published":
                await publish_archive_publication(session, receipt.operation_id)
            elif state == "review":
                await session.execute(
                    update(ArchiveMetadataPublication).values(state=PublicationState.REVIEW)
                )
        before = path.read_bytes()
        source = path if kind == "file" else path.parent
        target = source.with_name("renamed")
        async with factory() as session:
            with pytest.raises(ValidationError, match="metadata update"):
                await renames.rename_library_entry(session, source=source, target=target, kind=kind)
        assert path.read_bytes() == before
        assert not target.exists()


@pytest.mark.parametrize(
    "variant", ["orphan", "alias", "stage", "destination", "invalid", "changed_plan"]
)
async def test_reservations_protect_paths_not_only_live_library_rows(
    identity_probe_db, tmp_path, variant
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, stage, plan):
        await record(factory, plan)
        source, target = path, path.with_name("renamed.cbz")
        if variant == "orphan":
            async with factory.begin() as session:
                await session.execute(delete(LibraryFile))
        elif variant == "alias":
            alias = tmp_path / "alias"
            alias.symlink_to(path.parent, target_is_directory=True)
            source, target = alias / path.name, alias / "renamed.cbz"
        elif variant == "stage":
            source, target = stage.path, stage.path.with_name("renamed.cbz")
        elif variant == "destination":
            source, target = path.with_name("other.cbz"), path
            source.write_bytes(b"another file")
        elif variant == "invalid":
            async with factory.begin() as session:
                await session.execute(update(ArchiveMetadataPublication).values(plan_json="{}"))
        elif variant == "changed_plan":
            changed = plan.model_copy(
                update={"target": replace(plan.target, path=path.with_name("other.cbz"))}
            )
            async with factory.begin() as session:
                await session.execute(
                    update(ArchiveMetadataPublication).values(plan_json=changed.model_dump_json())
                )
        original = source.read_bytes()
        async with factory() as session:
            with pytest.raises(ValidationError, match="metadata update"):
                await renames.rename_library_entry(
                    session, source=source, target=target, kind="file"
                )
        assert source.read_bytes() == original


async def test_unrelated_rename_and_released_publication_remain_available(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        await record(factory, plan)
        unrelated = path.with_name("unrelated.cbz")
        unrelated.write_bytes(b"independent")
        async with factory() as session:
            await renames.rename_library_entry(
                session, source=unrelated, target=unrelated.with_name("new.cbz"), kind="file"
            )
        async with factory.begin() as session:
            await session.execute(
                update(ArchiveMetadataPublication).values(
                    state=PublicationState.ABANDONED, active_file_id=None, active_path_key=None
                )
            )
        target = path.with_name("renamed.cbz")
        async with factory() as session:
            await renames.rename_library_entry(session, source=path, target=target, kind="file")
        assert target.exists() and not path.exists()
        async with factory() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_path == str(target)


async def test_rename_admission_and_new_publication_cannot_race(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        entered, release = asyncio.Event(), asyncio.Event()
        sync = renames._sync_file_record

        async def pause_before_database_sync(*args, **kwargs):
            entered.set()
            await asyncio.wait_for(release.wait(), 10)
            await sync(*args, **kwargs)

        monkeypatch.setattr(renames, "_sync_file_record", pause_before_database_sync)
        target = path.with_name("renamed.cbz")
        async with factory() as session:
            rename = asyncio.create_task(
                renames.rename_library_entry(session, source=path, target=target, kind="file")
            )
            await asyncio.wait_for(entered.wait(), 10)
            publish = asyncio.create_task(record(factory, plan))
            try:
                await asyncio.sleep(0.1)
                assert not publish.done(), "Publication must wait for the rename transaction"
            finally:
                release.set()
                await asyncio.wait_for(rename, 10)
                result = await asyncio.gather(publish, return_exceptions=True)
            assert isinstance(result[0], ArchiveMetadataBindingError)
        assert target.exists() and not path.exists()


async def test_folder_rename_updates_only_literal_descendants(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    source = tmp_path / "Comics_100%"
    source.mkdir()
    sibling = tmp_path / "ComicsX100percent"
    sibling.mkdir()
    async with factory.begin() as session:
        moved = Series(title="Moved", sort_title="Moved", path=str(source / "inside"))
        untouched = Series(title="Untouched", sort_title="Untouched", path=str(sibling / "inside"))
        session.add_all([moved, untouched])
        await session.flush()
        moved_id, untouched_id = moved.id, untouched.id
    target = source.with_name("Renamed")
    async with factory() as session:
        await renames.rename_library_entry(session, source=source, target=target, kind="folder")
    async with factory() as session:
        assert (await session.get(Series, moved_id)).path == str(target / "inside")
        assert (await session.get(Series, untouched_id)).path == str(sibling / "inside")


async def test_inflight_publication_admission_blocks_rename(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        entered, release = asyncio.Event(), asyncio.Event()
        lock = publications._lock_binding

        async def pause_after_admission(*args, **kwargs):
            entered.set()
            await asyncio.wait_for(release.wait(), 10)
            await lock(*args, **kwargs)

        monkeypatch.setattr(publications, "_lock_binding", pause_after_admission)
        publish = asyncio.create_task(record(factory, plan))
        await asyncio.wait_for(entered.wait(), 10)
        async with factory() as session:
            rename = asyncio.create_task(
                renames.rename_library_entry(
                    session, source=path, target=path.with_name("renamed.cbz"), kind="file"
                )
            )
            try:
                await asyncio.sleep(0.1)
                assert not rename.done(), "Rename must wait for the publication admission"
            finally:
                release.set()
                await asyncio.wait_for(publish, 10)
                result = await asyncio.gather(rename, return_exceptions=True)
            assert isinstance(result[0], ValidationError)
        assert path.exists()


async def test_series_folder_rename_also_respects_publication(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        series_id = plan.target.binding.metadata.series.local_id
        async with factory.begin() as session:
            series = await session.get(Series, series_id)
            series.path = str(path.parent)
            series.library_root_id = plan.target.binding.library_root_id
        # Re-read the binding after the explicit series-path edit.
        async with factory() as session:
            bound = await read_archive_metadata_binding(
                session, plan.target.binding.library_file_id
            )
        plan = plan.model_copy(update={"target": replace(plan.target, binding=bound)})
        await record(factory, plan)
        async with factory() as session:
            with pytest.raises(ValidationError, match="metadata update"):
                await SeriesService(AsyncMock(), EventBus()).rename_series_folder(
                    session, series_id
                )
        assert path.exists()


async def test_cancelled_rename_restores_file_and_registration(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        entered = asyncio.Event()

        async def pause_before_database_sync(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(renames, "_sync_file_record", pause_before_database_sync)
        original = path.read_bytes()
        target = path.with_name("renamed.cbz")
        async with factory() as session:
            task = asyncio.create_task(
                renames.rename_library_entry(session, source=path, target=target, kind="file")
            )
            await asyncio.wait_for(entered.wait(), 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert path.exists(), "Cancellation must not strand the renamed file"
        assert path.read_bytes() == original and not target.exists()
        async with factory() as session:
            assert (
                await session.get(LibraryFile, plan.target.binding.library_file_id)
            ).file_path == str(path)


async def test_failed_rename_does_not_restore_someone_elses_replacement(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, _):
        target = path.with_name("renamed.cbz")
        saved_original = path.with_name("original.cbz")

        async def fail_after_external_replacement(*args, **kwargs):
            target.rename(saved_original)
            target.write_bytes(b"someone else's file")
            raise RuntimeError("database write failed")

        monkeypatch.setattr(renames, "_sync_file_record", fail_after_external_replacement)
        async with factory() as session:
            with pytest.raises(ValidationError, match="could not be completed"):
                await renames.rename_library_entry(session, source=path, target=target, kind="file")
        assert target.exists(), "Rollback must not move a replacement it does not own"
        assert target.read_bytes() == b"someone else's file" and not path.exists()


async def test_cancel_joins_slow_rename_before_releasing_coordination(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, _):
        entered, release = threading.Event(), threading.Event()
        original = renames._rename_path

        def slow_rename(*args):
            entered.set()
            assert release.wait(10)
            original(*args)

        monkeypatch.setattr(renames, "_rename_path", slow_rename)
        target = path.with_name("renamed.cbz")
        async with factory() as session:
            task = asyncio.create_task(
                renames.rename_library_entry(session, source=path, target=target, kind="file")
            )
            assert await asyncio.to_thread(entered.wait, 10)
            try:
                task.cancel()
                await asyncio.sleep(0.05)
                task.cancel()
                await asyncio.sleep(0.05)
                assert not task.done(), "The filesystem worker still owns the rename boundary"
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 10)
        assert path.exists() and not target.exists()


async def test_cancel_during_commit_does_not_undo_a_committed_rename(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        entered, release = asyncio.Event(), asyncio.Event()
        target = path.with_name("renamed.cbz")
        async with factory() as session:
            original_commit = session.commit

            async def slow_commit():
                entered.set()
                await asyncio.wait_for(release.wait(), 10)
                await original_commit()

            monkeypatch.setattr(session, "commit", slow_commit)
            task = asyncio.create_task(
                renames.rename_library_entry(session, source=path, target=target, kind="file")
            )
            await asyncio.wait_for(entered.wait(), 10)
            try:
                task.cancel()
                await asyncio.sleep(0.05)
                task.cancel()
                await asyncio.sleep(0.05)
                assert not task.done()
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 10)
        assert target.exists() and not path.exists()
        async with factory() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_path == str(target)


@pytest.mark.parametrize("kind", ["file", "folder"])
async def test_lost_commit_acknowledgment_does_not_undo_registered_rename(
    identity_probe_db, tmp_path, monkeypatch, kind
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        source = path if kind == "file" else path.parent
        target = source.with_name("renamed")
        destination_file = target if kind == "file" else target / path.name
        original = path.read_bytes()
        async with factory() as session:
            original_commit = session.commit

            async def committed_without_acknowledgment():
                await original_commit()
                raise RuntimeError("connection lost after commit")

            monkeypatch.setattr(session, "commit", committed_without_acknowledgment)
            with pytest.raises(ValidationError, match="could not be completed"):
                await renames.rename_library_entry(session, source=source, target=target, kind=kind)
        assert destination_file.exists(), "A committed rename must not be compensated"
        assert destination_file.read_bytes() == original and not source.exists()
        async with factory() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_path == str(destination_file)
