"""Durable intent survives the filesystem/transaction crash window."""

import asyncio
import importlib.util
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from zipfile import ZipFile

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, Integer, MetaData, Table, delete, inspect, update

from pullbox.core.archive_metadata import read_archive_metadata
from pullbox.models import Issue, LibraryFile, LibraryRoot, Series
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication
from pullbox.models.library import FileFormat
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity
from pullbox.services.archive_metadata_binding import (
    assemble_bound_archive_metadata,
    inspect_archive_metadata_target,
    read_archive_metadata_binding,
)
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    PublicationState,
    inspect_archive_publication,
    load_archive_publication,
    prepare_archive_publication,
    publish_archive_publication,
    reconcile_archive_publication,
    record_archive_publication,
)
from pullbox.services.archive_metadata_reconciliation import reconcile_archive_metadata
from pullbox.utilities.executors.archive_metadata_staging import stage_cbz_metadata_interruptible


@asynccontextmanager
async def prepared(factory, tmp_path, *, comicinfo=None, staged_mtime_ns=None):
    root_path = tmp_path / "comics"
    root_path.mkdir()
    path = root_path / "example.cbz"
    with ZipFile(path, "w") as archive:
        archive.writestr("page.jpg", b"page bytes")
        archive.writestr(
            "ComicInfo.xml", comicinfo or "<ComicInfo><Number>50-X</Number></ComicInfo>"
        )
    path.chmod(0o640)
    async with factory.begin() as session:
        root = LibraryRoot(name="Managed", path=str(root_path))
        series = Series(title="Example", sort_title="Example", year_start=1992)
        session.add_all([root, series])
        await session.flush()
        issue = Issue(series_id=series.id, issue_number=50, issue_number_text="50-X")
        session.add(issue)
        await session.flush()
        session.add_all(
            [
                SeriesExternalIdentity(
                    series_id=series.id,
                    identity_namespace="metron",
                    external_id="12",
                    verification_state="verified",
                    evidence_kind="provider_result",
                ),
                IssueExternalIdentity(
                    issue_id=issue.id,
                    identity_namespace="metron",
                    external_id="42",
                    verification_state="verified",
                    evidence_kind="provider_result",
                ),
            ]
        )
        file = LibraryFile(
            file_path=str(path),
            file_name=path.name,
            file_size=path.stat().st_size,
            file_format=FileFormat.CBZ,
            file_modified_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
            issue_id=issue.id,
            library_root_id=root.id,
        )
        session.add(file)
        await session.flush()
        file_id = file.id
    async with factory() as session:
        bound = await read_archive_metadata_binding(session, file_id)
    target = await inspect_archive_metadata_target(bound)
    evidence = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=10000)
    )
    series, issue = assemble_bound_archive_metadata(bound, evidence, now=datetime.now(UTC))
    async with stage_cbz_metadata_interruptible(
        path, root_path, series, issue, max_uncompressed_bytes=1000000
    ) as staged:
        if staged_mtime_ns is not None:
            os.utime(staged.path, ns=(staged_mtime_ns, staged_mtime_ns))
            info = staged.path.stat()
            staged = replace(
                staged,
                output_fingerprint=(
                    info.st_dev,
                    info.st_ino,
                    info.st_size,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                    info.st_mode,
                ),
            )
        plan = await prepare_archive_publication(target, staged, series, issue)
        assert plan is not None, "A verified stage needs durable pre-publication evidence"
        yield path, staged, plan


async def record(factory, plan, operation=None):
    operation = operation or uuid4()
    async with factory.begin() as session:
        saved = await record_archive_publication(session, plan, operation)
        assert saved is not None, "Publication intent must persist before replacement"
    return saved


async def load(factory, operation):
    async with factory() as session:
        result = await load_archive_publication(session, operation)
        assert result is not None
        return result


async def test_intent_round_trip_is_durable_and_does_not_publish(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, staged, plan):
        original = path.read_bytes()
        saved = await record(factory, plan)
        current = await load(factory, saved.operation_id)
        assert current == saved
        assert current.state is PublicationState.INTENDED
        assert current.plan == plan
        assert current.revision == 1
        assert path.read_bytes() == original and staged.path.exists()
        assert await record(factory, plan, saved.operation_id) == saved


async def test_record_is_caller_owned_and_rolls_back(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        operation = uuid4()
        async with factory() as session:
            assert await record_archive_publication(session, plan, operation) is not None
            await session.rollback()
        async with factory() as session:
            assert await load_archive_publication(session, operation) is None


async def test_only_one_intent_can_reserve_a_file(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):

        async def attempt():
            try:
                return await record(factory, plan)
            except ArchivePublicationError as exc:
                return exc.code

        results = await asyncio.gather(attempt(), attempt())
        assert sum(result == "publication_busy" for result in results) == 1
        assert sum(result != "publication_busy" for result in results) == 1


async def test_publication_preserves_mode_and_remains_reserved_for_db_finalization(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, staged, plan):
        saved = await record(factory, plan)
        async with factory.begin() as session:
            published = await publish_archive_publication(session, saved.operation_id)
        assert published.state is PublicationState.PUBLISHED
        assert published.revision == 2
        assert path.stat().st_mode & 0o777 == 0o640
        assert not staged.path.exists()
        with ZipFile(path) as archive:
            assert archive.read("page.jpg") == b"page bytes"
            assert "MetronInfo.xml" in archive.namelist()
        async with factory() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_size == plan.target.binding.file_size
        with pytest.raises(ArchivePublicationError, match="publication_busy"):
            await record(factory, plan)


async def test_crash_after_rename_before_commit_recovers_without_republishing(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        saved = await record(factory, plan)
        async with factory() as session:
            assert (
                await publish_archive_publication(session, saved.operation_id)
            ).state is PublicationState.PUBLISHED
            await session.rollback()  # Model process loss after atomic replacement.
        before = path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns
        current = await load(factory, saved.operation_id)
        assert current.state is PublicationState.INTENDED
        inspection = await inspect_archive_publication(current)
        async with factory.begin() as session:
            recovered = await reconcile_archive_publication(session, current, inspection)
        assert recovered.state is PublicationState.PUBLISHED
        assert (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) == before
        inspection = await inspect_archive_publication(recovered)
        async with factory.begin() as session:
            assert await reconcile_archive_publication(session, recovered, inspection) == recovered


async def test_crash_before_replacement_abandons_without_changing_files(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, staged, plan):
        original, output = path.read_bytes(), staged.path.read_bytes()
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)
        async with factory.begin() as session:
            abandoned = await reconcile_archive_publication(session, saved, inspection)
        assert abandoned.state is PublicationState.ABANDONED
        assert path.read_bytes() == original and staged.path.read_bytes() == output
        with pytest.raises(ArchivePublicationError, match="publication_not_intended"):
            async with factory.begin() as session:
                await publish_archive_publication(session, saved.operation_id)
        assert (await record(factory, plan)).state is PublicationState.INTENDED


@pytest.mark.parametrize("change", ["content", "missing", "symlink", "same_bytes_new_inode"])
async def test_recovery_never_claims_an_unproven_destination(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, staged, plan):
        saved = await record(factory, plan)
        if change == "content":
            path.write_bytes(b"changed")
        elif change == "missing":
            path.unlink()
        elif change == "symlink":
            path.unlink()
            path.symlink_to(staged.path)
        else:
            replacement = tmp_path / "copy.cbz"
            replacement.write_bytes(path.read_bytes())
            os.replace(replacement, path)
        inspection = await inspect_archive_publication(saved)
        async with factory.begin() as session:
            reviewed = await reconcile_archive_publication(session, saved, inspection)
        assert reviewed.state is PublicationState.REVIEW
        with pytest.raises(ArchivePublicationError, match="publication_busy"):
            await record(factory, plan)


@pytest.mark.parametrize(
    "change", ["disabled_root", "user_edit", "deleted_file", "changed_stage", "changed_source"]
)
async def test_stale_publication_never_replaces_source(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, staged, plan):
        saved = await record(factory, plan)
        if change == "changed_stage":
            staged.path.write_bytes(b"new stage")
        elif change == "changed_source":
            path.write_bytes(b"new source")
        else:
            async with factory.begin() as session:
                await session.execute(
                    {
                        "disabled_root": update(LibraryRoot).values(allow_managed_writes=False),
                        "user_edit": update(Issue).values(title="My edit"),
                        "deleted_file": delete(LibraryFile),
                    }[change]
                )
        before = path.read_bytes()
        with pytest.raises(ValueError):
            async with factory.begin() as session:
                await publish_archive_publication(session, saved.operation_id)
        assert path.read_bytes() == before
        assert (await load(factory, saved.operation_id)).state is PublicationState.INTENDED


async def test_recovery_inspection_is_rechecked_under_the_database_lock(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)
        path.write_bytes(b"edited after inspection")
        with pytest.raises(ArchivePublicationError, match="inspection_changed"):
            async with factory.begin() as session:
                await reconcile_archive_publication(session, saved, inspection)
        assert (await load(factory, saved.operation_id)).state is PublicationState.INTENDED


async def test_recovery_rejects_stale_journal_revision(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)
        async with factory.begin() as session:
            await publish_archive_publication(session, saved.operation_id)
        with pytest.raises(ArchivePublicationError, match="publication_changed"):
            async with factory.begin() as session:
                await reconcile_archive_publication(session, saved, inspection)


async def test_deleted_file_does_not_delete_recovery_evidence(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(delete(LibraryFile))
        assert (await load(factory, saved.operation_id)).plan == plan


async def test_publication_requires_committed_intent(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        before = path.read_bytes()
        operation = uuid4()
        async with factory.begin() as session:
            await record_archive_publication(session, plan, operation)
            with pytest.raises(ArchivePublicationError, match="intent_not_committed"):
                await publish_archive_publication(session, operation)
        assert path.read_bytes() == before


async def test_journal_retains_the_snapshot_actually_rendered_in_the_stage(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, stage, plan):
        stage = replace(stage, output_fingerprint=plan.stage_fingerprint)
        wrong_issue = plan.issue.model_copy(
            update={"values": plan.issue.values.model_copy(update={"title": "Never rendered"})}
        )
        with pytest.raises(ArchivePublicationError, match="snapshot_disagrees"):
            await prepare_archive_publication(plan.target, stage, plan.series, wrong_issue)


async def test_reused_operation_cannot_change_its_plan(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        altered = plan.model_copy(update={"output_digest": "0" * 64})
        with pytest.raises(ArchivePublicationError, match="operation_reused"):
            await record(factory, altered, saved.operation_id)
        assert await load(factory, saved.operation_id) == saved


async def test_failed_replace_retains_the_original_and_the_intent(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        saved = await record(factory, plan)
        before = path.read_bytes()

        def fail(*args):
            raise OSError("test rename failure")

        monkeypatch.setattr("pullbox.services.archive_metadata_publication.os.replace", fail)
        with pytest.raises(OSError, match="test rename failure"):
            async with factory.begin() as session:
                await publish_archive_publication(session, saved.operation_id)
        assert path.read_bytes() == before
        assert await load(factory, saved.operation_id) == saved


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/y6s7t8u9v012_add_archive_metadata_publications.py"
    )
    spec = importlib.util.spec_from_file_location("archive_publication_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_migration_round_trip_matches_model_without_backfill(identity_probe_db):
    from tests.integration.metadata_identity.test_archive_finalization_migration import (
        revision as completion_revision,
    )

    engine, _, _ = identity_probe_db
    table = ArchiveMetadataPublication.__table__
    async with engine.begin() as connection:
        await connection.run_sync(table.drop)
        await connection.run_sync(lambda conn: revision(conn).upgrade())
        await connection.run_sync(lambda conn: completion_revision(conn).upgrade())

        def compare(conn):
            expected = MetaData()
            Table("library_files", expected, Column("id", Integer, primary_key=True))
            table.to_metadata(expected)
            context = MigrationContext.configure(
                conn,
                opts={
                    "include_object": lambda obj, name, type_, reflected, compare_to: (
                        name == table.name if type_ == "table" else True
                    ),
                    "compare_server_default": True,
                },
            )
            return compare_metadata(context, expected)

        assert await connection.run_sync(compare) == []
        assert not (await connection.execute(table.select())).all()
        await connection.run_sync(lambda conn: completion_revision(conn).downgrade())
        await connection.run_sync(lambda conn: revision(conn).downgrade())
        assert table.name not in await connection.run_sync(
            lambda conn: inspect(conn).get_table_names()
        )
        await connection.run_sync(lambda conn: revision(conn).upgrade())
        await connection.run_sync(lambda conn: completion_revision(conn).upgrade())


@pytest.mark.parametrize("state", ["intended", "published", "review"])
async def test_downgrade_never_discards_unresolved_journal(identity_probe_db, tmp_path, state):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        async with factory.begin() as session:
            await session.execute(update(ArchiveMetadataPublication).values(state=state))
        with pytest.raises(RuntimeError, match="Resolve retained archive publication"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda conn: revision(conn).downgrade())
        assert (await load(factory, saved.operation_id)).plan == plan


async def test_corrupt_journal_fails_closed_without_mutating_files(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        saved = await record(factory, plan)
        before = path.read_bytes()
        async with factory.begin() as session:
            await session.execute(
                update(ArchiveMetadataPublication).values(plan_json='{"schema_version": 900}')
            )
        with pytest.raises(ArchivePublicationError, match="invalid_journal"):
            async with factory.begin() as session:
                await publish_archive_publication(session, saved.operation_id)
        assert path.read_bytes() == before


async def test_concurrent_publishers_replace_exactly_once(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)

        async def publish():
            try:
                async with factory.begin() as session:
                    return await publish_archive_publication(session, saved.operation_id)
            except ArchivePublicationError as exc:
                return exc.code

        results = await asyncio.gather(publish(), publish())
        assert sum(result == "publication_not_intended" for result in results) == 1
        assert (await load(factory, saved.operation_id)).state is PublicationState.PUBLISHED


async def test_recovery_and_publisher_use_the_same_serialization_boundary(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)

        async def action(recover):
            try:
                async with factory.begin() as session:
                    if recover:
                        return await reconcile_archive_publication(session, saved, inspection)
                    return await publish_archive_publication(session, saved.operation_id)
            except ArchivePublicationError as exc:
                return exc.code

        results = await asyncio.gather(action(True), action(False))
        assert sum(isinstance(result, str) for result in results) == 1
        assert (await load(factory, saved.operation_id)).state in {
            PublicationState.PUBLISHED,
            PublicationState.ABANDONED,
        }


async def test_cancel_after_replace_leaves_recoverable_intent(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        replaced = asyncio.Event()

        async def publish():
            async with factory.begin() as session:
                original_flush = session.flush

                async def cancel_boundary(*args, **kwargs):
                    if any(
                        isinstance(row, ArchiveMetadataPublication)
                        and row.state is PublicationState.PUBLISHED
                        for row in session.dirty
                    ):
                        replaced.set()
                        await asyncio.Event().wait()
                    await original_flush(*args, **kwargs)

                monkeypatch.setattr(session, "flush", cancel_boundary)
                await publish_archive_publication(session, saved.operation_id)

        task = asyncio.create_task(publish())
        await asyncio.wait_for(replaced.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        receipt = await load(factory, saved.operation_id)
        assert receipt.state is PublicationState.INTENDED
        inspection = await inspect_archive_publication(receipt)
        async with factory.begin() as session:
            assert (
                await reconcile_archive_publication(session, receipt, inspection)
            ).state is PublicationState.PUBLISHED


async def test_slow_rename_does_not_block_the_event_loop(identity_probe_db, tmp_path, monkeypatch):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        started, release, timed_out = threading.Event(), threading.Event(), threading.Event()
        real_replace = os.replace

        def slow_replace(*args):
            started.set()
            if not release.wait(1):
                timed_out.set()
            real_replace(*args)

        monkeypatch.setattr(
            "pullbox.services.archive_metadata_publication.os.replace", slow_replace
        )

        async def publish():
            async with factory.begin() as session:
                return await publish_archive_publication(session, saved.operation_id)

        task = asyncio.create_task(publish())
        try:
            assert await asyncio.to_thread(started.wait, 2)
            assert not timed_out.is_set(), "Filesystem publication blocked the event loop"
        finally:
            release.set()
            await task


@pytest.mark.parametrize("action", ["read", "record", "publish", "recover"])
async def test_journal_operations_do_not_flush_pending_library_edits(
    identity_probe_db, tmp_path, action
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (_, _, plan):
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)
        async with factory() as session:
            row = await session.get(Issue, plan.target.binding.metadata.issues[0].local_id)
            row.title = "Uncommitted user edit"
            with pytest.raises(ArchivePublicationError, match="pending_session_changes"):
                if action == "read":
                    await load_archive_publication(session, saved.operation_id)
                elif action == "record":
                    await record_archive_publication(session, plan, uuid4())
                elif action == "publish":
                    await publish_archive_publication(session, saved.operation_id)
                else:
                    await reconcile_archive_publication(session, saved, inspection)
            assert row in session.dirty
            await session.rollback()


async def test_abandoned_only_journal_can_be_downgraded_without_touching_library(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        saved = await record(factory, plan)
        inspection = await inspect_archive_publication(saved)
        async with factory.begin() as session:
            await reconcile_archive_publication(session, saved, inspection)
        before = path.read_bytes()
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).downgrade())
        async with factory() as session:
            assert await session.get(LibraryFile, plan.target.binding.library_file_id)
        assert path.read_bytes() == before
