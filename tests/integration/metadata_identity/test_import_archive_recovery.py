"""Restart/cancellation recovery settles evidence without rewriting archives."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete, select, update

from pullbox.models import Issue, LibraryFile, Series
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication
from pullbox.models.import_job import (
    ImportControlRequest,
    ImportedFile,
    ImportJob,
    ImportJobAction,
    ImportJobActionStatus,
    ImportJobStatus,
)
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    inspect_archive_publication,
    publish_archive_publication,
)
from pullbox.services.import_archive_recovery import recover_import_archive_publication
from pullbox.services.import_comicinfo_enrichment import run_pending_import_comicinfo_enrichment
from pullbox.services.import_job_actions import rollback_action
from pullbox.services.import_rollback_execution import rollback_import_job
from pullbox.tasks.import_archive_recovery import recover_import_archive_publications
from tests.integration.metadata_identity.test_archive_metadata_publication import (
    load,
    prepared,
    record,
)
from tests.integration.metadata_identity.test_import_archive_publication import owned, rollback


async def recover(factory, receipt):
    inspection = await inspect_archive_publication(receipt)
    async with factory.begin() as session:
        return await recover_import_archive_publication(session, receipt, inspection)


async def interrupted(factory, plan, phase):
    receipt = await record(factory, plan)
    if phase != "original":
        async with factory() as session:
            published = await publish_archive_publication(session, receipt.operation_id)
            if phase == "published":
                await session.commit()
                receipt = published
            else:
                await session.rollback()
    return receipt


@pytest.mark.parametrize("phase", ["original", "rename_gap", "published"])
@pytest.mark.parametrize("stop", ["none", "cancel", "cancelled", "rollback"])
async def test_restart_settles_once_and_keeps_original_rollback_proof(
    identity_probe_db, tmp_path, phase, stop
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, signature):
        receipt = await interrupted(factory, plan, phase)
        async with factory.begin() as session:
            if stop == "cancel":
                await session.execute(
                    update(ImportJob).values(control_request=ImportControlRequest.CANCEL)
                )
            elif stop == "cancelled":
                await session.execute(update(ImportJob).values(status=ImportJobStatus.CANCELLED))
            elif stop == "rollback":
                await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))
        before = path.read_bytes(), path.stat().st_ino
        source_before = source.read_bytes()
        result = await recover(factory, receipt)
        expected = (
            "abandoned" if phase == "original" else "finalized" if stop == "none" else "settled"
        )
        assert result.state.value == expected, (
            "Restart must settle evidence, not leave an active reservation"
        )
        async with factory() as session:
            row = await session.scalar(select(ArchiveMetadataPublication))
            assert row.active_file_id is None and row.active_path_key is None
            action = await session.get(ImportJobAction, ids[2])
            assert action.payload["destination_signature"] == signature
            details = (await session.get(ImportedFile, ids[1])).diagnostics["comicinfo_enrichment"]
            if phase != "original":
                assert action.payload["metadata_publication"] == str(receipt.operation_id)
                assert details["status"] == ("complete" if stop == "none" else "cancelled")
            assert (
                await session.get(LibraryFile, plan.target.binding.library_file_id)
            ).source_signature == signature
        assert (path.read_bytes(), path.stat().st_ino) == before
        assert await recover(factory, result) == result
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK
        assert not path.exists() and source.read_bytes() == source_before


async def test_cancelled_publication_does_not_overwrite_later_canonical_edits(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        receipt = await interrupted(factory, plan, "published")
        async with factory.begin() as session:
            await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))
            await session.execute(
                update(Series).values(title="Later title", sort_title="Later title")
            )
            await session.execute(update(Issue).values(title="Later issue"))
        result = await recover(factory, receipt)
        assert result.state.value == "settled"
        async with factory() as session:
            assert (await session.scalar(select(Series))).title == "Later title"
            assert (await session.scalar(select(Issue))).title == "Later issue"
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_hash == plan.output_digest and file.file_size == path.stat().st_size
        assert await rollback(factory, ids[2]) is ImportJobActionStatus.ROLLED_BACK


@pytest.mark.parametrize("change", ["bytes", "action", "registration", "delete"])
async def test_cancel_does_not_release_changed_or_unowned_successor(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        receipt = await interrupted(factory, plan, "published")
        async with factory.begin() as session:
            await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))
            if change == "action":
                action = await session.get(ImportJobAction, ids[2])
                action.payload = {**action.payload, "destination_path": "different"}
            elif change == "registration":
                await session.execute(update(LibraryFile).values(issue_id=None))
            elif change == "delete":
                await session.execute(delete(ImportJob).where(ImportJob.id == ids[0]))
        if change == "bytes":
            path.write_bytes(b"user replacement")
        before = path.read_bytes()
        try:
            result = await recover(factory, receipt)
        except ArchivePublicationError:
            result = await load(factory, receipt.operation_id)
        assert result.state.value in {"review", "published"}
        async with factory() as session:
            row = await session.scalar(select(ArchiveMetadataPublication))
            assert row.active_path_key is not None
        assert path.read_bytes() == before


async def test_settlement_rolls_back_with_caller_and_retries(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, ids, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        async with factory.begin() as session:
            await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))
            payload = deepcopy((await session.get(ImportJobAction, ids[2])).payload)
        inspection = await inspect_archive_publication(receipt)
        async with factory() as session:
            result = await recover_import_archive_publication(session, receipt, inspection)
            assert result.state.value == "settled"
            await session.rollback()
        assert await load(factory, receipt.operation_id) == receipt
        async with factory() as session:
            assert (await session.get(ImportJobAction, ids[2])).payload == payload
        assert (await recover(factory, receipt)).state.value == "settled"


async def test_concurrent_settlement_and_stale_replay_are_idempotent(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        async with factory.begin() as session:
            await session.execute(
                update(ImportJob).values(control_request=ImportControlRequest.CANCEL)
            )
        results = await asyncio.gather(recover(factory, receipt), recover(factory, receipt))
        assert results[0] == results[1] and results[0].state.value == "settled"
        async with factory.begin() as session:
            await session.execute(delete(ImportJob).where(ImportJob.id == ids[0]))
        path.write_bytes(b"later user edit")
        assert await recover(factory, receipt) == results[0]
        assert path.read_bytes() == b"later user edit"


async def test_restart_worker_recovers_only_owned_rows_and_inspects_without_transaction(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.tasks import import_archive_recovery as task

    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        async with factory() as session:

            async def inspect_without_transaction(saved):
                assert not session.in_transaction(), "Large hashes cannot hold a DB transaction"
                return await inspect_archive_publication(saved)

            monkeypatch.setattr(
                task, "inspect_archive_publication", inspect_without_transaction, raising=False
            )
            assert await recover_import_archive_publications(session, job_id=ids[0] + 1000) == 0
            assert await recover_import_archive_publications(session, job_id=ids[0]) == 1
            assert await recover_import_archive_publications(session) == 0
        assert (await load(factory, receipt.operation_id)).state.value == "finalized"
        assert path.exists()


async def test_startup_recovery_does_not_send_published_archive_to_legacy_writer(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, _, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        before = path.read_bytes()
        build, apply = AsyncMock(return_value={}), AsyncMock()
        await run_pending_import_comicinfo_enrichment(
            factory, build_comicinfo_payload=build, apply_comicinfo=apply, log_event=AsyncMock()
        )
        assert (await load(factory, receipt.operation_id)).state.value == "finalized"
        build.assert_not_awaited()
        apply.assert_not_awaited()
        assert path.read_bytes() == before


async def test_real_rollback_orchestrator_settles_before_removing_successor(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, source, plan, ids, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        async with factory.begin() as session:
            await session.execute(update(ImportJob).values(status=ImportJobStatus.ROLLING_BACK))

        async def reverse(session, action):
            await rollback_action(
                session,
                action_id=action.action_id,
                action_type=action.action_type,
                payload=action.payload,
                delete_series=AsyncMock(),
            )

        async with factory() as session:
            assert await rollback_import_job(
                session,
                ids[0],
                rollback_action=reverse,
                restore_review_state=AsyncMock(),
                recompute_series_counters=AsyncMock(),
                recompute_file_counters=AsyncMock(),
                log_event=AsyncMock(),
                emit_progress=AsyncMock(),
                estimate_remaining_seconds=lambda *_: None,
                job_stats=lambda _: {},
            )
            await session.commit()
        async with factory() as session:
            assert (await session.get(ImportJob, ids[0])).status is ImportJobStatus.ROLLED_BACK
        assert (await load(factory, receipt.operation_id)).state.value == "settled"
        assert not path.exists() and source.exists()


@pytest.mark.parametrize("change", ["bytes", "canonical", "owner"])
async def test_unresolved_publication_never_falls_through_to_legacy_rewrite(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, ids, _):
        await interrupted(factory, plan, "published")
        if change == "bytes":
            path.write_bytes(b"user replacement")
        else:
            async with factory.begin() as session:
                if change == "canonical":
                    await session.execute(update(Issue).values(title="User edit"))
                else:
                    action = await session.get(ImportJobAction, ids[2])
                    action.payload = {**action.payload, "original_source_path": "changed"}
        before = path.read_bytes()
        build, apply = AsyncMock(return_value={}), AsyncMock()
        await run_pending_import_comicinfo_enrichment(
            factory, build_comicinfo_payload=build, apply_comicinfo=apply, log_event=AsyncMock()
        )
        apply.assert_not_awaited()
        build.assert_not_awaited()
        assert path.read_bytes() == before
        async with factory() as session:
            row = await session.scalar(select(ArchiveMetadataPublication))
            assert row.active_path_key is not None
            details = (await session.get(ImportedFile, ids[1])).diagnostics["comicinfo_enrichment"]
            assert details["status"] == "pending"


async def test_cancelled_unpublished_work_never_falls_through_to_legacy_rewrite(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, _, _):
        receipt = await interrupted(factory, plan, "original")
        async with factory.begin() as session:
            await session.execute(
                update(ImportJob).values(control_request=ImportControlRequest.CANCEL)
            )
        before = path.read_bytes()
        build, apply = AsyncMock(return_value={}), AsyncMock()
        await run_pending_import_comicinfo_enrichment(
            factory, build_comicinfo_payload=build, apply_comicinfo=apply, log_event=AsyncMock()
        )
        apply.assert_not_awaited()
        build.assert_not_awaited()
        assert (await load(factory, receipt.operation_id)).state.value == "abandoned"
        assert path.read_bytes() == before


@pytest.mark.parametrize("invalid", ["plan", "operation_id"])
async def test_bounded_recovery_continues_past_invalid_journal(
    identity_probe_db, tmp_path, monkeypatch, invalid
):
    from pullbox.tasks import import_archive_recovery as task

    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        session.add(
            ArchiveMetadataPublication(
                operation_id="00000000-0000-0000-0000-000000000001"
                if invalid == "plan"
                else "invalid",
                active_path_key="a" * 64,
                plan_json="{}",
            )
        )
    async with owned(factory, tmp_path) as (_, _, plan, _, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        monkeypatch.setattr(task, "RECOVERY_PAGE_SIZE", 1)
        async with factory() as session:
            assert await recover_import_archive_publications(session) == 1
        assert (await load(factory, receipt.operation_id)).state.value == "finalized"
        async with factory() as session:
            invalid = await session.scalar(
                select(ArchiveMetadataPublication).order_by(ArchiveMetadataPublication.id)
            )
            assert invalid.plan_json == "{}" and invalid.active_path_key == "a" * 64


async def test_recovery_does_not_commit_pending_caller_edits(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (_, _, plan, _, _):
        receipt = await interrupted(factory, plan, "published")
        async with factory() as session:
            series = await session.scalar(select(Series))
            series.title = "Pending change"
            with pytest.raises(ArchivePublicationError, match="pending_session_changes"):
                await recover_import_archive_publications(session)
            await session.rollback()
        assert await load(factory, receipt.operation_id) == receipt
        async with factory() as session:
            assert (await session.scalar(select(Series))).title != "Pending change"


async def test_recovery_cancellation_during_inspection_is_retryable(
    identity_probe_db, tmp_path, monkeypatch
):
    from pullbox.tasks import import_archive_recovery as task

    _, factory, _ = identity_probe_db
    async with owned(factory, tmp_path) as (path, _, plan, _, _):
        receipt = await interrupted(factory, plan, "rename_gap")
        before = path.read_bytes()
        entered = asyncio.Event()

        async def held(_):
            entered.set()
            await asyncio.Event().wait()

        async def run():
            async with factory() as session:
                await recover_import_archive_publications(session)

        with monkeypatch.context() as patch:
            patch.setattr(task, "inspect_archive_publication", held)
            worker = asyncio.create_task(run())
            try:
                await asyncio.wait_for(entered.wait(), 10)
            finally:
                worker.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await worker
        assert await load(factory, receipt.operation_id) == receipt
        async with factory() as session:
            assert await recover_import_archive_publications(session) == 1
        assert path.read_bytes() == before


async def test_import_recovery_does_not_adopt_an_unowned_publication(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path) as (path, _, plan):
        receipt = await interrupted(factory, plan, "rename_gap")
        before = path.read_bytes()
        async with factory() as session:
            assert await recover_import_archive_publications(session) == 0
        assert await load(factory, receipt.operation_id) == receipt
        assert path.read_bytes() == before
