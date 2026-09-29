"""Canonical values and file state commit with publication completion, never before."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select, update

from pullbox.core.archive_metadata import read_archive_metadata
from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models import Issue, LibraryFile, LibraryRoot, Series
from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication
from pullbox.models.library import LibraryFileStorageMode
from pullbox.models.metadata_identity import IssueExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.publisher import Publisher
from pullbox.services.archive_metadata_binding import (
    ArchiveMetadataBindingError,
    assemble_bound_archive_metadata,
    inspect_archive_metadata_target,
    read_archive_metadata_binding,
)
from pullbox.services.archive_metadata_finalization import finalize_archive_publication
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    PublicationState,
    inspect_archive_publication,
    publish_archive_publication,
    reconcile_archive_publication,
    record_archive_publication,
)
from pullbox.services.archive_metadata_reconciliation import reconcile_archive_metadata
from pullbox.services.metadata_baselines import load_metadata_baseline
from tests.integration.metadata_identity.test_archive_metadata_publication import (
    load,
    prepared,
    record,
)

XML = """<ComicInfo><Series>Example</Series><Number>50-X</Number>
<Title>Local issue title</Title><Summary>Local summary</Summary>
<Publisher>Local Publisher</Publisher><Writer>A Writer</Writer>
<Year>1992</Year><Month>4</Month><Day>3</Day><PageCount>1</PageCount>
<LanguageISO>en</LanguageISO><Volume>2</Volume></ComicInfo>"""


async def published(factory, plan):
    saved = await record(factory, plan)
    async with factory.begin() as session:
        saved = await publish_archive_publication(session, saved.operation_id)
    return saved, await inspect_archive_publication(saved)


async def finish(factory, receipt, inspection):
    async with factory.begin() as session:
        result = await finalize_archive_publication(session, receipt, inspection)
        assert result is not None, "Published metadata must be finalized durably"
        assert result.state.value == "finalized"
        return result


@pytest.mark.parametrize(
    "comicinfo",
    [XML, XML.replace("<Volume>2</Volume>", "")],
    ids=["preserved-volume", "canonical-fields-only"],
)
async def test_finalization_adopts_canonical_values_and_file_evidence_together(
    identity_probe_db, tmp_path, comicinfo
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=comicinfo) as (path, _, plan):
        binding = plan.target.binding
        async with factory.begin() as session:
            file = await session.get(LibraryFile, binding.library_file_id)
            file.file_hash = "a" * 64
            file.source_signature = {"original": "ownership evidence"}
            file.naming_snapshot = {"source": "original naming"}
        saved, inspection = await published(factory, plan)
        before = path.read_bytes(), path.stat()
        completed = await finish(factory, saved, inspection)
        assert completed.revision == saved.revision + 1
        assert (path.read_bytes(), path.stat()) == before
        async with factory() as session:
            current = await read_archive_metadata_binding(session, binding.library_file_id)
            assert current.metadata.series.values == plan.series.values
            assert current.metadata.issues[0].values == plan.issue.values
            assert current.metadata.series.baseline == plan.series
            assert current.metadata.issues[0].baseline == plan.issue
            assert current.metadata.series.baseline_revision == 1
            assert current.metadata.issues[0].baseline_revision == 1
            file = await session.get(LibraryFile, binding.library_file_id)
            assert file.file_hash == plan.output_digest
            assert file.file_size == inspection.fingerprint[2]
            assert file.file_modified_at == datetime.fromtimestamp(path.stat().st_mtime, UTC)
            assert file.has_comicinfo
            assert file.source_signature == {"original": "ownership evidence"}
            assert file.naming_snapshot == {"source": "original naming"}
            row = await session.scalar(select(ArchiveMetadataPublication))
            assert row.active_file_id is None and row.active_path_key is None
            series = await session.get(Series, current.metadata.series.local_id)
            assert series.metadata_last_refreshed is None
            assert series.issue_catalog_last_synced_at is None
        assert await inspect_archive_metadata_target(current) is not None
        evidence = reconcile_archive_metadata(
            read_archive_metadata(path, "cbz", max_solid_scan_bytes=1000000)
        )
        assert assemble_bound_archive_metadata(current, evidence, now=datetime.now(UTC)) == (
            plan.series,
            plan.issue,
        )


async def test_finalization_is_caller_owned_and_can_retry_after_db_rollback(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        before = path.read_bytes()
        async with factory() as session:
            assert await finalize_archive_publication(session, saved, inspection) is not None
            await session.rollback()
        assert await load(factory, saved.operation_id) == saved
        async with factory() as session:
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_size == plan.target.binding.file_size
            assert (
                await load_metadata_baseline(
                    session,
                    MetadataEntityKind.ISSUE,
                    plan.target.binding.metadata.issues[0].local_id,
                )
                is None
            )
        await finish(factory, saved, inspection)
        assert path.read_bytes() == before


async def test_completed_replay_does_not_touch_later_user_edits_or_file_changes(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        completed = await finish(factory, saved, inspection)
        async with factory.begin() as session:
            await session.execute(update(Issue).values(title="Later user edit"))
        path.write_bytes(b"Later replacement")
        assert await finish(factory, saved, inspection) == completed
        async with factory() as session:
            assert await session.scalar(select(Issue.title)) == "Later user edit"
            baseline = await load_metadata_baseline(
                session, MetadataEntityKind.ISSUE, plan.target.binding.metadata.issues[0].local_id
            )
            assert baseline.revision == 1
        async with factory.begin() as session:
            assert (
                await reconcile_archive_publication(
                    session, completed, await inspect_archive_publication(completed)
                )
                == completed
            )
        assert path.read_bytes() == b"Later replacement"


async def test_concurrent_finalizers_apply_once(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (_, _, plan):
        saved, inspection = await published(factory, plan)
        results = await asyncio.gather(
            finish(factory, saved, inspection), finish(factory, saved, inspection)
        )
        assert results[0] == results[1]
        async with factory() as session:
            baseline = await load_metadata_baseline(
                session, MetadataEntityKind.ISSUE, plan.target.binding.metadata.issues[0].local_id
            )
            assert baseline.revision == 1


@pytest.mark.parametrize(
    "change", ["issue", "series", "root", "reference", "delete", "identity", "policy"]
)
async def test_stale_db_state_keeps_publication_reserved(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        async with factory.begin() as session:
            if change == "issue":
                await session.execute(update(Issue).values(description="New user description"))
            elif change == "series":
                await session.execute(update(Series).values(title="New series title"))
            elif change == "root":
                await session.execute(update(LibraryRoot).values(allow_managed_writes=False))
            elif change == "reference":
                await session.execute(
                    update(LibraryFile).values(storage_mode=LibraryFileStorageMode.REFERENCED)
                )
            elif change == "identity":
                await session.execute(update(IssueExternalIdentity).values(external_id="43"))
            elif change == "policy":
                session.add(MetadataSourceConfig(source="metron_api", enabled=True, priority=3))
            else:
                await session.execute(delete(LibraryFile))
        before = path.read_bytes()
        async with factory.begin() as session:
            with pytest.raises((ArchivePublicationError, ArchiveMetadataBindingError)):
                await finalize_archive_publication(session, saved, inspection)
        assert await load(factory, saved.operation_id) == saved
        assert path.read_bytes() == before
        async with factory() as session:
            assert (
                await load_metadata_baseline(
                    session,
                    MetadataEntityKind.ISSUE,
                    plan.target.binding.metadata.issues[0].local_id,
                )
                is None
            )


@pytest.mark.parametrize("change", ["file", "missing", "inspection", "revision", "plan"])
async def test_unproven_output_cannot_finalize(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        receipt = saved
        if change == "file":
            path.write_bytes(b"Unexpected changed output")
        elif change == "missing":
            path.unlink()
        elif change == "inspection":
            inspection = replace(inspection, digest="a" * 64)
        elif change == "revision":
            receipt = replace(saved, revision=0)
        else:
            receipt = replace(saved, plan=plan.model_copy(update={"output_digest": "a" * 64}))
        async with factory.begin() as session:
            with pytest.raises(ArchivePublicationError):
                await finalize_archive_publication(session, receipt, inspection)
        assert await load(factory, saved.operation_id) == saved


async def test_intent_requires_recovery_before_finalization(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (_, _, plan):
        saved = await record(factory, plan)
        async with factory() as session:
            await publish_archive_publication(session, saved.operation_id)
            await session.rollback()
        inspection = await inspect_archive_publication(saved)
        async with factory.begin() as session:
            with pytest.raises(ArchivePublicationError, match="publication_not_published"):
                await finalize_archive_publication(session, saved, inspection)
        async with factory.begin() as session:
            recovered = await reconcile_archive_publication(session, saved, inspection)
        await finish(factory, recovered, await inspect_archive_publication(recovered))


async def test_pending_user_edits_are_not_flushed(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (_, _, plan):
        saved, inspection = await published(factory, plan)
        async with factory() as session:
            issue = await session.get(Issue, plan.target.binding.metadata.issues[0].local_id)
            issue.title = "Unrelated pending edit"
            with pytest.raises(ArchivePublicationError, match="pending_session_changes"):
                await finalize_archive_publication(session, saved, inspection)
            assert issue in session.dirty
            await session.rollback()


async def test_completed_reservation_allows_a_new_intent(identity_probe_db, tmp_path):
    from pullbox.services.archive_metadata_publication import prepare_archive_publication
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        await finish(factory, saved, inspection)
        async with factory() as session:
            binding = await read_archive_metadata_binding(
                session, plan.target.binding.library_file_id
            )
        target = await inspect_archive_metadata_target(binding)
        evidence = reconcile_archive_metadata(
            read_archive_metadata(path, "cbz", max_solid_scan_bytes=1000000)
        )
        series, issue = assemble_bound_archive_metadata(binding, evidence, now=datetime.now(UTC))
        async with stage_cbz_metadata_interruptible(
            path, path.parent, series, issue, max_uncompressed_bytes=1000000
        ) as staged:
            next_plan = await prepare_archive_publication(target, staged, series, issue)
            async with factory.begin() as session:
                next_intent = await record_archive_publication(session, next_plan, uuid4())
                assert next_intent.state is PublicationState.INTENDED
            async with factory.begin() as session:
                next_publication = await publish_archive_publication(
                    session, next_intent.operation_id
                )
            await finish(
                factory, next_publication, await inspect_archive_publication(next_publication)
            )
            async with factory() as session:
                current = await read_archive_metadata_binding(session, binding.library_file_id)
                assert current.metadata.series.baseline_revision == 1
                assert current.metadata.issues[0].baseline_revision == 1


@pytest.mark.parametrize("failure", ["baseline", "file_changed"])
async def test_caught_late_failure_rolls_back_all_canonical_changes(
    identity_probe_db, tmp_path, monkeypatch, failure
):
    from pullbox.services import archive_metadata_finalization as finalizer

    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        if failure == "baseline":
            original = finalizer.save_metadata_baselines

            async def fail_after_baseline(*args):
                await original(*args)
                raise RuntimeError("Simulated database write failure")

            monkeypatch.setattr(finalizer, "save_metadata_baselines", fail_after_baseline)
            expected = RuntimeError
        else:
            original_check = finalizer._check_output
            calls = 0

            def change_after_db_writes(*args):
                nonlocal calls
                calls += 1
                if calls == 2:
                    path.write_bytes(b"External concurrent replacement")
                return original_check(*args)

            monkeypatch.setattr(finalizer, "_check_output", change_after_db_writes)
            expected = ArchivePublicationError
        async with factory.begin() as session:
            with pytest.raises(expected):
                await finalize_archive_publication(session, saved, inspection)
        assert await load(factory, saved.operation_id) == saved
        async with factory() as session:
            assert await session.scalar(select(Issue.title)) is None
            assert await session.scalar(select(func.count()).select_from(Publisher)) == 0
            assert (
                await load_metadata_baseline(
                    session,
                    MetadataEntityKind.ISSUE,
                    plan.target.binding.metadata.issues[0].local_id,
                )
                is None
            )
            file = await session.get(LibraryFile, plan.target.binding.library_file_id)
            assert file.file_size == plan.target.binding.file_size


async def test_cancellation_before_completion_flush_is_retryable(
    identity_probe_db, tmp_path, monkeypatch
):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        pending = asyncio.Event()
        before = path.read_bytes()

        async def finalize():
            async with factory.begin() as session:
                original = session.flush

                async def wait_at_completion(*args, **kwargs):
                    if any(
                        isinstance(row, ArchiveMetadataPublication)
                        and row.state.value == "finalized"
                        for row in session.dirty
                    ):
                        pending.set()
                        await asyncio.Event().wait()
                    await original(*args, **kwargs)

                monkeypatch.setattr(session, "flush", wait_at_completion)
                await finalize_archive_publication(session, saved, inspection)

        task = asyncio.create_task(finalize())
        await asyncio.wait_for(pending.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await load(factory, saved.operation_id) == saved
        assert path.read_bytes() == before
        await finish(factory, saved, inspection)


async def test_completed_receipt_survives_library_file_deletion(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    async with prepared(factory, tmp_path, comicinfo=XML) as (_, _, plan):
        saved, inspection = await published(factory, plan)
        completed = await finish(factory, saved, inspection)
        async with factory.begin() as session:
            await session.execute(delete(LibraryFile))
        assert await finish(factory, saved, inspection) == completed


@pytest.mark.parametrize("fraction", [499, 500, 1500, 2499])
async def test_file_timestamp_round_trip_uses_the_binding_stat_representation(
    identity_probe_db, tmp_path, fraction
):
    _, factory, _ = identity_probe_db
    async with prepared(
        factory, tmp_path, staged_mtime_ns=1_800_000_000_000_000_000 + fraction
    ) as (path, _, plan):
        saved, inspection = await published(factory, plan)
        await finish(factory, saved, inspection)
        async with factory() as session:
            binding = await read_archive_metadata_binding(
                session, plan.target.binding.library_file_id
            )
        assert binding.file_modified_at == datetime.fromtimestamp(path.stat().st_mtime, UTC)
        assert await inspect_archive_metadata_target(binding) is not None
