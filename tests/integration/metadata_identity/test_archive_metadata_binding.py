"""One real registered file, not folder names or embedded IDs, binds output."""

import asyncio
import os
from datetime import UTC, datetime
from zipfile import ZipFile

import pytest
from sqlalchemy import delete, event, select, update

from pullbox.core.archive_metadata import read_archive_metadata
from pullbox.core.metadata_identity import ExternalIdentityRef
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity_state import IdentityVerificationState as State
from pullbox.models import Issue, LibraryFile, LibraryRoot, Series
from pullbox.models.library import FileFormat, LibraryFileStorageMode
from pullbox.models.metadata_baseline import IssueMetadataBaseline
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.schemas.metadata_snapshot import FieldOrigin, MetadataSnapshot, MetadataValues
from pullbox.schemas.metadata_sources import MetadataDomain
from pullbox.services.archive_metadata_binding import (
    ArchiveMetadataBindingError,
    assemble_bound_archive_metadata,
    inspect_archive_metadata_target,
    read_archive_metadata_binding,
    revalidate_archive_metadata_binding,
    revalidate_archive_metadata_target,
)
from pullbox.services.archive_metadata_reconciliation import reconcile_archive_metadata
from pullbox.services.metadata_series_refresh_state import read_series_refresh_state
from pullbox.utilities.executors.archive_metadata_staging import stage_cbz_metadata_interruptible

NOW = datetime(2026, 9, 29, tzinfo=UTC)
PARENT = ExternalIdentityRef(Namespace.METRON, Kind.SERIES, "12")
ISSUE = ExternalIdentityRef(Namespace.METRON, Kind.ISSUE, "42")


async def seed(factory, tmp_path):
    root_path = tmp_path / "comics"
    folder = root_path / "Misleading folder name"
    folder.mkdir(parents=True)
    path = folder / "unrelated-name.cbz"
    with ZipFile(path, "w") as archive:
        archive.writestr("page.jpg", b"page bytes")
        archive.writestr(
            "ComicInfo.xml",
            "<ComicInfo><Series>Canonical series</Series><Number>50-x</Number>"
            "<Summary>My local summary</Summary></ComicInfo>",
        )
    async with factory.begin() as session:
        root = LibraryRoot(name="Managed", path=str(root_path))
        series = Series(title="Canonical series", sort_title="Canonical series", year_start=1992)
        session.add_all([root, series])
        await session.flush()
        issue = Issue(series_id=series.id, issue_number=50, issue_number_text="50-x")
        session.add_all(
            [
                issue,
                SeriesExternalIdentity(
                    series_id=series.id,
                    identity_namespace=Namespace.METRON,
                    external_id="12",
                    verification_state=State.VERIFIED,
                    evidence_kind="provider_result",
                ),
            ]
        )
        await session.flush()
        session.add(
            IssueExternalIdentity(
                issue_id=issue.id,
                identity_namespace=Namespace.METRON,
                external_id="42",
                verification_state=State.VERIFIED,
                evidence_kind="provider_result",
            )
        )
        file = LibraryFile(
            file_path=str(path),
            file_name=path.name,
            file_size=path.stat().st_size,
            file_modified_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
            file_format=FileFormat.CBZ,
            issue_id=issue.id,
            library_root_id=root.id,
        )
        session.add(file)
        await session.flush()
        return file.id, issue.id, series.id, root.id, path


async def binding(factory, file_id):
    async with factory() as session:
        result = await read_archive_metadata_binding(session, file_id)
        assert result is not None, "A registered file needs an independent metadata binding"
        return result


async def test_binding_uses_registered_issue_not_filename_and_assembles_local_values(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    file_id, issue_id, series_id, root_id, path = await seed(factory, tmp_path)
    before = path.read_bytes(), path.stat()
    saved = await binding(factory, file_id)
    assert saved.library_file_id == file_id
    assert saved.library_root_id == root_id
    assert saved.metadata.series.local_id == series_id
    assert tuple(item.local_id for item in saved.metadata.issues) == (issue_id,)
    assert saved.metadata.series.identities == (PARENT,)
    assert saved.metadata.issues[0].identities == (ISSUE,)
    target = await inspect_archive_metadata_target(saved)
    assert target.path == path.resolve()
    target.check_unchanged()
    archive = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=1024)
    )
    series, issue = assemble_bound_archive_metadata(saved, archive, now=NOW)
    assert series.values.title == "Canonical series"
    assert series.values.year_start == 1992 and series.values.volume is None
    assert issue.values.issue_number_text == "50-X"
    assert issue.values.description == "My local summary"
    assert next(
        origin for origin in issue.origins if origin.field == "description"
    ).embedded_documents == ("ComicInfo.xml",)
    async with factory() as session:
        await revalidate_archive_metadata_binding(session, saved)
    assert path.read_bytes() == before[0]
    assert (path.stat().st_ino, path.stat().st_mtime_ns, path.stat().st_mode) == (
        before[1].st_ino,
        before[1].st_mtime_ns,
        before[1].st_mode,
    )


@pytest.mark.parametrize(
    "change",
    [
        "missing_file",
        "unmatched",
        "wrong_issue",
        "referenced",
        "disabled_root",
        "reference_root",
        "stale_issue",
        "conflicted_series",
        "missing_issue_identity",
        "missing_parent_identity",
        "legacy_issue_drift",
        "legacy_parent_drift",
        "unsupported",
        "invalid_baseline",
    ],
)
async def test_untrusted_binding_is_rejected_before_file_work(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    file_id, issue_id, _, _, _ = await seed(factory, tmp_path)
    async with factory.begin() as session:
        statements = {
            "missing_file": delete(LibraryFile).where(LibraryFile.id == file_id),
            "unmatched": update(LibraryFile).values(issue_id=None),
            "referenced": update(LibraryFile).values(
                storage_mode=LibraryFileStorageMode.REFERENCED
            ),
            "disabled_root": update(LibraryRoot).values(enabled=False),
            "reference_root": update(LibraryRoot).values(allow_managed_writes=False),
            "stale_issue": update(IssueExternalIdentity).values(verification_state=State.STALE),
            "conflicted_series": update(SeriesExternalIdentity).values(
                verification_state=State.CONFLICTED
            ),
            "missing_issue_identity": delete(IssueExternalIdentity),
            "missing_parent_identity": delete(SeriesExternalIdentity),
            "legacy_issue_drift": update(Issue).values(comicvine_id=700),
            "legacy_parent_drift": update(Series).values(comicvine_id=600),
            "unsupported": update(LibraryFile).values(file_format=FileFormat.CBR),
        }
        if change in statements:
            await session.execute(statements[change])
        elif change == "invalid_baseline":
            session.add(IssueMetadataBaseline(issue_id=issue_id, revision=1, snapshot_json="{}"))
    async with factory() as session:
        with pytest.raises(ArchiveMetadataBindingError):
            await read_archive_metadata_binding(
                session,
                file_id,
                expected_issue_id=issue_id + 100 if change == "wrong_issue" else issue_id,
            )


@pytest.mark.parametrize(
    "change",
    [
        "issue_title",
        "series_title",
        "root_path",
        "root_policy",
        "storage_mode",
        "file_path",
        "file_size",
        "file_date",
        "identity_revision",
        "parent_revision",
        "reassigned",
        "baseline",
        "issue_deleted",
    ],
)
async def test_new_session_changes_invalidate_staged_binding(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    file_id, issue_id, series_id, _, path = await seed(factory, tmp_path)
    saved = await binding(factory, file_id)
    async with factory.begin() as session:
        statements = {
            "issue_title": update(Issue).values(title="User edit"),
            "series_title": update(Series).values(title="User series edit"),
            "root_path": update(LibraryRoot).values(path=str(tmp_path)),
            "root_policy": update(LibraryRoot).values(allow_managed_writes=False),
            "storage_mode": update(LibraryFile).values(
                storage_mode=LibraryFileStorageMode.REFERENCED
            ),
            "file_path": update(LibraryFile).values(file_path=str(path.with_name("another.cbz"))),
            "file_size": update(LibraryFile).values(file_size=1),
            "file_date": update(LibraryFile).values(file_modified_at=NOW),
            "identity_revision": update(IssueExternalIdentity).values(revision=2),
            "parent_revision": update(SeriesExternalIdentity).values(revision=2),
            "issue_deleted": delete(Issue).where(Issue.id == issue_id),
        }
        if change in statements:
            await session.execute(statements[change])
        elif change == "reassigned":
            other = Issue(series_id=series_id, issue_number=51)
            session.add(other)
            await session.flush()
            await session.execute(update(LibraryFile).values(issue_id=other.id))
        else:
            snapshot = MetadataSnapshot(
                entity_kind=Kind.ISSUE, identities=(ISSUE,), values=MetadataValues(title="Earlier")
            )
            session.add(
                IssueMetadataBaseline(
                    issue_id=issue_id, revision=1, snapshot_json=snapshot.model_dump_json()
                )
            )
    async with factory() as session:
        with pytest.raises(ArchiveMetadataBindingError):
            await revalidate_archive_metadata_binding(session, saved)


@pytest.mark.parametrize(
    "change",
    [
        "readonly",
        "missing",
        "symlink",
        "outside",
        "directory",
        "traversal",
        "size",
        "mtime",
        "readonly_parent",
    ],
)
async def test_source_policy_and_filesystem_are_independently_checked(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    file_id, _, _, _, path = await seed(factory, tmp_path)
    try:
        if change == "readonly":
            path.chmod(0o444)
        elif change == "readonly_parent":
            path.parent.chmod(0o555)
        elif change == "missing":
            path.unlink()
        elif change == "symlink":
            other = tmp_path / "elsewhere.cbz"
            path.rename(other)
            path.symlink_to(other)
        elif change == "directory":
            path.unlink()
            path.mkdir()
        elif change == "size":
            path.write_bytes(b"replaced")
        elif change == "mtime":
            os.utime(path, (1, 1))
        elif change in {"outside", "traversal"}:
            async with factory.begin() as session:
                await session.execute(
                    update(LibraryFile).values(
                        file_path=str(tmp_path / "elsewhere.cbz")
                        if change == "outside"
                        else str(path.parent / ".." / path.parent.name / path.name)
                    )
                )
        saved = await binding(factory, file_id)
        with pytest.raises(ArchiveMetadataBindingError):
            await inspect_archive_metadata_target(saved)
    finally:
        path.parent.chmod(0o755)


@pytest.mark.parametrize("change", ["file", "parent", "mode"])
async def test_inspected_target_cannot_bless_replaced_paths(identity_probe_db, tmp_path, change):
    _, factory, _ = identity_probe_db
    file_id, _, _, _, path = await seed(factory, tmp_path)
    target = await inspect_archive_metadata_target(await binding(factory, file_id))
    assert target is not None
    if change == "file":
        original = path.read_bytes()
        before = path.stat()
        path.unlink()
        path.write_bytes(original)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    elif change == "parent":
        old = path.parent.with_name("old")
        path.parent.rename(old)
        path.parent.symlink_to(old, target_is_directory=True)
    else:
        path.chmod(0o444)
    with pytest.raises(ArchiveMetadataBindingError):
        target.check_unchanged()


async def test_targeted_binding_does_not_load_unrelated_catalog_or_commit(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    file_id, _, series_id, _, _ = await seed(factory, tmp_path)
    async with factory.begin() as session:
        other = Issue(series_id=series_id, issue_number=99)
        session.add(other)
        await session.flush()
        session.add(IssueMetadataBaseline(issue_id=other.id, revision=1, snapshot_json="{}"))
    statements = []

    def capture(_connection, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        saved = await binding(factory, file_id)
        assert len(saved.metadata.issues) == 1
        assert len(statements) < 20
        assert not any(
            sql.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")) for sql in statements
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    async with factory() as session:
        series = await session.get(Series, series_id)
        series.title = "Unflushed user change"
        with pytest.raises(ArchiveMetadataBindingError):
            await read_archive_metadata_binding(session, file_id)
        assert series.title == "Unflushed user change"
        await session.rollback()
    async with factory() as session:
        assert (
            await session.scalar(select(Series.title).where(Series.id == series_id))
            == "Canonical series"
        )


async def test_embedded_exact_identity_cannot_change_registered_target(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    file_id, _, _, _, path = await seed(factory, tmp_path)
    saved = await binding(factory, file_id)
    with ZipFile(path, "a") as archive:
        archive.writestr(
            "MetronInfo.xml",
            '<MetronInfo><IDS><ID source="Metron" primary="true">999</ID></IDS></MetronInfo>',
        )
    evidence = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=1024)
    )
    with pytest.raises(ArchiveMetadataBindingError):
        assemble_bound_archive_metadata(saved, evidence, now=NOW)


@pytest.mark.parametrize("change", ["priority", "health_only"])
async def test_revalidation_reads_current_policy_even_in_populated_session(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    file_id, _, _, _, _ = await seed(factory, tmp_path)
    async with factory.begin() as session:
        session.add(MetadataSourceConfig(source="metron_api", enabled=True, priority=30))
    async with factory() as session:
        cached = await session.scalar(select(MetadataSourceConfig))
        saved = await read_archive_metadata_binding(session, file_id)
        async with factory.begin() as writer:
            await writer.execute(
                update(MetadataSourceConfig).values(
                    **(
                        {"priority": 5, "revision": 2}
                        if change == "priority"
                        else {"last_tested_at": NOW}
                    )
                )
            )
        if change == "priority":
            with pytest.raises(ArchiveMetadataBindingError):
                await revalidate_archive_metadata_binding(session, saved)
        else:
            await revalidate_archive_metadata_binding(session, saved)
        assert cached is not None


@pytest.mark.parametrize("current", [None, "", "My edited summary"])
async def test_archive_does_not_refill_explicit_database_edits_or_clears(
    identity_probe_db, tmp_path, current
):
    _, factory, _ = identity_probe_db
    file_id, issue_id, _, _, path = await seed(factory, tmp_path)
    previous = MetadataSnapshot(
        entity_kind=Kind.ISSUE,
        identities=(ISSUE,),
        values=MetadataValues(description="My local summary", issue_number_text="50-X"),
        origins=(
            FieldOrigin(
                field="description",
                domain=MetadataDomain.CORE,
                observed_at=NOW,
                embedded_documents=("ComicInfo.xml",),
            ),
        ),
    )
    async with factory.begin() as session:
        session.add(
            IssueMetadataBaseline(
                issue_id=issue_id, revision=1, snapshot_json=previous.model_dump_json()
            )
        )
        await session.execute(update(Issue).values(description=current))
    saved = await binding(factory, file_id)
    evidence = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=1024)
    )
    _, issue = assemble_bound_archive_metadata(saved, evidence, now=NOW)
    assert issue.values.description == current
    assert next(origin for origin in issue.origins if origin.field == "description").user_override


async def test_binding_and_real_paired_worker_preserve_original_until_owner_publication(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    file_id, _, _, _, path = await seed(factory, tmp_path)
    original = path.read_bytes()
    saved = await binding(factory, file_id)
    target = await inspect_archive_metadata_target(saved)
    evidence = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=1024)
    )
    series, issue = assemble_bound_archive_metadata(saved, evidence, now=NOW)
    async with stage_cbz_metadata_interruptible(
        target.path,
        target.path.parent,
        series,
        issue,
        max_uncompressed_bytes=1000000,
    ) as staged:
        async with factory() as session:
            await revalidate_archive_metadata_target(session, target)
        await asyncio.to_thread(target.check_unchanged)
        staged.check_unchanged()
        with ZipFile(staged.path) as archive:
            assert archive.read("page.jpg") == b"page bytes"
            assert b"My local summary" in archive.read("ComicInfo.xml")
            assert b"My local summary" in archive.read("MetronInfo.xml")
        assert path.read_bytes() == original
    assert set(path.parent.iterdir()) == {path}


@pytest.mark.parametrize("selection", [(), (True,), (-1,), (1, 1), tuple(range(1, 202))])
async def test_exact_subset_is_bounded_and_validated(identity_probe_db, selection):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        with pytest.raises(ValueError, match="distinct positive issue IDs"):
            await read_series_refresh_state(session, 1, issue_ids=selection)


async def test_managed_root_alias_cannot_mutate_registered_reference(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    file_id, issue_id, _, root_id, path = await seed(factory, tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(path.parent.parent, target_is_directory=True)
    async with factory.begin() as session:
        await session.execute(update(LibraryRoot).values(path=str(alias)))
        await session.execute(
            update(LibraryFile).values(file_path=str(alias / path.parent.name / path.name))
        )
        session.add(
            LibraryFile(
                file_path=str(path),
                file_name=path.name,
                file_size=path.stat().st_size,
                file_modified_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
                file_format=FileFormat.CBZ,
                issue_id=issue_id,
                library_root_id=root_id,
                storage_mode=LibraryFileStorageMode.REFERENCED,
            )
        )
    target = await inspect_archive_metadata_target(await binding(factory, file_id))
    async with factory() as session:
        with pytest.raises(ArchiveMetadataBindingError, match="reference_only"):
            await revalidate_archive_metadata_target(session, target)
