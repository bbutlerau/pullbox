"""Full and partial catalog writes share identity and ownership safeguards."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from pullbox.core.metadata_identity import ExternalIdentityRef
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Issue, IssueReaderState, LibraryFile, LibraryRoot, Series, User
from pullbox.models.issue import IssueStatus
from pullbox.models.library import FileFormat, LibraryFileStorageMode
from pullbox.models.metadata_identity import IssueExternalIdentity, IssueIdentityEvent
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_issue_catalog import (
    IssueCatalogConflictError,
    SourceIssueBatch,
    apply_issue_batch,
)
from pullbox.services.metadata_series_refresh import refresh_series_from_sources
from pullbox.services.metadata_series_refresh_state import read_series_refresh_state
from pullbox.services.metadata_writer_identity import metadata_write_scope
from tests.integration.metadata_identity.test_series_adoption import (  # noqa: F401
    bundle,
    configured_sources,
)
from tests.integration.metadata_identity.test_series_refresh import (
    RefreshAdapter,
    refresh_registry,
    seed,
)


def batch(data, *, issues=None):
    return SourceIssueBatch(
        data.series.source,
        data.series.external_id,
        data.issues if issues is None else issues,
        data.source_revision,
    )


async def apply(session, series_id, offered, **kwargs):
    async with metadata_write_scope(session):
        state = await read_series_refresh_state(session, series_id)
        return await apply_issue_batch(session, state, offered, datetime.now(UTC), **kwargs)


@pytest.mark.parametrize("monitored", [False, True])
async def test_partial_window_adds_only_its_new_identity_without_claiming_complete_sync(
    identity_probe_db, monitored
):
    _, factory, _ = identity_probe_db
    series_id, issue_id, _ = await seed(factory, count=2)
    data = bundle(numbers=("1", "2", "50-x"))
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        series.monitored = monitored
    async with factory() as session:
        before = await read_series_refresh_state(session, series_id)
        series = await session.get(Series, series_id)
        times = (series.issue_catalog_last_checked_at, series.issue_catalog_last_synced_at)
        created = await apply(session, series_id, batch(data, issues=data.issues[2:]))
        assert len(created) == 1, "A partial catalog window must actually register new issues"
        issue = await session.get(Issue, created[0])
        assert issue.issue_number_text == "50-X"
        assert issue.status is (IssueStatus.WANTED if monitored else IssueStatus.SKIPPED)
        assert (await session.get(Issue, issue_id)).status is IssueStatus.OWNED
        assert (series.issue_catalog_last_checked_at, series.issue_catalog_last_synced_at) == times
        assert series.path == "/reference/original"
        after = await read_series_refresh_state(session, series_id)
        assert after.series == before.series and after.checkpoints == before.checkpoints
        assert after.issues[:2] == before.issues
        assert (await load_metadata_baseline(session, Kind.ISSUE, created[0])).revision == 1
        await session.commit()
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Issue)) == 3
        assert await apply(session, series_id, batch(data, issues=data.issues[2:])) == ()
        await session.commit()


async def test_partial_update_preserves_local_edits_manual_skips_and_owned_status(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    series_id, issue_id, data = await seed(factory)
    async with factory.begin() as session:
        issue = await session.get(Issue, issue_id)
        issue.title = "My local title"
        issue.integrity_status = "verified"
    data.issues[0].title = "Provider title"
    data.issues[0].description = "New provider description"
    async with factory() as session:
        assert await apply(session, series_id, batch(data)) == ()
        issue = await session.get(Issue, issue_id)
        assert issue.description == "New provider description"
        assert issue.title == "My local title"
        assert issue.status is IssueStatus.OWNED and issue.manual_skip
        assert issue.integrity_status == "verified"
        await session.commit()


@pytest.mark.parametrize(
    "conflict",
    [
        "parent",
        "parent_alias",
        "source",
        "revision",
        "renumber",
        "identity",
        "duplicate_number",
        "duplicate_id",
        "missing",
    ],
)
async def test_invalid_batch_cannot_partially_mutate_existing_metadata(identity_probe_db, conflict):
    _, factory, _ = identity_probe_db
    series_id, issue_id, _ = await seed(factory, count=2)
    data = bundle(numbers=("1", "2", "3"))
    data.issues[0].description = "Must not save"
    offered = batch(data)
    if conflict == "parent":
        offered = replace(offered, series_external_id="999")
    elif conflict == "parent_alias":
        offered = replace(offered, series_external_id="042")
        for issue in offered.issues:
            issue.series_external_id = "042"
    elif conflict == "source":
        offered = replace(offered, source=Source.COMICVINE_API)
    elif conflict == "revision":
        offered = replace(offered, source_revision=9)
    elif conflict == "renumber":
        data.issues[1].issue_number_text = "50-o"
    elif conflict == "identity":
        data.issues[1].external_id = "999"
    elif conflict == "duplicate_number":
        data.issues[2].issue_number_text = "2"
    elif conflict == "duplicate_id":
        data.issues[2].external_id = data.issues[1].external_id
    else:
        offered = batch(data, issues=data.issues[:1])
    async with factory() as session:
        with pytest.raises(IssueCatalogConflictError):
            await apply(session, series_id, offered, complete=conflict == "missing")
        await session.commit()
    async with factory() as session:
        assert (await session.get(Issue, issue_id)).description is None
        assert await session.scalar(select(func.count()).select_from(Issue)) == 2
        assert (await load_metadata_baseline(session, Kind.ISSUE, issue_id)).revision == 1


async def test_partial_batch_crosswalk_stays_observation_and_rollback_is_atomic(identity_probe_db):
    _, factory, _ = identity_probe_db
    series_id, _, _ = await seed(factory)
    data = bundle(numbers=("1", "2"))
    data.issues[1].cross_identities = [ExternalIdentityRef(Namespace.COMICVINE, Kind.ISSUE, "901")]
    async with factory() as session:
        created = await apply(session, series_id, batch(data, issues=data.issues[1:]))
        assert len(created) == 1
        assert (await session.get(Issue, created[0])).comicvine_id is None
        claim = await session.scalar(
            select(IssueExternalIdentity).where(IssueExternalIdentity.issue_id == created[0])
        )
        assert claim.identity_namespace is Namespace.METRON
        event = await session.scalar(
            select(IssueIdentityEvent).where(
                IssueIdentityEvent.identity_namespace == Namespace.COMICVINE
            )
        )
        assert event.verification_state.value == "observed"
        await session.rollback()
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Issue)) == 1
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 1


async def test_scheduled_receipt_does_not_count_issues_created_before_its_snapshot(
    identity_probe_db, monkeypatch
):
    from pullbox.providers.metadata import sources
    from pullbox.services import metadata_series_refresh as refresh
    from pullbox.services.metadata_scheduled_refresh import refresh_scheduled_series

    _, factory, _ = identity_probe_db
    series_id, _, _ = await seed(factory)
    data = bundle(numbers=("1", "2"))
    adapter = RefreshAdapter(data)
    monkeypatch.setattr(sources, "metadata_sources", lambda: refresh_registry(adapter).factories)
    original = refresh.read_series_refresh_state
    inserted = False

    async def snapshot(session, local_id):
        nonlocal inserted
        if not inserted:
            inserted = True
            await session.rollback()
            async with factory() as other:
                await refresh_series_from_sources(
                    other, series_id, registry=refresh_registry(adapter)
                )
                await other.commit()
        return await original(session, local_id)

    monkeypatch.setattr(refresh, "read_series_refresh_state", snapshot)
    async with factory() as session:
        result = await refresh_scheduled_series(session, series_id)
        assert result.added == 0, (
            "ID ranges cannot attribute another operation's issue to this refresh"
        )
        assert not result.search_wanted
        await session.commit()


@pytest.mark.parametrize("complete", [False, True])
async def test_shared_writer_preserves_registered_artifact_and_private_reading_state(
    identity_probe_db, tmp_path, complete
):
    _, factory, _ = identity_probe_db
    series_id, issue_id, data = await seed(factory)
    path = tmp_path / "original.cbz"
    path.write_bytes(b"original archive bytes")
    now = datetime.now(UTC)
    async with factory.begin() as session:
        root = LibraryRoot(name="Reference library", path=str(tmp_path), allow_managed_writes=False)
        user = User(username="catalog-reader", password_hash="not-a-real-password")
        session.add_all([root, user])
        await session.flush()
        file = LibraryFile(
            file_path=str(path),
            file_name=path.name,
            file_size=path.stat().st_size,
            file_format=FileFormat.CBZ,
            file_modified_at=now,
            issue_id=issue_id,
            library_root_id=root.id,
            storage_mode=LibraryFileStorageMode.REFERENCED,
            naming_snapshot={"original": "untouched"},
            source_signature={"size": path.stat().st_size},
        )
        reader = IssueReaderState(
            user_id=user.id,
            issue_id=issue_id,
            last_page_index=9,
            page_count=32,
            content_revision="archive-revision",
            last_opened_at=now,
            progress_updated_at=now,
            state_version=7,
            want_to_read=True,
        )
        session.add_all([file, reader])
    async with factory() as session:
        data.issues[0].description = "New metadata only"
        assert await apply(session, series_id, batch(data), complete=complete) == ()
        await session.commit()
    async with factory() as session:
        saved_file = await session.scalar(select(LibraryFile))
        saved_reader = await session.scalar(select(IssueReaderState))
        assert saved_file.issue_id == issue_id and saved_file.file_path == str(path)
        assert saved_file.storage_mode is LibraryFileStorageMode.REFERENCED
        assert saved_file.naming_snapshot == {"original": "untouched"}
        assert saved_file.source_signature == {"size": path.stat().st_size}
        assert saved_file.file_modified_at == now
        assert saved_reader.issue_id == issue_id and saved_reader.last_page_index == 9
        assert saved_reader.page_count == 32 and saved_reader.state_version == 7
        assert saved_reader.content_revision == "archive-revision"
        assert saved_reader.progress_updated_at == now and saved_reader.want_to_read
        assert (await session.get(Issue, issue_id)).description == "New metadata only"
    assert path.read_bytes() == b"original archive bytes"


async def test_empty_partial_window_is_not_a_full_membership_deletion(identity_probe_db):
    _, factory, _ = identity_probe_db
    series_id, _, data = await seed(factory)
    async with factory() as session:
        before = await read_series_refresh_state(session, series_id)
        assert await apply(session, series_id, batch(data, issues=())) == ()
        assert await read_series_refresh_state(session, series_id) == before
        await session.commit()


async def test_failed_issue_baseline_write_rolls_back_rows_and_identity_events(
    identity_probe_db, monkeypatch
):
    from pullbox.services import metadata_issue_catalog as writer

    _, factory, _ = identity_probe_db
    series_id, _, _ = await seed(factory)
    data = bundle(numbers=("1", "2"))

    async def fail(*_args):
        raise RuntimeError("baseline rejected")

    monkeypatch.setattr(writer, "save_metadata_baselines", fail)
    async with factory() as session:
        with pytest.raises(RuntimeError, match="baseline rejected"):
            await apply(session, series_id, batch(data, issues=data.issues[1:]))
        await session.commit()
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Issue)) == 1
        assert await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 1
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 1
