"""Full refresh, arc retry and in-place repair preserve current identity ownership."""

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import delete, event, func, select, update

from pullbox.core.exceptions import ValidationError
from pullbox.core.metadata_identity import IdentityEvidenceKind, IdentityNamespace
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Issue, Series
from pullbox.models.issue import IssueStatus
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesExternalIdentity,
)
from pullbox.models.reader import IssueReaderState
from pullbox.models.user import User
from pullbox.providers.base import IssueMetadata, IssueSummary, SeriesMetadata
from pullbox.services.import_deferred_recovery_execution import prepare_deferred_recovery
from pullbox.services.metadata_service import MetadataService
from pullbox.services.metadata_writer_identity import attach_issue_summary_identities
from pullbox.services.story_arc_catalog import StoryArcCatalogService
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from tests.unit.test_import_reference_recovery import add_second_reference, reference_case
from tests.unit.test_story_arc_catalog import _provider, _root


def _full():
    return IssueMetadata(
        "91",
        "42",
        13,
        "Updated",
        "Description",
        "2020-01-01",
        None,
        None,
        64,
        None,
        issue_number_text="13A",
    )


async def _seed(factory, tmp_path):
    provider = MagicMock()
    provider.get_issue = AsyncMock(return_value=_full())
    service = MetadataService(provider, tmp_path)
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(
            session,
            42,
            SeriesMetadata(
                "42",
                "Parent",
                None,
                2020,
                None,
                None,
                None,
                None,
                None,
                1,
                None,
            ),
        )
        issue = (
            await service.upsert_issue_summaries(
                session,
                series,
                [
                    IssueSummary(
                        "91",
                        13,
                        "Original",
                        None,
                        None,
                        "issue",
                        "13A",
                    )
                ],
            )
        )[0]
        issue.status = IssueStatus.OWNED
        return service, provider, series.id, issue.id


@pytest.mark.parametrize("problem", [None, "id", "parent", "issue_conflict", "parent_conflict"])
async def test_full_issue_refresh_requires_exact_current_identity(
    identity_probe_db, tmp_path, problem
):
    _, factory, _ = identity_probe_db
    service, provider, series_id, issue_id = await _seed(factory, tmp_path)
    if problem in {"id", "parent"}:
        provider.get_issue.return_value = replace(
            _full(), **{"provider_id" if problem == "id" else "series_provider_id": "99"}
        )
    elif problem:
        model = IssueExternalIdentity if problem == "issue_conflict" else SeriesExternalIdentity
        async with factory.begin() as session:
            await session.execute(update(model).values(verification_state="conflicted"))
    async with factory.begin() as session:
        if problem:
            with pytest.raises(ValidationError):
                await service.fetch_issue(session, 91)
        else:
            await service.fetch_issue(session, 91)
    async with factory() as session:
        issue = await session.get(Issue, issue_id)
        assert issue.title == ("Original" if problem else "Updated")
        assert issue.series_id == series_id and issue.status == IssueStatus.OWNED
        assert issue.issue_number_text == "13A"
        history = list(
            await session.scalars(select(IssueIdentityEvent).order_by(IssueIdentityEvent.id))
        )
        assert len(history) == (1 if problem else 2)
        assert '"parent_identity"' in history[-1].request_json
    provider.get_issue.assert_awaited_once_with("91")
    assert len(provider.mock_calls) == 1


async def test_full_issue_refresh_has_no_write_lock_during_fetch_and_never_commits(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, provider, _, issue_id = await _seed(factory, tmp_path)
    async with factory() as session:

        async def fetch(_id):
            assert not session.in_transaction()
            return _full()

        provider.get_issue.side_effect = fetch
        await service.fetch_issue(session, 91)
        await session.rollback()
    async with factory() as session:
        assert (await session.get(Issue, issue_id)).title == "Original"
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 1


@pytest.mark.parametrize("kind", ["series", "issue"])
@pytest.mark.parametrize("state", ["released", "stale", "conflicted"])
async def test_arc_refresh_cannot_reuse_receipt_after_ownership_changes(
    identity_probe_db, tmp_path, kind, state
):
    _, factory, _ = identity_probe_db
    provider = _provider()
    service = StoryArcCatalogService(provider)
    preview = await service.preview("31")
    async with factory.begin() as session:
        root = await _root(session, tmp_path)
        result = await service.add(
            session, preview, ordered_issue_provider_ids=["11", "12"], library_root_id=root.id
        )
        arc_id, revision = result.id, result.revision
        model = SeriesExternalIdentity if kind == "series" else IssueExternalIdentity
        if state == "released":
            await session.execute(delete(model))
        else:
            await session.execute(update(model).values(verification_state=state))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError, match="identity needs review"):
            await service.refresh(session, arc_id, preview, expected_revision=revision)
        from pullbox.models.story_arc import StoryArc

        assert (await session.get(StoryArc, arc_id)).revision == revision
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 2


@pytest.mark.parametrize("conflict", [None, "issue", "series"])
async def test_reference_reparent_retains_owned_issue_and_updates_proof_atomically(
    identity_probe_db, tmp_path, conflict
):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        job, file, library, issue, metadata, path = await reference_case(session, tmp_path)
        original_parent_id, issue_id = issue.series_id, issue.id
        reader = User(username="identity-reader", password_hash="unused-test-hash")
        session.add(reader)
        await session.flush()
        reader_state = IssueReaderState(
            user_id=reader.id,
            issue_id=issue.id,
            last_page_index=13,
            page_count=30,
            content_revision="a" * 64,
            want_to_read=True,
        )
        session.add(reader_state)
        issue.comicvine_id = file.comicvine_issue_id = file.matched_issue_cv_id = 7001
        file.diagnostics = {
            "source_metadata": {"comicinfo": {"series": "Thunderbolts", "number": "104"}},
            "metadata_signals": {
                "series_name": "comicinfo",
                "issue_number": "comicinfo",
                "comicvine_issue_id": "comicinfo",
            },
        }
        issue.title = "Keep local title"
        parent = await session.get(Series, original_parent_id)
        await attach_issue_summary_identities(
            session,
            parent,
            [
                (
                    issue,
                    IssueSummary(
                        "7001",
                        104,
                        None,
                        None,
                        None,
                        "issue",
                        "104",
                    ),
                )
            ],
            None,
        )
        if conflict == "issue":
            await session.execute(
                update(IssueExternalIdentity).values(verification_state="conflicted")
            )
        elif conflict == "series":
            target = Series(title="Thunderbolts", sort_title="Thunderbolts", comicvine_id=700)
            session.add(target)
            await session.flush()
            session.add(
                SeriesExternalIdentity(
                    series_id=target.id,
                    identity_namespace=IdentityNamespace.COMICVINE,
                    external_id="700",
                    verification_state=IdentityVerificationState.CONFLICTED,
                    evidence_kind=IdentityEvidenceKind.LEGACY_BACKFILL,
                )
            )
        await session.commit()
        before = path.read_bytes(), path.stat().st_mtime_ns
        await prepare_deferred_recovery(session, job.id, metadata_service=metadata)
        await session.refresh(issue)
        await session.refresh(file)
        await session.refresh(library)
        assert issue.id == library.issue_id == file.matched_issue_id == issue_id
        assert issue.title == "Keep local title" and issue.status == IssueStatus.OWNED
        await session.refresh(reader_state)
        assert reader_state.issue_id == issue_id and reader_state.last_page_index == 13
        assert reader_state.page_count == 30 and reader_state.content_revision == "a" * 64
        assert reader_state.want_to_read and reader_state.state_version == 1
        assert metadata._provider.mock_calls == []
        assert (path.read_bytes(), path.stat().st_mtime_ns) == before
        history = list(
            await session.scalars(select(IssueIdentityEvent).order_by(IssueIdentityEvent.id))
        )
        if conflict:
            assert issue.series_id == original_parent_id
            assert len(history) == 1
            assert "review" in file.diagnostics["mixed_folder_recovery"]["reason"].lower()
            if conflict == "issue":
                assert (
                    await session.scalar(select(Series).where(Series.comicvine_id == 700)) is None
                )
        else:
            assert issue.series_id != original_parent_id
            assert len(history) == 2
            assert '"external_id":"700"' in history[-1].request_json
            assert '"source_instance":"comicvine_local"' in history[-1].request_json


async def test_reference_identity_conflict_does_not_block_later_safe_repair(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        job, file, library, issue, metadata, path = await reference_case(session, tmp_path)
        original_parent = issue.series_id
        issue.comicvine_id = file.comicvine_issue_id = file.matched_issue_cv_id = 7001
        file.diagnostics = {
            "source_metadata": {"comicinfo": {"series": "Thunderbolts", "number": "104"}},
            "metadata_signals": {
                "series_name": "comicinfo",
                "issue_number": "comicinfo",
                "comicvine_issue_id": "comicinfo",
            },
        }
        await attach_issue_summary_identities(
            session,
            await session.get(Series, original_parent),
            [
                (issue, IssueSummary("7001", 104, None, None, None, "issue", "104")),
            ],
            None,
        )
        await session.execute(update(IssueExternalIdentity).values(verification_state="conflicted"))
        other, other_library = await add_second_reference(session, job, file, library, tmp_path)
        other_issue = Issue(
            series_id=original_parent, comicvine_id=1002, issue_number=105, status=IssueStatus.OWNED
        )
        session.add(other_issue)
        await session.flush()
        other.matched_issue_id = other_library.issue_id = other_issue.id
        other.matched_issue_cv_id = other_issue.comicvine_id
        other.diagnostics = {"metadata_signals": {"issue_number": "release_title"}}
        first = metadata.get_catalog_issue_summaries_for_series.return_value[0]
        metadata.get_catalog_issue_summaries_for_series.return_value = [
            first,
            replace(first, provider_id="7002", issue_number=105, issue_number_text="105"),
        ]
        await session.commit()
        before = path.read_bytes(), path.stat().st_mtime_ns
        await prepare_deferred_recovery(session, job.id, metadata_service=metadata)
        await session.refresh(file)
        await session.refresh(issue)
        await session.refresh(other)
        await session.refresh(other_library)
        assert issue.series_id == original_parent
        assert "review" in file.diagnostics["mixed_folder_recovery"]["reason"].lower()
        recovered = await session.scalar(select(Issue).where(Issue.comicvine_id == 7002))
        assert recovered is not None and recovered.status is IssueStatus.OWNED
        assert other.matched_issue_id == other_library.issue_id == recovered.id
        assert (path.read_bytes(), path.stat().st_mtime_ns) == before
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 2
        assert job.progress_snapshot["deferred_recovery"]["reference_files_repaired"] == 1


async def test_full_issue_refresh_locks_parent_before_child_write(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    if engine.dialect.name != "postgresql":
        pytest.skip("SQLite serializes writers at transaction entry rather than row locks")
    service, _, _, _ = await _seed(factory, tmp_path)
    statements = []

    def track(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            await service.fetch_issue(session, 91)
        parent_lock = next(
            i for i, sql in enumerate(statements) if "FROM series " in sql and "FOR UPDATE" in sql
        )
        child_lock = next(
            i for i, sql in enumerate(statements) if "FROM issues " in sql and "FOR UPDATE" in sql
        )
        child_write = next(
            i for i, sql in enumerate(statements) if sql.startswith("UPDATE issues ")
        )
        assert parent_lock < child_lock < child_write
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)
