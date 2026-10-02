"""Real metadata writers maintain generic ownership without extra provider work."""

from dataclasses import asdict, replace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import event, func, select

from pullbox.core.events import EventBus
from pullbox.core.exceptions import ValidationError
from pullbox.core.metadata_identity import IdentityEvidenceKind, IdentityNamespace
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Issue, Series
from pullbox.models.import_job import ImportedSeries, ImportJob, ImportSourceType
from pullbox.models.issue import IssueStatus
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesExternalIdentity,
    SeriesIdentityEvent,
)
from pullbox.providers.base import IssueSummary, SeriesMetadata
from pullbox.services.catalog.reader import CatalogIssueSummary, CatalogSeriesMetadata
from pullbox.services.metadata_service import MetadataService
from pullbox.services.metadata_writer_identity import ImportIdentityOrigin
from pullbox.services.series_service import SeriesService


def _profile(key="42"):
    return SeriesMetadata(key, "Identity Series", None, 2020, None, None, None, None, None, 3, None)


def _summary(key="91", number=1):
    return IssueSummary(key, float(number), "Issue", None, None, "issue", str(number))


def _service(tmp_path):
    provider = MagicMock()
    provider.get_series = AsyncMock(side_effect=AssertionError("No network in writer"))
    provider.get_issues_for_series = AsyncMock(side_effect=AssertionError("No network in writer"))
    return MetadataService(provider, tmp_path), provider


@pytest.mark.parametrize("catalog", [False, True])
async def test_prefetched_writers_persist_transport_provenance_and_parent(
    identity_probe_db, tmp_path, catalog
):
    _, factory, _ = identity_probe_db
    service, provider = _service(tmp_path)
    profile, summary = _profile(), _summary()
    if catalog:
        profile = CatalogSeriesMetadata(**asdict(profile))
        summary = CatalogIssueSummary(**asdict(summary))
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(session, 42, profile)
        issues = await service.upsert_issue_summaries(session, series, [summary])
        series_id, issue_id = series.id, issues[0].id
    async with factory() as session:
        parent = await session.scalar(select(SeriesExternalIdentity))
        child = await session.scalar(select(IssueExternalIdentity))
        assert parent is not None and child is not None
        assert (parent.series_id, child.issue_id, child.external_id) == (series_id, issue_id, "91")
        for model in (SeriesIdentityEvent, IssueIdentityEvent):
            record = await session.scalar(select(model))
            assert record.evidence_kind == IdentityEvidenceKind.PROVIDER_RESULT
            assert (
                '"source_instance":"comicvine_local"'
                if catalog
                else '"source_instance":"comicvine_api"'
            ) in record.request_json
        assert (
            '"parent_identity"' in (await session.scalar(select(IssueIdentityEvent))).request_json
        )
        assert (await session.get(Issue, issue_id)).comicvine_id == 91
    assert provider.mock_calls == []


async def test_catalog_does_not_replace_live_fields_but_repairs_missing_generic_identity(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = Series(
            comicvine_id=42,
            title="Live title",
            sort_title="Live title",
            metadata_source="comicvine",
        )
        session.add(series)
        await session.flush()
        issue = Issue(
            series_id=series.id,
            comicvine_id=91,
            issue_number=1,
            issue_number_text="1",
            title="Live issue",
            metadata_source="comicvine",
        )
        session.add(issue)
        await session.flush()
        await service.upsert_series_metadata(
            session, 42, CatalogSeriesMetadata(**asdict(_profile()))
        )
        await service.upsert_issue_summaries(
            session, series, [CatalogIssueSummary(**asdict(_summary()))]
        )
        assert series.title == "Live title" and issue.title == "Live issue"
        assert await session.scalar(select(func.count()).select_from(SeriesExternalIdentity)) == 1
        assert await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 1


async def test_provider_id_disagreement_is_rejected_before_any_series_write(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        with pytest.raises(ValidationError):
            await service.upsert_series_metadata(session, 42, _profile("43"))
        assert await session.scalar(select(func.count()).select_from(Series)) == 0


@pytest.mark.parametrize("mode", ["new", "existing"])
async def test_series_identity_conflict_preserves_fields_and_leaves_no_partial_row(
    identity_probe_db, tmp_path, mode
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        owner = Series(
            title="Original", sort_title="Original", comicvine_id=42 if mode == "existing" else None
        )
        session.add(owner)
        await session.flush()
        session.add(
            SeriesExternalIdentity(
                series_id=owner.id,
                identity_namespace=IdentityNamespace.COMICVINE,
                external_id="43" if mode == "existing" else "42",
                verification_state=IdentityVerificationState.VERIFIED,
                evidence_kind=IdentityEvidenceKind.LEGACY_BACKFILL,
            )
        )
    async with factory.begin() as session:
        with pytest.raises(ValidationError):
            await service.upsert_series_metadata(session, 42, _profile())
        assert await session.scalar(select(func.count()).select_from(Series)) == 1
        assert (await session.scalar(select(Series))).title == "Original"
        assert await session.scalar(select(func.count()).select_from(SeriesIdentityEvent)) == 0


async def test_provisional_issue_gets_exact_identity_without_losing_owned_status(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = Series(title="Imported", sort_title="Imported", comicvine_id=42)
        session.add(series)
        await session.flush()
        issue = Issue(
            series_id=series.id, issue_number=13, issue_number_text="13A", status=IssueStatus.OWNED
        )
        session.add(issue)
        await session.flush()
        await service.upsert_issue_summaries(
            session, series, [replace(_summary(), issue_number=13, issue_number_text="13a")]
        )
        assert issue.status == IssueStatus.OWNED and issue.issue_number_text == "13A"
        assert issue.comicvine_id == 91
        assert (await session.scalar(select(IssueExternalIdentity))).issue_id == issue.id


async def test_conflicting_issue_id_cannot_silently_replace_existing_owned_issue(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = Series(title="Imported", sort_title="Imported", comicvine_id=42)
        session.add(series)
        await session.flush()
        issue = Issue(
            series_id=series.id,
            comicvine_id=92,
            issue_number=1,
            issue_number_text="1",
            title="Keep this",
            status=IssueStatus.OWNED,
        )
        session.add(issue)
        await session.flush()
        series_id, issue_id = series.id, issue.id
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        with pytest.raises(ValidationError):
            await service.upsert_issue_summaries(session, series, [_summary()])
        issue = await session.get(Issue, issue_id)
        assert (
            issue.comicvine_id == 92
            and issue.title == "Keep this"
            and issue.status == IssueStatus.OWNED
        )


async def test_other_series_id_collision_keeps_provisional_row_without_false_attachment(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        other = Series(title="Other", sort_title="Other", comicvine_id=43)
        target = Series(title="Target", sort_title="Target", comicvine_id=42)
        session.add_all([other, target])
        await session.flush()
        session.add(Issue(series_id=other.id, comicvine_id=91, issue_number=1))
        await session.flush()
        created = await service.upsert_issue_summaries(session, target, [_summary()])
        assert created[0].comicvine_id is None
        assert await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 0


@pytest.mark.parametrize(
    "source_type,method",
    [(ImportSourceType.MYLAR3, "mylar3_cv_id"), (ImportSourceType.FILESYSTEM, "user_override")],
)
async def test_targeted_import_records_local_review_provenance_without_provider_calls(
    identity_probe_db, tmp_path, source_type, method
):
    _, factory, _ = identity_probe_db
    metadata, provider = _service(tmp_path)
    provider.get_series_cached = AsyncMock(return_value=None)
    service = SeriesService(metadata_service=metadata, event_bus=EventBus())
    async with factory.begin() as session:
        job = ImportJob(source_path="/fixture/mylar.db", source_type=source_type)
        session.add(job)
        await session.flush()
        item = ImportedSeries(
            import_job_id=job.id, raw_series_name="Trusted import", cv_id=42, cv_match_method=method
        )
        session.add(item)
        await session.flush()
        await service.add_from_import_review_targeted(
            session, import_series=item, issue_summaries=[_summary()]
        )
        for model in (SeriesIdentityEvent, IssueIdentityEvent):
            record = await session.scalar(select(model))
            assert record is not None
            assert record.evidence_kind == (
                IdentityEvidenceKind.MYLAR_DATABASE
                if method == "mylar3_cv_id"
                else IdentityEvidenceKind.MIGRATION
            )
            assert '"record_kind":"imported_series"' in record.request_json
            assert '"source_instance"' not in record.request_json
    assert provider.mock_calls == []


async def test_writer_rollback_removes_metadata_and_identity_together(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory() as session:
        series = await service.upsert_series_metadata(session, 42, _profile())
        await service.upsert_issue_summaries(session, series, [_summary()])
        await session.rollback()
    async with factory() as session:
        for model in (
            Series,
            Issue,
            SeriesExternalIdentity,
            IssueExternalIdentity,
            SeriesIdentityEvent,
            IssueIdentityEvent,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_bulk_hydration_attaches_with_bounded_statement_count(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = Series(title="Long running", sort_title="Long running", comicvine_id=42)
        session.add(series)
        await session.flush()
        series_id = series.id
    statements = []

    def track(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            series = await session.get(Series, series_id)
            created = await service.upsert_issue_summaries(
                session, series, [_summary(str(i + 1000), i) for i in range(1, 451)]
            )
            assert len(created) == 450
            assert (
                await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 450
            )
        # Existing ORM creation uses per-row INSERT RETURNING on SQLite. Count only SELECTs.
        assert sum(statement.lstrip().upper().startswith("SELECT") for statement in statements) < 45
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)


@pytest.mark.parametrize("kind", ["series", "issue", "parent"])
async def test_new_hydration_cannot_bypass_later_conflicted_ownership(
    identity_probe_db,
    tmp_path,
    kind,
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(session, 42, _profile())
        await service.upsert_issue_summaries(session, series, [_summary()])
        series_id = series.id
        model = IssueExternalIdentity if kind == "issue" else SeriesExternalIdentity
        attachment = await session.scalar(select(model))
        attachment.verification_state = IdentityVerificationState.CONFLICTED
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        with pytest.raises(ValidationError):
            if kind == "series":
                await service.upsert_series_metadata(session, 42, _profile())
            else:
                await service.upsert_issue_summaries(session, series, [_summary()])
        assert await session.scalar(select(func.count()).select_from(SeriesIdentityEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 1


@pytest.mark.parametrize("record", ["missing", "unsaved", "different_match"])
async def test_local_import_origin_must_still_match_saved_review(
    identity_probe_db, tmp_path, record
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        job = ImportJob(source_path="/fixture/mylar.db", source_type=ImportSourceType.MYLAR3)
        session.add(job)
        await session.flush()
        item = ImportedSeries(import_job_id=job.id, raw_series_name="Changed", cv_id=43)
        session.add(item)
        await session.flush()
        origin = ImportIdentityOrigin(
            None if record == "unsaved" else (item.id if record == "different_match" else 99999)
        )
        with pytest.raises(ValidationError):
            await service.upsert_series_metadata(session, 42, _profile(), identity_origin=origin)
        assert await session.scalar(select(func.count()).select_from(Series)) == 0
        assert await session.scalar(select(func.count()).select_from(SeriesIdentityEvent)) == 0


async def test_late_issue_conflict_rolls_back_all_prior_attachment_batches(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(session, 42, _profile())
        session.add_all(
            [Issue(series_id=series.id, issue_number=i, title="Preserve") for i in range(1, 450)]
        )
        await session.flush()
        issue = Issue(series_id=series.id, comicvine_id=999, issue_number=450, title="Preserve")
        session.add(issue)
        await session.flush()
        series_id, issue_id = series.id, issue.id
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        with pytest.raises(ValidationError):
            await service.upsert_issue_summaries(
                session, series, [_summary(str(i + 1000), i) for i in range(1, 451)]
            )
        assert await session.scalar(select(func.count()).select_from(Issue)) == 450
        assert (await session.get(Issue, issue_id)).title == "Preserve"
        assert (
            await session.scalar(
                select(func.count()).select_from(Issue).where(Issue.title != "Preserve")
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 0
        assert await session.scalar(select(func.count()).select_from(IssueIdentityEvent)) == 0


async def test_large_catalog_stays_under_conservative_bind_limit(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    # Keep ORM's configurable INSERT pages small too; explicit lookups and
    # identity batches must remain bounded independently of ORM pagination.
    engine.update_execution_options(insertmanyvalues_page_size=20)
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(session, 42, _profile())
        series_id = series.id

    def bound(_conn, _cursor, _statement, params, _context, many):
        batches = (
            params if many and params and isinstance(params[0], tuple | list | dict) else [params]
        )
        assert all(len(batch) <= 999 for batch in batches)

    event.listen(engine.sync_engine, "before_cursor_execute", bound)
    try:
        async with factory.begin() as session:
            series = await session.get(Series, series_id)
            await service.upsert_issue_summaries(
                session, series, [_summary(str(i + 1000), i) for i in range(1, 1201)]
            )
            assert (
                await session.scalar(select(func.count()).select_from(IssueExternalIdentity))
                == 1200
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", bound)


async def test_hydration_locks_parent_before_updating_issue(identity_probe_db, tmp_path):
    engine, factory, _ = identity_probe_db
    if engine.dialect.name != "postgresql":
        pytest.skip("SQLite serializes writers at transaction entry rather than row locks")
    service, _ = _service(tmp_path)
    async with factory.begin() as session:
        series = await service.upsert_series_metadata(session, 42, _profile())
        await service.upsert_issue_summaries(session, series, [_summary()])
        series_id = series.id
    statements = []

    def track(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", track)
    try:
        async with factory.begin() as session:
            series = await session.get(Series, series_id)
            await service.upsert_issue_summaries(
                session, series, [replace(_summary(), title="Updated")]
            )
        parent_lock = next(
            i for i, sql in enumerate(statements) if "FROM series " in sql and "FOR UPDATE" in sql
        )
        child_write = next(
            i for i, sql in enumerate(statements) if sql.startswith("UPDATE issues ")
        )
        assert parent_lock < child_write
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", track)
