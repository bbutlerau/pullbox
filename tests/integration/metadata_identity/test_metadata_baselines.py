"""Canonical provenance survives sessions without acquiring identity ownership."""

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, select, update

from pullbox.core.metadata_identity import ExternalIdentityRef
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.core.metadata_identity_state import IdentityVerificationState as State
from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity
from pullbox.schemas.metadata_snapshot import FieldOrigin, MetadataSnapshot, MetadataValues
from pullbox.schemas.metadata_sources import MetadataDomain
from pullbox.services.metadata_baselines import (
    MetadataBaselineConflictError,
    MetadataBaselineWrite,
    load_metadata_baseline,
    save_metadata_baselines,
)


async def seed(factory, kind=Kind.SERIES):
    async with factory.begin() as session:
        series = Series(title="Original", sort_title="Original", path="/reference/original")
        session.add(series)
        await session.flush()
        if kind is Kind.SERIES:
            row = series
            claim = SeriesExternalIdentity(series_id=row.id)
        elif kind is Kind.ISSUE:
            row = Issue(series_id=series.id, title="Original", issue_number=1)
            session.add(row)
            await session.flush()
            claim = IssueExternalIdentity(issue_id=row.id)
        else:
            row = StoryArc(name="Original")
            session.add(row)
            await session.flush()
            claim = StoryArcExternalIdentity(
                story_arc_id=row.id, source="metron", namespace="story_arc"
            )
        if kind is not Kind.STORY_ARC:
            claim.identity_namespace = Namespace.METRON
        claim.external_id = "42"
        claim.verification_state = State.VERIFIED
        claim.evidence_kind = "provider_result"
        session.add(claim)
        return row.id


def snapshot(kind=Kind.SERIES, title="Original"):
    return MetadataSnapshot(
        entity_kind=kind,
        identities=(ExternalIdentityRef(Namespace.METRON, kind, "42"),),
        values=MetadataValues(title=title),
        origins=(
            FieldOrigin(
                field="title",
                domain=MetadataDomain.STORY_ARCS if kind is Kind.STORY_ARC else MetadataDomain.CORE,
                source=Source.METRON_API,
                observed_at=datetime(2026, 9, 28, tzinfo=UTC),
            ),
        ),
    )


@pytest.mark.parametrize("kind", list(Kind))
async def test_baseline_survives_new_sessions_and_database_connections(identity_probe_db, kind):
    engine, factory, _ = identity_probe_db
    local_id = await seed(factory, kind)
    value = snapshot(kind)
    async with factory.begin() as session:
        result = await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, value)])
        assert len(result) == 1 and result[0].revision == 1
    await engine.dispose()
    async with factory() as session:
        saved = await load_metadata_baseline(session, kind, local_id)
        assert saved is not None and saved.snapshot == value and saved.revision == 1


async def test_baseline_write_does_not_commit_entity_changes(identity_probe_db):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])
        row = await session.get(Series, local_id)
        row.title = "Not committed"
        await session.rollback()
    async with factory() as session:
        assert await load_metadata_baseline(session, Kind.SERIES, local_id) is None
        assert (await session.get(Series, local_id)).title == "Original"


async def test_stale_baseline_cannot_overwrite_or_partially_save_a_batch(identity_probe_db):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    issue_id = await seed(factory, Kind.ISSUE)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])
    async with factory.begin() as session:
        with pytest.raises(MetadataBaselineConflictError):
            await save_metadata_baselines(
                session,
                [
                    MetadataBaselineWrite(issue_id, snapshot(Kind.ISSUE)),
                    MetadataBaselineWrite(local_id, snapshot(title="Stale")),
                ],
            )
    async with factory() as session:
        assert await load_metadata_baseline(session, Kind.ISSUE, issue_id) is None
        saved = await load_metadata_baseline(session, Kind.SERIES, local_id)
        assert saved.snapshot.values.title == "Original" and saved.revision == 1


@pytest.mark.parametrize("revision", [0, 1])
async def test_concurrent_writers_have_one_winner(identity_probe_db, revision):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    if revision:
        async with factory.begin() as session:
            await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])

    async def write(title):
        async with factory.begin() as session:
            try:
                return await save_metadata_baselines(
                    session, [MetadataBaselineWrite(local_id, snapshot(title=title), revision)]
                )
            except MetadataBaselineConflictError:
                return None

    results = await asyncio.wait_for(asyncio.gather(write("First"), write("Second")), 15)
    assert results.count(None) == 1
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.SERIES, local_id)
        assert saved.revision == revision + 1
        assert (await session.get(Series, local_id)).path == "/reference/original"


@pytest.mark.parametrize("state", [State.STALE, State.CONFLICTED, None])
async def test_missing_or_unverified_ownership_cannot_gain_a_managed_baseline(
    identity_probe_db, state
):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        if state is None:
            await session.execute(delete(SeriesExternalIdentity))
        else:
            await session.execute(update(SeriesExternalIdentity).values(verification_state=state))
    async with factory.begin() as session:
        with pytest.raises(MetadataBaselineConflictError):
            await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])


@pytest.mark.parametrize("kind", list(Kind))
async def test_parent_deletion_cascades_only_its_baseline(identity_probe_db, kind):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory, kind)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot(kind))])
    async with factory.begin() as session:
        model = {Kind.SERIES: Series, Kind.ISSUE: Issue, Kind.STORY_ARC: StoryArc}[kind]
        await session.execute(delete(model).where(model.id == local_id))
    async with factory() as session:
        assert await load_metadata_baseline(session, kind, local_id) is None


async def test_local_override_roundtrip_stays_protected_from_provider_refresh(identity_probe_db):
    from pullbox.services.metadata_assembly import assemble_metadata
    from tests.unit.test_metadata_discovery import row, runtime

    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    initial = snapshot()
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, initial)])
    async with factory.begin() as session:
        saved = await load_metadata_baseline(session, Kind.SERIES, local_id)
        assert saved is not None
        changed = assemble_metadata(
            Kind.SERIES,
            initial.identities,
            [row(Source.METRON_API, title="Provider")],
            [runtime(Source.METRON_API).policy],
            now=datetime.now(UTC),
            current=MetadataValues(title="My title"),
            previous=saved.snapshot,
            replace_managed=True,
        )
        await save_metadata_baselines(
            session, [MetadataBaselineWrite(local_id, changed, saved.revision)]
        )
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.SERIES, local_id)
        assert saved.snapshot.values.title == "My title"
        assert next(item for item in saved.snapshot.origins if item.field == "title").user_override


async def test_baseline_migration_roundtrip_preserves_entities_and_matches_models(
    identity_probe_db,
):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Integer, MetaData, Table, func

    from pullbox.models import Base
    from tests.integration.metadata_identity.test_production_migration import _revision

    engine, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])
    names = {f"{kind.value}_metadata_baselines" for kind in Kind}
    async with engine.begin() as connection:

        def migrate(sync):
            revision = _revision("t1n2o3p4q567_add_metadata_baselines", sync)
            revision.downgrade()
            revision.upgrade()
            expected = MetaData()
            for parent in ("series", "issues", "story_arcs"):
                Table(parent, expected, Column("id", Integer, primary_key=True))
            for name in names:
                Base.metadata.tables[name].to_metadata(expected)
            context = MigrationContext.configure(
                sync,
                opts={
                    "include_object": lambda obj, name, type_, reflected, compare_to: (
                        name in names if type_ == "table" else True
                    ),
                    "compare_server_default": True,
                },
            )
            assert compare_metadata(context, expected) == []

        await connection.run_sync(migrate)
    async with factory() as session:
        assert (await session.get(Series, local_id)).title == "Original"
        assert await session.scalar(select(func.count()).select_from(SeriesExternalIdentity)) == 1
        assert await load_metadata_baseline(session, Kind.SERIES, local_id) is None
    async with factory.begin() as session:
        assert (
            await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])
        )[0].revision == 1


@pytest.mark.parametrize("kind", list(Kind))
async def test_database_constraints_reject_orphan_duplicate_and_invalid_baselines(
    identity_probe_db, kind
):
    from sqlalchemy import insert
    from sqlalchemy.exc import IntegrityError

    from pullbox.models import Base

    _, factory, _ = identity_probe_db
    local_id = await seed(factory, kind)
    table = Base.metadata.tables[f"{kind.value}_metadata_baselines"]
    values = {
        f"{kind.value}_id": local_id,
        "revision": 1,
        "snapshot_json": snapshot(kind).model_dump_json(),
    }
    async with factory.begin() as session:
        await session.execute(insert(table).values(**values))
    for invalid in ({}, {f"{kind.value}_id": 99999}, {"revision": 0}, {"snapshot_json": ""}):
        async with factory.begin() as session:
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    await session.execute(
                        update(table)
                        .where(table.c[f"{kind.value}_id"] == local_id)
                        .values(**invalid)
                        if invalid
                        else insert(table).values(**values)
                    )


async def test_corrupt_saved_provenance_is_not_silently_treated_as_missing(identity_probe_db):
    from pullbox.models import SeriesMetadataBaseline

    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, snapshot())])
        await session.execute(
            update(SeriesMetadataBaseline).values(snapshot_json='{"schema_version":999}')
        )
    async with factory() as session:
        with pytest.raises(MetadataBaselineConflictError, match="invalid"):
            await load_metadata_baseline(session, Kind.SERIES, local_id)


async def test_bulk_baseline_creation_uses_bounded_queries_not_per_issue_reads(identity_probe_db):
    from sqlalchemy import event

    engine, factory, _ = identity_probe_db
    async with factory.begin() as session:
        series = Series(title="Large", sort_title="Large")
        session.add(series)
        await session.flush()
        issues = [Issue(series_id=series.id, issue_number=n) for n in range(1, 101)]
        session.add_all(issues)
        await session.flush()
        session.add_all(
            [
                IssueExternalIdentity(
                    issue_id=issue.id,
                    identity_namespace=Namespace.METRON,
                    external_id=str(n),
                    verification_state=State.VERIFIED,
                    evidence_kind="provider_result",
                )
                for n, issue in enumerate(issues, 1)
            ]
        )
        writes = [
            MetadataBaselineWrite(
                issue.id,
                snapshot(Kind.ISSUE).model_copy(
                    update={
                        "identities": (ExternalIdentityRef(Namespace.METRON, Kind.ISSUE, str(n)),)
                    }
                ),
            )
            for n, issue in enumerate(issues, 1)
        ]
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with factory.begin() as session:
            assert len(await save_metadata_baselines(session, writes)) == 100
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert len(statements) <= 12
