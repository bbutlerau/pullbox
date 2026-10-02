"""Canonical credits use the existing reader relations without claiming creator IDs."""

from dataclasses import replace

import pytest
from sqlalchemy import delete, event, func, select

from pullbox.models import Issue
from pullbox.models.creator import Creator, IssueCreator
from pullbox.schemas.metadata_credits import parse_credits
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_credits import read_issue_credits, write_issue_credits
from pullbox.services.metadata_series_adoption import adopt_source_series_bundle
from pullbox.services.metadata_series_refresh import refresh_series_from_sources
from pullbox.services.metadata_service import MetadataService
from pullbox.utilities.comicinfo_creators import load_comicinfo_creator_fields
from tests.integration.metadata_identity.test_series_adoption import (  # noqa: F401
    bundle,
    configured_sources,
)
from tests.integration.metadata_identity.test_series_refresh import RefreshAdapter, refresh_registry
from tests.unit.test_metadata_assembly import Kind
from tests.unit.test_metadata_credits import candidate, metron_credits


def credited_bundle(name="Fixture Author"):
    return replace(bundle(numbers=("1",)), issues=(candidate(credits=metron_credits(name)),))


async def test_add_persists_credits_in_baseline_and_existing_xml_reader(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, credited_bundle(), monitored=True)
        issue_id = await session.scalar(select(Issue.id).where(Issue.series_id == result.series.id))
    async with factory() as session:
        assert await load_comicinfo_creator_fields(session, issue_id) == {
            "Writer": "Fixture Author"
        }
        saved = await load_metadata_baseline(session, Kind.ISSUE, issue_id)
        assert saved.snapshot.values.model_dump(mode="json")["credits"] == [
            {"name": "Fixture Author", "role": "writer"},
        ]
        assert (await session.scalar(select(Creator))).comicvine_id is None


@pytest.mark.parametrize("edit", ["none", "rename", "clear"])
async def test_refresh_replaces_only_managed_credits_and_preserves_edits(identity_probe_db, edit):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, credited_bundle(), monitored=True)
        series_id = result.series.id
        issue_id = await session.scalar(select(Issue.id).where(Issue.series_id == series_id))
        assert await load_comicinfo_creator_fields(session, issue_id) == {
            "Writer": "Fixture Author"
        }
    async with factory.begin() as session:
        if edit == "rename":
            (await session.scalar(select(Creator))).name = "Local Author"
        elif edit == "clear":
            await session.execute(delete(IssueCreator).where(IssueCreator.issue_id == issue_id))
    async with factory() as session:
        await refresh_series_from_sources(
            session,
            series_id,
            registry=refresh_registry(RefreshAdapter(credited_bundle("Replacement"))),
        )
        await session.commit()
    async with factory() as session:
        assert await load_comicinfo_creator_fields(session, issue_id) == (
            {}
            if edit == "clear"
            else {"Writer": "Local Author" if edit == "rename" else "Replacement"}
        )
        saved = await load_metadata_baseline(session, Kind.ISSUE, issue_id)
        assert next(o for o in saved.snapshot.origins if o.field == "credits").user_override == (
            edit != "none"
        )


async def test_caller_rollback_removes_new_credit_graph(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        await adopt_source_series_bundle(session, credited_bundle(), monitored=True)
        assert await session.scalar(select(func.count()).select_from(IssueCreator)) == 1
        await session.rollback()
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(IssueCreator)) == 0
        assert await session.scalar(select(func.count()).select_from(Creator)) == 0
        assert await session.scalar(select(func.count()).select_from(Issue)) == 0


async def test_descriptive_names_never_assign_or_rewrite_foreign_creator_identity(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        known = Creator(
            name="Fixture Author",
            comicvine_id=77,
            comicvine_url="https://comicvine.gamespot.com/person/4040-77/",
        )
        session.add(known)
        await session.flush()
        known_id = known.id
        result = await adopt_source_series_bundle(session, credited_bundle(), monitored=True)
        issue_id = await session.scalar(select(Issue.id).where(Issue.series_id == result.series.id))
        link = await session.scalar(select(IssueCreator).where(IssueCreator.issue_id == issue_id))
        assert link.creator_id != known_id
        assert (await session.get(Creator, link.creator_id)).comicvine_id is None
        # An already-linked legacy creator is retained, not replaced or renamed globally.
        await session.execute(delete(IssueCreator).where(IssueCreator.issue_id == issue_id))
        session.add(IssueCreator(issue_id=issue_id, creator_id=known_id, role="writer"))
    async with factory.begin() as session:
        await write_issue_credits(
            session,
            {
                issue_id: parse_credits(
                    [
                        {"name": "Fixture Author", "role": "writer, inker"},
                    ]
                )
            },
        )
    async with factory() as session:
        link = await session.scalar(select(IssueCreator).where(IssueCreator.issue_id == issue_id))
        assert link.creator_id == known_id and link.role == "inker, writer"
        known = await session.get(Creator, known_id)
        assert known.name == "Fixture Author" and known.comicvine_id == 77


async def test_credit_read_and_write_queries_are_batched_and_reuse_descriptive_rows(
    identity_probe_db,
):
    engine, factory, _ = identity_probe_db
    data = bundle(numbers=tuple(str(i) for i in range(401)))
    async with factory.begin() as session:
        await adopt_source_series_bundle(session, data, monitored=True)
        ids = list(await session.scalars(select(Issue.id).order_by(Issue.id)))
    statements = []

    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    credits = parse_credits([{"name": "Shared", "role": "writer"}])
    try:
        async with factory.begin() as session:
            for offset in range(0, len(ids), 200):
                await write_issue_credits(session, {i: credits for i in ids[offset : offset + 200]})
        assert len(statements) <= 13, "Credit queries must not scale one per issue"
        statements.clear()
        async with factory() as session:
            assert await read_issue_credits(session, ids) == dict.fromkeys(ids, credits)
        assert len(statements) == 3
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Creator)) == 1


async def test_credit_write_limits_fail_before_any_mutation(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="200"):
            await write_issue_credits(session, dict.fromkeys(range(201), ()))
        assert await session.scalar(select(func.count()).select_from(Creator)) == 0


@pytest.mark.parametrize("with_generic", [False, True])
async def test_legacy_writer_never_claims_same_name_as_foreign_identity(
    identity_probe_db, with_generic
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        original = Creator(name="Shared name", comicvine_id=77)
        session.add(original)
        if with_generic:
            session.add(Creator(name="Shared name"))
        await session.flush()
        original_id = original.id
        try:
            new = await MetadataService._get_or_create_creator(
                session,
                name="Shared name",
                comicvine_id=88,
                comicvine_url=None,
            )
        except Exception as exc:
            pytest.fail(f"A name collision must not break the legacy writer: {type(exc).__name__}")
        assert new.id != original_id, "Equal names must not reassign an existing provider identity"
        assert original.comicvine_id == 77 and new.comicvine_id == 88
        same = await MetadataService._get_or_create_creator(
            session,
            name="Updated name",
            comicvine_id=88,
            comicvine_url=None,
        )
        assert same.id == new.id


async def test_legacy_nameless_identity_write_uses_only_generic_creator(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        original = Creator(name="Shared", comicvine_id=77)
        session.add(original)
        await session.flush()
        generic = await MetadataService._get_or_create_creator(
            session,
            name="Shared",
            comicvine_id=None,
            comicvine_url=None,
        )
        assert generic.id != original.id and generic.comicvine_id is None
