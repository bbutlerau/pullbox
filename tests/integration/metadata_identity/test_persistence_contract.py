"""Prove the proposed ownership shape on real SQLite and PostgreSQL engines."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models.issue import Issue
from pullbox.models.series import Series
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity
from pullbox.services.metadata_identity_inventory import read_legacy_identity_page

if TYPE_CHECKING:
    from sqlalchemy import Insert, Table
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from tests.fixtures.metadata_identity_persistence import IdentityProbeDatabase


async def _seed(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[list[Series], list[Issue], list[StoryArc]]:
    async with factory() as session:
        series = [
            Series(title=f"Series {index}", sort_title=f"series {index}", comicvine_id=100 + index)
            for index in range(2)
        ]
        session.add_all(series)
        await session.flush()
        issues = [
            Issue(series_id=row.id, issue_number=1, comicvine_id=200 + index)
            for index, row in enumerate(series)
        ]
        arcs = [StoryArc(name=f"Arc {index}", comicvine_id=300 + index) for index in range(2)]
        session.add_all([*issues, *arcs])
        await session.commit()
        return series, issues, arcs


def _link(
    table: Table, target_id: int, namespace: str = "comicvine", external_id: str = "100"
) -> Insert:
    key = "series_id" if "series_id" in table.c else "issue_id"
    return insert(table).values(
        **{key: target_id}, identity_namespace=namespace, external_id=external_id
    )


@pytest.mark.parametrize("kind", ["series", "issue"])
async def test_one_target_can_hold_all_namespaces_without_provider_configuration(
    identity_probe_db: IdentityProbeDatabase, kind: str
) -> None:
    _engine, factory, probe = identity_probe_db
    series, issues, _arcs = await _seed(factory)
    target = (series if kind == "series" else issues)[0]
    table = getattr(probe, kind)
    async with factory.begin() as session:
        for namespace in ("comicvine", "metron", "gcd", "locg"):
            await session.execute(_link(table, target.id, namespace))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(table)) == 4
    assert {fk.target_fullname for fk in table.foreign_keys} == {
        f"{'series' if kind == 'series' else 'issues'}.id"
    }


@pytest.mark.parametrize("kind", ["series", "issue"])
@pytest.mark.parametrize(
    "collision", ["same_external_other_target", "same_target_other_external", "missing_target"]
)
async def test_database_rejects_invalid_ownership(
    identity_probe_db: IdentityProbeDatabase, kind: str, collision: str
) -> None:
    _engine, factory, probe = identity_probe_db
    series, issues, _arcs = await _seed(factory)
    targets = series if kind == "series" else issues
    table = getattr(probe, kind)
    async with factory.begin() as session:
        await session.execute(_link(table, targets[0].id))
    target_id, external_id = {
        "same_external_other_target": (targets[1].id, "100"),
        "same_target_other_external": (targets[0].id, "101"),
        "missing_target": (999999, "102"),
    }[collision]
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(_link(table, target_id, external_id=external_id))


async def test_parent_delete_cascades_through_series_and_issue_identities(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, probe = identity_probe_db
    series, issues, _arcs = await _seed(factory)
    async with factory.begin() as session:
        await session.execute(_link(probe.series, series[0].id))
        await session.execute(_link(probe.issue, issues[0].id))
        await session.execute(delete(Series).where(Series.id == series[0].id))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(probe.series)) == 0
        assert await session.scalar(select(func.count()).select_from(probe.issue)) == 0


async def test_legacy_and_proposed_identity_writes_share_one_transaction(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, probe = identity_probe_db
    series, _issues, _arcs = await _seed(factory)
    async with factory.begin() as session:
        await session.execute(_link(probe.series, series[0].id))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(
                update(Series).where(Series.id == series[1].id).values(comicvine_id=999)
            )
            await session.execute(_link(probe.series, series[1].id))
    async with factory() as session:
        assert (
            await session.scalar(select(Series.comicvine_id).where(Series.id == series[1].id))
            == 101
        )
        assert await session.scalar(select(func.count()).select_from(probe.series)) == 1


async def test_concurrent_external_ownership_has_one_winner(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, probe = identity_probe_db
    series, _issues, _arcs = await _seed(factory)
    ready = asyncio.Event()
    arrived = 0

    async def attach(target_id: int) -> str:
        nonlocal arrived
        arrived += 1
        if arrived == 2:
            ready.set()
        await ready.wait()
        try:
            async with factory.begin() as session:
                await session.execute(_link(probe.series, target_id))
        except IntegrityError:
            return "conflict"
        return "attached"

    results = await asyncio.wait_for(
        asyncio.gather(*(attach(row.id) for row in series)), timeout=10
    )
    assert sorted(results) == ["attached", "conflict"]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(probe.series)) == 1


async def test_arc_provider_uniqueness_does_not_collapse_import_scoped_identities(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, _probe = identity_probe_db
    _series, _issues, arcs = await _seed(factory)
    async with factory.begin() as session:
        session.add_all(
            [
                StoryArcExternalIdentity(
                    story_arc_id=arcs[0].id, source=source, namespace=namespace, external_id=value
                )
                for source, namespace, value in [
                    ("comicvine", "story_arc", "300"),
                    ("gcd", "story_arc", "400"),
                    ("mylar3", "source-a", "1"),
                    ("mylar3", "source-a", "2"),
                ]
            ]
        )
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            session.add(
                StoryArcExternalIdentity(
                    story_arc_id=arcs[0].id,
                    source="comicvine",
                    namespace="story_arc",
                    external_id="302",
                )
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArcExternalIdentity)) == 4


async def test_inventory_matches_legacy_columns_and_preserves_arc_disagreement(
    identity_probe_db: IdentityProbeDatabase,
) -> None:
    _engine, factory, _probe = identity_probe_db
    series, issues, arcs = await _seed(factory)
    async with factory.begin() as session:
        session.add(
            StoryArcExternalIdentity(
                story_arc_id=arcs[0].id,
                source="comicvine",
                namespace="story_arc",
                external_id="00399",
            )
        )
    async with factory() as session:
        for kind, targets in [
            (MetadataEntityKind.SERIES, series),
            (MetadataEntityKind.ISSUE, issues),
            (MetadataEntityKind.STORY_ARC, arcs),
        ]:
            page = await read_legacy_identity_page(session, kind)
            assert page.local_ids == tuple(row.id for row in targets)
            assert [item.identity.external_id for item in page.observations[:2]] == [
                str(row.comicvine_id) for row in targets
            ]
        assert len(page.disagreements) == 1
        assert {item.identity.external_id for item in page.disagreements[0].conflict.evidence} == {
            "300",
            "399",
        }
