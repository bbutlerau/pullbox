"""A completed source catalog must never advance a different source's cursor."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, select, update

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Base, Issue, Series, SeriesCatalogCheckpoint
from pullbox.models.metadata_identity import SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.services.metadata_catalog_checkpoints import (
    CatalogCheckpointConflictError,
    load_catalog_checkpoint,
    read_catalog_checkpoints,
    save_full_catalog_checkpoint,
)
from pullbox.services.metadata_series_adoption import (
    SeriesAdoptionError,
    adopt_source_series_bundle,
    fetch_source_series_bundle,
)
from pullbox.services.metadata_series_refresh import SeriesRefreshError, refresh_series_from_sources
from tests.integration.metadata_identity.test_series_adoption import (  # noqa: F401
    bundle,
    configured_sources,
)
from tests.integration.metadata_identity.test_series_refresh import RefreshAdapter, refresh_registry

START = datetime(2026, 1, 1, tzinfo=UTC)


async def seed(factory):
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, bundle(crosswalk=True))
        return result.series.id


async def save(session, series_id, **kwargs):
    return await save_full_catalog_checkpoint(
        session,
        series_id,
        **{
            "source": Source.METRON_API,
            "source_revision": 1,
            "identity_revision": 1,
            "external_id": "42",
            "started_at": START,
            **kwargs,
        },
    )


async def test_checkpoints_survive_restart_and_keep_source_cursors_independent(identity_probe_db):
    engine, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        first = await save(session, local_id)
        assert first is not None, "Full catalogs need a durable source-specific checkpoint"
        await save(
            session,
            local_id,
            source=Source.COMICVINE_API,
            external_id="500",
            started_at=START + timedelta(hours=1),
        )
    await engine.dispose()
    async with factory() as session:
        metron = await load_catalog_checkpoint(session, local_id, Source.METRON_API)
        cv = await load_catalog_checkpoint(session, local_id, Source.COMICVINE_API)
        assert metron == first and metron.checked_at == metron.full_synced_at == START
        assert cv.checked_at == START + timedelta(hours=1)
        assert await load_catalog_checkpoint(session, local_id, Source.COMICVINE_LOCAL) is None
        assert len(await read_catalog_checkpoints(session, local_id)) == 2


async def test_checkpoint_write_leaves_commit_to_the_catalog_caller(identity_probe_db):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory() as session:
        assert await save(session, local_id) is not None
        (await session.get(Series, local_id)).title = "Uncommitted"
        await session.rollback()
    async with factory() as session:
        assert await read_catalog_checkpoints(session, local_id) == ()
        assert (await session.get(Series, local_id)).title == "Source-only series"


@pytest.mark.parametrize(
    "change", ["policy", "disabled", "identity", "identity_revision", "stale", "conflicted"]
)
async def test_changed_source_or_identity_requires_a_full_read_again(identity_probe_db, change):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        assert await save(session, local_id) is not None
    async with factory.begin() as session:
        if change in {"policy", "disabled"}:
            await session.execute(
                update(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == "metron_api")
                .values(**({"revision": 2} if change == "policy" else {"enabled": False}))
            )
        else:
            await session.execute(
                update(SeriesExternalIdentity)
                .where(SeriesExternalIdentity.identity_namespace == "metron")
                .values(
                    **(
                        {"external_id": "43", "revision": 2}
                        if change == "identity"
                        else {"revision": 2}
                        if change == "identity_revision"
                        else {"verification_state": change, "revision": 2}
                    )
                )
            )
    async with factory() as session:
        assert await load_catalog_checkpoint(session, local_id, Source.METRON_API) is None
        assert len(await read_catalog_checkpoints(session, local_id)) == 1
        with pytest.raises(CatalogCheckpointConflictError):
            await save(session, local_id, expected_revision=1)


@pytest.mark.parametrize("revision", [0, 1])
async def test_concurrent_full_catalog_completions_have_one_winner(identity_probe_db, revision):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    if revision:
        async with factory.begin() as session:
            assert await save(session, local_id) is not None

    async def write(hour):
        async with factory.begin() as session:
            try:
                return await save(
                    session,
                    local_id,
                    expected_revision=revision,
                    started_at=START + timedelta(hours=hour),
                )
            except CatalogCheckpointConflictError:
                return None

    results = await asyncio.wait_for(asyncio.gather(write(1), write(2)), 15)
    assert results.count(None) == 1, "Only one writer may advance a captured checkpoint"
    async with factory() as session:
        assert (
            await load_catalog_checkpoint(session, local_id, Source.METRON_API)
        ).revision == revision + 1


@pytest.mark.parametrize("parent", [Series, SeriesExternalIdentity, MetadataSourceConfig])
async def test_deleting_checkpoint_owner_cascades_progress_only(identity_probe_db, parent):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        assert await save(session, local_id) is not None
    async with factory.begin() as session:
        await session.execute(delete(parent))
    async with factory() as session:
        assert await read_catalog_checkpoints(session, local_id) == ()
        if parent is not Series:
            assert (await session.get(Series, local_id)).title == "Source-only series"


async def test_refresh_records_request_start_not_completion_and_rolls_back_atomically(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory() as session:
        before = datetime.now(UTC)
        adapter = RefreshAdapter(bundle())
        request_starts = []
        original_issues = adapter.issues

        async def capture_start(*args, **kwargs):
            request_starts.append(datetime.now(UTC))
            return await original_issues(*args, **kwargs)

        adapter.issues = capture_start
        await refresh_series_from_sources(session, local_id, registry=refresh_registry(adapter))
        checkpoint = await load_catalog_checkpoint(session, local_id, Source.METRON_API)
        assert checkpoint is not None, "Ordinary full refresh must seed the incremental cursor"
        assert before <= checkpoint.checked_at <= request_starts[0]
        assert checkpoint.full_synced_at == checkpoint.checked_at
        await session.rollback()
    async with factory() as session:
        assert await read_catalog_checkpoints(session, local_id) == ()


async def test_unknown_bundle_age_and_existing_add_do_not_invent_a_checkpoint(identity_probe_db):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        assert await read_catalog_checkpoints(session, local_id) == ()
        # A legacy server bundle carries no proven live-read boundary.
        result = await adopt_source_series_bundle(session, bundle(crosswalk=True))
        assert not result.created
        assert await read_catalog_checkpoints(session, local_id) == ()


@pytest.mark.parametrize("count", [0, 2])
async def test_live_fetch_seeds_only_the_new_native_series_checkpoint(identity_probe_db, count):
    _, factory, _ = identity_probe_db
    data = bundle(crosswalk=True, numbers=tuple(str(i + 1) for i in range(count)))
    fetched = await fetch_source_series_bundle(
        refresh_registry(RefreshAdapter(data)),
        Source.METRON_API,
        "42",
        source_revision=1,
    )
    assert fetched.catalog_started_at is not None
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, fetched)
        local_id = result.series.id
    async with factory.begin() as session:
        original = await load_catalog_checkpoint(session, local_id, Source.METRON_API)
        assert original is not None and original.checked_at == fetched.catalog_started_at
        assert await load_catalog_checkpoint(session, local_id, Source.COMICVINE_API) is None
        result = await adopt_source_series_bundle(
            session, replace(fetched, catalog_started_at=datetime.now(UTC))
        )
        assert not result.created
        assert await load_catalog_checkpoint(session, local_id, Source.METRON_API) == original


@pytest.mark.parametrize("revalidate", [False, True])
async def test_cached_catalog_does_not_invent_a_new_live_read_boundary(
    identity_probe_db, revalidate
):
    from pydantic import SecretStr

    from pullbox.services.metadata_read_cache import MetadataReadCache
    from pullbox.services.metadata_sources import SourceRuntime

    _, factory, _ = identity_probe_db
    adapter = RefreshAdapter(bundle())
    registry = refresh_registry(adapter)
    registry.runtime[Source.METRON_API] = SourceRuntime(
        registry.runtime[Source.METRON_API].policy, SecretStr("checkpoint-test-token")
    )
    registry.read_cache = MetadataReadCache(factory)
    first = await fetch_source_series_bundle(registry, Source.METRON_API, "42", source_revision=1)
    assert first.catalog_started_at is None
    calls = len(adapter.calls)
    registry.revalidate_reads = revalidate
    second = await fetch_source_series_bundle(registry, Source.METRON_API, "42", source_revision=1)
    assert (second.catalog_started_at is not None) is revalidate
    assert len(adapter.calls) == calls + (2 if revalidate else 0)


async def test_revalidated_catalog_checkpoint_cannot_join_an_older_cached_flight(identity_probe_db):
    from pydantic import SecretStr

    from pullbox.services.metadata_read_cache import MetadataReadCache
    from pullbox.services.metadata_sources import SourceRuntime

    _, factory, _ = identity_probe_db
    registry = refresh_registry(RefreshAdapter(bundle()))
    registry.runtime[Source.METRON_API] = SourceRuntime(
        registry.runtime[Source.METRON_API].policy, SecretStr("checkpoint-test-token")
    )
    cache = MetadataReadCache(factory)

    async def unexpected_cache(*args, **kwargs):
        pytest.fail("Checkpoint reads cannot reuse an earlier in-flight cache response")

    cache.get = unexpected_cache
    registry.read_cache = cache
    registry.revalidate_reads = True
    fetched = await fetch_source_series_bundle(registry, Source.METRON_API, "42", source_revision=1)
    assert fetched.catalog_started_at is not None
    assert registry.read_cache is cache, "The caller's ordinary read cache must remain configured"


@pytest.mark.parametrize(
    "invalid", ["naive", "future", "old", "revision", "identity_revision", "local_generation"]
)
async def test_invalid_checkpoint_cannot_overwrite_progress(identity_probe_db, invalid):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        original = await save(session, local_id)
    args = {"expected_revision": 1, "started_at": START + timedelta(hours=1)}
    args.update(
        {
            "naive": {"started_at": START.replace(tzinfo=None)},
            "future": {"started_at": datetime.now(UTC) + timedelta(days=1)},
            "old": {"started_at": START - timedelta(seconds=1)},
            "revision": {"expected_revision": 0},
            "identity_revision": {"identity_revision": 2},
            "local_generation": {
                "source": Source.COMICVINE_LOCAL,
                "external_id": "500",
                "expected_revision": 0,
            },
        }[invalid]
    )
    async with factory.begin() as session:
        with pytest.raises(CatalogCheckpointConflictError):
            await save(session, local_id, **args)
    async with factory() as session:
        assert await read_catalog_checkpoints(session, local_id) == (original,)


async def test_local_generation_and_policy_rebinding_require_a_new_full_sync(identity_probe_db):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        await save(
            session,
            local_id,
            source=Source.COMICVINE_LOCAL,
            external_id="500",
            source_updated_at=START,
        )
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == "comicvine_local")
            .values(revision=2)
        )
    async with factory.begin() as session:
        assert await load_catalog_checkpoint(session, local_id, Source.COMICVINE_LOCAL) is None
        newer = await save(
            session,
            local_id,
            source=Source.COMICVINE_LOCAL,
            source_revision=2,
            external_id="500",
            source_updated_at=START + timedelta(days=1),
            started_at=START + timedelta(days=2),
            expected_revision=1,
        )
        assert newer.revision == 2 and newer.source_updated_at == START + timedelta(days=1)
        assert newer.full_synced_at == START + timedelta(days=2)


async def test_checkpoint_failure_rolls_back_new_series_and_issues(identity_probe_db):
    _, factory, _ = identity_probe_db
    data = replace(bundle(), catalog_started_at=datetime.now(UTC) + timedelta(days=1))
    async with factory.begin() as session:
        with pytest.raises(SeriesAdoptionError):
            await adopt_source_series_bundle(session, data)
    async with factory() as session:
        assert list(await session.scalars(select(Series))) == []
        assert list(await session.scalars(select(Issue))) == []
        assert list(await session.scalars(select(SeriesCatalogCheckpoint))) == []


async def test_inflight_checkpoint_change_rejects_refresh_without_overwriting_newer_progress(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        await save(session, local_id)
    data = bundle(crosswalk=True)
    data.series.description = "Late description"
    adapter = RefreshAdapter(data, wait=asyncio.Event())
    async with factory() as session:
        task = asyncio.create_task(
            refresh_series_from_sources(session, local_id, registry=refresh_registry(adapter))
        )
        try:
            await asyncio.wait_for(adapter.started.wait(), 5)
            async with factory.begin() as editor:
                newer = await save(
                    editor, local_id, started_at=START + timedelta(days=1), expected_revision=1
                )
            adapter.wait.set()
            with pytest.raises(SeriesRefreshError, match="changed"):
                await task
            await session.commit()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        assert await load_catalog_checkpoint(session, local_id, Source.METRON_API) == newer
        assert (await session.get(Series, local_id)).description != "Late description"


async def test_checkpoint_migration_roundtrip_preserves_library_and_does_not_backfill(
    identity_probe_db,
):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import Column, Integer, MetaData, String, Table

    from tests.integration.metadata_identity.test_production_migration import _revision

    engine, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        await save(session, local_id)
    async with engine.begin() as connection:

        def migrate(sync):
            revision = _revision("u2o3p4q5r678_add_catalog_checkpoints", sync)
            revision.downgrade()
            revision.upgrade()
            expected = MetaData()
            for parent in ("series", "series_external_identities"):
                Table(parent, expected, Column("id", Integer, primary_key=True))
            Table("metadata_source_configs", expected, Column("source", String(30), unique=True))
            name = "series_catalog_checkpoints"
            Base.metadata.tables[name].to_metadata(expected)
            context = MigrationContext.configure(
                sync,
                opts={
                    "include_object": lambda obj, item, type_, reflected, compare_to: (
                        item == name if type_ == "table" else True
                    ),
                    "compare_server_default": True,
                },
            )
            assert compare_metadata(context, expected) == []

        await connection.run_sync(migrate)
    async with factory.begin() as session:
        assert (await session.get(Series, local_id)).title == "Source-only series"
        assert await read_catalog_checkpoints(session, local_id) == ()
        assert await save(session, local_id) is not None


async def test_non_utc_request_boundary_is_normalized_without_changing_the_instant(
    identity_probe_db,
):
    from datetime import timezone

    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    offset = START.astimezone(timezone(timedelta(hours=-7)))
    async with factory.begin() as session:
        saved = await save(session, local_id, started_at=offset)
        assert saved.checked_at == START and saved.checked_at.tzinfo is UTC
    async with factory() as session:
        saved = await load_catalog_checkpoint(session, local_id, Source.METRON_API)
        assert saved.checked_at == START and saved.checked_at.tzinfo is UTC


@pytest.mark.parametrize(
    "invalid",
    [
        "orphan_series",
        "orphan_source",
        "orphan_identity",
        "revision",
        "source_revision",
        "identity_revision",
        "empty_id",
        "clock_order",
        "local_generation",
        "duplicate",
    ],
)
async def test_database_rejects_invalid_checkpoint_rows(identity_probe_db, invalid):
    from sqlalchemy import insert
    from sqlalchemy.exc import IntegrityError

    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        original = await save(session, local_id)
    values = {
        "series_id": local_id,
        "source": "metron_api",
        "source_revision": 1,
        "identity_id": original.identity_id,
        "identity_revision": 1,
        "external_id": "42",
        "revision": 1,
        "checked_at": START,
        "full_synced_at": START,
    }
    values.update(
        {
            "orphan_series": {"series_id": local_id + 1000},
            "orphan_source": {"source": "not_configured"},
            "orphan_identity": {"identity_id": original.identity_id + 1000},
            "revision": {"revision": 0},
            "source_revision": {"source_revision": 0},
            "identity_revision": {"identity_revision": 0},
            "empty_id": {"external_id": ""},
            "clock_order": {"full_synced_at": START + timedelta(seconds=1)},
            "local_generation": {"source": "comicvine_local"},
            "duplicate": {},
        }[invalid]
    )
    if invalid != "duplicate":
        async with factory.begin() as session:
            await session.execute(delete(SeriesCatalogCheckpoint))
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            await session.execute(insert(SeriesCatalogCheckpoint).values(**values))


async def test_checkpoint_failure_after_refresh_apply_rolls_back_metadata_too(
    identity_probe_db, monkeypatch
):
    from pullbox.models.metadata_baseline import SeriesMetadataBaseline
    from pullbox.services import metadata_series_refresh as refresh_module

    _, factory, _ = identity_probe_db
    local_id = await seed(factory)
    async with factory.begin() as session:
        original = await save(session, local_id)
    data = bundle(crosswalk=True)
    data.series.description = "Must roll back"

    async def refuse(*args, **kwargs):
        raise CatalogCheckpointConflictError("Concurrent checkpoint")

    monkeypatch.setattr(refresh_module, "save_full_catalog_checkpoint", refuse)
    async with factory() as session:
        with pytest.raises(SeriesRefreshError):
            await refresh_series_from_sources(
                session, local_id, registry=refresh_registry(RefreshAdapter(data))
            )
        await session.commit()
    async with factory() as session:
        assert (await session.get(Series, local_id)).description != "Must roll back"
        assert await load_catalog_checkpoint(session, local_id, Source.METRON_API) == original
        assert (await session.scalar(select(SeriesMetadataBaseline))).revision == 1
