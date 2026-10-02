"""Durable source response reuse never stands in for identity verification."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, update

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models.provider_cache import MetadataProviderCacheEntry as Entry
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    ProviderSeriesRead,
    SourceCapability,
    SourceStatus,
)
from pullbox.services.metadata_read_cache import MetadataReadCache

MODIFIED = "Mon, 28 Sep 2026 10:00:00 GMT"


def response(source=Source.METRON_API, identifier="42", title="Remote series"):
    return MetadataFetch[ProviderSeriesRead](
        status=SourceStatus.OK,
        validator=MODIFIED,
        data=ProviderSeriesRead(
            source=source,
            identity_namespace=source.identity_namespace,
            external_id=identifier,
            title=title,
        ),
    )


def read(cache, loader, *, source=Source.METRON_API, revision=1, page=1, revalidate=False):
    def validate(row):
        assert row.source is source and row.external_id == "42"
        assert row.title

    return cache.get(
        source,
        revision,
        SourceCapability.SERIES_DETAILS,
        "42",
        page,
        MetadataFetch[ProviderSeriesRead],
        loader,
        validate,
        conditional=True,
        revalidate=revalidate,
    )


async def test_restart_reuses_snapshot_then_revalidates_outside_transactions(identity_probe_db):
    engine, factory, _ = identity_probe_db
    now = datetime.now(UTC)
    calls = []

    async def load(validator):
        assert engine.pool.checkedout() == 0
        calls.append(validator)
        if validator:
            return MetadataFetch(status=SourceStatus.NOT_MODIFIED, validator=validator)
        return response()

    first = await read(MetadataReadCache(factory, now=lambda: now), load)
    first.data.title = "Local mutation"
    second = await read(MetadataReadCache(factory, now=lambda: now), load)
    assert calls == [None]
    assert second.data.title == "Remote series"
    now += timedelta(minutes=6)
    third = await read(MetadataReadCache(factory, now=lambda: now), load)
    assert third.status is SourceStatus.OK and third.data == second.data
    assert calls == [None, MODIFIED]
    async with factory() as session:
        row = await session.scalar(select(Entry))
        assert row.fetched_at == now
        assert row.provider_name == "metron_api"


async def test_revision_source_and_page_are_separate_cache_keys(identity_probe_db):
    _, factory, _ = identity_probe_db
    cache = MetadataReadCache(factory)
    calls = []
    for source, revision, page in [
        (Source.METRON_API, 1, 1),
        (Source.METRON_API, 2, 1),
        (Source.METRON_API, 2, 2),
        (Source.COMICVINE_API, 2, 2),
    ]:

        async def load(validator, source=source):
            calls.append(validator)
            return response(source)

        await read(cache, load, source=source, revision=revision, page=page)
        await read(cache, load, source=source, revision=revision, page=page)
    assert calls == [None] * 4
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Entry)) == 4


async def test_explicit_refresh_checks_upstream_and_outage_never_becomes_cached_success(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    cache = MetadataReadCache(factory)
    calls = []
    outcome = response()

    async def load(validator):
        calls.append(validator)
        return outcome

    await read(cache, load)
    outcome = MetadataFetch(status=SourceStatus.RATE_LIMITED, retry_after_seconds=60)
    failed = await read(cache, load, revalidate=True)
    assert failed.status is SourceStatus.RATE_LIMITED and failed.data is None
    outcome = response(title="Updated")
    updated = await read(cache, load, revalidate=True)
    assert updated.data.title == "Updated"
    assert calls == [None, MODIFIED, MODIFIED]


async def test_missing_is_short_lived_and_not_modified_without_snapshot_is_rejected(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    now = datetime.now(UTC)
    cache = MetadataReadCache(factory, now=lambda: now)
    calls = []
    outcome = MetadataFetch(status=SourceStatus.NOT_FOUND)

    async def load(validator):
        calls.append(validator)
        return outcome

    await read(cache, load)
    await read(cache, load)
    assert len(calls) == 1
    now += timedelta(seconds=31)
    outcome = MetadataFetch(status=SourceStatus.NOT_MODIFIED, validator=MODIFIED)
    result = await read(cache, load)
    assert result.status is SourceStatus.INCOMPATIBLE_RESPONSE
    assert calls == [None, None]


async def test_corrupt_cache_is_ignored_not_given_to_consumer(identity_probe_db):
    _, factory, _ = identity_probe_db
    cache = MetadataReadCache(factory)
    calls = []

    async def load(validator):
        calls.append(validator)
        return response()

    await read(cache, load)
    async with factory.begin() as session:
        await session.execute(
            update(Entry).values(payload={"status": "ok", "data": {"external_id": "99"}})
        )
    assert (await read(cache, load)).data.external_id == "42"
    assert calls == [None, None]


async def test_concurrent_requests_share_fetch_but_one_cancel_does_not_cancel_other(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    started, finish = asyncio.Event(), asyncio.Event()
    calls = []

    async def load(validator):
        calls.append(validator)
        started.set()
        await finish.wait()
        return response()

    first = asyncio.create_task(read(MetadataReadCache(factory), load))
    second = asyncio.create_task(read(MetadataReadCache(factory), load))
    try:
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.sleep(0.05)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        finish.set()
        assert (await second).data.title == "Remote series"
        assert calls == [None]
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)


async def test_last_cancel_drains_request_and_does_not_store(identity_probe_db):
    _, factory, _ = identity_probe_db
    started, stopped = asyncio.Event(), asyncio.Event()

    async def load(validator):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    running = asyncio.create_task(read(MetadataReadCache(factory), load))
    try:
        await asyncio.wait_for(started.wait(), 2)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert stopped.is_set()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Entry)) == 0
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_storage_limits_do_not_evict_legacy_cache_or_save_oversized_entries(
    identity_probe_db, monkeypatch
):
    from pullbox.services import metadata_read_cache as cache_module

    _, factory, _ = identity_probe_db
    monkeypatch.setattr(cache_module, "_MAX_ENTRIES", 2)
    now = datetime.now(UTC)
    async with factory.begin() as session:
        session.add(
            Entry(
                provider_name="comicvine",
                cache_kind="get_series",
                cache_key="legacy",
                request={},
                payload={},
                fetched_at=now,
                expires_at=now + timedelta(days=1),
            )
        )
    cache = MetadataReadCache(factory, now=lambda: now)

    async def load(validator):
        return response()

    for revision in range(1, 5):
        await read(cache, load, revision=revision)
        now += timedelta(seconds=1)
    async with factory() as session:
        entries = list(await session.scalars(select(Entry)))
        assert len(entries) == 3
        assert any(entry.cache_key == "legacy" for entry in entries)
        assert sorted(entry.request["revision"] for entry in entries if entry.request) == [3, 4]

    async def huge(validator):
        return response(title="x" * (257 * 1024))

    assert (await read(cache, huge, revision=5)).status is SourceStatus.OK
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Entry)) == 3


async def test_registry_cache_is_inert_for_disabled_source_and_explicit_validator_stays_raw(
    identity_probe_db,
):
    from pydantic import SecretStr

    from pullbox.services.metadata_discovery import MetadataSourceRegistry
    from pullbox.services.metadata_sources import SourceRuntime
    from tests.unit.test_metadata_discovery import Adapter, registration, runtime

    _, factory, _ = identity_probe_db
    calls = []

    class Detail(Adapter):
        async def series(self, identifier, *, validator=None):
            calls.append(validator)
            if validator:
                return MetadataFetch(status=SourceStatus.NOT_MODIFIED, validator=validator)
            return response()

    adapter = Detail(Source.METRON_API)
    policy = runtime(Source.METRON_API, revision=1).policy
    current = SourceRuntime(policy, SecretStr("test-source-cache-token"))
    registry = MetadataSourceRegistry(
        [current],
        read_cache=MetadataReadCache(factory),
        factories={Source.METRON_API: registration(adapter, capabilities=list(SourceCapability))},
    )
    await registry.series(Source.METRON_API, "42")
    await registry.series(Source.METRON_API, "42")
    assert calls == [None]
    policy.enabled = False
    assert (await registry.series(Source.METRON_API, "42")).status is SourceStatus.DISABLED
    assert calls == [None]
    policy.enabled = True
    raw = await registry.series(Source.METRON_API, "42", validator=MODIFIED)
    assert raw.status is SourceStatus.NOT_MODIFIED and raw.data is None
    assert calls == [None, MODIFIED]
    assert adapter.closed == 2


async def test_cache_wait_is_part_of_registry_deadline(identity_probe_db, monkeypatch):
    from pydantic import SecretStr

    from pullbox.services.metadata_discovery import MetadataSourceRegistry
    from pullbox.services.metadata_sources import SourceRuntime
    from tests.unit.test_metadata_discovery import Adapter, registration, runtime

    _, factory, _ = identity_probe_db
    cache = MetadataReadCache(factory)
    stopped = asyncio.Event()

    async def slow(*args):
        try:
            await asyncio.sleep(0.3)
            return None
        finally:
            stopped.set()

    monkeypatch.setattr(cache, "_load", slow)
    adapter = Adapter(Source.METRON_API)
    registry = MetadataSourceRegistry(
        [
            SourceRuntime(
                runtime(Source.METRON_API, revision=1).policy, SecretStr("test-cache-deadline")
            )
        ],
        factories={Source.METRON_API: registration(adapter, capabilities=list(SourceCapability))},
        read_cache=cache,
        total_timeout=0.03,
    )
    started = asyncio.get_running_loop().time()
    result = await registry.series(Source.METRON_API, "42")
    assert result.status is SourceStatus.TIMEOUT
    assert asyncio.get_running_loop().time() - started < 0.2
    assert stopped.is_set()
    assert adapter.closed == 0
