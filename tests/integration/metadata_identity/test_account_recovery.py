"""Explicit authentication recovery preserves holds and wakes deferred work safely."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from pullbox.models import MetadataSeriesRetry, Series
from pullbox.models.metadata_source_account import MetadataSourceAccount as Account
from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus
from pullbox.services.metadata_discovery import MetadataSourceError
from pullbox.services.metadata_series_retry import admit_retry, retry_candidates, settle_retry
from tests.integration.metadata_identity.test_source_account_admission import (  # noqa: F401
    accounts as account_setup,
)
from tests.integration.metadata_identity.test_source_account_admission import (
    configured_sources,  # noqa: F401
    reader,
)
from tests.unit.test_metadata_source_reads import ReadAdapter


@pytest.fixture(name="accounts")
async def recovery_accounts(account_setup):  # noqa: F811 - imported fixture
    return account_setup


async def held(factory):
    adapter = ReadAdapter(error=MetadataSourceError(SourceStatus.AUTHENTICATION_FAILED))
    registry = await reader(factory, adapter)
    async with factory.begin() as session:
        series = Series(title="Waiting series", sort_title="Waiting series")
        session.add(series)
        await session.flush()
        identifier = series.id
        admission = await admit_retry(
            session, "refresh_metadata", identifier, gcd_api_enabled=False, retry_only=False
        )
    result = await registry.series(adapter.source, "42")
    async with factory.begin() as session:
        await settle_retry(
            session,
            "refresh_metadata",
            identifier,
            admission,
            outcomes=(SourceOutcome(source=adapter.source, status=result.status),),
        )
    adapter.error = None
    return adapter, identifier


async def test_manual_probe_reopens_same_account_and_wakes_deferred_series_after_restart(accounts):
    engine, factory = accounts
    adapter, identifier = await held(factory)
    registry = await reader(factory, adapter)
    assert (await registry.check(adapter.source)).status is SourceStatus.AUTHENTICATION_FAILED
    assert (
        await registry.check(adapter.source, retry_authentication=True)
    ).status is SourceStatus.OK
    await engine.dispose()
    async with factory() as session:
        row = await session.scalar(select(MetadataSeriesRetry))
        assert row.status == "unavailable" and row.retry_at <= datetime.now(UTC)
        assert row.revision == 2
        runtime = list((await reader(factory, adapter)).runtime.values())
        assert await retry_candidates(session, "refresh_metadata", runtime, limit=5) == [identifier]
        admission = await admit_retry(
            session, "refresh_metadata", identifier, gcd_api_enabled=False, retry_only=True
        )
        assert not admission.held and adapter.source in admission.registry.runtime


async def test_only_one_manual_auth_probe_runs_and_cancellation_keeps_hold(accounts):
    _, factory = accounts
    adapter, _ = await held(factory)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def probe():
        calls.append(1)
        entered.set()
        await release.wait()

    adapter.check = probe
    registry = await reader(factory, adapter)
    task = asyncio.create_task(registry.check(adapter.source, retry_authentication=True))
    try:
        async with asyncio.timeout(3):
            await entered.wait()
        result = await (await reader(factory, adapter)).check(
            adapter.source, retry_authentication=True
        )
        assert result.status is SourceStatus.AUTHENTICATION_FAILED
        assert 0 < result.retry_after_seconds <= 45
        assert calls == [1]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        account = await session.scalar(select(Account))
        assert account.status == "authentication_failed" and account.lease_until is None
        assert (await session.scalar(select(MetadataSeriesRetry))).retry_at is None
    release.set()
    assert (
        await registry.check(adapter.source, retry_authentication=True)
    ).status is SourceStatus.OK


async def test_explicit_probe_does_not_bypass_a_timed_cooldown(accounts):
    _, factory = accounts
    adapter = ReadAdapter(error=MetadataSourceError(SourceStatus.RATE_LIMITED, 720))
    registry = await reader(factory, adapter)
    await registry.series(adapter.source, "42")

    async def forbidden():
        pytest.fail("Manual checks cannot bypass provider rate limits")

    adapter.check = forbidden
    result = await registry.check(adapter.source, retry_authentication=True)
    assert result.status is SourceStatus.RATE_LIMITED and 0 < result.retry_after_seconds <= 720


@pytest.mark.parametrize(
    "status", [SourceStatus.AUTHENTICATION_FAILED, SourceStatus.INCOMPATIBLE_RESPONSE]
)
async def test_failed_manual_check_cannot_wake_or_clear_auth_held_work(accounts, status):
    _, factory = accounts
    adapter, _ = await held(factory)
    adapter.error = MetadataSourceError(status)
    result = await (await reader(factory, adapter)).check(adapter.source, retry_authentication=True)
    assert result.status is status
    async with factory() as session:
        assert (await session.scalar(select(Account))).status == "authentication_failed"
        assert (await session.scalar(select(MetadataSeriesRetry))).retry_at is None


async def test_late_failure_settlement_does_not_strand_work_after_successful_probe(accounts):
    _, factory = accounts
    adapter, identifier = await held(factory)
    async with factory.begin() as session:
        other = Series(title="Late series", sort_title="Late series")
        session.add(other)
        await session.flush()
        identifier = other.id
        admission = await admit_retry(
            session, "refresh_metadata", identifier, gcd_api_enabled=False, retry_only=False
        )
    assert (
        await (await reader(factory, adapter)).check(adapter.source, retry_authentication=True)
    ).status is SourceStatus.OK
    async with factory.begin() as session:
        await settle_retry(
            session,
            "refresh_metadata",
            identifier,
            admission,
            outcomes=(
                SourceOutcome(source=adapter.source, status=SourceStatus.AUTHENTICATION_FAILED),
            ),
        )
    async with factory() as session:
        row = await session.scalar(
            select(MetadataSeriesRetry).where(MetadataSeriesRetry.series_id == identifier)
        )
        assert row.retry_at is not None and row.retry_at <= datetime.now(UTC), (
            "A stale failure must remain retryable after recovery"
        )


async def test_manual_probe_does_not_wake_unrelated_or_newer_retry_scopes(accounts):
    _, factory = accounts
    adapter, _ = await held(factory)
    async with factory.begin() as session:
        await session.execute(update(MetadataSeriesRetry).values(config_key="different-generation"))
    await (await reader(factory, adapter)).check(adapter.source, retry_authentication=True)
    async with factory() as session:
        row = await session.scalar(select(MetadataSeriesRetry))
        assert row.status == "authentication_failed" and row.retry_at is None


async def test_abandoned_auth_probe_expires_but_automatic_reads_stay_held(accounts):
    _, factory = accounts
    adapter, _ = await held(factory)
    runtime = (await reader(factory, adapter)).runtime[adapter.source]
    permit = await runtime.account_admission.admit(runtime, retry_authentication=True)
    assert permit.probe
    async with factory.begin() as session:
        await session.execute(
            update(Account).values(lease_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    registry = await reader(factory, adapter)
    assert (
        await registry.series(adapter.source, "42")
    ).status is SourceStatus.AUTHENTICATION_FAILED
    assert (
        await registry.check(adapter.source, retry_authentication=True)
    ).status is SourceStatus.OK


@pytest.mark.parametrize(
    "status", [SourceStatus.RATE_LIMITED, SourceStatus.TIMEOUT, SourceStatus.UNAVAILABLE]
)
async def test_transient_probe_failure_schedules_held_work_without_immediate_retry(
    accounts, status
):
    _, factory = accounts
    adapter, _ = await held(factory)
    adapter.error = MetadataSourceError(status, 720)
    assert (
        await (await reader(factory, adapter)).check(adapter.source, retry_authentication=True)
    ).status is status
    async with factory() as session:
        account = await session.scalar(select(Account))
        row = await session.scalar(select(MetadataSeriesRetry))
        assert row.status == status.value and row.retry_at == account.retry_at
        assert datetime.now(UTC) < row.retry_at <= datetime.now(UTC) + timedelta(seconds=720)


async def test_stale_manual_probe_cannot_wake_work_after_newer_failure(accounts):
    _, factory = accounts
    adapter, _ = await held(factory)
    runtime = (await reader(factory, adapter)).runtime[adapter.source]
    attempt = await runtime.account_admission.admit(runtime, retry_authentication=True)
    attempt.started = True
    attempt.outcome = SourceOutcome(source=adapter.source, status=SourceStatus.OK)
    async with factory.begin() as session:
        await session.execute(
            update(Account).values(revision=Account.revision + 1, lease_until=None)
        )
    await runtime.account_admission.finish(runtime, attempt)
    async with factory() as session:
        assert (await session.scalar(select(Account))).status == "authentication_failed"
        assert (await session.scalar(select(MetadataSeriesRetry))).retry_at is None


async def test_auth_probe_migration_roundtrip_preserves_holds_and_retry_work(accounts):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import MetaData

    from pullbox.models import Base
    from tests.integration.metadata_identity.test_production_migration import _revision

    engine, factory = accounts
    adapter, identifier = await held(factory)
    runtime = (await reader(factory, adapter)).runtime[adapter.source]
    permit = await runtime.account_admission.admit(runtime, retry_authentication=True)
    assert permit.probe
    async with engine.begin() as connection:

        def migrate(sync):
            revision = _revision("x5r6s7t8u901_allow_metadata_authentication_probes", sync)
            revision.downgrade()
            revision.upgrade()
            expected = MetaData()
            name = "metadata_source_accounts"
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
    async with factory() as session:
        row = await session.scalar(select(Account))
        assert row.status == "authentication_failed" and row.lease_until is None
        assert row.revision > permit.revision
        assert (await session.scalar(select(MetadataSeriesRetry))).series_id == identifier


async def test_status_reads_are_bounded_read_only_and_hide_replaced_account(accounts):
    from sqlalchemy import event

    from pullbox.core.encryption import encrypt_secret
    from pullbox.models.metadata_source import MetadataSourceConfig
    from pullbox.services.metadata_source_status import deferred_work, source_status

    engine, factory = accounts
    adapter, _ = await held(factory)
    statements = []

    def record(*args):
        statements.append(args[2])

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        async with factory() as session:
            result = await source_status(session, gcd_api_enabled=False)
            metron = next(item for item in result if item.source is adapter.source)
            assert metron.deferred_series == metron.deferred_work == 1
            assert metron.account.status is SourceStatus.AUTHENTICATION_FAILED
            await session.rollback()
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)
    assert len(statements) <= 10 and all(item.startswith("SELECT") for item in statements)
    async with factory.begin() as session:
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == adapter.source.value)
            .values(credential_secret=encrypt_secret("different-test-account"))
        )
    async with factory() as session:
        result = await source_status(session, gcd_api_enabled=False)
        assert next(item for item in result if item.source is adapter.source).account is None
        assert (
            await deferred_work(
                session, source=adapter.source, limit=10, offset=0, gcd_api_enabled=False
            )
        ).items[0].state == "ready"
    async with factory.begin() as session:
        await session.execute(
            update(MetadataSourceConfig)
            .where(MetadataSourceConfig.source == adapter.source.value)
            .values(enabled=False)
        )
    async with factory() as session:
        assert (
            await deferred_work(
                session, source=adapter.source, limit=10, offset=0, gcd_api_enabled=False
            )
        ).items[0].state == "source_disabled"


async def test_manual_probe_recovers_auth_retries_created_before_account_state(accounts):
    from sqlalchemy import delete

    _, factory = accounts
    adapter, _ = await held(factory)
    async with factory.begin() as session:
        await session.execute(delete(Account))
    result = await (await reader(factory, adapter)).check(adapter.source, retry_authentication=True)
    assert result.status is SourceStatus.OK
    async with factory() as session:
        row = await session.scalar(select(MetadataSeriesRetry))
        assert row.retry_at is not None, (
            "An explicit successful check must recover pre-account held work"
        )
