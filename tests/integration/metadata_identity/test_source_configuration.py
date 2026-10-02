"""Source policy storage, secret boundaries, and revision checks on both engines."""

import asyncio

import pytest
from pydantic import SecretStr
from sqlalchemy import select

from pullbox.core.comicvine_key import get_comicvine_api_key
from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.schemas.metadata_sources import SourcePolicyWrite
from pullbox.services.metadata_sources import (
    SourceConfigurationConflictError,
    read_source_policies,
    save_source_policy,
)


def policy(**kwargs):
    return SourcePolicyWrite(revision=0, enabled=True, priority=7, **kwargs)


async def test_source_policy_is_durable_and_secrets_are_never_read_back(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        saved = await save_source_policy(
            session,
            MetadataSource.METRON_API,
            policy(
                credential=SecretStr("synthetic-provider-token"), domain_priorities={"issues": 3}
            ),
        )
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "metron_api")
        )
        assert row is not None
        assert row.credential_secret.startswith("enc:")
        assert "synthetic-provider-token" not in row.credential_secret
        assert saved.revision == 1 and saved.credential_configured
        assert "synthetic-provider-token" not in saved.model_dump_json()
    async with factory() as session:
        result = {item.source: item for item in await read_source_policies(session)}
        assert result[MetadataSource.METRON_API].priority == 7
        assert result[MetadataSource.METRON_API].domain_priorities == {"issues": 3}


async def test_comicvine_configuration_reuses_the_existing_key_store(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        await save_source_policy(
            session, MetadataSource.COMICVINE_API, policy(credential=SecretStr("synthetic-cv-key"))
        )
        assert await get_comicvine_api_key(session) == "synthetic-cv-key"
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "comicvine_api")
        )
        assert row.credential_secret is None


async def test_reads_do_not_write_defaults_and_caller_controls_commit(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        result = await read_source_policies(session)
        assert len(result) == 5
        assert [item.source for item in result if item.enabled] == [
            MetadataSource.COMICVINE_LOCAL,
            MetadataSource.COMICVINE_API,
        ]
        assert list(await session.scalars(select(MetadataSourceConfig))) == []
        await save_source_policy(session, MetadataSource.METRON_API, policy())
        await session.rollback()
    async with factory() as session:
        assert list(await session.scalars(select(MetadataSourceConfig))) == []


@pytest.mark.parametrize(
    "source,body",
    [
        (MetadataSource.GCD_API_V2, policy()),
        (MetadataSource.COMICVINE_LOCAL, policy(credential=SecretStr("not-allowed"))),
        (MetadataSource.METRON_API, policy(credential=SecretStr("enc:not-plaintext"))),
        (MetadataSource.METRON_API, policy(credential=SecretStr("token"), clear_credential=True)),
        (MetadataSource.GCD_LOCAL, policy(domain_priorities={"artwork": 1})),
        (MetadataSource.METRON_API, policy(domain_priorities={"core": -1})),
        (MetadataSource.METRON_API, policy(settings={"database_path": "/unrelated/path"})),
    ],
)
async def test_invalid_source_policy_is_rejected_before_any_write(identity_probe_db, source, body):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        with pytest.raises(ValueError):
            await save_source_policy(session, source, body)
        assert list(await session.scalars(select(MetadataSourceConfig))) == []


async def test_concurrent_updates_cannot_overwrite_a_newer_policy(identity_probe_db):
    _, factory, _ = identity_probe_db

    async def save():
        try:
            async with factory.begin() as session:
                return await save_source_policy(session, MetadataSource.METRON_API, policy())
        except SourceConfigurationConflictError:
            return None

    results = await asyncio.wait_for(asyncio.gather(save(), save()), 15)
    assert results.count(None) == 1
    assert next(item for item in results if item is not None).revision == 1


async def test_health_receipt_cannot_overwrite_new_settings_or_newer_probe(identity_probe_db):
    from datetime import UTC, datetime, timedelta

    from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus
    from pullbox.services.metadata_sources import record_source_health

    _, factory, _ = identity_probe_db
    source = MetadataSource.METRON_API
    now = datetime.now(UTC)
    success = SourceOutcome(source=source, status=SourceStatus.OK)
    async with factory.begin() as session:
        saved = await save_source_policy(session, source, policy())
        assert await record_source_health(session, source, saved.revision, success, now)
        assert not await record_source_health(
            session, source, saved.revision, success, now - timedelta(seconds=1)
        )
    async with factory.begin() as session:
        await save_source_policy(
            session, source, SourcePolicyWrite(revision=1, enabled=False, priority=5)
        )
        assert not await record_source_health(
            session, source, 1, success, now + timedelta(seconds=1)
        )
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == source.value)
        )
        assert row.last_status is None and row.last_tested_at is None


async def test_secret_retention_clear_and_flagged_runtime_never_decrypts(
    identity_probe_db, monkeypatch
):
    from pullbox.schemas.metadata_sources import SourceStatus
    from pullbox.services import metadata_sources as module

    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        source = MetadataSource.METRON_API
        await save_source_policy(session, source, policy(credential=SecretStr("synthetic-token")))
        await save_source_policy(
            session, source, SourcePolicyWrite(revision=1, enabled=False, priority=5)
        )
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == source.value)
        )
        assert row.credential_secret.startswith("enc:")
        await save_source_policy(
            session,
            MetadataSource.GCD_API_V2,
            policy(credential=SecretStr("gated-token")),
            gcd_api_enabled=True,
        )

    def forbidden(_):
        raise AssertionError("Disabled sources must not decrypt credentials")

    monkeypatch.setattr(module, "decrypt_secret", forbidden)
    async with factory() as session:
        states = {
            item.policy.source: item
            for item in await module.load_source_runtime(session, gcd_api_enabled=False)
        }
        assert states[source].unavailable == SourceStatus.DISABLED
        assert states[MetadataSource.GCD_API_V2].unavailable == SourceStatus.FEATURE_DISABLED
    async with factory.begin() as session:
        result = await save_source_policy(
            session,
            source,
            SourcePolicyWrite(revision=2, enabled=False, priority=5, clear_credential=True),
        )
        assert not result.credential_configured


async def test_corrupt_configuration_is_isolated(identity_probe_db):
    from pullbox.schemas.metadata_sources import SourceStatus

    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        await save_source_policy(session, MetadataSource.METRON_API, policy())
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "metron_api")
        )
        row.settings = {"unknown": "not a valid setting"}
    async with factory() as session:
        results = {item.source: item for item in await read_source_policies(session)}
        assert (
            results[MetadataSource.METRON_API].configuration_status == SourceStatus.INVALID_CONFIG
        )
        assert results[MetadataSource.COMICVINE_LOCAL].enabled


async def test_actual_source_migration_roundtrip_preserves_canonical_credentials(identity_probe_db):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext
    from sqlalchemy import MetaData

    from tests.integration.metadata_identity.test_production_migration import _revision

    engine, factory, _ = identity_probe_db
    async with factory.begin() as session:
        await save_source_policy(
            session, MetadataSource.COMICVINE_API, policy(credential=SecretStr("canonical-token"))
        )
    async with engine.begin() as connection:

        def migrate(sync):
            checkpoints = _revision("u2o3p4q5r678_add_catalog_checkpoints", sync)
            checkpoints.downgrade()
            migration = _revision("s0m1n2o3p456_add_metadata_source_configuration", sync)
            migration.downgrade()
            migration.upgrade()
            expected = MetaData()
            MetadataSourceConfig.__table__.to_metadata(expected)
            context = MigrationContext.configure(
                sync,
                opts={
                    "include_object": lambda obj, name, type_, reflected, compare_to: (
                        (name == "metadata_source_configs") if type_ == "table" else True
                    )
                },
            )
            assert compare_metadata(context, expected) == []
            checkpoints.upgrade()

        await connection.run_sync(migrate)
    async with factory() as session:
        policies = await read_source_policies(session)
        assert len(list(await session.scalars(select(MetadataSourceConfig)))) == 5
        assert all(item.revision == 1 for item in policies)
        assert await get_comicvine_api_key(session) == "canonical-token"
