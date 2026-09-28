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
