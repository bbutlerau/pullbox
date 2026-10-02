"""Whole-order writes are atomic and never replace source credentials/options."""

import asyncio
from datetime import UTC, datetime

import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import select

from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.schemas.metadata_sources import SourcePolicyWrite, SourcePriorityWrite
from pullbox.services.metadata_sources import (
    SourceConfigurationConflictError,
    read_source_policies,
    save_source_policy,
    save_source_priorities,
)


def write(policies, **kwargs):
    return SourcePriorityWrite(
        order=list(reversed([item.source for item in policies])),
        revisions={item.source: item.revision for item in policies},
        **kwargs,
    )


async def test_order_roundtrip_preserves_secrets_config_and_health(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        await save_source_policy(
            session,
            MetadataSource.METRON_API,
            SourcePolicyWrite(
                revision=0, enabled=True, priority=25, credential=SecretStr("priority-test-token")
            ),
        )
        row = await session.scalar(select(MetadataSourceConfig))
        row.last_status = "ok"
        row.last_tested_at = row.last_success_at = datetime.now(UTC)
        secret = row.credential_secret
        tested = row.last_tested_at
    async with factory.begin() as session:
        policies = await read_source_policies(session)
        payload = write(policies, domain_orders={"core": list(MetadataSource)})
        result = await save_source_priorities(session, payload)
        assert [item.source for item in result] == payload.order
        assert all(item.revision == payload.revisions[item.source] + 1 for item in result)
        assert {item.source: item.domain_priorities["core"] for item in result} == {
            source: (index + 1) * 10 for index, source in enumerate(MetadataSource)
        }
    async with factory() as session:
        rows = {row.source: row for row in await session.scalars(select(MetadataSourceConfig))}
        assert rows["metron_api"].credential_secret == secret
        assert rows["metron_api"].last_tested_at == tested
        assert rows["metron_api"].last_status == "ok" and rows["metron_api"].enabled
        assert not rows["gcd_api_v2"].enabled
        assert [item.source for item in await read_source_policies(session)] == payload.order


async def test_stale_order_rolls_back_every_row_even_when_caller_commits(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        before = await read_source_policies(session)
        payload = write(before)
        payload.revisions[MetadataSource.METRON_API] = 8
        with pytest.raises(SourceConfigurationConflictError):
            await save_source_priorities(session, payload)
        assert list(await session.scalars(select(MetadataSourceConfig))) == []
    async with factory() as session:
        assert await read_source_policies(session) == before


async def test_priority_reset_and_caller_rollback(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        saved = await save_source_priorities(
            session,
            write(
                await read_source_policies(session), domain_orders={"issues": list(MetadataSource)}
            ),
        )
        assert all("issues" in item.domain_priorities for item in saved)
    async with factory() as session:
        cleared = await save_source_priorities(session, write(saved))
        assert all(not item.domain_priorities for item in cleared)
        await session.rollback()
    async with factory.begin() as session:
        assert await read_source_policies(session) == saved
        await save_source_priorities(session, write(saved))
    async with factory() as session:
        assert all(not item.domain_priorities for item in await read_source_policies(session))


async def test_concurrent_order_writes_have_only_one_winner(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory() as session:
        payload = write(await read_source_policies(session))

    async def save():
        try:
            async with factory.begin() as session:
                return await save_source_priorities(session, payload)
        except SourceConfigurationConflictError:
            return None

    results = await asyncio.wait_for(asyncio.gather(save(), save()), 15)
    assert results.count(None) == 1
    async with factory() as session:
        saved = await read_source_policies(session)
        assert [item.source for item in saved] == payload.order
        assert all(item.revision == 1 for item in saved)


@pytest.mark.parametrize(
    "mutation", ["duplicate", "missing_revision", "domain_duplicate", "artwork"]
)
def test_order_request_rejects_incomplete_or_unsupported_orders(mutation):
    payload = {
        "order": list(MetadataSource),
        "revisions": dict.fromkeys(MetadataSource, 0),
        "domain_orders": {},
    }
    if mutation == "duplicate":
        payload["order"][-1] = payload["order"][0]
    elif mutation == "missing_revision":
        payload["revisions"].pop(MetadataSource.GCD_LOCAL)
    elif mutation == "domain_duplicate":
        payload["domain_orders"] = {"core": [MetadataSource.COMICVINE_API] * 5}
    else:
        payload["domain_orders"] = {"artwork": list(MetadataSource)}
    with pytest.raises(ValidationError):
        SourcePriorityWrite.model_validate(payload)
