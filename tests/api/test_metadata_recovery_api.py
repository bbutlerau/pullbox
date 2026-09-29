"""Operators can inspect durable work and deliberately retry saved authentication."""

import os
import sys
from datetime import UTC, datetime

import httpx
from sqlalchemy import select

from pullbox.core.metadata_identity import MetadataSource
from pullbox.models import MetadataSeriesRetry, Series
from pullbox.models.metadata_source_account import MetadataSourceAccount as Account
from pullbox.services.metadata_account_admission import account_key
from pullbox.services.metadata_series_retry import config_key
from pullbox.services.metadata_sources import load_source_runtime
from tests.api.test_metadata_sources_api import csrf, policy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


async def seed(client, factory):
    result = await client.put(
        "/api/v1/metadata/sources/metron_api",
        json=policy(credential="recovery-api-test-token"),
        headers=csrf(client),
    )
    assert result.status_code == 200
    async with factory.begin() as session:
        runtime = next(
            item
            for item in await load_source_runtime(session, gcd_api_enabled=False)
            if item.policy.source is MetadataSource.METRON_API
        )
        session.add(
            Account(
                source="metron_api",
                account_key=account_key(runtime),
                status="authentication_failed",
            )
        )
        for index in range(13):
            series = Series(title=f"Waiting {index:02}", sort_title=f"Waiting {index:02}")
            session.add(series)
            await session.flush()
            for task in ("refresh_metadata", "sync_new_issues"):
                session.add(
                    MetadataSeriesRetry(
                        task_id=task,
                        series_id=series.id,
                        source="metron_api",
                        config_key=config_key(runtime),
                        status="authentication_failed",
                    )
                )


async def test_status_exposes_current_hold_and_distinct_series_not_just_last_test(
    authenticated_client, sec_db
):
    await seed(authenticated_client, sec_db)
    result = await authenticated_client.get("/api/v1/metadata/sources")
    source = next(item for item in result.json() if item["source"] == "metron_api")
    assert source.get("account", {}).get("status") == "authentication_failed", (
        "Settings must show the live hold even without a manual health check"
    )
    assert source["deferred_series"] == 13 and source["deferred_work"] == 26
    assert "recovery-api-test-token" not in result.text and "account_key" not in result.text
    page = await authenticated_client.get("/settings?tab=metadata")
    assert '"deferred_series": 13' in page.text


async def test_deferred_work_is_paginated_and_read_only(authenticated_client, sec_db):
    await seed(authenticated_client, sec_db)
    url = "/api/v1/metadata/retries?source=metron_api&limit=10"
    first = await authenticated_client.get(url)
    assert first.status_code == 200
    data = first.json()
    assert data["total"] == 26 and data["has_more"] and len(data["items"]) == 10
    assert data["items"][0]["series_title"] == "Waiting 00"
    assert data["items"][0]["state"] == "authentication_required"
    second = await authenticated_client.get(url + "&offset=10")
    assert {row["id"] for row in data["items"]}.isdisjoint(
        row["id"] for row in second.json()["items"]
    )
    assert "config_key" not in first.text and "recovery-api-test-token" not in first.text
    assert (await authenticated_client.get(url.replace("limit=10", "limit=101"))).status_code == 422
    async with sec_db() as session:
        assert (await session.scalar(select(Account))).status == "authentication_failed"


async def test_retry_list_requires_interactive_operator(unauthenticated_client, sec_api_key):
    url = "/api/v1/metadata/retries"
    assert (await unauthenticated_client.get(url)).status_code in {401, 403}
    assert (
        await unauthenticated_client.get(url, headers={"X-API-Key": sec_api_key})
    ).status_code in {401, 403}


async def test_test_connection_explicitly_recovers_authentication_and_wakes_series(
    authenticated_client, sec_db, monkeypatch
):
    from pullbox.core.provider_cooldown import ProviderCooldown
    from pullbox.providers.metadata import sources
    from tests.unit.test_metron_source import envelope, series_row

    await seed(authenticated_client, sec_db)
    calls = []

    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=envelope([series_row()]))

    adapter = sources.MetronSource
    monkeypatch.setattr(
        sources,
        "MetronSource",
        lambda token: adapter(
            token,
            transport=httpx.MockTransport(handle),
            cooldown=ProviderCooldown(),
            minimum_interval=0,
        ),
    )
    response = await authenticated_client.post(
        "/api/v1/metadata/sources/metron_api/test", headers=csrf(authenticated_client)
    )
    assert response.status_code == 200 and response.json()["outcome"]["status"] == "ok"
    assert len(calls) == 1
    async with sec_db() as session:
        rows = list(await session.scalars(select(MetadataSeriesRetry)))
        assert len(rows) == 26 and all(row.retry_at <= datetime.now(UTC) for row in rows)
    status = await authenticated_client.get("/api/v1/metadata/retries?source=metron_api")
    assert all(row["state"] == "ready" for row in status.json()["items"])
