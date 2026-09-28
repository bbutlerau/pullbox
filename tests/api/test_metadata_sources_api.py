"""Source configuration/search uses real auth and releases DB reads before I/O."""

import os
import sys

from sqlalchemy import select

from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.services.auth_service import SESSION_COOKIE_NAME, AuthService
from tests.unit.test_catalog_reader import installed_reader

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


def csrf(client):
    return {
        "X-CSRF-Token": AuthService.get_csrf_token_from_session(
            client.cookies.get(SESSION_COOKIE_NAME)
        )
    }


def policy(**updates):
    return {"revision": 0, "enabled": True, "priority": 5, **updates}


async def test_source_policy_roundtrip_masks_secret_and_rejects_stale_revision(
    authenticated_client, sec_db
):
    url = "/api/v1/metadata/sources/metron_api"
    saved = await authenticated_client.put(
        url, json=policy(credential="synthetic-source-token"), headers=csrf(authenticated_client)
    )
    assert saved.status_code == 200
    assert saved.json()["credential_configured"] and saved.json()["revision"] == 1
    assert "synthetic-source-token" not in saved.text
    read = await authenticated_client.get("/api/v1/metadata/sources")
    assert read.status_code == 200 and len(read.json()) == 5
    assert "credential_secret" not in read.text and "synthetic-source-token" not in read.text
    stale = await authenticated_client.put(
        url, json=policy(priority=1), headers=csrf(authenticated_client)
    )
    assert stale.status_code == 409
    async with sec_db() as session:
        stored = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "metron_api")
        )
        assert stored.credential_secret.startswith("enc:") and stored.priority == 5


async def test_source_settings_require_operator_and_csrf(
    authenticated_client, unauthenticated_client, sec_api_key
):
    url = "/api/v1/metadata/sources/metron_api"
    assert (await unauthenticated_client.get("/api/v1/metadata/sources")).status_code in {401, 403}
    assert (
        await unauthenticated_client.put(url, json=policy(), headers={"X-API-Key": sec_api_key})
    ).status_code in {401, 403}
    assert (await authenticated_client.put(url, json=policy())).status_code == 403
    assert (await authenticated_client.post(url + "/test")).status_code == 403


async def test_feature_flag_blocks_activation_and_connection_test(authenticated_client):
    url = "/api/v1/metadata/sources/gcd_api_v2"
    response = await authenticated_client.put(
        url, json=policy(), headers=csrf(authenticated_client)
    )
    assert response.status_code == 400
    response = await authenticated_client.post(url + "/test", headers=csrf(authenticated_client))
    assert response.status_code == 200
    assert response.json()["outcome"]["status"] == "feature_disabled"


async def test_live_catalog_search_api_keeps_per_source_outcomes(
    authenticated_client, monkeypatch, tmp_path
):
    from pullbox.providers.metadata import sources

    monkeypatch.setattr(sources, "get_catalog_reader", lambda: installed_reader_cached)
    installed_reader_cached = installed_reader(tmp_path)
    result = await authenticated_client.post(
        "/api/v1/metadata/search",
        json={"query": "Dark Knight", "sources": ["comicvine_local", "metron_api"]},
        headers=csrf(authenticated_client),
    )
    assert result.status_code == 200
    assert result.json()["results"][0]["external_id"] == "10"
    assert [item["status"] for item in result.json()["sources"]] == ["ok", "disabled"]
    assert result.json()["sources"][0]["total"] is None


async def test_search_releases_transaction_before_provider_work(authenticated_client, monkeypatch):
    from pullbox.api.v1 import metadata_sources as routes
    from pullbox.schemas.metadata_sources import SeriesDiscoveryRead

    original = routes.load_source_runtime
    held = []

    async def load(session, **kwargs):
        held.append(session)
        return await original(session, **kwargs)

    async def discover(self, query):
        assert held and not held[0].in_transaction()
        return SeriesDiscoveryRead(results=[], sources=[])

    monkeypatch.setattr(routes, "load_source_runtime", load)
    monkeypatch.setattr(routes.MetadataSourceRegistry, "discover", discover)
    response = await authenticated_client.post(
        "/api/v1/metadata/search", json={"query": "test"}, headers=csrf(authenticated_client)
    )
    assert response.status_code == 200


async def test_health_persists_only_safe_status(
    authenticated_client, monkeypatch, tmp_path, sec_db
):
    from pullbox.providers.metadata import sources

    reader = installed_reader(tmp_path)
    monkeypatch.setattr(sources, "get_catalog_reader", lambda: reader)
    url = "/api/v1/metadata/sources/comicvine_local"
    assert (
        await authenticated_client.put(url, json=policy(), headers=csrf(authenticated_client))
    ).status_code == 200
    result = await authenticated_client.post(url + "/test", headers=csrf(authenticated_client))
    assert result.status_code == 200 and result.json()["recorded"] is True
    assert result.json()["outcome"]["status"] == "ok"
    async with sec_db() as session:
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "comicvine_local")
        )
        assert row.last_status == "ok" and row.last_tested_at and row.last_success_at


async def test_search_requires_auth_and_csrf_and_rejects_invalid_paging(
    authenticated_client, unauthenticated_client
):
    url = "/api/v1/metadata/search"
    assert (await unauthenticated_client.post(url, json={"query": "x"})).status_code in {401, 403}
    assert (await authenticated_client.post(url, json={"query": "x"})).status_code == 403
    response = await authenticated_client.post(
        url,
        json={"query": "x", "offsets": {"comicvine_api": 1}},
        headers=csrf(authenticated_client),
    )
    assert response.status_code == 422


async def test_priority_api_is_atomic_and_revision_checked(authenticated_client):
    sources = (await authenticated_client.get("/api/v1/metadata/sources")).json()
    payload = {
        "order": list(reversed([item["source"] for item in sources])),
        "revisions": {item["source"]: item["revision"] for item in sources},
        "domain_orders": {},
    }
    url = "/api/v1/metadata/priorities"
    result = await authenticated_client.put(url, json=payload, headers=csrf(authenticated_client))
    assert result.status_code == 200
    assert [item["source"] for item in result.json()] == payload["order"]
    stale = await authenticated_client.put(url, json=payload, headers=csrf(authenticated_client))
    assert stale.status_code == 409
    assert (await authenticated_client.get("/api/v1/metadata/sources")).json() == result.json()


async def test_priority_api_requires_operator_and_csrf(
    authenticated_client, unauthenticated_client, sec_api_key
):
    url = "/api/v1/metadata/priorities"
    assert (await authenticated_client.put(url, json={})).status_code == 403
    assert (
        await unauthenticated_client.put(url, json={}, headers={"X-API-Key": sec_api_key})
    ).status_code in {401, 403}


async def test_existing_key_save_invalidates_source_revision_and_old_health(
    authenticated_client, sec_db
):
    from datetime import UTC, datetime

    from pullbox.core.metadata_identity import MetadataSource
    from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus
    from pullbox.services.metadata_sources import record_source_health

    url = "/api/v1/metadata/sources/comicvine_api"
    response = await authenticated_client.put(
        url,
        json=policy(enabled=False, priority=13, domain_priorities={"issues": 1}),
        headers=csrf(authenticated_client),
    )
    assert response.status_code == 200
    revision = response.json()["revision"]
    now = datetime.now(UTC)
    outcome = SourceOutcome(source=MetadataSource.COMICVINE_API, status=SourceStatus.OK)
    async with sec_db.begin() as session:
        assert await record_source_health(
            session, MetadataSource.COMICVINE_API, revision, outcome, now
        )
    saved = await authenticated_client.post(
        "/api/v1/config/comicvine/save",
        json={"api_key": "new-synthetic-key-for-revision"},
        headers=csrf(authenticated_client),
    )
    assert saved.status_code == 200 and saved.json()["saved"]
    result = (await authenticated_client.get("/api/v1/metadata/sources")).json()
    source = next(item for item in result if item["source"] == "comicvine_api")
    assert source["revision"] == revision + 1
    assert not source["enabled"] and source["priority"] == 13
    assert source["domain_priorities"] == {"issues": 1}
    assert source["last_status"] is None and source["last_success_at"] is None
    async with sec_db.begin() as session:
        assert not await record_source_health(
            session, MetadataSource.COMICVINE_API, revision, outcome, now
        )


async def test_metron_configuration_health_and_discovery_use_real_adapter(
    authenticated_client, monkeypatch, sec_db, tmp_path
):
    import httpx

    from pullbox.core.provider_cooldown import ProviderCooldown
    from pullbox.providers.metadata import sources
    from tests.unit.test_metron_source import envelope, series_row

    token = "synthetic-api-metron-token"
    requests = []
    fail = False

    def handle(request):
        requests.append(request)
        assert request.headers["authorization"] == f"Bearer {token}"
        if fail:
            return httpx.Response(401, text=token)
        return httpx.Response(200, json=envelope([series_row()]))

    adapter = sources.MetronSource
    monkeypatch.setattr(
        sources,
        "MetronSource",
        lambda credential: adapter(
            credential,
            transport=httpx.MockTransport(handle),
            cooldown=ProviderCooldown(),
            minimum_interval=0,
        ),
    )
    reader = installed_reader(tmp_path)
    monkeypatch.setattr(sources, "get_catalog_reader", lambda: reader)
    url = "/api/v1/metadata/sources/metron_api"
    saved = await authenticated_client.put(
        url, json=policy(credential=token), headers=csrf(authenticated_client)
    )
    assert saved.status_code == 200 and saved.json()["credential_configured"]
    health = await authenticated_client.post(url + "/test", headers=csrf(authenticated_client))
    assert health.status_code == 200 and health.json()["outcome"]["status"] == "ok"
    async with sec_db() as session:
        row = await session.scalar(
            select(MetadataSourceConfig).where(MetadataSourceConfig.source == "metron_api")
        )
        assert row.last_status == "ok" and row.last_success_at
    result = await authenticated_client.post(
        "/api/v1/metadata/search",
        json={"query": "Fixture", "sources": ["metron_api"]},
        headers=csrf(authenticated_client),
    )
    assert result.status_code == 200
    assert result.json()["results"][0]["external_id"] == "1"
    assert result.json()["results"][0]["cross_identities"][0]["external_id"] == "9001"
    assert len(requests) == 2
    fail = True
    partial = await authenticated_client.post(
        "/api/v1/metadata/search",
        json={"query": "Dark Knight", "sources": ["comicvine_local", "metron_api"]},
        headers=csrf(authenticated_client),
    )
    assert partial.status_code == 200
    assert partial.json()["results"][0]["source"] == "comicvine_local"
    assert {row["source"]: row["status"] for row in partial.json()["sources"]} == {
        "comicvine_local": "ok",
        "metron_api": "authentication_failed",
    }
    assert token not in saved.text + health.text + result.text + partial.text
