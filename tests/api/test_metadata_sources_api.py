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
