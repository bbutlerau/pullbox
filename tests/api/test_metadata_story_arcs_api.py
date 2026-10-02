"""Authenticated arc discovery and preview never mutate the local library."""

import httpx
import pytest
from sqlalchemy import func, select

from pullbox.core.provider_cooldown import ProviderCooldown
from pullbox.models.story_arc import StoryArc
from pullbox.providers.metadata import sources
from tests.api.test_metadata_sources_api import csrf, policy
from tests.unit.test_metron_source import envelope, issue_row

pytest_plugins = ["conftest_security"]


@pytest.fixture
def metron_transport(monkeypatch):
    calls = []

    def handle(request):
        calls.append(request)
        if request.url.path.endswith("issue_list/"):
            return httpx.Response(200, json=envelope([issue_row()]))
        row = {"id": 4, "name": "Metron-only arc", "cv_id": None, "gcd_id": None}
        return httpx.Response(200, json=envelope([row]) if request.url.path == "/api/arc/" else row)

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
    return calls


async def test_metron_arc_search_preview_and_members_without_comicvine_or_writes(
    authenticated_client, sec_db, metron_transport
):
    saved = await authenticated_client.put(
        "/api/v1/metadata/sources/metron_api",
        json=policy(credential="synthetic-arc-token"),
        headers=csrf(authenticated_client),
    )
    assert saved.status_code == 200
    async with sec_db() as session:
        count = await session.scalar(select(func.count()).select_from(StoryArc))
    found = await authenticated_client.post(
        "/api/v1/metadata/story-arcs/search",
        json={"query": "Arc", "sources": ["metron_api"]},
        headers=csrf(authenticated_client),
    )
    assert found.status_code == 200
    assert found.json()["results"][0]["source"] == "metron_api"
    preview = await authenticated_client.post(
        "/api/v1/metadata/story-arcs/preview",
        json={"source": "metron_api", "external_id": "0004"},
        headers=csrf(authenticated_client),
    )
    assert preview.status_code == 200
    data = preview.json()
    assert data["external_id"] == "4"
    assert data["arc"]["data"]["cross_identities"] == []
    assert data["issues"]["data"]["results"][0]["issue_number_text"] == "50-x"
    assert not data["issues"]["data"]["order_is_reading_order"]
    assert "synthetic-arc-token" not in found.text + preview.text
    assert [r.url.path for r in metron_transport] == [
        "/api/arc/",
        "/api/arc/4/",
        "/api/arc/4/issue_list/",
    ]
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == count


@pytest.mark.parametrize(
    "path,body",
    [
        ("search", {"query": "Shared"}),
        ("preview", {"source": "metron_api", "external_id": "4"}),
        ("issues", {"source": "metron_api", "external_id": "4", "source_revision": 0}),
    ],
)
async def test_arc_operations_require_auth_and_csrf(
    authenticated_client, unauthenticated_client, path, body
):
    url = f"/api/v1/metadata/story-arcs/{path}"
    assert (await unauthenticated_client.post(url, json=body)).status_code in {401, 403}
    assert (await authenticated_client.post(url, json=body)).status_code == 403


async def test_arc_preview_detects_midflight_source_change_without_holding_db_transaction(
    authenticated_client, sec_db, monkeypatch
):
    from pullbox.api.v1 import metadata_sources as routes
    from pullbox.schemas.metadata_sources import MetadataFetch, SourcePolicyWrite, SourceStatus
    from pullbox.services.metadata_sources import save_source_policy

    held = []
    original = routes.load_source_runtime

    async def load(session, **kwargs):
        held.append(session)
        return await original(session, **kwargs)

    async def detail(self, source, identifier, **kwargs):
        assert held and not held[0].in_transaction()
        async with sec_db.begin() as session:
            await save_source_policy(
                session, source, SourcePolicyWrite(revision=0, enabled=False, priority=1)
            )
        return MetadataFetch(status=SourceStatus.NOT_FOUND)

    monkeypatch.setattr(routes, "load_source_runtime", load)
    monkeypatch.setattr(routes.MetadataSourceRegistry, "story_arc", detail)
    response = await authenticated_client.post(
        "/api/v1/metadata/story-arcs/preview",
        json={"source": "comicvine_local", "external_id": "4"},
        headers=csrf(authenticated_client),
    )
    assert response.status_code == 409


async def test_stale_arc_member_page_does_not_start_provider_work(
    authenticated_client, monkeypatch
):
    from pullbox.api.v1 import metadata_sources as routes

    async def unexpected(*args, **kwargs):
        pytest.fail("Stale source revision must not call providers")

    monkeypatch.setattr(routes.MetadataSourceRegistry, "story_arc_issues", unexpected)
    result = await authenticated_client.post(
        "/api/v1/metadata/story-arcs/issues",
        json={"source": "comicvine_local", "external_id": "4", "source_revision": 99},
        headers=csrf(authenticated_client),
    )
    assert result.status_code == 409


@pytest.mark.parametrize(
    "body",
    [
        {"source": "metron_api", "external_id": "../4"},
        {"source": "metron_api", "external_id": True},
        {"source": "metron_api", "external_id": "0"},
        {"source": "comicvine_api", "external_id": "4050-4"},
        {"source": "comicvine_api", "external_id": str(2**63)},
        {"source": "locg", "external_id": "4"},
    ],
)
async def test_arc_preview_rejects_wrong_kind_and_invalid_source_identity(
    authenticated_client, body, metron_transport
):
    result = await authenticated_client.post(
        "/api/v1/metadata/story-arcs/preview", json=body, headers=csrf(authenticated_client)
    )
    assert result.status_code == 422
    assert not metron_transport


async def test_arc_preview_retains_feature_flag_and_local_capability_boundary(
    authenticated_client, metron_transport
):
    for source, status in (("gcd_api_v2", "feature_disabled"), ("comicvine_local", "unsupported")):
        result = await authenticated_client.post(
            "/api/v1/metadata/story-arcs/preview",
            json={"source": source, "external_id": "4"},
            headers=csrf(authenticated_client),
        )
        assert result.status_code == 200
        assert result.json()["arc"]["status"] == status
        assert result.json()["issues"]["status"] == "not_queried"
    assert not metron_transport
