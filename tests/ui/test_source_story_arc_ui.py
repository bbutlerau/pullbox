"""Real source registry and catalog commands behind the Story Arc browser flow."""

import json
import re
from html import unescape
from urllib.parse import urlencode

from sqlalchemy import func, select, update

from pullbox.models import StoryArc
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import IssueStoryArc
from tests.api.test_metadata_sources_api import csrf, policy
from tests.api.test_source_arc_catalog_api import arc_command_setup as _arc_command_setup
from tests.unit.test_metron_source import issue_row

pytest_plugins = ["conftest_security"]
arc_command_setup = _arc_command_setup
PREVIEW = "/story-arcs/catalog/metron_api/4"


def seed(html):
    match = re.search(r"<script[^>]*data-preview-data>(.*?)</script>", html, re.S)
    assert match, html[:200]
    return json.loads(match[1])


def form(fixture, data):
    return [
        ("source_revision", str(data["sourceRevision"])),
        ("fingerprint", data["fingerprint"]),
        ("file_defaults_fingerprint", data["fileDefaultsFingerprint"]),
        ("library_root_id", str(fixture["root_id"])),
        ("issue_provider_ids", "101"),
        ("issue_provider_ids", "100"),
        ("reading_orders", "1"),
        ("reading_orders", "2"),
        ("skipped_issue_provider_ids", "100"),
    ]


async def post(client, path, values):
    return await client.post(
        path,
        content=urlencode(values),
        headers={**csrf(client), "Content-Type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )


async def test_native_search_add_refresh_uses_existing_review_and_saver(
    authenticated_client, arc_command_setup, sec_db
):
    client, fixture = authenticated_client, arc_command_setup
    search = await client.get("/story-arcs/add?q=Native&source=metron_api")
    assert search.status_code == 200 and f'href="{PREVIEW}"' in search.text
    assert 'data-testid="story-arc-source-filter"' in search.text
    assert "Metron" in search.text and "Searching metadata sources" in search.text
    reviewed = await client.get(PREVIEW)
    assert reviewed.status_code == 200
    data = seed(reviewed.text)
    assert data["ready"] and data["sourceRevision"] == 1
    assert data["sourceLabel"] == "Metron"
    assert [r["issue_number"] for r in data["members"]] == ["13a", "50-x"]
    assert "data-order-controls" in reviewed.text and 'name="order_reviewed"' not in reviewed.text
    created = await post(client, PREVIEW, form(fixture, data))
    assert created.status_code == 303, created.text
    location = created.headers["location"]
    arc_id = int(location.split("/")[-1].split("?")[0])
    detail = await client.get(location)
    assert f'href="/story-arcs/{arc_id}/catalog-refresh"' in detail.text
    assert "Checking Metron for updates" in detail.text
    search = await client.get("/story-arcs/add?q=Native&source=metron_api")
    assert f'href="/story-arcs/{arc_id}"' in search.text
    assert f'href="{PREVIEW}"' not in search.text
    fixture["issues"] = [issue_row(101, "50-x"), issue_row(102, "50-o")]
    refresh_url = f"/story-arcs/{arc_id}/catalog-refresh"
    refresh = await client.get(refresh_url)
    assert refresh.status_code == 200 and 'name="source_revision" value="1"' in refresh.text
    assert "Compare Metron" in refresh.text and "Comic Vine" not in refresh.text
    fingerprint = re.search(r'name="fingerprint" value="([a-f0-9]+)"', refresh.text)[1]
    revision = re.search(r'name="expected_revision" value="(\d+)"', refresh.text)[1]
    updated = await post(
        client,
        refresh_url,
        {
            "fingerprint": fingerprint,
            "expected_revision": revision,
            "source_revision": "1",
            "confirm_refresh": "true",
        },
    )
    assert updated.status_code == 303 and "catalog-refreshed" in updated.headers["location"]
    async with sec_db() as session:
        rows = list(
            await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.sequence_number))
        )
        assert [r.source_issue_id for r in rows] == ["101", "100", "102"]
        assert rows[1].resolution_state.value == "skipped"
        assert rows[2].evidence["catalog_review_required"]
        assert (await session.get(StoryArc, arc_id)).comicvine_id is None
    assert not list(fixture["root"].iterdir())


async def test_search_pages_reuse_candidates_but_refresh_ownership(
    authenticated_client, arc_command_setup
):
    client, fixture = authenticated_client, arc_command_setup
    fixture["arc_rows"] = [{"id": i + 4, "name": f"Event {i}"} for i in range(103)]
    first = await client.get("/story-arcs/add?q=Event&source=metron_api")
    assert (
        first.status_code == 200 and first.text.count('data-testid="story-arc-result-card"') == 20
    )
    assert "source=metron_api" in unescape(first.text)
    assert [r.url.params["page"] for r in fixture["calls"]] == ["1", "2"]
    fixture["calls"].clear()
    second = await client.get(
        "/story-arcs/add?q=Event&source=metron_api&page=2", headers={"HX-Request": "true"}
    )
    assert second.status_code == 200 and "<!DOCTYPE" not in second.text
    assert "/catalog/metron_api/24" in second.text and not fixture["calls"]
    assert re.search(r'id="story-arc-add-header-metrics"[^>]*hx-swap-oob="outerHTML"', second.text)


async def test_source_policy_change_rejects_review_without_file_or_graph_writes(
    authenticated_client, arc_command_setup, sec_db
):
    client, fixture = authenticated_client, arc_command_setup
    reviewed = await client.get(PREVIEW)
    assert reviewed.status_code == 200
    async with sec_db.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(revision=2))
    fixture["calls"].clear()
    result = await post(client, PREVIEW, form(fixture, seed(reviewed.text)))
    assert result.status_code == 303 and "error=" in result.headers["location"]
    assert not fixture["calls"] and not list(fixture["root"].iterdir())
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0


async def test_search_keeps_native_results_when_other_source_unavailable(
    authenticated_client, arc_command_setup
):
    result = await authenticated_client.get("/story-arcs/add?q=Native")
    assert result.status_code == 200 and f'href="{PREVIEW}"' in result.text
    assert "ComicVine API" in result.text
    assert "needs credentials" in result.text
    assert "does not support series search" not in result.text


async def test_disabled_source_and_invalid_selection_never_execute(
    authenticated_client, arc_command_setup
):
    client, fixture = authenticated_client, arc_command_setup
    saved = await client.put(
        "/api/v1/metadata/sources/metron_api",
        json=policy(revision=1, enabled=False),
        headers=csrf(client),
    )
    assert saved.status_code == 200
    result = await client.get(PREVIEW)
    assert result.status_code == 200 and not seed(result.text)["ready"]
    assert not fixture["calls"]
    for path in ("/story-arcs/add?q=Event&source=locg", "/story-arcs/catalog/locg/4"):
        assert (await client.get(path)).status_code == 422


async def test_native_browser_commands_require_auth_and_csrf(
    authenticated_client, unauthenticated_client, arc_command_setup
):
    assert (await unauthenticated_client.get(PREVIEW, follow_redirects=False)).status_code in {
        303,
        401,
    }
    assert (await authenticated_client.post(PREVIEW, data={})).status_code == 403
    assert not arc_command_setup["calls"]


async def test_default_source_form_accepts_empty_source(authenticated_client, arc_command_setup):
    response = await authenticated_client.get("/story-arcs/add?q=Native&source=")
    assert response.status_code == 200
    assert f'href="{PREVIEW}"' in response.text
    assert arc_command_setup["calls"]
