"""Legacy browser URLs use the same source-bound commands as newly added arcs."""

import json
import re
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select, update

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models import StoryArc
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import IssueStoryArc
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.story_arc_catalog import StoryArcCatalogService
from tests.ui.test_story_arc_catalog_ui import _csrf, _root
from tests.ui.test_story_arc_catalog_ui import catalog_provider as catalog_provider

pytest_plugins = ["conftest_security"]


@pytest.fixture
async def migrated_catalog(sec_db, catalog_provider, monkeypatch):
    reads = AsyncMock(wraps=catalog_provider.get_story_arc)
    monkeypatch.setattr(catalog_provider, "get_story_arc", reads)
    return catalog_provider, reads


def preview_data(html):
    match = re.search(r"<script[^>]*data-preview-data>(.*?)</script>", html, re.S)
    assert match, html[:300]
    return json.loads(match[1])


async def test_legacy_preview_and_add_save_canonical_metadata(
    authenticated_client, sec_db, migrated_catalog, tmp_path
):
    client = authenticated_client
    root_id = await _root(sec_db, str(tmp_path))
    preview = await client.get("/story-arcs/catalog/42")
    data = preview_data(preview.text)
    assert data["sourceRevision"] == 1
    assert data["ready"] and data["sourceLabel"] == "ComicVine API"
    response = await client.post(
        "/story-arcs/catalog/42",
        data={
            "source_revision": data["sourceRevision"],
            "fingerprint": data["fingerprint"],
            "file_defaults_fingerprint": data["fileDefaultsFingerprint"],
            "library_root_id": root_id,
            "issue_provider_ids": ["102", "101"],
            "reading_orders": ["1", "2"],
            "skipped_issue_provider_ids": ["101"],
        },
        headers=_csrf(client),
        follow_redirects=False,
    )
    assert response.status_code == 303 and "catalog-added" in response.headers["location"]
    async with sec_db() as session:
        arc = await session.scalar(select(StoryArc))
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc.id)
        assert saved is not None and saved.snapshot.values.description == arc.description
        assert arc.diagnostics["provider_catalog"]["snapshot"]["source"] == "comicvine_api"
        members = list(
            await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.sequence_number))
        )
        assert [row.source_issue_id for row in members] == ["102", "101"]
        assert members[1].resolution_state.value == "skipped"
    assert not list(tmp_path.iterdir())


async def test_pre_v2_arc_refresh_uses_canonical_cascade_without_recreating_arc(
    authenticated_client, sec_db, migrated_catalog, tmp_path
):
    provider, _ = migrated_catalog
    root_id = await _root(sec_db, str(tmp_path))
    service = StoryArcCatalogService(provider)
    original = await service.preview("42")
    async with sec_db() as session:
        arc = await service.add(
            session,
            original,
            ordered_issue_provider_ids=["102", "101"],
            skipped_issue_provider_ids=["101"],
            library_root_id=root_id,
        )
        await session.commit()
        arc_id = arc.id
        arc.cover_url = "https://comicvine.gamespot.com/a/uploads/story-arcs/42.jpg"
        await session.commit()
    provider.metadata = replace(provider.metadata, issue_provider_ids=("101", "103"))
    url = f"/story-arcs/{arc_id}/catalog-refresh"
    page = await authenticated_client.get(url)
    assert 'name="source_revision" value="1"' in page.text
    assert re.search(rf'src="/api/v1/story-arcs/{arc_id}/cover\?v=[a-f0-9]{{12}}"', page.text)
    assert 'href="https://comicvine.gamespot.com/story-arc/4045-42/"' in page.text
    values = {
        key: re.search(rf'name="{key}" value="([^"]+)"', page.text)[1]
        for key in ("source_revision", "expected_revision", "fingerprint")
    }
    response = await authenticated_client.post(
        url,
        data={**values, "confirm_refresh": "true"},
        headers=_csrf(authenticated_client),
        follow_redirects=False,
    )
    assert response.status_code == 303 and "catalog-refreshed" in response.headers["location"]
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 1
        arc = await session.get(StoryArc, arc_id)
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
        assert saved is not None
        assert "metadata_refresh" in arc.diagnostics
        rows = list(
            await session.scalars(select(IssueStoryArc).order_by(IssueStoryArc.sequence_number))
        )
        assert [row.source_issue_id for row in rows] == ["102", "101", "103"]
        assert rows[1].resolution_state.value == "skipped"
        assert rows[2].evidence["catalog_review_required"]


async def test_legacy_preview_honors_disabled_source_without_calls(
    authenticated_client, sec_db, migrated_catalog
):
    _, reads = migrated_catalog
    async with sec_db.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False, revision=2))
    page = await authenticated_client.get("/story-arcs/catalog/42")
    assert page.status_code == 200 and not preview_data(page.text)["ready"]
    reads.assert_not_awaited()
    assert "Enable and save this source" in page.text


async def test_legacy_stale_form_cannot_bypass_source_revision(
    authenticated_client, sec_db, migrated_catalog, tmp_path
):
    _, reads = migrated_catalog
    root_id = await _root(sec_db, str(tmp_path))
    preview = await authenticated_client.get("/story-arcs/catalog/42")
    data = preview_data(preview.text)
    async with sec_db.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False, revision=2))
    reads.reset_mock()
    response = await authenticated_client.post(
        "/story-arcs/catalog/42",
        data={
            "fingerprint": data["fingerprint"],
            "source_revision": "1",
            "file_defaults_fingerprint": data["fileDefaultsFingerprint"],
            "library_root_id": root_id,
            "issue_provider_ids": ["101", "102"],
            "reading_orders": ["1", "2"],
        },
        headers=_csrf(authenticated_client),
        follow_redirects=False,
    )
    assert response.status_code == 303 and "error=" in response.headers["location"]
    reads.assert_not_awaited()
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0


async def test_pre_upgrade_form_requires_new_preview_without_provider_calls(
    authenticated_client, sec_db, migrated_catalog, tmp_path
):
    _, reads = migrated_catalog
    root_id = await _root(sec_db, str(tmp_path))
    response = await authenticated_client.post(
        "/story-arcs/catalog/42",
        data={
            "fingerprint": "a" * 64,
            "library_root_id": root_id,
            "issue_provider_ids": ["101", "102"],
            "reading_orders": ["1", "2"],
        },
        headers=_csrf(authenticated_client),
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/story-arcs/catalog/42?error=review"
    reads.assert_not_awaited()
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0


async def test_legacy_search_respects_disabled_policy_instead_of_calling_comicvine(
    authenticated_client, sec_db, migrated_catalog
):
    provider, _ = migrated_catalog
    async with sec_db.begin() as session:
        await session.execute(update(MetadataSourceConfig).values(enabled=False, revision=2))
    response = await authenticated_client.get("/story-arcs/catalog?q=event")
    assert response.status_code == 200
    assert "Story Arc search failed" in response.text
    assert "No Story Arc matches" not in response.text
    assert provider.searches == []
