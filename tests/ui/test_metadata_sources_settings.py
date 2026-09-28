"""Metadata settings expose masked policy without activating a provider."""

import json
import os
import re
import sys

from pullbox.core.encryption import encrypt_secret
from pullbox.models.metadata_source import MetadataSourceConfig

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


async def test_metadata_settings_has_seeded_shared_order_and_preserves_existing_controls(
    authenticated_client,
    sec_db,
):
    async with sec_db.begin() as session:
        session.add(
            MetadataSourceConfig(
                source="metron_api",
                enabled=False,
                priority=1,
                revision=2,
                credential_secret=encrypt_secret("do-not-render-provider-token"),
            )
        )
    response = await authenticated_client.get("/settings?tab=metadata")
    assert response.status_code == 200
    assert 'data-testid="metadata-source-priority"' in response.text
    assert "data-order-controls" in response.text
    assert 'data-testid="metadata-source-health"' in response.text
    assert 'data-testid="metron-access"' in response.text
    assert 'id="metron-token"' in response.text
    assert 'for="metron-token"' in response.text
    assert "Save Metron settings" in response.text
    assert "Remove saved token" in response.text
    assert "Save metadata priority" in response.text
    assert "Advanced domain priorities" in response.text
    assert "ComicVine access" in response.text and "Local Comic Vine catalog" in response.text
    seed = re.search(r"const seed = (.+);", response.text)
    assert seed
    data = json.loads(seed.group(1))
    assert data[0]["source"] == "metron_api" and data[0]["credential_configured"]
    assert all("credential_secret" not in item for item in data)
    assert "do-not-render-provider-token" not in response.text
    assert "gcd_api_v2" in response.text and "feature_disabled" in response.text


async def test_metadata_settings_htmx_keeps_source_controls_and_never_probes_on_load(
    authenticated_client,
    monkeypatch,
):
    from pullbox.services.metadata_discovery import MetadataSourceRegistry

    async def forbidden(*args, **kwargs):
        raise AssertionError("Rendering settings must not probe providers")

    monkeypatch.setattr(MetadataSourceRegistry, "check", forbidden)
    response = await authenticated_client.get(
        "/settings?tab=metadata", headers={"HX-Request": "true"}
    )
    assert response.status_code == 200
    assert 'data-testid="metadata-source-priority"' in response.text
    assert 'data-testid="metadata-source-health"' in response.text
    assert 'data-testid="metron-access"' in response.text
