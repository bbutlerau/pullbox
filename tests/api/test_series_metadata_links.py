"""Linking a server-read provider series never adopts a second library series."""

import os
import sys

import pytest
from sqlalchemy import func, select

from pullbox.models import Series
from pullbox.models.metadata_identity import SeriesExternalIdentity, SeriesIdentityEvent
from pullbox.schemas.metadata_sources import MetadataFetch, SourceStatus
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from tests.api.test_metadata_sources_api import csrf, policy
from tests.unit.test_metadata_discovery import row

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


@pytest.fixture
async def link_target(authenticated_client, sec_db, monkeypatch):
    configured = await authenticated_client.put(
        "/api/v1/metadata/sources/metron_api",
        json=policy(credential="synthetic-token"),
        headers=csrf(authenticated_client),
    )
    assert configured.status_code == 200
    async with sec_db.begin() as session:
        series = Series(
            title="Existing series", sort_title="existing series", path="/kept/original"
        )
        session.add(series)
        await session.flush()
        series_id = series.id

    async def profile(self, source, external_id):
        return MetadataFetch(
            status=SourceStatus.OK,
            data=row(source, external_id, "Provider series").model_copy(
                update={"year_start": 1986, "publisher": "DC", "issue_count": 126}
            ),
        )

    monkeypatch.setattr(MetadataSourceRegistry, "series", profile)
    return series_id


def url(series_id):
    return f"/api/v1/series/{series_id}/metadata-links"


async def preview(client, series_id):
    return await client.post(
        url(series_id) + "/preview",
        json={"source": "metron_api", "external_id": "42"},
        headers=csrf(client),
    )


def confirmation(data):
    return {
        "event_id": data["review"]["event_id"],
        "fingerprint": data["review"]["fingerprint"],
        "review_revision": data["review"]["review_revision"],
        "source_revision": data["source_revision"],
    }


async def test_preview_confirm_and_reload_preserve_existing_series(
    authenticated_client, link_target, sec_db
):
    result = await preview(authenticated_client, link_target)
    assert result.status_code == 200, result.text
    data = result.json()
    assert data["current"]["title"] == "Existing series"
    assert data["candidate"]["title"] == "Provider series"
    assert data["candidate"]["issue_count"] == 126
    assert data["review"]["verification_state"] == "observed"
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(SeriesExternalIdentity)) == 0
        assert await session.scalar(select(func.count()).select_from(Series)) == 1

    endpoint = url(link_target) + "/confirm"
    for replayed in (False, True):
        linked = await authenticated_client.post(
            endpoint, json=confirmation(data), headers=csrf(authenticated_client)
        )
        assert linked.status_code == 200, linked.text
        assert linked.json()["replayed"] is replayed
    async with sec_db() as session:
        series = await session.get(Series, link_target)
        assert series.title == "Existing series" and series.path == "/kept/original"
        assert series.comicvine_id is None
        assert await session.scalar(select(func.count()).select_from(Series)) == 1
        identity = await session.scalar(select(SeriesExternalIdentity))
        assert identity.external_id == "42" and identity.verification_state.value == "verified"
    panel = await authenticated_client.get(url(link_target))
    assert panel.status_code == 200
    assert panel.json()["identities"][0]["external_id"] == "42"
    assert "request_json" not in panel.text and "synthetic-token" not in panel.text
    page = await authenticated_client.get(f"/series/{link_target}")
    assert 'data-testid="series-metadata-links"' in page.text
    assert "Link provider" in page.text and "series-metadata-links.js" in page.text


@pytest.mark.parametrize("change", ["policy", "target", "owner", "source_revision", "other_link"])
async def test_changed_review_rejects_link_atomically(
    authenticated_client, link_target, sec_db, change
):
    result = await preview(authenticated_client, link_target)
    assert result.status_code == 200
    body = confirmation(result.json())
    if change == "policy":
        result = await authenticated_client.put(
            "/api/v1/metadata/sources/metron_api",
            json=policy(revision=1, enabled=False),
            headers=csrf(authenticated_client),
        )
        assert result.status_code == 200
    elif change == "source_revision":
        body["source_revision"] += 1
    elif change == "target":
        async with sec_db.begin() as session:
            (await session.get(Series, link_target)).title = "Newer user edit"
    elif change == "other_link":
        async with sec_db.begin() as session:
            (await session.get(Series, link_target)).comicvine_id = 500
    else:
        async with sec_db.begin() as session:
            other = Series(title="Other owner", sort_title="other owner")
            session.add(other)
            await session.flush()
            session.add(
                SeriesExternalIdentity(
                    series_id=other.id,
                    identity_namespace="metron",
                    external_id="42",
                    verification_state="verified",
                    evidence_kind="user_selection",
                )
            )
    result = await authenticated_client.post(
        url(link_target) + "/confirm", json=body, headers=csrf(authenticated_client)
    )
    assert result.status_code == 409, result.text
    async with sec_db() as session:
        assert (
            await session.scalar(
                select(SeriesExternalIdentity).where(
                    SeriesExternalIdentity.series_id == link_target
                )
            )
            is None
        )


async def test_link_requires_operator_csrf_and_server_evidence(
    authenticated_client, unauthenticated_client, sec_api_key, link_target, sec_db, monkeypatch
):
    endpoint = url(link_target) + "/preview"
    body = {"source": "metron_api", "external_id": "42"}
    assert (await authenticated_client.post(endpoint, json=body)).status_code == 403
    assert (await unauthenticated_client.post(endpoint, json=body)).status_code in {401, 403}
    assert (
        await unauthenticated_client.post(endpoint, json=body, headers={"X-API-Key": sec_api_key})
    ).status_code in {401, 403}
    forged = await authenticated_client.post(
        endpoint, json={**body, "title": "Forged"}, headers=csrf(authenticated_client)
    )
    assert forged.status_code == 422

    async def failed(self, source, external_id):
        return MetadataFetch(status=SourceStatus.TIMEOUT)

    monkeypatch.setattr(MetadataSourceRegistry, "series", failed)
    response = await preview(authenticated_client, link_target)
    assert response.status_code == 409
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(SeriesIdentityEvent)) == 0


async def test_conflicting_provider_crosswalk_cannot_be_linked(
    authenticated_client, link_target, sec_db, monkeypatch
):
    from pullbox.core.metadata_identity import (
        ExternalIdentityRef,
        IdentityNamespace,
        MetadataEntityKind,
    )

    async with sec_db.begin() as session:
        (await session.get(Series, link_target)).comicvine_id = 500

    async def conflicting(self, source, external_id):
        return MetadataFetch(
            status=SourceStatus.OK,
            data=row(source, external_id).model_copy(
                update={
                    "cross_identities": [
                        ExternalIdentityRef(
                            IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "999"
                        )
                    ]
                }
            ),
        )

    monkeypatch.setattr(MetadataSourceRegistry, "series", conflicting)
    response = await preview(authenticated_client, link_target)
    assert response.status_code == 409
    assert "different" in response.text
    async with sec_db() as session:
        assert await session.scalar(select(func.count()).select_from(SeriesIdentityEvent)) == 0
