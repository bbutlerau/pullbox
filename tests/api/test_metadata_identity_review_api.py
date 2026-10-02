"""Operator identity review uses real auth, CSRF, saved evidence and revisions."""

import os
import sys

import pytest
from sqlalchemy import select

from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.models import Series
from pullbox.models.metadata_identity import SeriesIdentityEvent
from pullbox.services.auth_service import SESSION_COOKIE_NAME, AuthService
from pullbox.services.metadata_identity_review import record_identity_observation
from tests.integration.metadata_identity.test_identity_review import claim

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
pytest_plugins = ["conftest_security"]


@pytest.fixture
async def saved_claim(sec_db):
    async with sec_db.begin() as session:
        series = Series(title="Needs identity review", sort_title="needs identity review")
        session.add(series)
        await session.flush()
        saved = await record_identity_observation(
            session, claim(series.id, MetadataEntityKind.SERIES)
        )
        return series.id, saved.event_id


def url(saved_claim):
    local_id, event_id = saved_claim
    return f"/api/v1/metadata-identities/series/{local_id}/review/{event_id}"


def csrf(client):
    return {
        "X-CSRF-Token": AuthService.get_csrf_token_from_session(
            client.cookies.get(SESSION_COOKIE_NAME)
        )
    }


async def test_review_roundtrip_uses_session_actor_and_safe_payload(
    authenticated_client, sec_user, sec_db, saved_claim
):
    preview = await authenticated_client.get(url(saved_claim))
    assert preview.status_code == 200
    data = preview.json()
    assert data["external_id"] == "42"
    assert "request_json" not in data and "evidence_locator" not in data
    payload = {
        "action": "confirm",
        "fingerprint": data["fingerprint"],
        "review_revision": data["review_revision"],
    }
    applied = await authenticated_client.post(
        url(saved_claim), json=payload, headers=csrf(authenticated_client)
    )
    assert applied.status_code == 200
    assert applied.json()["replayed"] is False
    again = await authenticated_client.post(
        url(saved_claim), json=payload, headers=csrf(authenticated_client)
    )
    assert again.status_code == 200 and again.json()["replayed"] is True
    async with sec_db() as session:
        event = await session.scalar(
            select(SeriesIdentityEvent).order_by(SeriesIdentityEvent.id.desc())
        )
        import json

        assert json.loads(event.request_json)["actor_user_id"] == sec_user.id
        assert (await session.get(Series, saved_claim[0])).comicvine_id == 42


async def test_review_requires_operator_session_and_csrf(
    authenticated_client, unauthenticated_client, sec_api_key, saved_claim
):
    preview = await authenticated_client.get(url(saved_claim))
    assert preview.status_code == 200
    data = preview.json()
    payload = {
        "action": "reject",
        "fingerprint": data["fingerprint"],
        "review_revision": data["review_revision"],
    }
    assert (await authenticated_client.post(url(saved_claim), json=payload)).status_code == 403
    assert (await unauthenticated_client.get(url(saved_claim))).status_code in {401, 403}
    assert (
        await unauthenticated_client.post(
            url(saved_claim), json=payload, headers={"X-API-Key": sec_api_key}
        )
    ).status_code in {401, 403}
    forged = await authenticated_client.post(
        url(saved_claim), json={**payload, "actor_user_id": 999}, headers=csrf(authenticated_client)
    )
    assert forged.status_code == 422


async def test_review_rejects_mismatched_target_and_stale_revision(
    authenticated_client, sec_db, saved_claim
):
    preview = await authenticated_client.get(url(saved_claim))
    assert preview.status_code == 200
    data = preview.json()
    async with sec_db.begin() as session:
        await record_identity_observation(session, claim(saved_claim[0], MetadataEntityKind.SERIES))
    result = await authenticated_client.post(
        url(saved_claim),
        headers=csrf(authenticated_client),
        json={
            "action": "confirm",
            "fingerprint": data["fingerprint"],
            "review_revision": data["review_revision"],
        },
    )
    assert result.status_code == 409
    result = await authenticated_client.get(
        f"/api/v1/metadata-identities/series/999/review/{saved_claim[1]}"
    )
    assert result.status_code == 404


async def test_claims_listing_is_bounded_and_does_not_leak_evidence(
    authenticated_client, saved_claim
):
    response = await authenticated_client.get(
        f"/api/v1/metadata-identities/series/{saved_claim[0]}/claims?limit=1"
    )
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["event_id"] == saved_claim[1]
    assert "request_json" not in response.text
