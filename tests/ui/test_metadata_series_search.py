"""The actual Add Series page uses shared discovery, not a second provider path."""

import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import (
    ProviderSeriesRead,
    SeriesDiscoveryRead,
    SourceOutcome,
    SourceStatus,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry

sys.path.insert(0, str(Path(__file__).parents[1]))
pytest_plugins = ["conftest_security"]


def candidate(source, identifier, title):
    return ProviderSeriesRead(
        source=source,
        identity_namespace=source.identity_namespace,
        external_id=str(identifier),
        title=title,
        year_start=2024,
        issue_count=12,
    )


@pytest.fixture
def discovery(monkeypatch):
    monkeypatch.setattr(
        "pullbox.core.comicvine_key.get_comicvine_api_key",
        AsyncMock(side_effect=ValueError("No legacy discovery in this test")),
    )
    result = SeriesDiscoveryRead(
        results=[candidate(Source.METRON_API, 42, "Metron match")],
        sources=[
            SourceOutcome(source=Source.METRON_API, status=SourceStatus.OK),
            SourceOutcome(source=Source.COMICVINE_API, status=SourceStatus.TIMEOUT),
        ],
    )
    full = AsyncMock(return_value=result)
    quick = AsyncMock(return_value=result)
    monkeypatch.setattr(MetadataSourceRegistry, "discover_all", full)
    monkeypatch.setattr(MetadataSourceRegistry, "discover", quick)
    return full, quick


async def test_add_page_displays_source_control_partial_results_and_source_bound_add(
    authenticated_client, discovery
):
    response = await authenticated_client.get("/series/add?q=Example")
    assert response.status_code == 200
    assert 'data-testid="add-series-source-select"' in response.text
    assert "All enabled sources" in response.text
    assert "Metron match" in response.text
    assert 'data-series-source="metron_api"' in response.text
    assert 'data-series-external-id="42"' in response.text
    assert "ComicVine API: timed out" in response.text
    assert 'data-testid="add-series-source-outcomes"' in response.text
    assert "No online search was made" not in response.text
    query = discovery[0].await_args.args[0]
    assert query.sources is None and query.limit_per_source == 100
    assert query.search_mode == "full"
    discovery[1].assert_not_awaited()


async def test_filter_preview_and_pagination_keep_source_and_do_not_repeat_search(
    authenticated_client, discovery
):
    discovery[0].return_value = SeriesDiscoveryRead(
        results=[candidate(Source.METRON_API, n, f"Series {n:02}") for n in range(1, 46)],
        sources=[SourceOutcome(source=Source.METRON_API, status=SourceStatus.OK)],
    )
    first = await authenticated_client.get("/series/add?q=Example&source=metron_api&sort=title")
    assert "Series 01" in first.text and "Series 21" not in first.text
    second = await authenticated_client.get(
        "/series/add?q=Example&source=metron_api&sort=title&page=2",
        headers={"HX-Request": "true"},
    )
    assert "Series 21" in second.text and "Series 01" not in second.text
    assert "source=metron_api" in second.text
    assert discovery[0].await_count == 1
    assert discovery[0].await_args.args[0].sources == [Source.METRON_API]
    await authenticated_client.get("/series/add?q=Example&source=metron_api&sort=-issue_count")
    assert discovery[0].await_count == 1
    await authenticated_client.get("/series/add?q=Example&source=metron_api&search_mode=preview")
    query = discovery[1].await_args.args[0]
    assert query.sources == [Source.METRON_API] and query.limit_per_source == 20
    assert query.search_mode == "preview"


async def test_native_namespaces_do_not_collide_or_round_large_ids(authenticated_client, discovery):
    large_id = "123456789012345678901234567890"
    discovery[0].return_value.results = [
        candidate(Source.COMICVINE_API, 42, "CV match"),
        candidate(Source.METRON_API, 42, "Metron match"),
        candidate(Source.METRON_API, large_id, "Large identity"),
    ]
    response = await authenticated_client.get("/series/add?q=Example&source=metron_api")
    assert 'id="metadata-result-comicvine-42"' in response.text
    assert 'id="metadata-result-metron-42"' in response.text
    assert f'data-series-external-id="{large_id}"' in response.text
    assert f"selectResult({large_id}," not in response.text


async def test_unknown_source_is_rejected_before_provider_calls(authenticated_client, discovery):
    response = await authenticated_client.get("/series/add?q=Example&source=not-a-provider")
    assert response.status_code == 422
    discovery[0].assert_not_awaited()


async def test_cached_candidates_use_fresh_exact_owner_and_never_fuzzy_titles(
    authenticated_client, sec_db, discovery
):
    from pullbox.core.metadata_identity import IdentityEvidenceKind, IdentityNamespace
    from pullbox.core.metadata_identity_state import IdentityVerificationState
    from pullbox.models.metadata_identity import SeriesExternalIdentity
    from pullbox.models.series import Series

    url = "/series/add?q=Example&source=metron_api"
    first = await authenticated_client.get(url)
    assert 'data-series-external-id="42"' in first.text
    async with sec_db() as session:
        series = Series(title="Metron match", sort_title="Metron match", comicvine_id=42)
        session.add(series)
        await session.commit()
        series_id = series.id
    same_title_and_number = await authenticated_client.get(url)
    assert 'data-add-series-trigger="true"' in same_title_and_number.text
    async with sec_db() as session:
        session.add(
            SeriesExternalIdentity(
                series_id=series_id,
                identity_namespace=IdentityNamespace.METRON,
                external_id="42",
                verification_state=IdentityVerificationState.VERIFIED,
                evidence_kind=IdentityEvidenceKind.USER_SELECTION,
            )
        )
        await session.commit()
    owned = await authenticated_client.get(url)
    assert 'data-add-series-trigger="true"' not in owned.text
    assert f'href="/series/{series_id}"' in owned.text
    assert 'data-testid="add-series-existing-title-link"' in owned.text
    assert discovery[0].await_count == 1


@pytest.mark.parametrize("state", ["stale", "conflicted"])
async def test_unverified_claim_does_not_become_owned_through_legacy_bridge(
    authenticated_client, sec_db, discovery, state
):
    from pullbox.core.metadata_identity import IdentityEvidenceKind, IdentityNamespace
    from pullbox.core.metadata_identity_state import IdentityVerificationState
    from pullbox.models.metadata_identity import SeriesExternalIdentity
    from pullbox.models.series import Series

    discovery[0].return_value.results = [candidate(Source.COMICVINE_API, 42, "CV match")]
    async with sec_db() as session:
        series = Series(title="Existing", sort_title="Existing", comicvine_id=42)
        session.add(series)
        await session.flush()
        session.add(
            SeriesExternalIdentity(
                series_id=series.id,
                identity_namespace=IdentityNamespace.COMICVINE,
                external_id="42",
                verification_state=IdentityVerificationState(state),
                evidence_kind=IdentityEvidenceKind.USER_SELECTION,
            )
        )
        await session.commit()
    response = await authenticated_client.get("/series/add?q=Example&source=comicvine_api")
    assert "Identity needs review" in response.text
    assert 'data-add-series-trigger="true"' not in response.text
    assert 'data-testid="add-series-existing-title-link"' not in response.text


async def test_search_result_urls_cannot_be_active_content(authenticated_client, discovery):
    row = discovery[0].return_value.results[0]
    row.resource_url = "javascript:alert(42)"
    row.image_url = "https://private.example/secret.png"
    response = await authenticated_client.get("/series/add?q=Example&source=metron_api")
    assert "Metron match" in response.text
    assert "javascript:alert" not in response.text
    assert "private.example" not in response.text
