"""Real Add Series controls retain selection, focus and exact provider identity."""

from unittest.mock import AsyncMock

import pytest
from playwright.sync_api import expect

from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_sources import ProviderSeriesRead, SourceCapability
from pullbox.services.metadata_discovery import SourcePage, SourceRegistration
from pullbox.services.metadata_sources import SourceRuntime, default_policy
from tests.e2e.accessibility import assert_no_axe_violations
from tests.e2e.test_source_series_add import preview

pytestmark = pytest.mark.e2e


@pytest.fixture
def metron_search(monkeypatch):
    from pullbox.providers.metadata import sources
    from pullbox.ui import series_routes

    calls = []

    class Adapter:
        async def search(self, query, offset):
            calls.append((query.query, offset, query.search_mode))
            rows = [
                ProviderSeriesRead(
                    source=Source.METRON_API,
                    identity_namespace=Source.METRON_API.identity_namespace,
                    external_id=str(index),
                    title=f"Search example {index:02}",
                    year_start=2024,
                    publisher="Test publisher",
                    issue_count=12,
                    resource_url=f"https://metron.cloud/series/{index}/",
                )
                for index in range(1, 46)
            ]
            stop = offset + query.limit_per_source
            return SourcePage(
                rows[offset:stop], total=len(rows), next_offset=stop if stop < len(rows) else None
            )

        async def close(self):
            pass

    registrations = sources.metadata_sources()
    registrations[Source.METRON_API] = SourceRegistration(
        frozenset({SourceCapability.SERIES_SEARCH}), lambda runtime: Adapter()
    )
    monkeypatch.setattr(sources, "metadata_sources", lambda: registrations)
    monkeypatch.setattr(
        series_routes,
        "load_source_runtime",
        AsyncMock(
            return_value=[
                SourceRuntime(
                    default_policy(Source.METRON_API).model_copy(update={"enabled": True})
                ),
                SourceRuntime(default_policy(Source.COMICVINE_API)),
            ]
        ),
    )
    return calls


def test_source_switch_does_not_name_the_previous_provider_while_loading(
    authed_page, seeded_server, metron_search
):
    page = authed_page
    page.goto(f"{seeded_server}/series/add?q=Swamp+Thing&source=comicvine_api")
    pending = []
    page.route("**/series/add?**source=metron_api**", lambda route: pending.append(route))
    try:
        page.get_by_test_id("add-series-source-select").get_by_role("button").click()
        page.get_by_role("option", name="Metron", exact=True).click()
        loader = page.locator("#add-series-results-loading")
        expect(loader).to_be_visible()
        expect(loader).to_contain_text("Searching metadata sources")
        expect(loader).not_to_contain_text("ComicVine API")
    finally:
        for route in pending:
            route.abort()
        page.unroute("**/series/add?**source=metron_api**")


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 1280), ("light", 320)])
def test_source_picker_pagination_and_result_action(
    authed_page, seeded_server, monkeypatch, metron_search, theme, width, browser_name
):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 900})
    page.emulate_media(reduced_motion="reduce")
    query = f"Search example {browser_name} {theme} {width}"
    page.goto(f"{seeded_server}/series/add?q={query}&sort=title")
    page.evaluate("theme => applyTheme(theme)", theme)
    expect(page.get_by_test_id("add-series-result-card")).to_have_count(20)
    expect(page.get_by_test_id("add-series-source-outcomes")).to_contain_text(
        "ComicVine API: needs credentials"
    )
    source = page.get_by_test_id("add-series-source-select")
    source.get_by_role("button").click()
    page.get_by_role("option", name="Metron", exact=True).click()
    expect(source).to_have_attribute("data-dropdown-value", "metron_api")
    expect(page.get_by_test_id("add-series-source-outcomes")).to_have_count(0)
    assert len(metron_search) == 2
    page.get_by_test_id("series-pagination-next").click()
    expect(page.get_by_test_id("add-series-result-title").first).to_have_text(
        "Search example 21 (2024)"
    )
    assert "source=metron_api" in page.url
    assert len(metron_search) == 2
    assert page.locator("#content").evaluate("el => el.scrollWidth <= el.clientWidth + 1")
    requests = []

    def respond(route):
        requests.append(route.request.post_data_json)
        route.fulfill(json=preview(identifier="21"))

    page.route("**/api/v1/metadata/series/preview", respond)
    trigger = page.locator('[data-add-series-trigger="true"]').first
    trigger.focus()
    page.keyboard.press("Enter")
    expect(page.get_by_role("button", name="Add series", exact=True)).to_be_enabled()
    assert requests[0]["source"] == "metron_api" and requests[0]["external_id"] == "21"
    page.keyboard.press("Escape")
    expect(trigger).to_be_focused()
    assert_no_axe_violations(
        page, name=f"metadata-search-{theme}-{width}", include=["#add-series-app"]
    )
    page.locator("#content").screenshot(
        path=f"output/playwright/metadata-search-{browser_name}-{theme}-{width}.png",
        animations="disabled",
    )
