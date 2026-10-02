"""Series provider links preserve drafts and use the shared modal/dropdown contracts."""

import pytest
from playwright.sync_api import expect

from tests.e2e.accessibility import assert_no_axe_violations
from tests.e2e.pages.series_detail import SeriesDetailPage

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 390)])
def test_empty_provider_search_is_not_an_outage_and_can_be_retried(
    authed_page, seeded_server, theme, width
):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 1000})
    title = "Batman: The Dark Knight: Golden Dawn"
    page.route(
        "**/api/v1/series/1/metadata-links",
        lambda route: route.fulfill(
            json={
                "current": {"title": title},
                "identities": [],
                "sources": [{"source": "metron_api", "label": "Metron"}],
            }
        ),
    )
    searches = []
    candidate = {
        "source": "metron_api",
        "identity_namespace": "metron",
        "external_id": "123",
        "title": "Batman: The Dark Knight",
        "year_start": 2012,
        "publisher": "DC",
        "issue_count": 1,
    }

    def search(route):
        searches.append(route.request.post_data_json)
        matched = len(searches) == 2
        route.fulfill(
            json={
                "results": [candidate] if matched else [],
                "sources": [
                    {
                        "source": "metron_api",
                        "status": "ok" if matched else "empty",
                        "total": 1 if matched else 0,
                        "next_offset": None,
                    }
                ],
            }
        )

    page.route("**/api/v1/metadata/search", search)
    SeriesDetailPage(page, seeded_server).goto(1)
    page.evaluate("theme => applyTheme(theme)", theme)
    page.get_by_role("button", name="Link provider", exact=True).click()
    dialog = page.get_by_role("dialog", name="Link metadata provider", exact=True)
    query = dialog.get_by_label("Series title", exact=True)
    expect(query).to_have_value(title)
    button = dialog.get_by_role("button", name="Search provider", exact=True)
    button.click()
    empty = dialog.get_by_text("No matches found. Try a shorter title or another provider.")
    expect(empty).to_be_visible()
    expect(dialog.get_by_role("alert")).to_be_hidden()
    expect(button).to_be_enabled()
    expect(query).to_have_value(title)
    assert_no_axe_violations(
        page,
        name=f"series-provider-empty-{theme}-{width}",
        include=['[aria-labelledby="series-link-title"]'],
    )
    assert len(searches) == 1
    query.fill("Golden Dawn")
    query.press("Enter")
    expect(dialog.get_by_role("button", name="Review Batman: The Dark Knight")).to_be_visible()
    expect(empty).to_be_hidden()
    expect(dialog.get_by_role("alert")).to_be_hidden()
    assert searches[1]["query"] == "Golden Dawn"
    # A subsequent empty response clears the previous candidates without closing the dialog.
    button.click()
    expect(empty).to_be_visible()
    expect(dialog.get_by_role("button", name="Review Batman: The Dark Knight")).to_be_hidden()
    expect(dialog.get_by_role("alert")).to_be_hidden()
    expect(dialog.get_by_role("button", name="Link this series")).to_be_hidden()
    assert len(searches) == 3


@pytest.mark.parametrize("status", ["unavailable", "authentication_failed", "rate_limited"])
def test_provider_search_failures_are_not_shown_as_no_matches(authed_page, seeded_server, status):
    page = authed_page
    page.route(
        "**/api/v1/series/1/metadata-links",
        lambda route: route.fulfill(
            json={
                "current": {"title": "Batman"},
                "identities": [],
                "sources": [{"source": "metron_api", "label": "Metron"}],
            }
        ),
    )
    page.route(
        "**/api/v1/metadata/search",
        lambda route: route.fulfill(
            json={"results": [], "sources": [{"source": "metron_api", "status": status}]}
        ),
    )
    SeriesDetailPage(page, seeded_server).goto(1)
    page.get_by_role("button", name="Link provider", exact=True).click()
    dialog = page.get_by_role("dialog", name="Link metadata provider", exact=True)
    button = dialog.get_by_role("button", name="Search provider", exact=True)
    button.click()
    expect(dialog.get_by_role("alert")).to_be_visible()
    expect(dialog.get_by_text("No matches found.", exact=False)).to_be_hidden()
    expect(button).to_be_enabled()


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 390)])
def test_provider_review_search_pagination_and_confirmation(
    authed_page, seeded_server, theme, width
):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 1000})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    current = {
        "title": "GCD library series",
        "year_start": 1986,
        "publisher": "DC",
        "issue_count": 126,
    }
    data = {
        "current": current,
        "identities": [{"namespace": "gcd", "external_id": "2999", "state": "verified"}],
        "sources": [{"source": "metron_api", "label": "Metron"}],
    }
    page.route("**/api/v1/series/1/metadata-links", lambda route: route.fulfill(json=data))
    profiles = [
        {
            **current,
            "source": "metron_api",
            "identity_namespace": "metron",
            "external_id": str(i),
            "title": f"Provider series {i}",
        }
        for i in range(1, 21)
    ]

    def search(route):
        query = route.request.post_data_json
        offset = query["offsets"]["metron_api"]
        route.fulfill(
            json={
                "results": profiles[offset : offset + 10],
                "sources": [
                    {
                        "source": "metron_api",
                        "status": "ok",
                        "next_offset": 10 if not offset else None,
                    }
                ],
            }
        )

    page.route("**/api/v1/metadata/search", search)
    preview = {
        "current": current,
        "candidate": profiles[10],
        "source_revision": 1,
        "review": {
            "event_id": 1,
            "fingerprint": "a" * 64,
            "review_revision": 1,
            "owner_local_id": None,
            "current_external_id": None,
            "external_id": "11",
        },
    }
    page.route(
        "**/api/v1/series/1/metadata-links/preview", lambda route: route.fulfill(json=preview)
    )
    attempts = []

    def confirm(route):
        attempts.append(route.request.post_data_json)
        if len(attempts) == 1:
            route.fulfill(status=409, json={"detail": "Source settings changed. Preview again."})
        else:
            data["identities"].append(
                {"namespace": "metron", "external_id": "11", "state": "verified"}
            )
            route.fulfill(json={"event_id": 2, "replayed": False})

    page.route("**/api/v1/series/1/metadata-links/confirm", confirm)
    SeriesDetailPage(page, seeded_server).goto(1)
    page.evaluate("theme => applyTheme(theme)", theme)
    card = page.get_by_test_id("series-metadata-links")
    expect(card).to_contain_text("GCD 2999")
    card.evaluate("node => { node.keepThisCard = true; }")
    button = card.get_by_role("button", name="Link provider", exact=True)
    button.press("Enter")
    dialog = page.get_by_role("dialog", name="Link metadata provider", exact=True)
    expect(dialog.get_by_label("Series title", exact=True)).to_be_focused()
    expect(dialog.locator('[data-dropdown-select-contract="v1"]')).to_be_visible()
    dialog.get_by_role("button", name="Search provider", exact=True).click()
    expect(
        dialog.get_by_role("button", name="Review Provider series 1", exact=True)
    ).to_be_visible()
    dialog.get_by_role("button", name="Next", exact=True).press("Enter")
    expect(
        dialog.get_by_role("button", name="Review Provider series 11", exact=True)
    ).to_be_visible()
    dialog.get_by_role("button", name="Review Provider series 11", exact=True).click()
    expect(dialog.get_by_role("heading", name="Confirm this is the same series")).to_be_focused()
    expect(dialog).to_contain_text("Linking does not combine their issue lists.")
    assert_no_axe_violations(
        page,
        name=f"series-provider-link-{theme}-{width}",
        include=['[aria-labelledby="series-link-title"]'],
    )
    assert dialog.evaluate("node => node.scrollWidth <= node.clientWidth + 1")
    dialog.get_by_role("button", name="Link this series", exact=True).click()
    expect(dialog.get_by_role("alert")).to_contain_text("Source settings changed")
    expect(
        dialog.get_by_test_id("series-link-review").get_by_text("Provider series 11", exact=True)
    ).to_be_visible()
    dialog.get_by_role("button", name="Back to matches", exact=True).click()
    dialog.get_by_role("button", name="Review Provider series 11", exact=True).click()
    dialog.get_by_role("button", name="Link this series", exact=True).click()
    expect(dialog).to_be_hidden()
    expect(button).to_be_focused()
    expect(card).to_contain_text("Metron 11")
    expect(card.get_by_role("status")).to_contain_text("Use Refresh metadata")
    assert card.evaluate("node => node.keepThisCard")
    assert all("external_id" not in item for item in attempts)
    button.press("Enter")
    dialog.press("Escape")
    expect(button).to_be_focused()
    assert not errors
