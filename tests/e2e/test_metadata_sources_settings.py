"""Provider priority uses stable keyed rows and the real authenticated API."""

import pytest
from playwright.sync_api import expect

from tests.e2e.accessibility import assert_no_axe_violations
from tests.e2e.pages.settings import SettingsPage

pytestmark = pytest.mark.e2e


def test_metadata_priorities_save_reload_and_domain_reset(authed_page, seeded_server):
    page = authed_page
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    SettingsPage(page, seeded_server).goto("metadata")
    card = page.get_by_test_id("metadata-source-priority")
    expect(card).to_be_visible()
    global_order = card.get_by_test_id("metadata-order-global")
    rows = global_order.locator("[data-source-row]")
    initial = rows.locator("[data-source-label]").all_text_contents()
    assert len(initial) == 5
    down = global_order.locator('[data-order-direction="down"]')
    down.first.press("Enter")
    expected = [initial[1], initial[0], *initial[2:]]
    expect(rows.locator("[data-source-label]")).to_have_text(expected)
    expect(
        global_order.get_by_role("button", name=f"Move {initial[0]} down", exact=True)
    ).to_be_focused()
    card.get_by_text("Advanced domain priorities", exact=True).click()
    card.get_by_label("Use a separate order for Artwork").check()
    artwork = card.get_by_test_id("metadata-order-artwork")
    expect(artwork.locator("[data-source-row]")).to_have_count(3)
    artwork.locator('[data-order-direction="down"]').first.click()
    with page.expect_response("**/api/v1/metadata/priorities") as saved:
        card.get_by_role("button", name="Save metadata priority", exact=True).click()
    assert saved.value.status == 200
    expect(card.get_by_role("status")).to_contain_text("Metadata priority saved")
    page.reload()
    expect(rows.locator("[data-source-label]")).to_have_text(expected)
    card.get_by_text("Advanced domain priorities", exact=True).click()
    expect(card.get_by_label("Use a separate order for Artwork")).to_be_checked()
    card.get_by_label("Use a separate order for Artwork").uncheck()
    global_order.locator('[data-order-direction="up"]').nth(1).click()
    with page.expect_response("**/api/v1/metadata/priorities") as saved:
        card.get_by_role("button", name="Save metadata priority", exact=True).click()
    assert saved.value.status == 200
    assert all(not row["domain_priorities"] for row in saved.value.json())
    assert not errors


def test_metadata_priority_conflict_keeps_draft_and_rows(authed_page, seeded_server):
    page = authed_page
    SettingsPage(page, seeded_server).goto("metadata")
    card = page.get_by_test_id("metadata-source-priority")
    expect(card).to_be_visible()
    card.evaluate("node => { node.keepThisNode = true; }")
    order = card.get_by_test_id("metadata-order-global")
    order.locator('[data-order-direction="down"]').first.click()
    draft = order.locator("[data-source-label]").all_text_contents()
    page.route(
        "**/api/v1/metadata/priorities",
        lambda route: route.fulfill(
            status=409,
            json={"detail": "Source settings changed; reload before saving"},
        ),
    )
    card.get_by_role("button", name="Save metadata priority", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("Settings changed")
    expect(order.locator("[data-source-label]")).to_have_text(draft)
    assert card.evaluate("node => node.keepThisNode") is True
    card.get_by_role("button", name="Load saved priority", exact=True).click()
    expect(card.get_by_role("button", name="Save metadata priority", exact=True)).to_be_disabled()
    assert card.evaluate("node => node.keepThisNode") is True


def test_metadata_health_is_scoped_and_keeps_priority_draft(authed_page, seeded_server):
    page = authed_page
    SettingsPage(page, seeded_server).goto("metadata")
    card = page.get_by_test_id("metadata-source-priority")
    expect(card).to_be_visible()
    card.get_by_test_id("metadata-order-global").locator(
        '[data-order-direction="down"]'
    ).first.click()
    page.route(
        "**/api/v1/metadata/sources/comicvine_local/test",
        lambda route: route.fulfill(
            json={
                "outcome": {
                    "source": "comicvine_local",
                    "status": "rate_limited",
                    "retry_after_seconds": 30,
                },
                "recorded": True,
            }
        ),
    )
    health = card.get_by_test_id("metadata-source-health")
    health.get_by_role("button", name="Test ComicVine Local Catalog", exact=True).click()
    expect(health.get_by_role("status")).to_contain_text("Rate limited")
    expect(health.get_by_role("status")).to_contain_text("30")
    expect(card.get_by_role("button", name="Save metadata priority", exact=True)).to_be_enabled()
    disabled = health.get_by_role("button", name="Test GCD API v2", exact=True)
    expect(disabled).to_be_disabled()
    assert "feature_disabled" not in disabled.inner_text()


def test_metadata_credential_refresh_preserves_unsaved_priority(authed_page, seeded_server):
    page = authed_page
    SettingsPage(page, seeded_server).goto("metadata")
    card = page.get_by_test_id("metadata-source-priority")
    order = card.get_by_test_id("metadata-order-global")
    order.locator('[data-order-direction="down"]').first.click()
    draft = order.locator("[data-source-label]").all_text_contents()
    sources = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
    for item in sources:
        if item["source"] == "comicvine_api":
            item["credential_configured"] = True
            item["availability"] = None
    page.route("**/api/v1/metadata/sources", lambda route: route.fulfill(json=sources))
    page.evaluate("window.dispatchEvent(new CustomEvent('metadata-credentials-updated'))")
    expect(card.get_by_role("button", name="Test ComicVine API", exact=True)).to_be_enabled()
    expect(order.locator("[data-source-label]")).to_have_text(draft)
    expect(card.get_by_role("button", name="Save metadata priority", exact=True)).to_be_enabled()


def test_metadata_save_keeps_controls_and_focus_while_pending(authed_page, seeded_server):
    page = authed_page
    SettingsPage(page, seeded_server).goto("metadata")
    card = page.get_by_test_id("metadata-source-priority")
    card.get_by_test_id("metadata-order-global").locator(
        '[data-order-direction="down"]'
    ).first.click()
    page.evaluate("""() => {
      const original = window.fetch.bind(window);
      window.fetch = (url, options) => url.endsWith('/api/v1/metadata/priorities')
        ? new Promise(() => {}) : original(url, options);
    }""")
    button = card.get_by_role("button", name="Save metadata priority", exact=True)
    button.evaluate("node => { node.keepThisButton = true; }")
    button.click()
    button = card.get_by_role("button", name="Saving...", exact=True)
    expect(button).to_be_visible()
    expect(button).to_be_disabled()
    assert button.evaluate("node => node.keepThisButton") is True
    expect(card.get_by_role("button", name="Reset", exact=True)).to_be_disabled()
    expect(card.get_by_test_id("metadata-order-global").locator("[data-source-row]")).to_have_count(
        5
    )


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("width", [1280, 320])
def test_metadata_priority_accessibility_and_boundary_focus(
    authed_page, seeded_server, theme, width
):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 900})
    page.emulate_media(reduced_motion="reduce")
    SettingsPage(page, seeded_server).goto("metadata")
    page.evaluate("theme => document.documentElement.dataset.theme = theme", theme)
    card = page.get_by_test_id("metadata-source-priority")
    order = card.get_by_test_id("metadata-order-global")
    first = order.locator("[data-source-label]").first.inner_text()
    order.locator('[data-order-direction="down"]').first.press("Enter")
    order.get_by_role("button", name=f"Move {first} up", exact=True).press("Enter")
    expect(order.get_by_role("button", name=f"Move {first} down", exact=True)).to_be_focused()
    card.get_by_text("Advanced domain priorities", exact=True).click()
    card.get_by_label("Use a separate order for Artwork").check()
    assert card.evaluate("element => element.scrollWidth <= element.clientWidth + 1")
    assert_no_axe_violations(
        page,
        name=f"metadata-priority-{theme}-{width}",
        include=["[data-testid='metadata-source-priority']"],
    )
    card.screenshot(
        path=f"test-results/metadata-priority-{theme}-{width}.png", animations="disabled"
    )
