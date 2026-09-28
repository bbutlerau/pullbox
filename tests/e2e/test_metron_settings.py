"""Metron credentials use the real masked, revision-checked settings API."""

import pytest
from playwright.sync_api import expect

from tests.e2e.accessibility import assert_no_axe_violations
from tests.e2e.pages.settings import SettingsPage

pytestmark = pytest.mark.e2e
TOKEN = "synthetic-browser-metron-token"


@pytest.fixture
def metron_page(authed_page, seeded_server):
    page = authed_page
    SettingsPage(page, seeded_server).goto("metadata")
    csrf = page.evaluate("readCsrfTokenFromBody()")
    endpoint = seeded_server + "/api/v1/metadata/sources/metron_api"
    original = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
    assert not next(row for row in original if row["source"] == "metron_api")[
        "credential_configured"
    ]

    def clear():
        policies = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
        item = next(row for row in policies if row["source"] == "metron_api")
        body = {key: item[key] for key in ("revision", "priority", "domain_priorities", "settings")}
        response = page.request.put(
            endpoint,
            data={**body, "enabled": False, "clear_credential": True},
            headers={"X-CSRF-Token": csrf},
        )
        assert response.ok

    clear()
    page.reload()
    expect(
        page.get_by_test_id("metron-access").get_by_text("No token is saved.", exact=True)
    ).to_be_visible()
    yield page
    page.unroute_all(behavior="ignoreErrors")
    current = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
    revisions = {row["source"]: row["revision"] for row in current}
    for item in original:
        body = {key: item[key] for key in ("enabled", "priority", "domain_priorities", "settings")}
        response = page.request.put(
            seeded_server + "/api/v1/metadata/sources/" + item["source"],
            data={
                **body,
                "revision": revisions[item["source"]],
                "clear_credential": item["source"] == "metron_api",
            },
            headers={"X-CSRF-Token": csrf},
        )
        assert response.ok


def save_metron(page):
    with page.expect_response("**/api/v1/metadata/sources/metron_api") as saved:
        page.get_by_role("button", name="Save Metron settings", exact=True).click()
    assert saved.value.status == 200
    expect(page.get_by_test_id("metron-access").get_by_role("status")).to_contain_text("saved")
    return saved.value


def test_metron_token_save_replace_preserve_disable_and_explicit_clear(metron_page):
    page = metron_page
    requests = []
    page.on("request", lambda request: requests.append(request.url))
    card = page.get_by_test_id("metron-access")
    expect(card).to_be_visible()
    token = card.get_by_label("Metron API token", exact=True)
    assert token.get_attribute("type") == "password"
    card.get_by_label("Enable Metron").check()
    token.fill(TOKEN)
    saved = save_metron(page)
    assert saved.json()["credential_configured"] is True
    assert TOKEN not in saved.text()
    expect(token).to_have_value("")
    page.reload()
    expect(token).to_have_value("")
    expect(card.get_by_label("Enable Metron")).to_be_checked()
    expect(card.get_by_text("A token is saved.", exact=True)).to_be_visible()
    assert TOKEN not in page.content()
    card.get_by_label("Enable Metron").uncheck()
    assert save_metron(page).json()["credential_configured"] is True
    token.fill(TOKEN + "-replacement")
    assert save_metron(page).json()["credential_configured"] is True
    card.get_by_label("Remove saved token").check()
    expect(token).to_be_disabled()
    assert save_metron(page).json()["credential_configured"] is False
    expect(card.get_by_text("No token is saved.", exact=True)).to_be_visible()
    assert not any(url.endswith("/test") or "metron.cloud" in url for url in requests)


def test_metron_and_priority_drafts_survive_each_others_saves(metron_page):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    order = page.get_by_test_id("metadata-order-global")
    order.locator('[data-order-direction="down"]').first.click()
    draft = order.locator("[data-source-label]").all_text_contents()
    card.get_by_label("Enable Metron").check()
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    save_metron(page)
    expect(order.locator("[data-source-label]")).to_have_text(draft)
    card.get_by_label("Metron API token", exact=True).fill(TOKEN + "-next")
    with page.expect_response("**/api/v1/metadata/priorities") as priority:
        page.get_by_role("button", name="Save metadata priority", exact=True).click()
    assert priority.value.status == 200
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value(TOKEN + "-next")
    saved = save_metron(page).json()
    policy = next(row for row in priority.value.json() if row["source"] == "metron_api")
    assert saved["priority"] == policy["priority"]
    assert saved["domain_priorities"] == policy["domain_priorities"]
    assert saved["revision"] == policy["revision"] + 1


def test_metron_stale_save_requires_explicit_reload_without_overwriting_priority(
    metron_page, seeded_server
):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    policies = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
    csrf = page.evaluate("readCsrfTokenFromBody()")
    external_order = list(reversed([row["source"] for row in policies]))
    response = page.request.put(
        seeded_server + "/api/v1/metadata/priorities",
        data={
            "order": external_order,
            "revisions": {row["source"]: row["revision"] for row in policies},
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert response.ok
    card.get_by_label("Enable Metron").check()
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    with page.expect_response("**/api/v1/metadata/sources/metron_api") as saved:
        card.get_by_role("button", name="Save Metron settings", exact=True).click()
    assert saved.value.status == 409
    expect(card.get_by_role("alert")).to_contain_text("changed")
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value(TOKEN)
    card.get_by_role("button", name="Load saved Metron settings", exact=True).click()
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value("")
    card.get_by_label("Enable Metron").check()
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    result = save_metron(page).json()
    expected = next(row for row in response.json() if row["source"] == "metron_api")
    assert result["priority"] == expected["priority"]
    assert result["revision"] == expected["revision"] + 1
    # Loading Metron does not authorize overwriting another session's source order.
    page.get_by_test_id("metadata-order-global").locator(
        '[data-order-direction="down"]'
    ).first.click()
    with page.expect_response("**/api/v1/metadata/priorities") as priority:
        page.get_by_role("button", name="Save metadata priority", exact=True).click()
    assert priority.value.status == 409


def test_metron_errors_are_safe_and_pending_controls_stay_mounted(metron_page):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    page.route(
        "**/api/v1/metadata/sources/metron_api",
        lambda route: route.fulfill(
            status=500,
            json={"detail": TOKEN},
        ),
    )
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("Could not save")
    assert TOKEN not in card.inner_text()
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value(TOKEN)
    page.evaluate("""() => {
      const original = window.fetch.bind(window);
      window.fetch = (url, options) => url.endsWith('/api/v1/metadata/sources/metron_api')
        ? new Promise(() => {}) : original(url, options);
    }""")
    button = card.get_by_role("button", name="Save Metron settings", exact=True)
    button.evaluate("node => { node.retained = true; }")
    button.press("Enter")
    pending = card.get_by_role("button", name="Saving Metron...", exact=True)
    expect(pending).to_be_visible()
    expect(pending).to_be_disabled()
    assert pending.evaluate("node => node.retained")
    expect(card.get_by_label("Metron API token", exact=True)).to_be_disabled()
    expect(page.get_by_role("button", name="Test Metron", exact=True)).to_be_disabled()


def test_metron_check_uses_saved_settings_not_token_draft(metron_page):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    card.get_by_label("Enable Metron").check()
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    save_metron(page)
    card.get_by_label("Metron API token", exact=True).fill("unsaved-token")
    page.route(
        "**/api/v1/metadata/sources/metron_api/test",
        lambda route: route.fulfill(
            json={
                "outcome": {"source": "metron_api", "status": "authentication_failed"},
                "recorded": True,
            },
        ),
    )
    with page.expect_request("**/api/v1/metadata/sources/metron_api/test") as request:
        page.get_by_role("button", name="Test Metron", exact=True).click()
    assert request.value.post_data is None
    expect(page.get_by_test_id("metadata-source-health").get_by_role("status")).to_contain_text(
        "Authentication required"
    )
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value("unsaved-token")


def test_metron_requires_token_before_enabling(metron_page):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    writes = []
    page.on(
        "request", lambda request: writes.append(request.url) if request.method == "PUT" else None
    )
    card.get_by_label("Enable Metron").check()
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("Enter a token before enabling Metron")
    assert not writes
    card.get_by_label("Metron API token", exact=True).fill("token with spaces")
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("without spaces")
    assert not writes
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    save_metron(page)
    writes.clear()
    card.get_by_label("Remove saved token").check()
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("Disable Metron before removing")
    assert not writes


def test_metron_reload_does_not_adopt_an_unseen_priority_change(metron_page, seeded_server):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    policies = page.request.get(seeded_server + "/api/v1/metadata/sources").json()
    policy = next(row for row in policies if row["source"] == "metron_api")
    csrf = page.evaluate("readCsrfTokenFromBody()")
    response = page.request.put(
        seeded_server + "/api/v1/metadata/sources/metron_api",
        data={"revision": policy["revision"], "enabled": False, "priority": 999},
        headers={"X-CSRF-Token": csrf},
    )
    assert response.ok
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("alert")).to_contain_text("changed")
    card.get_by_role("button", name="Load saved Metron settings", exact=True).click()
    expect(card.get_by_label("Metron API token", exact=True)).to_have_value("")
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    assert save_metron(page).json()["priority"] == 999
    page.get_by_test_id("metadata-order-global").locator(
        '[data-order-direction="down"]'
    ).first.click()
    with page.expect_response("**/api/v1/metadata/priorities") as priority:
        page.get_by_role("button", name="Save metadata priority", exact=True).click()
    assert priority.value.status == 409


def test_metron_navigation_clears_draft_and_aborts_owned_request(metron_page, seeded_server):
    page = metron_page
    card = page.get_by_test_id("metron-access")
    card.get_by_label("Metron API token", exact=True).fill(TOKEN)
    page.evaluate("""() => {
      window.metronState = Alpine.$data(document.querySelector('[data-testid="metadata-source-priority"]'));
      const original = window.fetch.bind(window);
      window.fetch = (url, options) => {
        if (!url.endsWith('/api/v1/metadata/sources/metron_api')) return original(url, options);
        window.metronSignal = options.signal;
        return new Promise((resolve, reject) => {
          options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
        });
      };
    }""")
    card.get_by_role("button", name="Save Metron settings", exact=True).click()
    expect(card.get_by_role("button", name="Saving Metron...", exact=True)).to_be_disabled()
    SettingsPage(page, seeded_server).switch_tab("general")
    assert page.evaluate("window.metronSignal.aborted")
    assert page.evaluate("window.metronState.metronToken") == ""
    assert page.evaluate("window.metronState.alive") is False


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 320)])
def test_metron_keyboard_reflow_and_accessibility(metron_page, theme, width):
    page = metron_page
    page.set_viewport_size({"width": width, "height": 900})
    page.emulate_media(reduced_motion="reduce")
    page.evaluate("theme => document.documentElement.dataset.theme = theme", theme)
    card = page.get_by_test_id("metron-access")
    card.get_by_label("Enable Metron").press("Space")
    expect(card.get_by_label("Enable Metron")).to_be_checked()
    card.get_by_label("Enable Metron").press("Tab")
    expect(card.get_by_label("Metron API token", exact=True)).to_be_focused()
    assert card.evaluate("node => node.scrollWidth <= node.clientWidth + 1")
    assert_no_axe_violations(
        page, name=f"metron-settings-{theme}-{width}", include=["[data-testid='metron-access']"]
    )
    card.screenshot(path=f"test-results/metron-settings-{theme}-{width}.png", animations="disabled")
