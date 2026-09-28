"""The Add dialog binds one preview identity and revision, never browser metadata."""

from contextlib import suppress
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from playwright.sync_api import expect

from tests.e2e.accessibility import assert_no_axe_violations

pytestmark = pytest.mark.e2e


def preview(source="metron_api", identifier="42", **updates):
    result = {
        "source": source,
        "external_id": identifier,
        "source_revision": 7,
        "series": {
            "status": "ok",
            "data": {
                "source": source,
                "identity_namespace": "metron" if source == "metron_api" else "comicvine",
                "external_id": identifier,
                "title": "Verified series",
                "year_start": 2024,
                "publisher": "Test publisher",
                "issue_count": 12,
            },
        },
        "issues": {"status": "ok", "data": {"results": [], "total": 12, "next_page": 2}},
    }
    result.update(updates)
    return result


def open_result(page, source="metron_api", identifier="42"):
    page.evaluate(
        "payload => selectResult(payload)",
        {
            "source": source,
            "externalId": identifier,
            "title": "Search title",
            "year": 2020,
            "publisher": "Search publisher",
        },
    )


@pytest.mark.parametrize("source", ["comicvine_local", "comicvine_api", "metron_api"])
def test_preview_then_add_sends_only_source_identity_revision_and_root(
    authed_page, seeded_server, source
):
    page = authed_page
    held = []
    adds = []
    page.route("**/api/v1/metadata/series/preview", lambda route: held.append(route))

    def added(route):
        adds.append(route.request.post_data_json)
        assert route.request.headers.get("x-csrf-token")
        route.fulfill(json={"id": 91, "title": "Verified series"})

    page.route("**/api/v1/series", added)
    page.goto(f"{seeded_server}/series/add")
    open_result(page, source)
    button = page.get_by_role("button", name="Add series", exact=True)
    expect(button).to_be_disabled()
    expect(page.get_by_test_id("add-series-preview-status")).to_contain_text("Loading preview")
    assert len(held) == 1
    assert held[0].request.post_data_json == {"source": source, "external_id": "42"}
    assert held[0].request.headers.get("x-csrf-token")
    held[0].fulfill(json=preview(source))
    expect(page.get_by_test_id("add-series-dialog")).to_contain_text("Verified series (2024)")
    expect(button).to_be_enabled()
    button.click()
    expect(page.get_by_test_id("add-series-dialog")).not_to_be_visible()
    assert adds == [
        {
            "source": source,
            "external_id": "42",
            "source_revision": 7,
            "library_root_id": adds[0]["library_root_id"],
        }
    ]
    assert isinstance(adds[0]["library_root_id"], int)


def test_partial_preview_keeps_profile_and_requires_successful_retry(authed_page, seeded_server):
    page = authed_page
    calls = []

    def respond(route):
        calls.append(True)
        result = preview()
        if len(calls) == 1:
            result["issues"] = {"status": "rate_limited", "retry_after_seconds": 60}
        route.fulfill(json=result)

    page.route("**/api/v1/metadata/series/preview", respond)
    page.goto(f"{seeded_server}/series/add")
    open_result(page)
    dialog = page.get_by_test_id("add-series-dialog")
    expect(dialog).to_contain_text("Verified series (2024)")
    expect(dialog).to_contain_text("60 seconds")
    expect(dialog.get_by_role("button", name="Add series", exact=True)).to_be_disabled()
    dialog.get_by_role("button", name="Retry preview", exact=True).click()
    expect(dialog.get_by_role("button", name="Add series", exact=True)).to_be_enabled()
    assert len(calls) == 2


def test_closed_preview_cannot_overwrite_new_selection(authed_page, seeded_server):
    page = authed_page
    held = []
    page.route("**/api/v1/metadata/series/preview", lambda route: held.append(route))
    page.goto(f"{seeded_server}/series/add")
    open_result(page, identifier="41")
    expect(page.get_by_test_id("add-series-preview-status")).to_be_visible()
    page.get_by_role("button", name="Cancel", exact=True).click()
    open_result(page, identifier="42")
    expect(page.get_by_test_id("add-series-preview-status")).to_be_visible()
    assert len(held) == 2
    held[1].fulfill(json=preview(identifier="42"))
    expect(page.get_by_test_id("add-series-dialog")).to_contain_text("Verified series (2024)")
    stale = preview(identifier="41")
    stale["series"]["data"]["title"] = "Stale series"
    with suppress(Exception):
        held[0].fulfill(json=stale)
    expect(page.get_by_test_id("add-series-dialog")).not_to_contain_text("Stale series")
    assert (
        page.evaluate("Alpine.$data(document.getElementById('add-series-app')).selectedExternalId")
        == "42"
    )


@pytest.mark.parametrize("failure", ["http", "identity", "revision", "issues"])
def test_invalid_preview_cannot_enable_add(authed_page, seeded_server, failure):
    page = authed_page
    result = preview()
    if failure == "identity":
        result["series"]["data"]["external_id"] = "999"
    elif failure == "revision":
        result["source_revision"] = -1
    elif failure == "issues":
        result["issues"] = {"status": "ok", "data": None}
    page.route(
        "**/api/v1/metadata/series/preview",
        lambda route: route.fulfill(
            status=409 if failure == "http" else 200,
            json={"detail": "Source settings changed. Preview again."}
            if failure == "http"
            else result,
        ),
    )
    page.goto(f"{seeded_server}/series/add")
    open_result(page)
    expect(page.get_by_role("button", name="Retry preview", exact=True)).to_be_visible()
    expect(page.get_by_role("button", name="Add series", exact=True)).to_be_disabled()


def test_rejected_add_requires_new_preview_and_does_not_double_submit(authed_page, seeded_server):
    page = authed_page
    held = []
    previews = []

    def respond(route):
        previews.append(True)
        route.fulfill(json=preview())

    page.route("**/api/v1/metadata/series/preview", respond)
    page.route("**/api/v1/series", lambda route: held.append(route))
    page.goto(f"{seeded_server}/series/add")
    open_result(page)
    button = page.get_by_role("button", name="Add series", exact=True)
    expect(button).to_be_enabled()
    button.click()
    expect(page.get_by_role("button", name="Adding...", exact=True)).to_be_disabled()
    page.evaluate("Alpine.$data(document.getElementById('add-series-app')).addSeriesToLibrary()")
    page.keyboard.press("Escape")
    expect(page.get_by_test_id("add-series-dialog")).to_be_visible()
    assert len(held) == 1
    held[0].fulfill(status=409, json={"detail": "Source settings changed. Preview again."})
    expect(page.get_by_role("alert").filter(has_text="Source settings changed")).to_be_visible()
    expect(button).to_be_disabled()
    page.get_by_role("button", name="Retry preview", exact=True).click()
    expect(button).to_be_enabled()
    assert len(previews) == 2


def test_real_local_search_preview_add_and_existing_owner(
    authed_page, seeded_server, monkeypatch, tmp_path
):
    from pullbox.api.v1 import series as series_api
    from pullbox.providers.metadata import sources
    from pullbox.services.catalog import reader as catalog_reader
    from tests.unit.test_catalog_reader import installed_reader

    reader = installed_reader(tmp_path)
    monkeypatch.setattr(catalog_reader, "get_catalog_reader", lambda: reader)
    monkeypatch.setattr(sources, "get_catalog_reader", lambda: reader)
    # This test owns the browser/database/folder workflow, not scheduled downloads.
    events = AsyncMock()
    monkeypatch.setattr(series_api, "get_event_bus", lambda: events)
    page = authed_page
    page.goto(f"{seeded_server}/series/add?q=Dark+Knight")
    trigger = page.locator('[data-add-series-trigger="true"]').first
    expect(trigger).to_have_attribute("data-series-source", "comicvine_local")
    trigger.click()
    dialog = page.get_by_test_id("add-series-dialog")
    button = dialog.get_by_role("button", name="Add series", exact=True)
    expect(button).to_be_enabled()
    with page.expect_response(
        lambda response: (
            response.url.endswith("/api/v1/series") and response.request.method == "POST"
        )
    ) as added:
        button.click()
    response = added.value
    assert response.status == 201, response.text()
    record = response.json()
    assert record["comicvine_id"] == 10 and record["title"] == "Batman"
    assert Path(record["path"]).is_dir()
    expect(dialog).not_to_be_visible()
    expect(page.get_by_test_id("add-series-existing-title-link")).to_have_attribute(
        "href", f"/series/{record['id']}"
    )
    assert events.emit.await_count == 1


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 1280), ("light", 320)])
def test_preview_dialog_keyboard_reflow_and_accessibility(authed_page, seeded_server, theme, width):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 900})
    page.emulate_media(reduced_motion="reduce")
    result = preview()
    result["series"]["data"]["title"] = '<img src=x onerror="window.injected=true">'
    page.route("**/api/v1/metadata/series/preview", lambda route: route.fulfill(json=result))
    page.goto(f"{seeded_server}/series/add")
    page.evaluate("theme => applyTheme(theme)", theme)
    trigger = page.get_by_test_id("add-series-search-input")
    trigger.focus()
    open_result(page)
    dialog = page.get_by_test_id("add-series-dialog")
    expect(dialog.get_by_role("button", name="Add series", exact=True)).to_be_enabled()
    assert page.evaluate("window.injected") is None
    assert dialog.evaluate("el => el.scrollWidth <= el.clientWidth + 1")
    page.keyboard.press("Shift+Tab")
    expect(dialog.get_by_role("button", name="Add series", exact=True)).to_be_focused()
    page.keyboard.press("Tab")
    expect(dialog.get_by_role("button", name="Close add series dialog")).to_be_focused()
    assert_no_axe_violations(
        page, name=f"source-add-{theme}-{width}", include=["[data-testid='add-series-dialog']"]
    )
    dialog.screenshot(path=f"test-results/source-add-{theme}-{width}.png", animations="disabled")
    page.keyboard.press("Escape")
    expect(dialog).not_to_be_visible()
    expect(trigger).to_be_focused()
