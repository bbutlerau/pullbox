"""Existing-file metadata approval uses the stable modal and progress contracts."""

import pytest
from playwright.sync_api import expect

from tests.e2e.accessibility import assert_no_axe_violations

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize("theme,width", [("light", 1280), ("dark", 390), ("tron", 1280)])
def test_file_metadata_preview_write_progress_and_focus(authed_page, seeded_server, theme, width):
    page = authed_page
    page.set_viewport_size({"width": width, "height": 1000})
    state = {"job": None}
    endpoint = "**/api/v1/issues/1/file-metadata"
    page.route(endpoint + "/job", lambda route: route.fulfill(json=state))
    page.route(
        endpoint + "/preview",
        lambda route: route.fulfill(
            json={
                "file_id": 1,
                "file_name": "Batman 001.cbz",
                "review_key": "a" * 64,
                "unchanged": False,
                "documents": ["ComicInfo.xml", "MetronInfo.xml"],
                "changes": [
                    {
                        "document": "MetronInfo.xml",
                        "field": "Summary",
                        "before": None,
                        "after": "Preserved summary",
                    }
                ],
            }
        ),
    )

    def write(route):
        assert route.request.post_data_json == {"review_key": "a" * 64}
        state["job"] = {
            "id": "test",
            "state": "RUNNING",
            "percent": 42,
            "message": "Verifying preserved comic pages",
            "error": None,
        }
        route.fulfill(status=202, json={"job_id": "test", "state": "RUNNING"})

    page.route(endpoint + "/write", write)
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{seeded_server}/issues/1")
    page.evaluate("theme => applyTheme(theme)", theme)
    button = page.get_by_role("button", name="Write file metadata", exact=True)
    expect(button).to_be_visible()
    button.click()
    dialog = page.get_by_role("dialog", name="Write file metadata", exact=True)
    expect(dialog).to_be_visible()
    expect(dialog.get_by_text("Preserved summary", exact=False)).to_be_visible()
    assert_no_axe_violations(
        page,
        name=f"file-metadata-{theme}-{width}",
        include=['[aria-labelledby="file-metadata-title"]'],
    )
    dialog.get_by_role("button", name="Write both metadata files").click()
    expect(dialog.get_by_role("progressbar")).to_have_attribute("aria-valuenow", "42")
    expect(dialog.get_by_role("button", name="Cancel write")).to_be_visible()
    state["job"]["state"] = "COMPLETED"
    state["job"]["percent"] = 100
    expect(dialog.get_by_text("Both metadata files are up to date.")).to_be_visible()
    dialog.get_by_role("button", name="Preview again", exact=True).click()
    expect(dialog.get_by_role("button", name="Write both metadata files")).to_be_visible()
    dialog.get_by_role("button", name="Close", exact=True).click()
    expect(dialog).not_to_be_visible()
    expect(button).to_be_focused()
    assert not errors
