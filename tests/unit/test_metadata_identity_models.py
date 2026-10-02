"""Production identity ownership uses concrete parents and retained history."""

import pytest

from pullbox.models import Base


@pytest.mark.parametrize("kind,parent", [("series", "series"), ("issue", "issues")])
def test_active_identity_tables_are_registered_with_real_foreign_keys(kind, parent):
    name = f"{kind}_external_identities"
    assert name in Base.metadata.tables
    table = Base.metadata.tables[name]
    assert {fk.target_fullname for fk in table.foreign_keys} == {f"{parent}.id"}
    assert all(fk.ondelete == "CASCADE" for fk in table.foreign_keys)
    assert {
        "identity_namespace",
        "external_id",
        "verification_state",
        "evidence_kind",
        "resource_url",
        "evidence_locator",
        "verified_at",
        "last_seen_at",
        "revision",
        "created_at",
        "updated_at",
    } <= set(table.c.keys())


@pytest.mark.parametrize(
    "kind,parent", [("series", "series"), ("issue", "issues"), ("story_arc", "story_arcs")]
)
def test_history_belongs_to_parent_not_attachment_or_provider_configuration(kind, parent):
    name = f"{kind}_identity_events"
    assert name in Base.metadata.tables
    table = Base.metadata.tables[name]
    assert {fk.target_fullname for fk in table.foreign_keys} == {f"{parent}.id"}
    assert all(fk.ondelete == "CASCADE" for fk in table.foreign_keys)
    assert {
        "event_key",
        "request_fingerprint",
        "request_json",
        "verification_state",
        "evidence_kind",
        "created_at",
    } <= set(table.c.keys())
    assert "updated_at" not in table.c
