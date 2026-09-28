"""Preserve existing canonical ComicVine ownership in generic identity storage."""

import hashlib
import json

import sqlalchemy as sa

from alembic import op

revision = "q8k9l0m1n234"
down_revision = "p7j8k9l0m123"
branch_labels = None
depends_on = None

_PAGE_SIZE = 400
_OPERATION_ID = "b69082d3-830e-4a72-9ad7-818c9d169381"
_KINDS = (("series", "series"), ("issue", "issues"))


def _json(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _expected(kind: str, local_id: int, external_id: str) -> tuple[dict, dict]:
    """Frozen v1 event envelope; migrations must not import evolving runtime policy."""
    origin = {"record_kind": kind, "record_id": local_id}
    revision_digest = _digest(f"legacy_comicvine:{kind}:{local_id}:{external_id}")
    slot = {
        "version": 1,
        "operation_id": _OPERATION_ID,
        "entity_kind": kind,
        "local_id": local_id,
        "identity_namespace": "comicvine",
        "action": "verify",
        "origin": origin,
        "evidence_revision": revision_digest,
    }
    payload = {
        "version": 1,
        "operation_id": _OPERATION_ID,
        "local_id": local_id,
        "identity": {"namespace": "comicvine", "entity_kind": kind, "external_id": external_id},
        "action": "verify",
        "evidence_kind": "legacy_backfill",
        "evidence_revision": revision_digest,
        "origin": origin,
        "actor": "migration",
        "actor_user_id": None,
        "review_revision": None,
    }
    request_json = _json(payload)
    common = {
        f"{kind}_id": local_id,
        "identity_namespace": "comicvine",
        "external_id": external_id,
        "verification_state": "verified",
        "evidence_kind": "legacy_backfill",
    }
    return (
        {
            **common,
            "resource_url": None,
            "evidence_locator": _json(origin),
            "verified_at": None,
            "last_seen_at": None,
            "revision": 1,
        },
        {
            **common,
            "event_key": _digest(_json(slot)),
            "request_fingerprint": _digest(request_json),
            "request_json": request_json,
        },
    )


def _tables(kind: str, parent: str) -> tuple[sa.Table, sa.Table, sa.Table]:
    metadata = sa.MetaData()
    connection = op.get_bind()
    return tuple(
        sa.Table(name, metadata, autoload_with=connection)
        for name in (
            parent,
            f"{kind}_external_identities",
            f"{kind}_identity_events",
        )
    )


def _valid_id(value: object) -> bool:
    return type(value) is int and 0 < value < 2**63


def _check_equal(row: sa.RowMapping, expected: dict) -> None:
    if any(row[key] != value for key, value in expected.items()):
        raise RuntimeError(
            "Metadata identity data differs from its legacy backfill; "
            "review before retry or downgrade."
        )


def upgrade() -> None:
    connection = op.get_bind()
    for kind, parent in _KINDS:
        legacy, active, events = _tables(kind, parent)
        target_key = f"{kind}_id"
        cursor = 0
        while rows := connection.execute(
            sa.select(legacy.c.id, legacy.c.comicvine_id)
            .where(legacy.c.id > cursor)
            .order_by(legacy.c.id)
            .limit(_PAGE_SIZE)
        ).all():
            cursor = rows[-1].id
            valid = [row for row in rows if _valid_id(row.comicvine_id)]
            if not valid:
                continue
            target_ids = [row.id for row in valid]
            current = {
                row[target_key]: row
                for row in connection.execute(
                    sa.select(active).where(
                        active.c[target_key].in_(target_ids),
                        active.c.identity_namespace == "comicvine",
                    )
                ).mappings()
            }
            expected = [_expected(kind, row.id, str(row.comicvine_id)) for row in valid]
            prior_events = {
                row.event_key: row
                for row in connection.execute(
                    sa.select(events).where(
                        events.c.event_key.in_([event["event_key"] for _, event in expected]),
                        events.c[target_key].in_(target_ids),
                    )
                ).mappings()
            }
            new_owners, new_events = [], []
            for owner, event in expected:
                if owner[target_key] in current:
                    _check_equal(current[owner[target_key]], owner)
                else:
                    new_owners.append(owner)
                if event["event_key"] in prior_events:
                    _check_equal(prior_events[event["event_key"]], event)
                else:
                    new_events.append(event)
            if new_owners:
                connection.execute(sa.insert(active), new_owners)
            if new_events:
                connection.execute(sa.insert(events), new_events)


def downgrade() -> None:
    connection = op.get_bind()
    arc_events = sa.table("story_arc_identity_events", sa.column("id"))
    if connection.execute(sa.select(arc_events.c.id).limit(1)).first() is not None:
        raise RuntimeError("Cannot discard retained Story Arc identity history during downgrade.")
    tables = []
    # Preflight every row before deleting anything, including on SQLite.
    for kind, parent in _KINDS:
        legacy, active, events = _tables(kind, parent)
        for table, expected_index in ((active, 0), (events, 1)):
            tables.append(table)
            cursor = 0
            while (
                rows := connection.execute(
                    sa.select(table, legacy.c.comicvine_id)
                    .select_from(table.outerjoin(legacy, table.c[f"{kind}_id"] == legacy.c.id))
                    .where(table.c.id > cursor)
                    .order_by(table.c.id)
                    .limit(_PAGE_SIZE)
                )
                .mappings()
                .all()
            ):
                cursor = rows[-1]["id"]
                for row in rows:
                    if not _valid_id(row["comicvine_id"]):
                        raise RuntimeError(
                            "Cannot discard metadata identity data without an equivalent legacy ID."
                        )
                    expected = _expected(kind, row[f"{kind}_id"], str(row["comicvine_id"]))[
                        expected_index
                    ]
                    _check_equal(row, expected)
    for table in reversed(tables):
        connection.execute(sa.delete(table))
