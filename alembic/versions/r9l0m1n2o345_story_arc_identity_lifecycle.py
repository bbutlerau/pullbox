"""Add canonical Story Arc identity lifecycle without changing import scopes."""

import hashlib
import json

import sqlalchemy as sa

from alembic import op

revision = "r9l0m1n2o345"
down_revision = "q8k9l0m1n234"
branch_labels = None
depends_on = None

_NAME = "story_arc_external_identities"
_PAGE = 200
_OPERATION = "593a9848-6071-40d7-9cd5-fffece1dc7f4"
_PROVIDERS = ("comicvine", "metron", "gcd", "locg")
_STATES = ("observed", "verified", "conflicted", "stale", "rejected")
_EVIDENCE = (
    "legacy_backfill",
    "mylar_database",
    "series_json",
    "comicinfo_xml",
    "metroninfo_xml",
    "provider_result",
    "provider_crosswalk",
    "user_selection",
    "migration",
)
_FIELDS = (
    "verification_state",
    "evidence_kind",
    "evidence_locator",
    "verified_at",
    "last_seen_at",
    "revision",
)
_CHECKS = {
    "identityverificationstate": "verification_state IN " + str(_STATES),
    "identityevidencekind": "evidence_kind IN " + str(_EVIDENCE),
    "ck_story_arc_identity_active_state": "verification_state IN ('verified','stale','conflicted')",
    "ck_story_arc_identity_revision": "revision > 0",
}


def _json(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _tables() -> tuple[sa.Table, sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return tuple(
        sa.Table(name, metadata, autoload_with=op.get_bind())
        for name in (
            "story_arcs",
            _NAME,
            "story_arc_identity_events",
        )
    )


def _canonical(active: sa.Table):
    return sa.and_(active.c.namespace == "story_arc", active.c.source.in_(_PROVIDERS))


def _pages(table: sa.Table, condition=None):
    cursor = 0
    while (
        rows := op.get_bind()
        .execute(
            sa.select(table)
            .where(table.c.id > cursor, condition if condition is not None else sa.true())
            .order_by(table.c.id)
            .limit(_PAGE)
        )
        .mappings()
        .all()
    ):
        cursor = rows[-1].id
        yield rows


def _fail() -> None:
    raise RuntimeError(
        "Story Arc identity evidence needs review before migration or downgrade; "
        "no ownership may be discarded."
    )


def _valid(value: str) -> bool:
    return bool(
        value and len(value) <= 255 and value.isascii() and value.isdecimal() and value[0] != "0"
    )


def _preflight(arcs: sa.Table, active: sa.Table) -> None:
    conn = op.get_bind()
    if conn.execute(
        sa.select(active.c.story_arc_id)
        .where(_canonical(active))
        .group_by(active.c.story_arc_id, active.c.source)
        .having(sa.func.count() > 1)
        .limit(1)
    ).first():
        _fail()
    for rows in _pages(active, _canonical(active)):
        for row in rows:
            if not _valid(row.external_id) or (
                row.source == "comicvine" and int(row.external_id) >= 2**63
            ):
                _fail()
        parents = dict(
            conn.execute(
                sa.select(arcs.c.id, arcs.c.comicvine_id).where(
                    arcs.c.id.in_([row.story_arc_id for row in rows])
                )
            ).all()
        )
        cv_rows = [row for row in rows if row.source == "comicvine"]
        owners = {
            value: local_id
            for local_id, value in conn.execute(
                sa.select(arcs.c.id, arcs.c.comicvine_id).where(
                    sa.cast(arcs.c.comicvine_id, sa.BigInteger).in_(
                        [int(row.external_id) for row in cv_rows]
                    )
                )
            )
        }
        for row in rows:
            if row.story_arc_id not in parents:
                _fail()
            if row.source == "comicvine" and (
                parents[row.story_arc_id] not in (None, int(row.external_id))
                or owners.get(int(row.external_id), row.story_arc_id) != row.story_arc_id
            ):
                _fail()


def _expected(local_id: int, namespace: str, external_id: str) -> tuple[dict, dict]:
    # Frozen v1 event envelope; do not import mutable runtime event policy.
    origin = {"record_kind": "story_arc", "record_id": local_id}
    evidence_revision = _digest(f"legacy_arc:{namespace}:{local_id}:{external_id}")
    slot = {
        "version": 1,
        "operation_id": _OPERATION,
        "entity_kind": "story_arc",
        "local_id": local_id,
        "identity_namespace": namespace,
        "action": "verify",
        "origin": origin,
        "evidence_revision": evidence_revision,
    }
    payload = {
        "version": 1,
        "operation_id": _OPERATION,
        "local_id": local_id,
        "identity": {
            "namespace": namespace,
            "entity_kind": "story_arc",
            "external_id": external_id,
        },
        "action": "verify",
        "evidence_kind": "legacy_backfill",
        "evidence_revision": evidence_revision,
        "origin": origin,
        "actor": "migration",
        "actor_user_id": None,
        "review_revision": None,
    }
    serialized = _json(payload)
    owner = {
        "verification_state": "verified",
        "evidence_kind": "legacy_backfill",
        "evidence_locator": _json(origin),
        "verified_at": None,
        "last_seen_at": None,
        "revision": 1,
    }
    event = {
        "story_arc_id": local_id,
        "identity_namespace": namespace,
        "external_id": external_id,
        "verification_state": "verified",
        "evidence_kind": "legacy_backfill",
        "event_key": _digest(_json(slot)),
        "request_fingerprint": _digest(serialized),
        "request_json": serialized,
    }
    return owner, event


def _equal(row, expected: dict) -> bool:
    return all(row[key] == value for key, value in expected.items())


def _validate_events(active: sa.Table, events: sa.Table) -> None:
    for rows in _pages(events):
        keys = [(row.story_arc_id, row.identity_namespace, row.external_id) for row in rows]
        owners = {
            tuple(row): True
            for row in op.get_bind().execute(
                sa.select(active.c.story_arc_id, active.c.source, active.c.external_id).where(
                    _canonical(active),
                    sa.tuple_(active.c.story_arc_id, active.c.source, active.c.external_id).in_(
                        keys
                    ),
                )
            )
        }
        for row in rows:
            if (
                row.story_arc_id,
                row.identity_namespace,
                row.external_id,
            ) not in owners or not _equal(
                row, _expected(row.story_arc_id, row.identity_namespace, row.external_id)[1]
            ):
                _fail()


def upgrade() -> None:
    conn = op.get_bind()
    arcs, active, events = _tables()
    _preflight(arcs, active)
    # Refuse prior decisions before SQLite performs non-transactional DDL.
    _validate_events(active, events)
    existing = {col["name"] for col in sa.inspect(conn).get_columns(_NAME)}
    columns = (
        sa.Column("verification_state", sa.String(max(map(len, _STATES)))),
        sa.Column("evidence_kind", sa.String(max(map(len, _EVIDENCE)))),
        sa.Column("evidence_locator", sa.Text),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("last_seen_at", sa.DateTime(timezone=True)),
        sa.Column("revision", sa.Integer, nullable=False, server_default="1"),
    )
    for column in columns:
        if column.name not in existing:
            op.add_column(_NAME, column)
    arcs, active, events = _tables()
    # Preserve column-only ComicVine arcs as canonical relations too.
    for rows in _pages(arcs, arcs.c.comicvine_id > 0):
        present = set(
            conn.execute(
                sa.select(active.c.story_arc_id).where(
                    active.c.story_arc_id.in_([row.id for row in rows]),
                    active.c.source == "comicvine",
                    active.c.namespace == "story_arc",
                )
            ).scalars()
        )
        values = [
            {
                "story_arc_id": row.id,
                "source": "comicvine",
                "namespace": "story_arc",
                "external_id": str(row.comicvine_id),
                "source_url": row.comicvine_url,
                "evidence": {},
            }
            for row in rows
            if row.id not in present
        ]
        if values:
            conn.execute(sa.insert(active), values)
    for rows in _pages(active, _canonical(active)):
        expected = [
            (row, *_expected(row.story_arc_id, row.source, row.external_id)) for row in rows
        ]
        prior = {
            row.event_key: row
            for row in conn.execute(
                sa.select(events).where(
                    events.c.event_key.in_([event["event_key"] for _, _, event in expected])
                )
            ).mappings()
        }
        creates = []
        for row, owner, event in expected:
            untouched = (
                all(row[field] is None for field in _FIELDS if field != "revision")
                and row.revision == 1
            )
            if not untouched and not _equal(row, owner):
                _fail()
            if untouched:
                conn.execute(sa.update(active).where(active.c.id == row.id).values(**owner))
            if event["event_key"] in prior:
                if not _equal(prior[event["event_key"]], event):
                    _fail()
            else:
                creates.append(event)
        if creates:
            conn.execute(sa.insert(events), creates)
    checks = {row["name"] for row in sa.inspect(conn).get_check_constraints(_NAME)}
    if missing := _CHECKS.keys() - checks:
        with op.batch_alter_table(_NAME) as batch:
            for name in sorted(missing):
                batch.create_check_constraint(name, _CHECKS[name])
    indexes = {row["name"] for row in sa.inspect(conn).get_indexes(_NAME)}
    if "uq_story_arc_canonical_provider" not in indexes:
        predicate = sa.text(
            "namespace = 'story_arc' AND source IN ('comicvine','metron','gcd','locg')"
        )
        op.create_index(
            "uq_story_arc_canonical_provider",
            _NAME,
            ["story_arc_id", "source"],
            unique=True,
            sqlite_where=predicate,
            postgresql_where=predicate,
        )


def downgrade() -> None:
    arcs, active, events = _tables()
    _preflight(arcs, active)
    _validate_events(active, events)
    for rows in _pages(active):
        for row in rows:
            expected = (
                _expected(row.story_arc_id, row.source, row.external_id)[0]
                if (row.namespace == "story_arc" and row.source in _PROVIDERS)
                else {field: 1 if field == "revision" else None for field in _FIELDS}
            )
            if not _equal(row, expected):
                _fail()
    op.get_bind().execute(sa.delete(events))
    op.drop_index("uq_story_arc_canonical_provider", table_name=_NAME)
    with op.batch_alter_table(_NAME) as batch:
        for name in _CHECKS:
            batch.drop_constraint(name, type_="check")
        for field in reversed(_FIELDS):
            batch.drop_column(field)
