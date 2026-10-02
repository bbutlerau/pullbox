"""Add provider identity ownership and retained evidence history."""

import sqlalchemy as sa

from alembic import op

revision = "p7j8k9l0m123"
down_revision = "o6i7j8k9l012"
branch_labels = None
depends_on = None

_KINDS = (("series", "series"), ("issue", "issues"), ("story_arc", "story_arcs"))
_NAMESPACES = ("comicvine", "metron", "gcd", "locg")
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


def _enum(name: str, values: tuple[str, ...]) -> sa.Enum:
    return sa.Enum(*values, name=name, native_enum=False, create_constraint=True)


def _decimal_id_check() -> str:
    remaining = "external_id"
    for digit in "0123456789":
        remaining = f"replace({remaining}, '{digit}', '')"
    return (
        "length(external_id) BETWEEN 1 AND 255 AND "
        "substr(external_id, 1, 1) IN ('1','2','3','4','5','6','7','8','9') AND "
        f"{remaining} = ''"
    )


def _claim_columns(kind: str, parent: str) -> list[sa.Column]:
    return [
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            f"{kind}_id",
            sa.Integer,
            sa.ForeignKey(f"{parent}.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("identity_namespace", _enum("identitynamespace", _NAMESPACES), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column(
            "verification_state", _enum("identityverificationstate", _STATES), nullable=False
        ),
        sa.Column("evidence_kind", _enum("identityevidencekind", _EVIDENCE), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    ]


def upgrade() -> None:
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    for kind, parent in _KINDS:
        name = f"{kind}_external_identities"
        if kind != "story_arc" and name not in existing:
            op.create_table(
                name,
                *_claim_columns(kind, parent),
                sa.Column("resource_url", sa.String(1000)),
                sa.Column("evidence_locator", sa.Text),
                sa.Column("verified_at", sa.DateTime(timezone=True)),
                sa.Column("last_seen_at", sa.DateTime(timezone=True)),
                sa.Column("revision", sa.Integer, server_default="1", nullable=False),
                sa.Column(
                    "updated_at",
                    sa.DateTime(timezone=True),
                    server_default=sa.func.now(),
                    nullable=False,
                ),
                sa.UniqueConstraint("identity_namespace", "external_id", name=f"uq_{name}_owner"),
                sa.UniqueConstraint(
                    f"{kind}_id", "identity_namespace", name=f"uq_{name}_namespace"
                ),
                sa.CheckConstraint(
                    "verification_state IN ('verified', 'stale', 'conflicted')",
                    name=f"ck_{name}_active_state",
                ),
                sa.CheckConstraint(_decimal_id_check(), name=f"ck_{name}_decimal_id"),
                sa.CheckConstraint("revision > 0", name=f"ck_{name}_revision"),
            )
        name = f"{kind}_identity_events"
        if name not in existing:
            op.create_table(
                name,
                *_claim_columns(kind, parent),
                sa.Column("event_key", sa.String(64), nullable=False),
                sa.Column("request_fingerprint", sa.String(64), nullable=False),
                sa.Column("request_json", sa.Text, nullable=False),
                sa.UniqueConstraint(f"{kind}_id", "event_key", name=f"uq_{name}_retry"),
                sa.CheckConstraint(_decimal_id_check(), name=f"ck_{name}_decimal_id"),
                sa.CheckConstraint("length(event_key) = 64", name=f"ck_{name}_event_key"),
                sa.CheckConstraint(
                    "length(request_fingerprint) = 64", name=f"ck_{name}_fingerprint"
                ),
                sa.CheckConstraint(
                    "length(request_json) BETWEEN 2 AND 8192", name=f"ck_{name}_request"
                ),
            )
        # A SQLite DDL interruption can leave the table without its index.
        indexes = {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(name)}
        if f"ix_{name}_claim" not in indexes:
            op.create_index(
                f"ix_{name}_claim", name, [f"{kind}_id", "identity_namespace", "external_id", "id"]
            )


def downgrade() -> None:
    names = [f"{kind}_identity_events" for kind, _ in _KINDS]
    names += ["issue_external_identities", "series_external_identities"]
    for name in names:
        table = sa.table(name, sa.column("id"))
        if op.get_bind().execute(sa.select(table.c.id).limit(1)).first() is not None:
            raise RuntimeError(
                "Cannot remove metadata identity storage while retained identity data exists."
            )
    for name in names:
        op.drop_table(name)
