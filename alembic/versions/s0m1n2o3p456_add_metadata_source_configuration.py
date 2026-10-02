"""Add independent metadata source configuration and priority policy."""

import sqlalchemy as sa

from alembic import op

revision = "s0m1n2o3p456"
down_revision = "r9l0m1n2o345"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = op.create_table(
        "metadata_source_configs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(30), unique=True, nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("domain_priorities", sa.JSON(), nullable=False),
        sa.Column("settings", sa.JSON(), nullable=False),
        sa.Column("credential_secret", sa.Text()),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("last_tested_at", sa.DateTime(timezone=True)),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("last_status", sa.String(40)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "source IN ('comicvine_local','comicvine_api','metron_api','gcd_local','gcd_api_v2')",
            name="ck_metadata_source_slug",
        ),
        sa.CheckConstraint("priority BETWEEN 0 AND 1000", name="ck_metadata_source_priority"),
        sa.CheckConstraint("revision > 0", name="ck_metadata_source_revision"),
        sa.CheckConstraint(
            "credential_secret IS NULL OR source IN ('metron_api','gcd_api_v2')",
            name="ck_metadata_source_credential_owner",
        ),
    )
    op.bulk_insert(
        table,
        [
            {
                "source": source,
                "enabled": source.startswith("comicvine_"),
                "priority": (index + 1) * 10,
                "domain_priorities": {},
                "settings": {},
                "revision": 1,
            }
            for index, source in enumerate(
                ("comicvine_local", "comicvine_api", "metron_api", "gcd_local", "gcd_api_v2")
            )
        ],
    )


def downgrade() -> None:
    # Explicit schema downgrade removes source policy/credentials, not identities
    # or the existing ComicVine credential in system_config.
    op.drop_table("metadata_source_configs")
