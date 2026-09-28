"""Persist provider account cooldowns independently of individual series retries."""

import sqlalchemy as sa

from alembic import op

revision = "w4q5r6s7t890"
down_revision = "v3p4q5r6s789"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "metadata_source_accounts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(30), nullable=False, unique=True),
        sa.Column("account_key", sa.String(64), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(30)),
        sa.Column("retry_at", sa.DateTime(timezone=True)),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "source IN ('comicvine_api','metron_api','gcd_api_v2')",
            name="ck_metadata_account_source",
        ),
        sa.CheckConstraint("revision > 0", name="ck_metadata_account_revision"),
        sa.CheckConstraint(
            "(status IS NULL AND retry_at IS NULL AND lease_until IS NULL) OR "
            "(status IS NOT NULL AND status = 'authentication_failed' "
            "AND retry_at IS NULL AND lease_until IS NULL) OR "
            "(status IS NOT NULL AND status IN ('rate_limited','timeout','unavailable') "
            "AND retry_at IS NOT NULL)",
            name="ck_metadata_account_state",
        ),
    )


def downgrade() -> None:
    op.drop_table("metadata_source_accounts")
