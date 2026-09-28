"""Separate deferred metadata source work from the sweep cursor."""

import sqlalchemy as sa

from alembic import op

revision = "v3p4q5r6s789"
down_revision = "u2o3p4q5r678"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "metadata_series_retries",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_id", sa.String(30), nullable=False),
        sa.Column(
            "series_id",
            sa.Integer(),
            sa.ForeignKey("series.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("config_key", sa.String(64), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
        sa.Column("retry_at", sa.DateTime(timezone=True)),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("task_id", "series_id", "source", name="uq_metadata_series_retry"),
        sa.CheckConstraint(
            "task_id IN ('sync_new_issues','refresh_metadata')", name="ck_metadata_retry_task"
        ),
        sa.CheckConstraint(
            "source IN ('*','comicvine_local','comicvine_api','metron_api',"
            "'gcd_local','gcd_api_v2')",
            name="ck_metadata_retry_source",
        ),
        sa.CheckConstraint("revision > 0", name="ck_metadata_retry_revision"),
        sa.CheckConstraint(
            "status IN ('rate_limited','timeout','unavailable','authentication_failed')",
            name="ck_metadata_retry_status",
        ),
        sa.CheckConstraint(
            "(status = 'authentication_failed' AND retry_at IS NULL) OR "
            "(status != 'authentication_failed' AND retry_at IS NOT NULL)",
            name="ck_metadata_retry_deadline",
        ),
    )
    op.create_index(
        "ix_metadata_retry_due", "metadata_series_retries", ["task_id", "retry_at", "series_id"]
    )


def downgrade() -> None:
    op.drop_table("metadata_series_retries")
