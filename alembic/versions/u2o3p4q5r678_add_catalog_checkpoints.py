"""Keep full-catalog checkpoints bound to their source and verified identity."""

import sqlalchemy as sa

from alembic import op

revision = "u2o3p4q5r678"
down_revision = "t1n2o3p4q567"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing global timestamps do not prove which provider supplied a catalog.
    op.create_table(
        "series_catalog_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "series_id",
            sa.Integer(),
            sa.ForeignKey("series.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "source",
            sa.String(30),
            sa.ForeignKey("metadata_source_configs.source", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_revision", sa.Integer(), nullable=False),
        sa.Column(
            "identity_id",
            sa.Integer(),
            sa.ForeignKey("series_external_identities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("identity_revision", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("full_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("series_id", "source", name="uq_catalog_checkpoint_series_source"),
        sa.CheckConstraint(
            "revision > 0 AND source_revision > 0 AND identity_revision > 0",
            name="ck_catalog_checkpoint_revisions",
        ),
        sa.CheckConstraint(
            "length(external_id) BETWEEN 1 AND 255", name="ck_catalog_checkpoint_id"
        ),
        sa.CheckConstraint("full_synced_at <= checked_at", name="ck_catalog_checkpoint_time"),
        sa.CheckConstraint(
            "source NOT IN ('comicvine_local', 'gcd_local') OR source_updated_at IS NOT NULL",
            name="ck_catalog_checkpoint_generation",
        ),
    )
    op.create_index("ix_catalog_checkpoint_source", "series_catalog_checkpoints", ["source"])
    op.create_index("ix_catalog_checkpoint_identity", "series_catalog_checkpoints", ["identity_id"])


def downgrade() -> None:
    # Discard only sync progress, never canonical metadata, files or ownership.
    op.drop_table("series_catalog_checkpoints")
