"""Persist canonical metadata baselines without inventing legacy provenance."""

import sqlalchemy as sa

from alembic import op

revision = "t1n2o3p4q567"
down_revision = "s0m1n2o3p456"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for kind, parent in (("series", "series"), ("issue", "issues"), ("story_arc", "story_arcs")):
        op.create_table(
            f"{kind}_metadata_baselines",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                f"{kind}_id",
                sa.Integer(),
                sa.ForeignKey(f"{parent}.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("revision", sa.Integer(), nullable=False),
            sa.Column("snapshot_json", sa.Text(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint(f"{kind}_id", name=f"uq_{kind}_metadata_baseline_target"),
            sa.CheckConstraint("revision > 0", name=f"ck_{kind}_metadata_baseline_revision"),
            sa.CheckConstraint(
                "length(snapshot_json) BETWEEN 2 AND 1048576",
                name=f"ck_{kind}_metadata_baseline_size",
            ),
        )


def downgrade() -> None:
    # Downgrade discards provenance, never entities, ownership, or source files.
    for kind in ("story_arc", "issue", "series"):
        op.drop_table(f"{kind}_metadata_baselines")
