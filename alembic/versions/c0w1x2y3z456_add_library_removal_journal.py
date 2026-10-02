"""Retain authorization and private staging evidence for library removal."""

import sqlalchemy as sa

from alembic import op

revision = "c0w1x2y3z456"
down_revision = "b9v0w1x2y345"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "library_removals",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("operation_id", sa.String(36), nullable=False, unique=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("state", sa.String(10), nullable=False),
        sa.Column("plan_json", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "state IN ('intended','detached','complete','abandoned','review')",
            name="ck_library_removal_state",
        ),
        sa.CheckConstraint(
            "length(plan_json) BETWEEN 2 AND 65536", name="ck_library_removal_plan_size"
        ),
    )
    op.create_index("ix_library_removals_active", "library_removals", ["active"])


def downgrade() -> None:
    table = sa.table("library_removals", sa.column("state"))
    if op.get_bind().scalar(
        sa.select(sa.func.count()).select_from(table).where(table.c.state != "abandoned")
    ):
        raise RuntimeError("Resolve retained library removal evidence before downgrading.")
    op.drop_table("library_removals")
