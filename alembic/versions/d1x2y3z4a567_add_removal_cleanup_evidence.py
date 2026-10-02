"""Retain bounded backup and cleanup evidence after removal detachment."""

import sqlalchemy as sa

from alembic import op

revision = "d1x2y3z4a567"
down_revision = "c0w1x2y3z456"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "library_removals",
        sa.Column(
            "cleanup_json",
            sa.Text(),
            sa.CheckConstraint(
                "cleanup_json IS NULL OR length(cleanup_json) BETWEEN 2 AND 4096",
                name="ck_library_removal_cleanup_size",
            ),
            nullable=True,
        ),
    )


def downgrade() -> None:
    table = sa.table("library_removals", sa.column("cleanup_json"))
    if op.get_bind().scalar(
        sa.select(sa.func.count()).select_from(table).where(table.c.cleanup_json.is_not(None))
    ):
        raise RuntimeError("Resolve retained removal cleanup evidence before downgrading.")
    with op.batch_alter_table("library_removals") as batch:
        batch.drop_constraint("ck_library_removal_cleanup_size", type_="check")
        batch.drop_column("cleanup_json")
