"""Track terminal removal workspace cleanup without discarding journal evidence."""

import sqlalchemy as sa

from alembic import op

revision = "f3z4a5b6c789"
down_revision = "e2y3z4a5b678"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "library_removals",
        sa.Column("workspace_cleaned", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "ix_library_removal_workspace_pending",
        "library_removals",
        ["workspace_cleaned", "active", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_library_removal_workspace_pending", table_name="library_removals")
    op.drop_column("library_removals", "workspace_cleaned")
