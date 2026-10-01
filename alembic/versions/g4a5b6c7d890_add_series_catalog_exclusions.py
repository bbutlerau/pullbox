"""Retain explicit GCD Add exclusions independently of issue ownership."""

import sqlalchemy as sa

from alembic import op

revision = "g4a5b6c7d890"
down_revision = "f3z4a5b6c789"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "series", sa.Column("catalog_exclusions", sa.JSON(), nullable=False, server_default="[]")
    )


def downgrade() -> None:
    table = sa.table("series", sa.column("catalog_exclusions", sa.JSON()))
    if (
        op.get_bind()
        .execute(
            sa.select(table.c.catalog_exclusions)
            .where(sa.cast(table.c.catalog_exclusions, sa.String()) != "[]")
            .limit(1)
        )
        .first()
        is not None
    ):
        raise RuntimeError(
            "Retained catalog exclusions prevent downgrade; preserve the review first."
        )
    op.drop_column("series", "catalog_exclusions")
