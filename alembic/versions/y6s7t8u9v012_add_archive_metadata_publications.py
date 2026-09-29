"""Retain paired archive publication intent across process and DB failures."""

import sqlalchemy as sa

from alembic import op

revision = "y6s7t8u9v012"
down_revision = "x5r6s7t8u901"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "archive_metadata_publications",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("operation_id", sa.String(36), nullable=False, unique=True),
        sa.Column(
            "library_file_id", sa.Integer(), sa.ForeignKey("library_files.id", ondelete="SET NULL")
        ),
        sa.Column(
            "active_file_id",
            sa.Integer(),
            sa.ForeignKey("library_files.id", ondelete="SET NULL"),
            unique=True,
        ),
        sa.Column("active_path_key", sa.String(64), unique=True),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(9), nullable=False),
        sa.Column("plan_json", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("revision > 0", name="ck_archive_publication_revision"),
        sa.CheckConstraint(
            "length(plan_json) BETWEEN 2 AND 4194304", name="ck_archive_publication_size"
        ),
        sa.CheckConstraint(
            "state IN ('intended','published','abandoned','review')",
            name="archive_publication_state",
        ),
        sa.CheckConstraint(
            "(state = 'abandoned' AND active_file_id IS NULL AND active_path_key IS NULL) OR "
            "(state <> 'abandoned' AND active_path_key IS NOT NULL)",
            name="ck_archive_publication_reservation",
        ),
    )


def downgrade() -> None:
    table = sa.table("archive_metadata_publications", sa.column("state"))
    if op.get_bind().scalar(
        sa.select(sa.func.count()).select_from(table).where(table.c.state != "abandoned")
    ):
        raise RuntimeError("Resolve retained archive publication work before downgrading.")
    op.drop_table("archive_metadata_publications")
