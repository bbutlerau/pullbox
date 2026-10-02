"""Retain completed canonical publication receipts without active reservations."""

import sqlalchemy as sa

from alembic import op

revision = "z7t8u9v0w123"
down_revision = "y6s7t8u9v012"
branch_labels = None
depends_on = None


def _constraints(*, finalized: bool) -> None:
    terminal = "'abandoned','finalized'" if finalized else "'abandoned'"
    states = "'intended','published','abandoned','review'" + (",'finalized'" if finalized else "")
    with op.batch_alter_table("archive_metadata_publications") as batch:
        batch.drop_constraint("archive_publication_state", type_="check")
        batch.drop_constraint("ck_archive_publication_reservation", type_="check")
        batch.create_check_constraint("archive_publication_state", f"state IN ({states})")
        batch.create_check_constraint(
            "ck_archive_publication_reservation",
            f"(state IN ({terminal}) AND active_file_id IS NULL AND active_path_key IS NULL) OR "
            f"(state NOT IN ({terminal}) AND active_path_key IS NOT NULL)",
        )


def upgrade() -> None:
    _constraints(finalized=True)


def downgrade() -> None:
    table = sa.table("archive_metadata_publications", sa.column("state"))
    if op.get_bind().scalar(
        sa.select(sa.func.count()).select_from(table).where(table.c.state == "finalized")
    ):
        raise RuntimeError("Cannot downgrade completed archive publication evidence.")
    _constraints(finalized=False)
