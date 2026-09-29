"""Index retained removal trash receipts for bounded retention lookups."""

import hashlib
import json
import os
from pathlib import Path

import sqlalchemy as sa

from alembic import op

revision = "e2y3z4a5b678"
down_revision = "d1x2y3z4a567"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("library_removals", sa.Column("trash_path_key", sa.String(64), nullable=True))
    table = sa.table(
        "library_removals", sa.column("id"), sa.column("plan_json"), sa.column("trash_path_key")
    )
    connection = op.get_bind()
    last_id = 0
    while rows := connection.execute(
        sa.select(table.c.id, table.c.plan_json)
        .where(table.c.id > last_id)
        .order_by(table.c.id)
        .limit(8)
    ).all():
        for row_id, encoded in rows:
            last_id = row_id
            if not isinstance(encoded, str) or len(encoded.encode("utf-8")) > 65536:
                raise RuntimeError("Invalid retained removal evidence; repair before upgrading.")
            plan = json.loads(encoded)
            if not isinstance(plan, dict):
                raise RuntimeError("Invalid retained removal plan; repair before upgrading.")
            value = plan.get("trash_path")
            if value is None:
                continue
            if (
                not isinstance(value, str)
                or not Path(value).is_absolute()
                or ".." in Path(value).parts
            ):
                raise RuntimeError("Invalid retained trash path; repair before upgrading.")
            connection.execute(
                table.update()
                .where(table.c.id == row_id)
                .values(trash_path_key=hashlib.sha256(os.fsencode(value)).hexdigest())
            )
    op.create_index("ix_library_removals_trash_path_key", "library_removals", ["trash_path_key"])


def downgrade() -> None:
    op.drop_index("ix_library_removals_trash_path_key", table_name="library_removals")
    op.drop_column("library_removals", "trash_path_key")
