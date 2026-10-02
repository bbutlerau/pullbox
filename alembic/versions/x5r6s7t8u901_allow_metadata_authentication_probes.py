"""Allow a bounded deliberate probe without discarding an authentication hold."""

import sqlalchemy as sa

from alembic import op

revision = "x5r6s7t8u901"
down_revision = "w4q5r6s7t890"
branch_labels = None
depends_on = None


def _constraint(*, allow_probe: bool) -> None:
    auth_lease = "" if allow_probe else " AND lease_until IS NULL"
    with op.batch_alter_table("metadata_source_accounts") as batch:
        batch.drop_constraint("ck_metadata_account_state", type_="check")
        batch.create_check_constraint(
            "ck_metadata_account_state",
            "(status IS NULL AND retry_at IS NULL AND lease_until IS NULL) OR "
            "(status IS NOT NULL AND status = 'authentication_failed' "
            f"AND retry_at IS NULL{auth_lease}) OR "
            "(status IS NOT NULL AND status IN ('rate_limited','timeout','unavailable') "
            "AND retry_at IS NOT NULL)",
        )


def upgrade() -> None:
    _constraint(allow_probe=True)


def downgrade() -> None:
    # Retain the authentication hold; only the unsupported active probe is dropped.
    account = sa.table(
        "metadata_source_accounts",
        sa.column("status"),
        sa.column("lease_until"),
        sa.column("revision"),
    )
    op.execute(
        account.update()
        .where(account.c.status == "authentication_failed", account.c.lease_until.isnot(None))
        .values(lease_until=None, revision=account.c.revision + 1)
    )
    _constraint(allow_probe=False)
