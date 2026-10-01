"""Allow paired metadata jobs without changing utility history or lifecycle."""

from sqlalchemy import text

from alembic import op

revision = "h5b6c7d8e901"
down_revision = "g4a5b6c7d890"
branch_labels = None
depends_on = None

_OLD = (
    "file_convert",
    "mass_convert_pipeline",
    "mass_rename",
    "db_check_cleanup",
    "series_rescan",
    "export_library",
    "integrity_check",
    "library_permissions",
    "rollback",
)


def _replace(values: tuple[str, ...]) -> None:
    with op.batch_alter_table("utility_jobs") as batch:
        batch.drop_constraint("ck_utility_jobs_job_type", type_="check")
        batch.create_check_constraint(
            "ck_utility_jobs_job_type", "job_type IN (" + ", ".join(repr(v) for v in values) + ")"
        )


def upgrade() -> None:
    op.create_index("ix_import_files_library_file_id", "import_files", ["library_file_id"])
    _replace((*_OLD, "file_metadata"))


def downgrade() -> None:
    if op.get_bind().scalar(
        text("SELECT count(*) FROM utility_jobs WHERE job_type = 'file_metadata'")
    ):
        raise RuntimeError("Remove saved file metadata jobs before downgrading.")
    _replace(_OLD)
    op.drop_index("ix_import_files_library_file_id", table_name="import_files")
