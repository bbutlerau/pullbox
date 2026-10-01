"""The narrow job-type extension retains utility records and child rows."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError


def test_metadata_job_migration_preserves_utility_history(monkeypatch):
    path = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/h5b6c7d8e901_add_file_metadata_job.py"
    )
    spec = importlib.util.spec_from_file_location("file_metadata_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.exec_driver_sql(
            "CREATE TABLE utility_jobs (id TEXT PRIMARY KEY, job_type TEXT, "
            "CONSTRAINT ck_utility_jobs_job_type CHECK (job_type IN ('series_rescan')))"
        )
        connection.exec_driver_sql(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, "
            "job_id TEXT REFERENCES utility_jobs(id) ON DELETE CASCADE)"
        )
        connection.exec_driver_sql("INSERT INTO utility_jobs VALUES ('existing', 'series_rescan')")
        connection.exec_driver_sql("INSERT INTO children VALUES (1, 'existing')")
        connection.exec_driver_sql(
            "CREATE TABLE import_files (id INTEGER PRIMARY KEY, library_file_id INTEGER)"
        )
        connection.exec_driver_sql("INSERT INTO import_files VALUES (1, 7)")
        with pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "INSERT INTO utility_jobs VALUES ('writer', 'file_metadata')"
            )
        monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
        migration.upgrade()
        plan = connection.exec_driver_sql(
            "EXPLAIN QUERY PLAN SELECT id FROM import_files WHERE library_file_id=7 LIMIT 1"
        ).all()
        assert any("ix_import_files_library_file_id" in row[3] for row in plan)
        connection.exec_driver_sql("INSERT INTO utility_jobs VALUES ('writer', 'file_metadata')")
        assert connection.exec_driver_sql("SELECT job_id FROM children").scalar_one() == "existing"
        with pytest.raises(RuntimeError, match="Remove saved file metadata jobs"):
            migration.downgrade()
        connection.exec_driver_sql("DELETE FROM utility_jobs WHERE id='writer'")
        migration.downgrade()
        assert not connection.exec_driver_sql("PRAGMA foreign_key_check").all()
    engine.dispose()
