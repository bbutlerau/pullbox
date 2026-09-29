"""Tests for UT-F.3 — utility settings integration.

Verifies default values, validation boundaries, directory creation,
and edge cases for the five utility-specific system config keys.

Run:
    pytest tests/utilities/test_utility_settings.py -v
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pullbox.models.config import DEFAULT_SYSTEM_CONFIG

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ── Default Values ─────────────────────────────────────────────


class TestUtilitySettingDefaults:
    """Verify utility settings exist in DEFAULT_SYSTEM_CONFIG with correct values."""

    def test_worker_count_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_worker_count"]
        assert val == "4"
        assert vtype == "int"

    def test_trash_folder_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_trash_folder"]
        assert val == ""
        assert vtype == "string"

    def test_export_folder_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_export_folder"]
        assert val == ""
        assert vtype == "string"

    def test_trash_retention_days_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_trash_retention_days"]
        assert val == "30"
        assert vtype == "int"

    def test_job_retention_days_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_job_retention_days"]
        assert val == "30"
        assert vtype == "int"

    def test_log_level_default(self) -> None:
        val, vtype = DEFAULT_SYSTEM_CONFIG["utility_log_level"]
        assert val == "info"
        assert vtype == "string"


# ── Validation: Worker Count ───────────────────────────────────


class TestWorkerCountValidation:
    """Verify worker count validation accepts 1-16, rejects out-of-range."""

    @pytest.mark.asyncio
    async def test_worker_count_minimum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_worker_count", "1")

    @pytest.mark.asyncio
    async def test_worker_count_maximum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_worker_count", "16")

    @pytest.mark.asyncio
    async def test_worker_count_midrange_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_worker_count", "4")

    @pytest.mark.asyncio
    async def test_worker_count_zero_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 16"):
            validate_utility_setting("utility_worker_count", "0")

    @pytest.mark.asyncio
    async def test_worker_count_negative_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 16"):
            validate_utility_setting("utility_worker_count", "-1")

    @pytest.mark.asyncio
    async def test_worker_count_above_max_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 16"):
            validate_utility_setting("utility_worker_count", "17")

    @pytest.mark.asyncio
    async def test_worker_count_very_large_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 16"):
            validate_utility_setting("utility_worker_count", "999")

    @pytest.mark.asyncio
    async def test_worker_count_non_integer_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError):
            validate_utility_setting("utility_worker_count", "abc")

    @pytest.mark.asyncio
    async def test_worker_count_float_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError):
            validate_utility_setting("utility_worker_count", "4.5")


# ── Validation: Job Retention Days ──────────────────────────────


class TestRetentionDaysValidation:
    """Verify retention days validation accepts 1-365, rejects out-of-range."""

    @pytest.mark.asyncio
    async def test_retention_minimum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_job_retention_days", "1")

    @pytest.mark.asyncio
    async def test_retention_maximum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_job_retention_days", "365")

    @pytest.mark.asyncio
    async def test_retention_midrange_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_job_retention_days", "30")

    @pytest.mark.asyncio
    async def test_retention_zero_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 365"):
            validate_utility_setting("utility_job_retention_days", "0")

    @pytest.mark.asyncio
    async def test_retention_negative_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 365"):
            validate_utility_setting("utility_job_retention_days", "-5")

    @pytest.mark.asyncio
    async def test_retention_above_max_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 365"):
            validate_utility_setting("utility_job_retention_days", "366")


class TestTrashRetentionDaysValidation:
    """Verify trash retention days accepts 1-365, rejects out-of-range."""

    @pytest.mark.asyncio
    async def test_trash_retention_minimum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_trash_retention_days", "1")

    @pytest.mark.asyncio
    async def test_trash_retention_maximum_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_trash_retention_days", "365")

    @pytest.mark.asyncio
    async def test_trash_retention_midrange_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        validate_utility_setting("utility_trash_retention_days", "30")

    @pytest.mark.asyncio
    async def test_trash_retention_zero_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="between 1 and 365"):
            validate_utility_setting("utility_trash_retention_days", "0")

    @pytest.mark.asyncio
    async def test_trash_retention_non_integer_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="must be an integer"):
            validate_utility_setting("utility_trash_retention_days", "abc")


# ── Validation: Log Level ──────────────────────────────────────


class TestLogLevelValidation:
    """Verify log level validation accepts valid levels, rejects invalid."""

    @pytest.mark.asyncio
    async def test_valid_log_levels_accepted(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        for level in ("DEBUG", "INFO", "WARNING", "ERROR"):
            validate_utility_setting("utility_log_level", level)

    @pytest.mark.asyncio
    async def test_log_level_case_insensitive(self, db_session: AsyncSession) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        for level in ("debug", "info", "warning", "error"):
            validate_utility_setting("utility_log_level", level)

    @pytest.mark.asyncio
    async def test_invalid_log_level_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="must be one of"):
            validate_utility_setting("utility_log_level", "TRACE")

    @pytest.mark.asyncio
    async def test_empty_log_level_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="must be one of"):
            validate_utility_setting("utility_log_level", "")

    @pytest.mark.asyncio
    async def test_verbose_log_level_rejected(self, db_session: AsyncSession) -> None:
        from pullbox.core.exceptions import ValidationError
        from pullbox.utilities.settings import validate_utility_setting

        with pytest.raises(ValidationError, match="must be one of"):
            validate_utility_setting("utility_log_level", "verbose")


# ── Validation: Unknown Keys Pass Through ──────────────────────


class TestUnknownKeyPassthrough:
    """Verify that non-utility keys are not validated by utility validator."""

    def test_unknown_key_not_validated(self) -> None:
        from pullbox.utilities.settings import validate_utility_setting

        # Should not raise — unknown keys are ignored
        validate_utility_setting("some_other_key", "any_value")


# ── Settings Persistence ───────────────────────────────────────


class TestSettingsPersistence:
    """Verify utility settings can be stored and retrieved from DB."""

    @pytest.mark.asyncio
    async def test_settings_persist_after_update(self, db_session: AsyncSession) -> None:
        from sqlalchemy import select

        from pullbox.models.config import SystemConfig

        config = SystemConfig(
            key="utility_worker_count",
            value="8",
            value_type="int",
        )
        db_session.add(config)
        await db_session.flush()

        result = await db_session.execute(
            select(SystemConfig).where(SystemConfig.key == "utility_worker_count")
        )
        stored = result.scalar_one()
        assert stored.value == "8"

    @pytest.mark.asyncio
    async def test_partial_update_preserves_other_settings(self, db_session: AsyncSession) -> None:
        """Updating one utility setting doesn't affect others."""
        from sqlalchemy import select

        from pullbox.models.config import SystemConfig

        db_session.add(SystemConfig(key="utility_worker_count", value="4", value_type="int"))
        db_session.add(SystemConfig(key="utility_job_retention_days", value="30", value_type="int"))
        await db_session.flush()

        # Update only worker_count
        result = await db_session.execute(
            select(SystemConfig).where(SystemConfig.key == "utility_worker_count")
        )
        config = result.scalar_one()
        config.value = "8"
        await db_session.flush()

        # Verify retention_days unchanged
        result = await db_session.execute(
            select(SystemConfig).where(SystemConfig.key == "utility_job_retention_days")
        )
        retention = result.scalar_one()
        assert retention.value == "30"


# ── Directory Resolution ───────────────────────────────────────


class TestDirectoryResolution:
    """Verify trash and export folder path resolution logic."""

    def test_trash_folder_defaults_to_library_root(self) -> None:
        from pullbox.utilities.settings import resolve_utility_directory

        result = resolve_utility_directory(
            db_value="",
            default_parent=Path("/data/comics"),
            default_subdir=".trash",
        )
        assert result == Path("/data/comics/.trash")

    def test_export_folder_defaults_to_data_dir(self) -> None:
        from pullbox.utilities.settings import resolve_utility_directory

        result = resolve_utility_directory(
            db_value="",
            default_parent=Path("/data"),
            default_subdir="exports",
        )
        assert result == Path("/data/exports")

    def test_runtime_export_folder_expands_data_placeholder(self) -> None:
        from pullbox.utilities.settings import resolve_export_directory

        result = resolve_export_directory(
            "{data}/exports",
            library_root=Path("/srv/comics"),
            data_dir=Path("/srv/data"),
        )
        assert result == Path("/srv/data/exports")

    def test_runtime_export_folder_defaults_when_blank(self) -> None:
        from pullbox.utilities.settings import resolve_export_directory

        result = resolve_export_directory(
            "",
            library_root=Path("/srv/comics"),
            data_dir=Path("/srv/data"),
        )
        assert result == Path("/srv/data/exports")

    def test_runtime_trash_folder_expands_library_placeholder(self) -> None:
        from pullbox.utilities.settings import resolve_trash_directory

        result = resolve_trash_directory(
            "{library}/.trash",
            library_root=Path("/srv/comics"),
            data_dir=Path("/srv/data"),
        )
        assert result == Path("/srv/comics/.trash")

    def test_runtime_trash_folder_blank_keeps_trash_disabled(self) -> None:
        from pullbox.utilities.settings import resolve_trash_directory

        result = resolve_trash_directory(
            "",
            library_root=Path("/srv/comics"),
            data_dir=Path("/srv/data"),
        )
        assert result is None

    def test_custom_path_used_when_set(self) -> None:
        from pullbox.utilities.settings import resolve_utility_directory

        result = resolve_utility_directory(
            db_value="/custom/trash",
            default_parent=Path("/data/comics"),
            default_subdir=".trash",
        )
        assert result == Path("/custom/trash")

    def test_path_with_spaces(self) -> None:
        from pullbox.utilities.settings import resolve_utility_directory

        result = resolve_utility_directory(
            db_value="/my comics/trash folder",
            default_parent=Path("/data/comics"),
            default_subdir=".trash",
        )
        assert result == Path("/my comics/trash folder")

    def test_path_with_unicode(self) -> None:
        from pullbox.utilities.settings import resolve_utility_directory

        result = resolve_utility_directory(
            db_value="/mnt/storage/コミック/.trash",
            default_parent=Path("/data/comics"),
            default_subdir=".trash",
        )
        assert result == Path("/mnt/storage/コミック/.trash")


# ── Directory Creation ──────────────────────────────────────────


class TestDirectoryCreation:
    """Verify utility directories are created when they don't exist."""

    @pytest.mark.asyncio
    async def test_creates_trash_directory(self, tmp_path: Path) -> None:
        from pullbox.utilities.settings import ensure_utility_directories

        trash_dir = tmp_path / "comics" / ".trash"
        export_dir = tmp_path / "exports"

        await ensure_utility_directories(trash_dir, export_dir)

        assert trash_dir.is_dir()
        assert export_dir.is_dir()

    @pytest.mark.asyncio
    async def test_existing_directories_not_errored(self, tmp_path: Path) -> None:
        """Creating directories that already exist doesn't raise."""
        from pullbox.utilities.settings import ensure_utility_directories

        trash_dir = tmp_path / ".trash"
        export_dir = tmp_path / "exports"
        trash_dir.mkdir()
        export_dir.mkdir()

        await ensure_utility_directories(trash_dir, export_dir)

        assert trash_dir.is_dir()
        assert export_dir.is_dir()

    @pytest.mark.asyncio
    async def test_creates_nested_directories(self, tmp_path: Path) -> None:
        """Deep paths are created with parents=True."""
        from pullbox.utilities.settings import ensure_utility_directories

        trash_dir = tmp_path / "deep" / "nested" / ".trash"
        export_dir = tmp_path / "deep" / "nested" / "exports"

        await ensure_utility_directories(trash_dir, export_dir)

        assert trash_dir.is_dir()
        assert export_dir.is_dir()

    @pytest.mark.asyncio
    async def test_readonly_parent_raises(self, tmp_path: Path) -> None:
        """Read-only parent directory raises OSError."""
        from pullbox.utilities.settings import ensure_utility_directories

        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)

        trash_dir = readonly_dir / ".trash"
        export_dir = tmp_path / "exports"

        try:
            with pytest.raises(OSError):
                await ensure_utility_directories(trash_dir, export_dir)
        finally:
            readonly_dir.chmod(0o755)


class TestTrashCleanup:
    """Verify utility trash cleanup helpers enforce retention and immediate purge."""

    def test_move_to_trash_resets_modified_time_and_preserves_relative_path(
        self,
        tmp_path: Path,
    ) -> None:
        from pullbox.utilities.settings import move_file_to_utility_trash

        source = tmp_path / "library" / "Series" / "Issue 001.cbz"
        source.parent.mkdir(parents=True)
        source.write_text("content")

        old_timestamp = (datetime.now(tz=UTC) - timedelta(days=45)).timestamp()
        os.utime(source, (old_timestamp, old_timestamp))

        trash_dir = tmp_path / ".trash"
        moved = move_file_to_utility_trash(
            source,
            trash_dir,
            relative_path=Path("Series") / source.name,
        )

        assert moved == trash_dir / "Series" / source.name
        assert moved.exists()
        modified_at = datetime.fromtimestamp(moved.stat().st_mtime, tz=UTC)
        assert modified_at > datetime.now(tz=UTC) - timedelta(minutes=1)

    def test_move_to_trash_deduplicates_when_destination_exists(
        self,
        tmp_path: Path,
    ) -> None:
        from pullbox.utilities.settings import move_file_to_utility_trash

        source = tmp_path / "library" / "Series" / "Issue 001.cbz"
        source.parent.mkdir(parents=True)
        source.write_text("incoming")

        trash_dir = tmp_path / ".trash"
        existing = trash_dir / "Series" / source.name
        existing.parent.mkdir(parents=True)
        existing.write_text("existing")

        moved = move_file_to_utility_trash(
            source,
            trash_dir,
            relative_path=Path("Series") / source.name,
        )

        assert moved == trash_dir / "Series" / "Issue 001 (1).cbz"
        assert moved.exists()
        assert existing.exists()
        assert not source.exists()

    async def test_cleanup_retention_deletes_old_files_and_prunes_empty_directories(
        self,
        tmp_path: Path,
        db_session: AsyncSession,
    ) -> None:
        from pullbox.services.library_trash_cleanup import cleanup_trash

        trash_dir = tmp_path / ".trash"
        nested_dir = trash_dir / "nested"
        nested_dir.mkdir(parents=True)
        old_file = nested_dir / "old.cbz"
        new_file = trash_dir / "new.cbz"
        old_file.write_text("old")
        new_file.write_text("new")

        old_timestamp = (datetime.now(tz=UTC) - timedelta(days=45)).timestamp()
        os.utime(old_file, (old_timestamp, old_timestamp))

        await db_session.commit()
        deleted = (await cleanup_trash(db_session, trash_dir, retention_days=30)).deleted_entries

        assert deleted == 2
        assert not old_file.exists()
        assert new_file.exists()
        assert not nested_dir.exists()

    async def test_cleanup_retention_keeps_freshly_trashed_old_file(
        self,
        tmp_path: Path,
        db_session: AsyncSession,
    ) -> None:
        from pullbox.services.library_trash_cleanup import cleanup_trash
        from pullbox.utilities.settings import (
            move_file_to_utility_trash,
        )

        source = tmp_path / "library" / "Series" / "Issue 001.cbz"
        source.parent.mkdir(parents=True)
        source.write_text("content")

        old_timestamp = (datetime.now(tz=UTC) - timedelta(days=45)).timestamp()
        os.utime(source, (old_timestamp, old_timestamp))

        trash_dir = tmp_path / ".trash"
        moved = move_file_to_utility_trash(
            source,
            trash_dir,
            relative_path=Path("Series/Issue 001.cbz"),
        )

        await db_session.commit()
        deleted = (await cleanup_trash(db_session, trash_dir, retention_days=30)).deleted_entries

        assert deleted == 0
        assert moved.exists()

    async def test_empty_trash_removes_all_contents_but_preserves_root(
        self, tmp_path: Path, db_session: AsyncSession
    ) -> None:
        from pullbox.services.library_trash_cleanup import cleanup_trash

        trash_dir = tmp_path / ".trash"
        nested_dir = trash_dir / "nested"
        nested_dir.mkdir(parents=True)
        (nested_dir / "a.cbz").write_text("a")
        (trash_dir / "b.cb7").write_text("b")

        await db_session.commit()
        deleted = (await cleanup_trash(db_session, trash_dir)).deleted_entries

        assert deleted == 3
        assert trash_dir.exists()
        assert list(trash_dir.iterdir()) == []
