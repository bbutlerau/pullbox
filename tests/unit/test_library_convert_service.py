"""Unit tests for immediate library conversion recovery behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

pytest_plugins = ["tests.conftest_security"]

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.asyncio
async def test_convert_library_file_discards_private_artifact_when_backup_fails(
    sec_db,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    from pullbox.core.exceptions import ValidationError
    from pullbox.models.library import FileFormat, LibraryFile, LibraryRoot
    from pullbox.services import library_conversion_files as files
    from pullbox.services import library_convert_service as service

    source = tmp_path / "Convert Me.cbr"
    converted = tmp_path / "Convert Me.cbz"
    source.write_text("source", encoding="utf-8")

    async def convert_file(_source: Path, _target_format: str, *, output_path: Path) -> Path:
        output_path.write_text("converted", encoding="utf-8")
        return output_path

    async def fail_move(*_args: object, **_kwargs: object) -> Path:
        raise FileExistsError("trash collision")

    monkeypatch.setattr(files, "convert_file_interruptible", convert_file)
    monkeypatch.setattr(files, "transfer_file_interruptible", fail_move)

    async with sec_db() as session:
        root = LibraryRoot(name="Comics", path=str(tmp_path), enabled=True)
        session.add(root)
        await session.flush()
        library_file = LibraryFile(
            file_path=str(source),
            file_name=source.name,
            file_size=source.stat().st_size,
            file_format=FileFormat.CBR,
            file_modified_at=datetime.now(tz=UTC),
            library_root_id=root.id,
        )
        session.add(library_file)
        await session.commit()
        library_file_id = library_file.id

        with pytest.raises(ValidationError, match="A CBZ file with that name already exists"):
            await service.convert_library_file(
                session,
                source=source,
                trash_dir=tmp_path / ".trash",
                trash_relative_path=source.name,
            )

        assert source.exists() is True
        assert converted.exists() is False
        refreshed = await session.get(LibraryFile, library_file_id)
        assert refreshed is not None
        assert refreshed.file_path == str(source)
        assert refreshed.file_format == FileFormat.CBR


@pytest.mark.asyncio
async def test_convert_library_file_retains_original_and_journal_when_registration_fails(
    sec_db,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    from pullbox.core.exceptions import ValidationError
    from pullbox.models.library import FileFormat, LibraryFile, LibraryRoot
    from pullbox.services import library_conversion_files as files
    from pullbox.services import library_convert_service as service
    from pullbox.services.library_conversion_recovery import recover_library_conversions

    source = tmp_path / "Restore Me.cbr"
    converted = tmp_path / "Restore Me.cbz"
    source.write_text("source", encoding="utf-8")

    async def convert_file(_source: Path, _target_format: str, *, output_path: Path) -> Path:
        output_path.write_text("converted", encoding="utf-8")
        return output_path

    async def fail_sync(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("database write failed")

    sync = service._sync_converted_file_record
    monkeypatch.setattr(files, "convert_file_interruptible", convert_file)
    monkeypatch.setattr(service, "_sync_converted_file_record", fail_sync)

    async with sec_db() as session:
        root = LibraryRoot(name="Comics", path=str(tmp_path), enabled=True)
        session.add(root)
        await session.flush()
        library_file = LibraryFile(
            file_path=str(source),
            file_name=source.name,
            file_size=source.stat().st_size,
            file_format=FileFormat.CBR,
            file_modified_at=datetime.now(tz=UTC),
            library_root_id=root.id,
        )
        session.add(library_file)
        await session.commit()

        with pytest.raises(ValidationError, match="Conversion could not be completed"):
            await service.convert_library_file(
                session,
                source=source,
                trash_dir=tmp_path / ".trash",
                trash_relative_path=source.name,
            )

        assert source.exists() is True
        assert converted.exists() is True
        assert len(list((tmp_path / ".trash").rglob("*.cbr"))) == 1
        monkeypatch.setattr(service, "_sync_converted_file_record", sync)
        assert await recover_library_conversions(session) == 1
        assert not source.exists()
        assert converted.read_text() == "converted"


@pytest.mark.asyncio
async def test_convert_library_file_rejects_referenced_source_before_conversion(
    sec_db,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    from pullbox.core.exceptions import ValidationError
    from pullbox.models.library import (
        FileFormat,
        LibraryFile,
        LibraryFileStorageMode,
        LibraryRoot,
    )
    from pullbox.services import library_conversion_files as files
    from pullbox.services import library_convert_service as service

    source = tmp_path / "Referenced.cbr"
    original = b"user-owned comic"
    source.write_bytes(original)
    convert = pytest.fail
    monkeypatch.setattr(files, "convert_file_interruptible", convert)

    async with sec_db() as session:
        root = LibraryRoot(name="Comics", path=str(tmp_path), enabled=True)
        session.add(root)
        await session.flush()
        session.add(
            LibraryFile(
                file_path=str(source),
                file_name=source.name,
                file_size=source.stat().st_size,
                file_format=FileFormat.CBR,
                file_modified_at=datetime.now(tz=UTC),
                library_root_id=root.id,
                storage_mode=LibraryFileStorageMode.REFERENCED,
            )
        )
        await session.commit()

        with pytest.raises(ValidationError, match="Referenced library files cannot be converted"):
            await service.convert_library_file(
                session,
                source=source,
                trash_dir=tmp_path / ".trash",
                trash_relative_path=source.name,
            )

    assert source.read_bytes() == original
