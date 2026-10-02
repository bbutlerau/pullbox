"""Ownership safety tests for immediate Library browser renames."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pullbox.core.exceptions import ValidationError
from pullbox.models.library import (
    FileFormat,
    LibraryFile,
    LibraryFileStorageMode,
    LibraryRoot,
)
from pullbox.services.library_rename_service import _rename_path, rename_library_entry

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["file", "folder"])
async def test_rename_rejects_target_containing_referenced_file(
    db_session: AsyncSession,
    tmp_path: Path,
    kind: str,
) -> None:
    root_path = tmp_path / "library"
    folder = root_path / "Existing"
    folder.mkdir(parents=True)
    source_file = folder / "Issue 001.cbz"
    original = b"user-owned comic"
    source_file.write_bytes(original)
    root = LibraryRoot(name="Comics", path=str(root_path), enabled=True)
    db_session.add(root)
    await db_session.flush()
    db_session.add(
        LibraryFile(
            file_path=str(source_file),
            file_name=source_file.name,
            file_size=len(original),
            file_format=FileFormat.CBZ,
            file_modified_at=datetime.now(tz=UTC),
            library_root_id=root.id,
            storage_mode=LibraryFileStorageMode.REFERENCED,
        )
    )
    await db_session.flush()
    source = source_file if kind == "file" else folder
    target = source.with_name(f"Renamed {source.name}")

    with pytest.raises(ValidationError, match="Referenced library files cannot be renamed"):
        await rename_library_entry(
            db_session,
            source=source,
            target=target,
            kind=kind,
        )

    assert source_file.read_bytes() == original
    assert not target.exists()


@pytest.mark.parametrize("kind", ["file", "folder"])
def test_rename_never_overwrites_a_racing_destination(tmp_path, monkeypatch, kind):
    source, target = tmp_path / "source", tmp_path / "target"
    if kind == "folder":
        source.mkdir()
    else:
        source.write_bytes(b"original")
    exists = Path.exists
    raced = False

    def create_after_check(path):
        nonlocal raced
        if path == target and not raced:
            raced = True
            if kind == "folder":
                target.mkdir()
            else:
                target.write_bytes(b"other")
            return False
        return exists(path)

    monkeypatch.setattr(Path, "exists", create_after_check)
    with pytest.raises(ValidationError, match="already exists"):
        _rename_path(source, target)
    assert source.exists() and target.exists()
    if kind == "file":
        assert source.read_bytes() == b"original" and target.read_bytes() == b"other"
