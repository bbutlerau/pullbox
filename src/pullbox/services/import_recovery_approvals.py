"""Keep narrowly approved archive exceptions bound to the reviewed source."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pullbox.core.library_file_ownership import (
    ReferencedFileValidationError,
    build_file_identity_signature,
    validate_file_identity_signature,
)

if TYPE_CHECKING:
    from pullbox.models.import_job import ImportedFile


@dataclass(frozen=True, slots=True)
class SourceApproval:
    code: str
    signature: dict[str, int | str]


def source_approval(file: ImportedFile) -> SourceApproval | None:
    """Reuse an approval only for the same artifact, allowing mount renumbering."""
    exception = dict(file.diagnostics or {}).get("safety_exception")
    if not isinstance(exception, dict) or exception.get("allowed_once") is not True:
        return None
    block = exception.get("previous_block")
    if not isinstance(block, dict) or block.get("overrideable") is not True:
        return None
    code = block.get("code")
    if code not in {"archive_decompressed_size_limit", "single_page_comic"}:
        return None
    previous = dict(file.source_signature or {})
    if not isinstance(previous.get("device"), int):
        return None
    try:
        current = build_file_identity_signature(Path(file.file_path))
        validate_file_identity_signature({**previous, "device": current["device"]}, current)
    except (OSError, RuntimeError, ValueError, ReferencedFileValidationError):
        return None
    return SourceApproval(str(code), current)
