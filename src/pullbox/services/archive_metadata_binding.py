"""Independent database and filesystem binding for coordinated archive writes."""

import asyncio
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import NotFoundError
from pullbox.core.library_file_ownership import (
    ReferencedFileMutationError,
    require_mutable_library_target,
)
from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Issue, LibraryFile, LibraryRoot
from pullbox.models.library import FileFormat, LibraryFileStorageMode
from pullbox.schemas.metadata_snapshot import MetadataSnapshot
from pullbox.services.archive_metadata_reconciliation import ArchiveMetadataReconciliation
from pullbox.services.metadata_assembly import assemble_metadata
from pullbox.services.metadata_series_refresh_state import (
    RefreshEntityState,
    SeriesRefreshState,
    read_series_refresh_state,
)

type FileFingerprint = tuple[int, int, int, int, int, int]
type DirectoryFingerprint = tuple[Path, int, int, int]


class ArchiveMetadataBindingError(ValueError):
    """A file cannot safely use the captured canonical metadata."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ArchiveMetadataBinding:
    """Immutable read set, not a lease, publication journal or filesystem lock."""

    library_file_id: int
    library_root_id: int
    root_path: str
    file_path: str
    file_size: int
    file_modified_at: datetime
    metadata: SeriesRefreshState


async def read_archive_metadata_binding(
    session: AsyncSession,
    library_file_id: int,
    *,
    expected_issue_id: int | None = None,
) -> ArchiveMetadataBinding:
    """Read one managed CBZ and its exact parent without provider or file I/O.

    Use a short clean reader session, then release it before inspecting/staging
    archives. The invoking workflow must revalidate this read set within its
    journal/serialization boundary; a successful read does not authorize a later
    blind replacement. Reference-only and unverified legacy bindings fail closed.
    """
    if session.new or session.dirty or session.deleted:
        raise ArchiveMetadataBindingError("pending_session_changes")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in (
            library_file_id,
            *((expected_issue_id,) if expected_issue_id is not None else ()),
        )
    ):
        raise ArchiveMetadataBindingError("invalid_target")
    row = (
        await session.execute(
            select(LibraryFile, LibraryRoot, Issue.series_id)
            .join(LibraryRoot, LibraryRoot.id == LibraryFile.library_root_id)
            .join(Issue, Issue.id == LibraryFile.issue_id)
            .where(LibraryFile.id == library_file_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        raise ArchiveMetadataBindingError("target_missing")
    file, root, series_id = row
    if file.storage_mode is not LibraryFileStorageMode.MANAGED:
        raise ArchiveMetadataBindingError("reference_only")
    if not root.enabled or not root.allow_managed_writes:
        raise ArchiveMetadataBindingError("root_not_managed")
    if file.file_format is not FileFormat.CBZ:
        raise ArchiveMetadataBindingError("unsupported_format")
    if file.issue_id is None or (
        expected_issue_id is not None and expected_issue_id != file.issue_id
    ):
        raise ArchiveMetadataBindingError("issue_changed")
    try:
        metadata = await read_series_refresh_state(session, series_id, issue_ids=(file.issue_id,))
        for entity in (metadata.series, metadata.issues[0]):
            _require_verified(entity)
        if not {ref.namespace for ref in metadata.issues[0].identities} <= {
            ref.namespace for ref in metadata.series.identities
        }:
            raise ArchiveMetadataBindingError("parent_identity_missing")
        # Validate baseline ownership even before reading untrusted archive input.
        _assemble(metadata, None, datetime.now(UTC))
    except (ValueError, NotFoundError) as exc:
        if isinstance(exc, ArchiveMetadataBindingError):
            raise
        raise ArchiveMetadataBindingError("metadata_requires_review") from None
    return ArchiveMetadataBinding(
        file.id,
        root.id,
        root.path,
        file.file_path,
        file.file_size,
        file.file_modified_at,
        metadata,
    )


def _require_verified(entity: RefreshEntityState) -> None:
    if not entity.claims or any(
        state is not IdentityVerificationState.VERIFIED for _, state, _ in entity.claims
    ):
        raise ArchiveMetadataBindingError("identity_requires_review")
    comicvine = next(
        (ref for ref in entity.identities if ref.namespace is IdentityNamespace.COMICVINE), None
    )
    if (comicvine is not None and entity.comicvine_id != int(comicvine.external_id)) or (
        entity.comicvine_id is not None and comicvine is None
    ):
        raise ArchiveMetadataBindingError("identity_requires_review")


async def revalidate_archive_metadata_binding(
    session: AsyncSession, binding: ArchiveMetadataBinding
) -> None:
    """Reject stale targets, ownership, policies, baselines and user values."""
    current = await read_archive_metadata_binding(
        session, binding.library_file_id, expected_issue_id=binding.metadata.issues[0].local_id
    )
    if current != binding:
        raise ArchiveMetadataBindingError("binding_changed")


@dataclass(frozen=True)
class ArchiveMetadataTarget:
    binding: ArchiveMetadataBinding
    path: Path
    fingerprint: FileFingerprint
    directories: tuple[DirectoryFingerprint, ...]

    def check_unchanged(self) -> None:
        """Synchronous stat boundary; offload on async paths and check before publish."""
        if _inspect_target(self.binding) != self:
            raise ArchiveMetadataBindingError("source_changed")


async def inspect_archive_metadata_target(binding: ArchiveMetadataBinding) -> ArchiveMetadataTarget:
    """Inspect outside the DB session; never open archives or perform write probes."""
    return await asyncio.to_thread(_inspect_target, binding)


async def revalidate_archive_metadata_target(
    session: AsyncSession, target: ArchiveMetadataTarget
) -> None:
    """Revalidate the DB binding and canonical-path ownership before publication."""
    await revalidate_archive_metadata_binding(session, target.binding)
    try:
        await require_mutable_library_target(
            session, target.path, include_descendants=False, operation="updated"
        )
    except ReferencedFileMutationError:
        raise ArchiveMetadataBindingError("reference_only") from None


def _inspect_target(binding: ArchiveMetadataBinding) -> ArchiveMetadataTarget:
    try:
        raw_root, raw_file = Path(binding.root_path), Path(binding.file_path)
        if any(
            not path.is_absolute()
            or ".." in path.parts
            or any(ord(c) < 32 or ord(c) == 127 for c in str(path))
            for path in (raw_root, raw_file)
        ):
            raise ArchiveMetadataBindingError("unsafe_path")
        if not raw_file.is_relative_to(raw_root) or raw_file == raw_root:
            raise ArchiveMetadataBindingError("outside_root")
        if raw_file.suffix.casefold() not in {".cbz", ".zip"}:
            raise ArchiveMetadataBindingError("unsupported_format")
        root = raw_root.resolve(strict=True)
        path = root / raw_file.relative_to(raw_root)
        if path.resolve(strict=True) != path:
            raise ArchiveMetadataBindingError("unsafe_path")
        directories = []
        parent = root
        for part in ("", *path.parent.relative_to(root).parts):
            parent = parent / part
            info = parent.lstat()
            if not stat.S_ISDIR(info.st_mode):
                raise ArchiveMetadataBindingError("unsafe_path")
            directories.append((parent, info.st_dev, info.st_ino, info.st_mode))
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise ArchiveMetadataBindingError("unsafe_path")
        if (
            not info.st_mode & 0o222
            or not directories[-1][3] & 0o222
            or not os.access(path, os.R_OK | os.W_OK)
            or not os.access(path.parent, os.W_OK | os.X_OK)
        ):
            raise ArchiveMetadataBindingError("readonly_source")
        if (
            info.st_size != binding.file_size
            or datetime.fromtimestamp(info.st_mtime, UTC) != binding.file_modified_at
        ):
            raise ArchiveMetadataBindingError("source_changed")
        return ArchiveMetadataTarget(
            binding,
            path,
            (
                info.st_dev,
                info.st_ino,
                info.st_size,
                info.st_mtime_ns,
                info.st_ctime_ns,
                info.st_mode,
            ),
            tuple(directories),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        if isinstance(exc, ArchiveMetadataBindingError):
            raise
        raise ArchiveMetadataBindingError("source_unavailable") from None


def assemble_bound_archive_metadata(
    binding: ArchiveMetadataBinding,
    archive: ArchiveMetadataReconciliation,
    *,
    now: datetime,
) -> tuple[MetadataSnapshot, MetadataSnapshot]:
    """Combine DB values and local evidence without granting embedded IDs ownership."""
    try:
        return _assemble(binding.metadata, archive, now)
    except ValueError:
        raise ArchiveMetadataBindingError("metadata_requires_review") from None


def _assemble(
    metadata: SeriesRefreshState, archive: ArchiveMetadataReconciliation | None, now: datetime
) -> tuple[MetadataSnapshot, MetadataSnapshot]:
    series, issue = metadata.series, metadata.issues[0]
    return (
        assemble_metadata(
            MetadataEntityKind.SERIES,
            series.identities,
            (),
            metadata.policies,
            now=now,
            current=series.values,
            previous=series.baseline,
            overrides=series.overrides,
            archive=archive,
        ),
        assemble_metadata(
            MetadataEntityKind.ISSUE,
            issue.identities,
            (),
            metadata.policies,
            now=now,
            current=issue.values,
            previous=issue.baseline,
            overrides=issue.overrides,
            parent_identities=series.identities,
            archive=archive,
        ),
    )
