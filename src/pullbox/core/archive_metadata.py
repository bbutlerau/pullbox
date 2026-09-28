"""Read bounded embedded metadata without extracting comic pages to disk."""

from __future__ import annotations

import enum
import io
import tarfile
import threading
import zipfile
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from stat import S_IFMT, S_ISREG
from typing import IO, TYPE_CHECKING

from py7zr.io import Py7zIO, WriterFactory

from pullbox.core.archive import ArchiveError, ArchiveResourceLimitError, comicinfo_member_sort_key
from pullbox.core.file_safety import has_archive_member_path_traversal
from pullbox.core.rar_backend import RarBackendUnavailableError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from contextlib import AbstractContextManager
    from pathlib import Path

MAX_METADATA_BYTES = 2 * 1024 * 1024


class MetadataReadDiagnostic(enum.StrEnum):
    DUPLICATE_ENTRIES = "duplicate_entries"
    UNSAFE_ENTRY = "unsafe_entry"
    SIZE_LIMIT = "size_limit"
    SOLID_SCAN_LIMIT = "solid_scan_limit"
    UNREADABLE = "unreadable"


@dataclass(frozen=True)
class MetadataFile:
    name: str
    entry_count: int = 0
    payload: bytes | None = None
    diagnostics: tuple[MetadataReadDiagnostic, ...] = ()


@dataclass(frozen=True)
class ArchiveMetadataFiles:
    comicinfo: MetadataFile
    metroninfo: MetadataFile


def read_archive_metadata(
    path: Path, archive_type: str, *, max_solid_scan_bytes: int
) -> ArchiveMetadataFiles:
    """Read both XML members in one archive session; this is not a safety approval."""
    if isinstance(max_solid_scan_bytes, bool) or max_solid_scan_bytes <= 0:
        raise ValueError("Solid metadata scan budget must be positive")
    try:
        if archive_type == "cbz":
            with zipfile.ZipFile(path, "r") as archive:
                return read_open_zip_metadata(archive)
        if archive_type == "cbt":
            with tarfile.open(path, "r:*") as tar:
                candidates = [
                    _Candidate(member, member.name, member.size, None, member.isfile())
                    for member in tar.getmembers()
                ]

                def open_tar(member: tarfile.TarInfo) -> IO[bytes]:
                    stream = tar.extractfile(member)
                    if stream is None:
                        raise ArchiveError("Metadata member is unavailable")
                    return stream

                return _read_members(candidates, open_tar, (tarfile.TarError,))
        if archive_type == "cbr":
            return _read_rar(path, max_solid_scan_bytes)
        if archive_type == "cb7":
            return _read_7z(path, max_solid_scan_bytes)
    except (
        OSError,
        EOFError,
        RuntimeError,
        ValueError,
        zipfile.BadZipFile,
        tarfile.TarError,
    ) as exc:
        raise ArchiveError("Archive metadata could not be inspected") from exc
    raise ArchiveError("Unsupported archive format for embedded metadata")


@dataclass(frozen=True)
class _Candidate[T]:
    member: T
    name: str
    size: int
    compressed_size: int | None
    regular: bool


def _select[T](
    candidates: Sequence[_Candidate[T]], name: str
) -> tuple[MetadataFile, _Candidate[T] | None]:
    matches = sorted(
        (
            candidate
            for candidate in candidates
            if PurePosixPath(candidate.name.replace("\\", "/")).name.casefold() == name.casefold()
        ),
        key=lambda candidate: comicinfo_member_sort_key(candidate.name),
    )
    diagnostics = []
    if len(matches) > 1:
        diagnostics.append(MetadataReadDiagnostic.DUPLICATE_ENTRIES)
    result = MetadataFile(name, len(matches), diagnostics=tuple(diagnostics))
    if not matches:
        return result, None
    chosen = matches[0]
    if (
        not chosen.regular
        or has_archive_member_path_traversal(chosen.name)
        or "\x00" in chosen.name
    ):
        return _with_error(result, MetadataReadDiagnostic.UNSAFE_ENTRY), None
    if (
        chosen.size < 0
        or chosen.size > MAX_METADATA_BYTES
        or (
            chosen.compressed_size is not None
            and (chosen.compressed_size < 0 or chosen.compressed_size > MAX_METADATA_BYTES)
        )
    ):
        return _with_error(result, MetadataReadDiagnostic.SIZE_LIMIT), None
    return result, chosen


def _with_error(result: MetadataFile, diagnostic: MetadataReadDiagnostic) -> MetadataFile:
    return replace(result, payload=None, diagnostics=(*result.diagnostics, diagnostic))


def _read_members[T](
    candidates: Sequence[_Candidate[T]],
    open_member: Callable[[T], AbstractContextManager[IO[bytes]]],
    errors: tuple[type[Exception], ...] = (),
    *,
    solid_blocked: bool = False,
) -> ArchiveMetadataFiles:
    results = []
    read_errors: tuple[type[Exception], ...] = (
        OSError,
        EOFError,
        RuntimeError,
        ValueError,
        ArchiveError,
        *errors,
    )
    for name in ("ComicInfo.xml", "MetronInfo.xml"):
        result, chosen = _select(candidates, name)
        if chosen is not None:
            if solid_blocked:
                result = _with_error(result, MetadataReadDiagnostic.SOLID_SCAN_LIMIT)
            else:
                try:
                    with open_member(chosen.member) as stream:
                        payload = stream.read(MAX_METADATA_BYTES + 1)
                    if len(payload) > MAX_METADATA_BYTES:
                        result = _with_error(result, MetadataReadDiagnostic.SIZE_LIMIT)
                    elif len(payload) != chosen.size:
                        result = _with_error(result, MetadataReadDiagnostic.UNREADABLE)
                    else:
                        result = replace(result, payload=payload)
                except read_errors:
                    result = _with_error(result, MetadataReadDiagnostic.UNREADABLE)
        results.append(result)
    return ArchiveMetadataFiles(*results)


def read_open_zip_metadata(archive: zipfile.ZipFile) -> ArchiveMetadataFiles:
    """Reuse an already-open ZIP inspection without reopening or extracting it."""
    candidates = [
        _Candidate(
            member,
            member.filename,
            member.file_size,
            member.compress_size,
            not member.is_dir()
            and (S_IFMT(member.external_attr >> 16) == 0 or S_ISREG(member.external_attr >> 16)),
        )
        for member in archive.infolist()
    ]
    return _read_members(
        candidates, lambda member: archive.open(member, "r"), (zipfile.BadZipFile,)
    )


def _read_rar(path: Path, max_solid_scan_bytes: int) -> ArchiveMetadataFiles:
    import rarfile  # type: ignore[import-untyped]

    from pullbox.core.rar_backend import configure_rarfile_backend

    try:
        configure_rarfile_backend()
        with rarfile.RarFile(path, "r") as archive:
            candidates = [
                _Candidate(
                    member,
                    str(member.filename),
                    int(member.file_size),
                    int(member.compress_size),
                    bool(member.is_file()) and not bool(member.is_symlink()),
                )
                for member in archive.infolist()
            ]
            return _read_members(
                candidates,
                lambda member: archive.open(member, "r"),
                (rarfile.Error,),
                solid_blocked=bool(archive.is_solid())
                and sum(member.size for member in candidates) > max_solid_scan_bytes,
            )
    except (rarfile.Error, RarBackendUnavailableError) as exc:
        raise ArchiveError("RAR metadata could not be inspected") from exc


class _BoundedBuffer(Py7zIO):
    def __init__(self) -> None:
        self._buffer = io.BytesIO()
        self._size = 0
        self._lock = threading.Lock()

    def write(self, data: bytes | bytearray) -> int:
        with self._lock:
            end = self._buffer.tell() + len(data)
            if end > MAX_METADATA_BYTES:
                raise ArchiveResourceLimitError("Embedded metadata size limit exceeded")
            written = self._buffer.write(data)
            self._size = max(self._size, end)
            return written

    def read(self, size: int | None = None) -> bytes:
        with self._lock:
            return self._buffer.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        with self._lock:
            base = {0: 0, 1: self._buffer.tell(), 2: self._size}.get(whence)
            if base is None or not 0 <= base + offset <= MAX_METADATA_BYTES:
                raise ArchiveResourceLimitError("Embedded metadata seek limit exceeded")
            return self._buffer.seek(offset, whence)

    def flush(self) -> None:
        return None

    def size(self) -> int:
        with self._lock:
            return self._size


class _MetadataFactory(WriterFactory):
    def __init__(self, names: Sequence[str]) -> None:
        self.products = {name: _BoundedBuffer() for name in names}

    def create(self, filename: str) -> Py7zIO:
        product = self.products.get(filename)
        if product is None:
            raise ArchiveError("Unexpected metadata extraction target")
        return product


def _read_7z(path: Path, max_solid_scan_bytes: int) -> ArchiveMetadataFiles:
    import py7zr
    from py7zr.exceptions import ArchiveError as SevenZipError
    from py7zr.exceptions import PasswordRequired

    try:
        with py7zr.SevenZipFile(path, "r", max_extract_size=max_solid_scan_bytes) as archive:
            candidates = [
                _Candidate(
                    member.filename,
                    member.filename,
                    int(member.uncompressed or 0),
                    # Solid 7z reports the packed block, not a member's compressed size.
                    None,
                    bool(member.is_file) and not bool(member.is_symlink),
                )
                for member in archive.list()
            ]
            plans = [_select(candidates, name) for name in ("ComicInfo.xml", "MetronInfo.xml")]
            solid_blocked = (
                archive.archiveinfo().solid
                and sum(member.size for member in candidates) > max_solid_scan_bytes
            )
            targets = []
            results = []
            for result, chosen in plans:
                if chosen is not None:
                    if solid_blocked:
                        result = _with_error(result, MetadataReadDiagnostic.SOLID_SCAN_LIMIT)
                    elif sum(member.name == chosen.name for member in candidates) > 1:
                        # py7zr selects by name, so it cannot address one exact duplicate safely.
                        result = _with_error(result, MetadataReadDiagnostic.UNREADABLE)
                    else:
                        targets.append(chosen)
                results.append(result)
            if not targets:
                return ArchiveMetadataFiles(*results)
            factory = _MetadataFactory([member.name for member in targets])
            try:
                archive.extract(
                    targets=[member.name for member in targets], recursive=False, factory=factory
                )
                for index, (_result, chosen) in enumerate(plans):
                    if chosen not in targets or chosen is None:
                        continue
                    buffer = factory.products[chosen.name]
                    if buffer.size() != chosen.size:
                        results[index] = _with_error(
                            results[index], MetadataReadDiagnostic.UNREADABLE
                        )
                    else:
                        buffer.seek(0)
                        results[index] = replace(results[index], payload=buffer.read())
            except ArchiveResourceLimitError:
                results = [
                    _with_error(result, MetadataReadDiagnostic.SIZE_LIMIT)
                    if chosen in targets
                    else result
                    for result, (_old, chosen) in zip(results, plans, strict=True)
                ]
            except (
                SevenZipError,
                PasswordRequired,
                ArchiveError,
                OSError,
                EOFError,
                RuntimeError,
                ValueError,
            ):
                results = [
                    _with_error(result, MetadataReadDiagnostic.UNREADABLE)
                    if chosen in targets
                    else result
                    for result, (_old, chosen) in zip(results, plans, strict=True)
                ]
            return ArchiveMetadataFiles(*results)
    except (SevenZipError, PasswordRequired) as exc:
        raise ArchiveError("7z metadata could not be inspected") from exc
