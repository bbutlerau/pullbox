"""Bounded archive inputs for single-construction paired CBZ conversion."""

import io
import os
import stat
import tarfile
import tempfile
import threading
import zipfile
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO, BinaryIO

from py7zr.io import Py7zIO, WriterFactory

from pullbox.core.file_safety import FileSafetyError


@dataclass
class MetadataArchiveSource:
    entries: list[zipfile.ZipInfo]
    open: Callable[[zipfile.ZipInfo], AbstractContextManager[IO[bytes]]]
    close: Callable[[], None]
    comment: bytes = b""


def _entry(name: str, size: int, directory: bool = False) -> zipfile.ZipInfo:
    if "\x00" in name:
        raise FileSafetyError("Archive contains an unsafe member name")
    entry = zipfile.ZipInfo(name + ("/" if directory and not name.endswith("/") else ""))
    entry.file_size = size
    entry.compress_size = 0
    entry.compress_type = zipfile.ZIP_DEFLATED
    entry.external_attr = ((stat.S_IFDIR | 0o755) if directory else (stat.S_IFREG | 0o644)) << 16
    return entry


@contextmanager
def open_metadata_archive(
    stream: BinaryIO,
    path: Path,
    *,
    limit: int,
    scratch_parent: Path,
    validate: Callable[[list[zipfile.ZipInfo]], None],
    check_cancelled: Callable[[], None],
    progress: Callable[[int, int], None],
) -> Iterator[MetadataArchiveSource]:
    """Never extract archive-supplied paths; solid 7z uses private numbered spools.

    The caller owns the open source descriptor and verifies its identity again
    before publication. No intermediate CBZ is constructed for non-ZIP input.
    """
    header = stream.read(512)
    stream.seek(0)
    with ExitStack() as stack:
        if header.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
            archive = stack.enter_context(zipfile.ZipFile(stream))
            result = MetadataArchiveSource(
                archive.infolist(), lambda entry: archive.open(entry), stack.close, archive.comment
            )
        elif header.startswith(b"Rar!\x1a\x07"):
            result = _open_rar(stream, stack)
        elif header.startswith(b"7z\xbc\xaf\x27\x1c"):
            result = _open_7z(
                stream, stack, limit, scratch_parent, validate, check_cancelled, progress
            )
        elif header[257:262] == b"ustar" or path.suffix.casefold() in {".cbt", ".tar"}:
            result = _open_tar(stream, stack, limit, check_cancelled)
        else:
            raise FileSafetyError("Unsupported or unreadable source archive for paired metadata")
        validate(result.entries)
        check_cancelled()
        yield result


def _open_tar(
    stream: BinaryIO, stack: ExitStack, limit: int, check_cancelled: Callable[[], None]
) -> MetadataArchiveSource:
    try:
        archive = stack.enter_context(tarfile.open(fileobj=stream, mode="r:*"))  # noqa: SIM115
        members = []
        total = 0
        for member in archive:
            check_cancelled()
            if not (member.isfile() or member.isdir()):
                raise FileSafetyError("Archive contains an unsafe or unsupported member")
            total += member.size
            if member.size < 0 or total > limit:
                raise FileSafetyError("Archive decompressed size exceeds limit")
            members.append(member)
        entries = [_entry(member.name, member.size, member.isdir()) for member in members]
        original = dict(zip(entries, members, strict=True))

        @contextmanager
        def read(entry: zipfile.ZipInfo) -> Iterator[IO[bytes]]:
            if entry.is_dir():
                yield io.BytesIO()
                return
            try:
                opened = archive.extractfile(original[entry])
                if opened is None:
                    raise FileSafetyError("Archive member could not be read")
                with opened:
                    yield opened
            except tarfile.TarError as exc:
                raise FileSafetyError("Source TAR archive member could not be read") from exc

        return MetadataArchiveSource(entries, read, stack.close)
    except tarfile.TarError as exc:
        raise FileSafetyError("Source TAR archive could not be inspected") from exc


def _open_rar(stream: BinaryIO, stack: ExitStack) -> MetadataArchiveSource:
    import rarfile  # type: ignore[import-untyped]

    from pullbox.core.rar_backend import configure_rarfile_backend

    configure_rarfile_backend()
    try:
        archive = stack.enter_context(rarfile.RarFile(stream, errors="strict"))
        members = archive.infolist()
        if archive.needs_password() or any(
            member.is_symlink()
            or getattr(member, "file_redir", None) is not None
            or not (member.is_file() or member.is_dir())
            or (
                member.host_os == rarfile.RAR_OS_UNIX
                and stat.S_IFMT(member.mode) not in {0, stat.S_IFREG, stat.S_IFDIR}
            )
            or (member.flags & (rarfile.RAR_FILE_SPLIT_BEFORE | rarfile.RAR_FILE_SPLIT_AFTER))
            for member in members
        ):
            raise FileSafetyError("Archive contains an unsafe or unsupported member")
        entries = [
            _entry(str(member.filename), int(member.file_size), bool(member.is_dir()))
            for member in members
        ]
        original = dict(zip(entries, members, strict=True))

        @contextmanager
        def read(entry: zipfile.ZipInfo) -> Iterator[IO[bytes]]:
            if entry.is_dir():
                yield io.BytesIO()
                return
            try:
                opened: IO[bytes] = archive.open(original[entry])
                with opened:
                    yield opened
            except rarfile.Error as exc:
                raise FileSafetyError("Source RAR archive member could not be read") from exc

        comment = archive.comment
        return MetadataArchiveSource(
            entries, read, stack.close, comment.encode("utf-8") if comment else b""
        )
    except rarfile.Error as exc:
        raise FileSafetyError("Source RAR archive could not be inspected") from exc


class _DiskMember(Py7zIO):
    def __init__(
        self, path: Path, expected_size: int, stack: ExitStack, advanced: Callable[[int], None]
    ) -> None:
        self.expected_size = expected_size
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        self.stream = stack.enter_context(os.fdopen(fd, "w+b"))
        self.written = 0
        self.advanced = advanced

    def write(self, data: bytes | bytearray) -> int:
        if self.stream.tell() != self.written or self.written + len(data) > self.expected_size:
            raise FileSafetyError("Archive expanded beyond its declared size")
        count = self.stream.write(data)
        self.written += count
        self.advanced(count)
        return count

    def read(self, size: int | None = None) -> bytes:
        return self.stream.read(size if size is not None else -1)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.stream.seek(offset, whence)

    def flush(self) -> None:
        self.stream.flush()

    def size(self) -> int:
        return self.written

    def close(self) -> None:
        self.stream.close()


class _DiskFactory(WriterFactory):
    def __init__(
        self,
        directory: Path,
        entries: list[zipfile.ZipInfo],
        stack: ExitStack,
        advanced: Callable[[int], None],
    ) -> None:
        self.paths = {entry.filename: directory / str(index) for index, entry in enumerate(entries)}
        self.entries = {entry.filename: entry for entry in entries}
        self.products: dict[str, _DiskMember] = {}
        self.stack = stack
        self.advanced = advanced

    def create(self, filename: str) -> Py7zIO:
        entry = self.entries.get(filename)
        if entry is None or entry.is_dir() or filename in self.products:
            raise FileSafetyError("Unexpected archive extraction target")
        product = _DiskMember(self.paths[filename], entry.file_size, self.stack, self.advanced)
        self.products[filename] = product
        return product


def _open_7z(
    stream: BinaryIO,
    stack: ExitStack,
    limit: int,
    scratch_parent: Path,
    validate: Callable[[list[zipfile.ZipInfo]], None],
    check_cancelled: Callable[[], None],
    progress: Callable[[int, int], None],
) -> MetadataArchiveSource:
    import py7zr
    from py7zr.exceptions import ArchiveError, PasswordRequired

    try:
        archive = stack.enter_context(py7zr.SevenZipFile(stream, "r", max_extract_size=limit))
        members = archive.list()
        if archive.needs_password() or any(
            member.is_symlink or not (member.is_file or member.is_directory) for member in members
        ):
            raise FileSafetyError("Archive contains an unsafe or unsupported member")
        entries = [
            _entry(member.filename, member.uncompressed, member.is_directory) for member in members
        ]
        validate(entries)
        check_cancelled()
        directory = Path(
            stack.enter_context(
                tempfile.TemporaryDirectory(prefix=".pullbox-7z-", dir=scratch_parent)
            )
        )
        total = sum(entry.file_size for entry in entries)
        current = 0
        lock = threading.Lock()

        def advanced(count: int) -> None:
            nonlocal current
            with lock:
                check_cancelled()
                current += count
                progress(current, total)

        factory = _DiskFactory(directory, entries, stack, advanced)
        progress(0, total)
        archive.extract(
            targets=[entry.filename for entry in entries if not entry.is_dir()],
            recursive=False,
            factory=factory,
        )
        for entry in entries:
            if entry.is_dir():
                continue
            product = factory.products.get(entry.filename)
            if product is None or product.size() != entry.file_size:
                raise FileSafetyError("Archive payload size disagrees with its directory")
            product.close()

        def read(entry: zipfile.ZipInfo) -> IO[bytes]:
            return io.BytesIO() if entry.is_dir() else factory.paths[entry.filename].open("rb")

        return MetadataArchiveSource(entries, read, stack.close)
    except (ArchiveError, PasswordRequired) as exc:
        raise FileSafetyError("Source 7z archive could not be inspected") from exc
