"""Bounded, non-following trash traversal and identity-checked leaf deletion."""

import errno
import os
import stat
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from pathlib import Path

from pullbox.core.exceptions import ValidationError
from pullbox.services.archive_metadata_binding import FileFingerprint
from pullbox.services.library_conversion_files import directories

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_PRIVATE_PREFIXES = (".pullbox-removal-", ".pullbox-conversion-", ".pullbox-metadata-")
type DirectoryProof = tuple[Path, int, int, int]


def _fingerprint(info: os.stat_result) -> FileFingerprint:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode


@dataclass(frozen=True)
class TrashEntry:
    path: Path
    fingerprint: FileFingerprint
    parents: tuple[DirectoryProof, ...]
    protected: bool = False


def _open_checked(path: Path, parents: tuple[DirectoryProof, ...]) -> int:
    expected = {item[0]: item[1:] for item in parents}
    fd = os.open(path.anchor, _DIRECTORY_FLAGS)
    current = Path(path.anchor)
    try:
        for part in (None, *path.parts[1:]):
            if part is not None:
                new_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = new_fd
                current /= part
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino, info.st_mode) != expected.get(current):
                raise ValidationError(
                    "Trash directory changed; recheck its location before retrying."
                )
        return fd
    except BaseException:
        os.close(fd)
        raise


def walk_trash(root: Path) -> Generator[TrashEntry, None, None]:
    """Keep at most one open iterator per depth; private workspaces are never entered."""
    if os.name != "posix":
        raise ValidationError("Safe trash cleanup is not available on this platform yet.")
    if root == Path(root.anchor) or root.resolve() != root:
        raise ValidationError(
            "Trash must be a real directory, not a filesystem root or symbolic link."
        )
    if not root.exists():
        return
    proof = directories(root / ".trash-probe")
    root_fd = _open_checked(root, proof)
    device = os.fstat(root_fd).st_dev

    def visit(
        fd: int, path: Path, parents: tuple[DirectoryProof, ...], depth: int
    ) -> Iterator[TrashEntry]:
        with os.scandir(fd) as children:
            for child in children:
                try:
                    info = child.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                blocked = (
                    child.name.startswith(_PRIVATE_PREFIXES) or info.st_dev != device or depth > 128
                )
                entry = TrashEntry(path / child.name, _fingerprint(info), parents, blocked)
                if not blocked and stat.S_ISDIR(info.st_mode):
                    try:
                        nested = os.open(child.name, _DIRECTORY_FLAGS, dir_fd=fd)
                    except OSError:
                        yield TrashEntry(entry.path, entry.fingerprint, parents, True)
                        continue
                    try:
                        actual = os.fstat(nested)
                        if (actual.st_dev, actual.st_ino, actual.st_mode) != (
                            info.st_dev,
                            info.st_ino,
                            info.st_mode,
                        ):
                            yield TrashEntry(entry.path, entry.fingerprint, parents, True)
                            continue
                        yield from visit(
                            nested,
                            entry.path,
                            (*parents, (entry.path, info.st_dev, info.st_ino, info.st_mode)),
                            depth + 1,
                        )
                    finally:
                        os.close(nested)
                yield entry

    try:
        yield from visit(root_fd, root, proof, 0)
    finally:
        os.close(root_fd)


def remove_entry(entry: TrashEntry) -> bool:
    """Only unlink one unchanged entry or prune one empty directory, never recurse."""
    fd = _open_checked(entry.path.parent, entry.parents)
    try:
        try:
            info = os.stat(entry.path.name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        expected = entry.fingerprint
        directory = stat.S_ISDIR(expected[5])
        if (info.st_dev, info.st_ino, info.st_mode) != (expected[0], expected[1], expected[5]) or (
            not directory and _fingerprint(info) != expected
        ):
            raise ValidationError("Trash entry changed during cleanup; it was left untouched.")
        if directory:
            try:
                os.rmdir(entry.path.name, dir_fd=fd)
            except OSError as exc:
                if exc.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                    return False
                raise
        elif stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            os.unlink(entry.path.name, dir_fd=fd)
        else:
            raise ValidationError("Unsupported trash entry was left untouched.")
        os.fsync(fd)
        return True
    finally:
        os.close(fd)
