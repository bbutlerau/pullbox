"""Private removal work with bounded copies and cooperative cancellation."""

import asyncio
import hashlib
import os
import stat
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from threading import Event

from pullbox.core.exceptions import ValidationError
from pullbox.services.archive_metadata_binding import FileFingerprint
from pullbox.services.archive_metadata_publication import _fingerprint
from pullbox.services.library_conversion_files import matches, sync_directory
from pullbox.services.library_mutation_coordination import finish_short_mutation
from pullbox.services.library_removal import RemovalPlan, _check_locations


def check_stop(stop: Event) -> None:
    if stop.is_set():
        raise InterruptedError("Library removal cleanup cancelled")


def require_payload(path: Path, expected: FileFingerprint, *, partial: bool = False) -> None:
    actual = _fingerprint(path)
    if partial and stat.S_ISDIR(expected[5]):
        valid = actual and actual[:2] == expected[:2] and actual[5] == expected[5]
    else:
        valid = matches(actual, expected)
    if not valid:
        raise ValidationError("The retained removal payload changed; review before retrying.")


def _claim(plan: RemovalPlan) -> int:
    # A filesystem lock spans slow work without holding SQLite's writer mutex.
    # Failing closed is preferable to a time-based lease that can expire on a live worker.
    if os.name != "posix":
        raise ValidationError("Removal cleanup requires filesystem lock support.")
    import fcntl

    _check_locations(plan)
    path = plan.stage.parent / "cleanup.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValidationError("Removal cleanup lock changed.")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _check_locations(plan)
        if path.lstat().st_ino != info.st_ino:
            raise ValidationError("Removal cleanup lock changed.")
        return fd
    except BaseException:
        os.close(fd)
        raise


@asynccontextmanager
async def removal_claim(plan: RemovalPlan) -> AsyncIterator[None]:
    task = asyncio.create_task(asyncio.to_thread(_claim, plan))
    fd: int | None = None
    try:
        try:
            fd = await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                with suppress(asyncio.CancelledError, Exception):
                    await asyncio.shield(task)
            if not task.cancelled() and task.exception() is None:
                fd = task.result()
            raise
        yield
    finally:
        if fd is not None:
            await finish_short_mutation(asyncio.create_task(asyncio.to_thread(os.close, fd)))


def create_backup_stage(plan: RemovalPlan) -> FileFingerprint:
    _check_locations(plan)
    assert plan.trash_stage is not None
    if stat.S_ISDIR(plan.fingerprint[5]):
        plan.trash_stage.mkdir(mode=0o700)
    else:
        fd = os.open(plan.trash_stage, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
    sync_directory(plan.trash_stage.parent)
    result = _fingerprint(plan.trash_stage)
    assert result is not None
    return result


def _copy_file(source: Path, destination: Path, stop: Event, *, exists: bool = False) -> None:
    check_stop(stop)
    before = _fingerprint(source)
    if before is None or not stat.S_ISREG(before[5]):
        raise ValidationError("Removal backup source changed.")
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if os.fstat(source_fd).st_ino != before[1]:
            raise ValidationError("Removal backup source changed.")
        target_fd = os.open(
            destination,
            os.O_WRONLY | os.O_NOFOLLOW | (os.O_TRUNC if exists else os.O_CREAT | os.O_EXCL),
            stat.S_IMODE(before[5]) & 0o777,
        )
        with os.fdopen(target_fd, "wb") as output:
            while chunk := os.read(source_fd, 1024 * 1024):
                check_stop(stop)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
    finally:
        os.close(source_fd)
    if _fingerprint(source) != before:
        raise ValidationError("Removal backup source changed during copying.")


def copy_payload(plan: RemovalPlan, stop: Event) -> tuple[FileFingerprint, str]:
    _check_locations(plan)
    require_payload(plan.stage, plan.fingerprint)
    assert plan.trash_stage is not None

    def copy(source: Path, target: Path, depth: int, *, exists: bool = False) -> None:
        check_stop(stop)
        if depth > 128:
            raise ValidationError("Removal backup contains too many nested folders.")
        mode = source.lstat().st_mode
        if source.lstat().st_dev != plan.fingerprint[0]:
            raise ValidationError("Removal backup cannot cross a nested filesystem mount.")
        if stat.S_ISDIR(mode):
            if not exists:
                target.mkdir(mode=stat.S_IMODE(mode) & 0o777)
            with os.scandir(source) as entries:
                for entry in entries:
                    copy(source / entry.name, target / entry.name, depth + 1)
            sync_directory(target)
        elif stat.S_ISREG(mode):
            _copy_file(source, target, stop, exists=exists)
        elif stat.S_ISLNK(mode):
            os.symlink(os.readlink(source), target)
        else:
            raise ValidationError("Removal backup contains an unsupported filesystem entry.")

    copy(plan.stage, plan.trash_stage, 0, exists=True)
    digest = payload_digest(plan.stage, stop)
    if payload_digest(plan.trash_stage, stop) != digest:
        raise ValidationError("Removal backup does not match the retained source.")
    require_payload(plan.stage, plan.fingerprint)
    _check_locations(plan)
    result = _fingerprint(plan.trash_stage)
    assert result is not None
    return result, digest


def payload_digest(path: Path, stop: Event) -> str:
    digest = hashlib.sha256()

    def visit(item: Path, depth: int) -> None:
        check_stop(stop)
        if depth > 128:
            raise ValidationError("Removal backup contains too many nested folders.")
        before = _fingerprint(item)
        if before is None:
            raise ValidationError("Removal backup changed.")
        mode = before[5]
        if stat.S_ISDIR(mode):
            digest.update(b"directory\0")
            children: list[Path] = []
            for child in item.iterdir():
                check_stop(stop)
                if len(children) >= 100_000:
                    raise ValidationError("Removal backup folder exceeds the entry limit.")
                children.append(child)
            for child in sorted(children):
                name = os.fsencode(child.name)
                digest.update(len(name).to_bytes(8, "big") + name)
                visit(child, depth + 1)
            digest.update(b"end\0")
        elif stat.S_ISREG(mode):
            digest.update(b"file\0" + before[2].to_bytes(8, "big"))
            fd = os.open(item, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                if os.fstat(fd).st_ino != before[1]:
                    raise ValidationError("Removal backup changed.")
                while chunk := os.read(fd, 1024 * 1024):
                    check_stop(stop)
                    digest.update(chunk)
            finally:
                os.close(fd)
        elif stat.S_ISLNK(mode):
            link = os.fsencode(os.readlink(item))
            digest.update(b"link\0" + len(link).to_bytes(8, "big") + link)
        else:
            raise ValidationError("Removal backup contains an unsupported filesystem entry.")
        if _fingerprint(item) != before:
            raise ValidationError("Removal backup changed while checking it.")

    visit(path, 0)
    return digest.hexdigest()


def remove_payload(path: Path, expected: FileFingerprint, stop: Event, *, partial: bool) -> None:
    """Unlink within directory descriptors; never follow a nested symlink."""
    check_stop(stop)
    require_payload(path, expected, partial=partial)

    def remove(parent: int, name: str, depth: int) -> None:
        check_stop(stop)
        if depth > 128:
            raise ValidationError("Removal cleanup contains too many nested folders.")
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if before.st_dev != expected[0]:
            raise ValidationError("Removal cleanup cannot cross a nested filesystem mount.")
        if not stat.S_ISDIR(before.st_mode):
            os.unlink(name, dir_fd=parent)
            return
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise ValidationError("Removal cleanup folder changed.")
            with os.scandir(fd) as entries:
                for entry in entries:
                    remove(fd, entry.name, depth + 1)
            os.fsync(fd)
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
                raise ValidationError("Removal cleanup folder changed.")
            os.rmdir(name, dir_fd=parent)
        finally:
            os.close(fd)

    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        remove(parent, path.name, 0)
        os.fsync(parent)
    finally:
        os.close(parent)
