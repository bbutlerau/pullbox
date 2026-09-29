"""Real non-ZIP containers exercise paired conversion without an intermediate CBZ."""

import io
import stat
import struct
import tarfile
import zipfile
import zlib
from unittest.mock import patch

import py7zr
import pytest

from pullbox.core.exceptions import JobCancelledError
from pullbox.core.file_safety import FileSafetyError
from pullbox.services.archive_metadata_rendering import ArchiveMetadataRenderError
from tests.unit.test_archive_metadata_writing import assert_pair, snapshots, write


def nonzip_archive(path, members=None, *, mode=stat.S_IFREG | 0o644):
    members = members or [("pages/001.jpg", b"page bytes" * 10000)]
    if path.suffix == ".cb7":
        with py7zr.SevenZipFile(path, "w") as archive:
            for name, payload in members:
                archive.writestr(payload, name)
    elif path.suffix == ".cbt":
        with tarfile.open(path, "w") as archive:
            for name, payload in members:
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mode = mode & 0o777
                archive.addfile(info, io.BytesIO(payload))
    else:
        # Minimal stored RAR4 fixture. The real rarfile parser checks header/data CRCs.
        def block(kind, flags, body):
            header = struct.pack("<BHH", kind, flags, 7 + len(body)) + body
            return struct.pack("<H", zlib.crc32(header) & 0xFFFF) + header

        data = bytearray(b"Rar!\x1a\x07\x00")
        data.extend(block(0x73, 0, b"\x00" * 6))
        for name, payload in members:
            encoded = name.encode("ascii")
            header = struct.pack(
                "<LLBLLBBHL",
                len(payload),
                len(payload),
                3,
                zlib.crc32(payload),
                0x50210000,
                20,
                0x30,
                len(encoded),
                mode,
            )
            data.extend(block(0x74, 0x8000, header + encoded))
            data.extend(payload)
        data.extend(block(0x7B, 0, b""))
        path.write_bytes(data)
    return path


@pytest.fixture(autouse=True)
def stored_rar_needs_no_extractor(monkeypatch):
    # Stored RAR entries are read in-process; production compressed RAR uses UnRAR.
    monkeypatch.setattr("pullbox.core.rar_backend.configure_rarfile_backend", lambda: None)


@pytest.mark.parametrize("suffix", [".cbr", ".cb7", ".cbt"])
def test_conversion_builds_one_cbz_with_consistent_pair_and_original_pages(tmp_path, suffix):
    source = nonzip_archive(
        tmp_path / f"source{suffix}",
        [
            ("pages/001.jpg", b"page bytes" * 10000),
            ("meta/ComicInfo.xml", b"<ComicInfo><Number>50-x</Number></ComicInfo>"),
        ],
    )
    original = source.read_bytes()
    target = tmp_path / "converted.cbz"
    modes = []
    init = zipfile.ZipFile.__init__

    def tracked(self, file, mode="r", *args, **kwargs):
        modes.append(mode)
        init(self, file, mode, *args, **kwargs)

    with patch.object(zipfile.ZipFile, "__init__", tracked):
        assert write(source, target)
    assert [mode for mode in modes if mode in {"w", "x", "a"}] == ["w"]
    assert source.read_bytes() == original
    assert_pair(target)
    with zipfile.ZipFile(target) as archive:
        assert archive.read("pages/001.jpg") == b"page bytes" * 10000
        assert set(archive.namelist()) == {"pages/001.jpg", "ComicInfo.xml", "MetronInfo.xml"}
    assert set(tmp_path.iterdir()) == {source, target}


@pytest.mark.parametrize("suffix", [".cbr", ".cb7", ".cbt"])
@pytest.mark.parametrize("reason", ["conflict", "duplicate", "dangerous", "oversize"])
def test_nonzip_failure_preserves_original_and_has_no_output(tmp_path, suffix, reason):
    members = [("001.jpg", b"pages")]
    if reason == "conflict":
        members += [("ComicInfo.xml", b"<ComicInfo><Number>99</Number></ComicInfo>")]
    elif reason == "duplicate":
        members += [("001.JPG", b"other pages")]
    elif reason == "dangerous":
        members += [("payload.exe", b"unsafe")]
    source = nonzip_archive(tmp_path / f"source{suffix}", members)
    original = source.read_bytes()
    target = tmp_path / "output.cbz"
    from pullbox.services.archive_metadata_writing import write_cbz_metadata

    with pytest.raises((FileSafetyError, ArchiveMetadataRenderError)):
        write_cbz_metadata(
            source,
            target,
            *snapshots(),
            max_uncompressed_bytes=1 if reason == "oversize" else 1000000,
        )
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("suffix", [".cbr", ".cb7", ".cbt"])
@pytest.mark.parametrize("cancel_stage", ["transferring", "verifying", "publishing"])
def test_nonzip_cancel_never_publishes(tmp_path, suffix, cancel_stage):
    source = nonzip_archive(tmp_path / f"source{suffix}")
    original = source.read_bytes()

    def cancel(stage, *_args):
        if stage == cancel_stage:
            raise JobCancelledError("stop")

    with pytest.raises(JobCancelledError):
        write(source, tmp_path / "out.cbz", progress_callback=cancel)
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("name", ["../escape.jpg", "/absolute.jpg", "x/../escape.jpg"])
def test_tar_traversal_is_rejected_before_reading_pages(tmp_path, name):
    source = nonzip_archive(tmp_path / "source.cbt", [(name, b"bad")])
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "out.cbz")
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE])
def test_tar_special_members_are_never_followed(tmp_path, kind):
    source = tmp_path / "source.cbt"
    with tarfile.open(source, "w") as archive:
        member = tarfile.TarInfo("001.jpg")
        member.type = kind
        member.linkname = "/etc/passwd"
        archive.addfile(member)
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "out.cbz")
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("kind", [stat.S_IFLNK, stat.S_IFIFO, stat.S_IFCHR, stat.S_IFBLK])
def test_rar_special_member_is_not_mistaken_for_a_page(tmp_path, kind):
    source = nonzip_archive(tmp_path / "source.cbr", mode=kind | 0o777)
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "out.cbz")


def test_rar_hardlink_redirection_is_rejected_before_reading(tmp_path, monkeypatch):
    import rarfile

    source = nonzip_archive(tmp_path / "source.cbr")
    entries = rarfile.RarFile.infolist

    def with_redirection(self):
        members = entries(self)
        members[0].file_redir = (rarfile.RAR5_XREDIR_HARD_LINK, 0, "other.jpg")
        assert members[0].is_file() and not members[0].is_symlink()
        return members

    monkeypatch.setattr(rarfile.RarFile, "infolist", with_redirection)
    monkeypatch.setattr(
        rarfile.RarFile,
        "open",
        lambda *args, **kwargs: pytest.fail(
            "Hard-link redirection must be rejected before reading archive members"
        ),
    )
    with pytest.raises(FileSafetyError, match="unsafe"):
        write(source, tmp_path / "out.cbz")
    assert list(tmp_path.iterdir()) == [source]


def test_solid_extraction_is_cancellable_and_confined_to_owner_workspace(tmp_path, monkeypatch):
    from pullbox.core import metadata_archive_source

    source = nonzip_archive(tmp_path / "source.cb7")
    owner_workspace = tmp_path / "private-stage"
    owner_workspace.mkdir()
    created = []
    create = metadata_archive_source.tempfile.TemporaryDirectory

    def track(*args, **kwargs):
        directory = create(*args, **kwargs)
        created.append(directory.name)
        return directory

    def cancel(stage, current, *_args):
        if stage == "extracting" and current:
            raise JobCancelledError("stop solid extraction")

    monkeypatch.setattr(metadata_archive_source.tempfile, "TemporaryDirectory", track)
    original = source.read_bytes()
    with pytest.raises(JobCancelledError):
        write(source, owner_workspace / "output.cbz", progress_callback=cancel)
    from pathlib import Path

    assert created and all(Path(path).parent == owner_workspace for path in created)
    assert all(not Path(path).exists() for path in created)
    assert source.read_bytes() == original
    assert not list(owner_workspace.iterdir())


@pytest.mark.parametrize("suffix", [".cbr", ".cb7", ".cbt"])
def test_nonzip_changed_source_or_racing_destination_is_not_overwritten(tmp_path, suffix):
    source = nonzip_archive(tmp_path / f"source{suffix}")
    target = tmp_path / "out.cbz"

    def replace(stage, *_args):
        if stage == "publishing":
            target.write_bytes(b"someone else's file")

    original = source.read_bytes()
    with pytest.raises(FileExistsError):
        write(source, target, progress_callback=replace)
    assert source.read_bytes() == original
    assert target.read_bytes() == b"someone else's file"

    target.unlink()

    def change_source(stage, *_args):
        if stage == "publishing":
            source.write_bytes(b"external replacement")

    with pytest.raises(FileSafetyError, match="changed"):
        write(source, target, progress_callback=change_source)
    assert source.read_bytes() == b"external replacement"
    assert not target.exists()


@pytest.mark.parametrize("suffix", [".cb7", ".cbt"])
async def test_real_worker_stages_nonzip_pair_privately(tmp_path, suffix):
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    source = nonzip_archive(tmp_path / f"source{suffix}")
    original = source.read_bytes()
    async with stage_cbz_metadata_interruptible(
        source, tmp_path, *snapshots(), max_uncompressed_bytes=1000000
    ) as staged:
        assert_pair(staged.path)
        staged.check_unchanged()
        assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("suffix", [".cbr", ".cbt"])
def test_corrupt_nonzip_payload_never_publishes(tmp_path, suffix):
    source = nonzip_archive(tmp_path / f"source{suffix}", [("001.jpg", b"known page payload")])
    data = source.read_bytes()
    if suffix == ".cbr":
        data = data.replace(b"known page payload", b"wrong page payload")
    else:
        data = data[:513]
    source.write_bytes(data)
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "out.cbz")
    assert source.read_bytes() == data
    assert list(tmp_path.iterdir()) == [source]


def test_tar_budget_is_checked_before_advancing_past_oversized_member(tmp_path, monkeypatch):
    from pullbox.services.archive_metadata_writing import write_cbz_metadata

    source = nonzip_archive(tmp_path / "source.cbt")
    advance = tarfile.TarFile.next

    def bounded_next(self):
        assert not self.members or self.firstmember is not None, (
            "Inspect the declared size before advancing past the first header"
        )
        return advance(self)

    monkeypatch.setattr(tarfile.TarFile, "next", bounded_next)
    with pytest.raises(FileSafetyError, match="size"):
        write_cbz_metadata(source, tmp_path / "out.cbz", *snapshots(), max_uncompressed_bytes=10)
