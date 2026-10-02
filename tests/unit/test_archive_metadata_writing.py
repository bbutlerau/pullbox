"""Actual ZIP bytes exercise publication, bounded streaming and failure cleanup."""

import os
import stat
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree as ET

import pytest
from structlog.testing import capture_logs

from pullbox.core.exceptions import JobCancelledError
from pullbox.core.file_safety import FileSafetyError
from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metroninfo import parse_metroninfo
from pullbox.core.metroninfo_schema import validate_metroninfo_xml
from pullbox.schemas.metadata_snapshot import MetadataSnapshot, MetadataValues
from pullbox.services import archive_metadata_writing as writing
from pullbox.services.archive_metadata_rendering import ArchiveMetadataRenderError


def snapshots():
    return (
        MetadataSnapshot(
            entity_kind=MetadataEntityKind.SERIES,
            identities=(
                ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "42"),
            ),
            values=MetadataValues(title="Example", publisher="Publisher"),
        ),
        MetadataSnapshot(
            entity_kind=MetadataEntityKind.ISSUE,
            identities=(
                ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "7"),
            ),
            values=MetadataValues(issue_number_text="50-x"),
        ),
    )


def archive(path, *extras):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as zf:
        zf.comment = b"Preserved archive comment"
        page = zipfile.ZipInfo("pages/001.jpg", (2020, 1, 2, 3, 4, 6))
        page.comment = b"Page comment"
        page.external_attr = (stat.S_IFREG | 0o644) << 16
        zf.writestr(page, b"page bytes" * 200_000)
        for name, data in extras:
            zf.writestr(name, data)
    return path


def write(source, target, **kwargs):
    return writing.write_cbz_metadata(
        source, target, *snapshots(), max_uncompressed_bytes=20_000_000, **kwargs
    )


def assert_pair(path):
    with zipfile.ZipFile(path) as zf:
        assert zf.testzip() is None
        ci = ET.fromstring(zf.read("ComicInfo.xml"))
        mi_bytes = zf.read("MetronInfo.xml")
        validate_metroninfo_xml(mi_bytes)
        mi = parse_metroninfo(mi_bytes)
        assert ci.findtext("Series") == mi.series == "Example"
        assert ci.findtext("Number") == mi.number == "50-x"


def test_copy_renders_pair_in_one_target_construction_and_one_source_open(tmp_path):
    source = archive(tmp_path / "source.cbz")
    target = tmp_path / "target.cbz"
    original = source.read_bytes()
    calls = []
    original_init = zipfile.ZipFile.__init__

    def tracked(self, file, mode="r", *args, **kwargs):
        calls.append((getattr(file, "name", file), mode))
        original_init(self, file, mode, *args, **kwargs)

    with patch.object(zipfile.ZipFile, "__init__", tracked):
        assert write(source, target)
    assert len([mode for _path, mode in calls if mode in {"w", "x", "a"}]) == 1
    assert len([mode for _path, mode in calls if mode == "r"]) == 2  # source + verification
    assert source.read_bytes() == original
    assert_pair(target)
    with zipfile.ZipFile(source) as before, zipfile.ZipFile(target) as after:
        assert before.read("pages/001.jpg") == after.read("pages/001.jpg")
        assert before.comment == after.comment
        b, a = before.getinfo("pages/001.jpg"), after.getinfo("pages/001.jpg")
        assert (b.date_time, b.comment, b.external_attr) == (
            a.date_time,
            a.comment,
            a.external_attr,
        )
    assert set(tmp_path.iterdir()) == {source, target}


def test_refresh_is_opt_in_and_preserves_mode_and_other_hardlink(tmp_path):
    source = archive(tmp_path / "source.cbz")
    source.chmod(0o640)
    linked = tmp_path / "linked.cbz"
    os.link(source, linked)
    original = source.read_bytes()
    with pytest.raises(ValueError, match="replacement"):
        write(source, source)
    assert source.read_bytes() == original
    assert write(source, source, replace_source=True)
    assert stat.S_IMODE(source.stat().st_mode) == 0o640
    assert linked.read_bytes() == original
    assert_pair(source)


def test_refresh_second_pass_is_noop(tmp_path):
    source = archive(tmp_path / "source.cbz")
    assert write(source, source, replace_source=True)
    before = source.stat()
    original = source.read_bytes()
    assert not write(source, source, replace_source=True)
    assert source.read_bytes() == original
    assert source.stat().st_ino == before.st_ino
    assert source.stat().st_mtime_ns == before.st_mtime_ns


def test_nested_case_insensitive_metadata_becomes_one_root_pair(tmp_path):
    source = archive(tmp_path / "source.cbz", ("meta/cOMICiNFO.XML", b"<ComicInfo/>"))
    target = tmp_path / "target.cbz"
    assert write(source, target)
    assert_pair(target)
    with zipfile.ZipFile(target) as zf:
        assert zf.namelist() == ["pages/001.jpg", "ComicInfo.xml", "MetronInfo.xml"]


@pytest.mark.parametrize(
    "members",
    [
        [("ComicInfo.xml", b"<ComicInfo><Notes>[cv_issue_id:99]</Notes></ComicInfo>")],
        [("ComicInfo.xml", b"<ComicInfo><Series>Other series</Series></ComicInfo>")],
        [("MetronInfo.xml", b"<broken")],
        [("ComicInfo.xml", b"<ComicInfo/>"), ("metadata/ComicInfo.xml", b"<ComicInfo/>")],
    ],
)
def test_metadata_conflict_or_unreadable_preserves_original_and_no_target(tmp_path, members):
    source = archive(tmp_path / "source.cbz", *members)
    original = source.read_bytes()
    target = tmp_path / "target.cbz"
    with pytest.raises(ArchiveMetadataRenderError):
        write(source, target)
    assert source.read_bytes() == original
    assert set(tmp_path.iterdir()) == {source}


@pytest.mark.parametrize("name", ["../outside.jpg", "/absolute.jpg", "C:\\bad.jpg", "run.exe"])
def test_unsafe_members_fail_before_any_publication(tmp_path, name):
    source = archive(tmp_path / "source.cbz", (name, b"bad"))
    before = source.read_bytes()
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "target.cbz")
    assert source.read_bytes() == before
    assert set(tmp_path.iterdir()) == {source}


@pytest.mark.parametrize("mode", [stat.S_IFLNK, stat.S_IFIFO, stat.S_IFCHR])
def test_special_archive_members_are_not_copied(tmp_path, mode):
    member = zipfile.ZipInfo("link.jpg")
    member.create_system = 3
    member.external_attr = (mode | 0o644) << 16
    source = archive(tmp_path / "source.cbz", (member, b"target"))
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "target.cbz")
    assert set(tmp_path.iterdir()) == {source}


def test_configured_dangerous_extension_opt_out_does_not_disable_traversal(tmp_path):
    source = archive(tmp_path / "source.cbz", ("legacy.js", b"legacy text"))
    assert write(source, tmp_path / "allowed.cbz", block_dangerous=False)
    source = archive(source, ("../legacy.js", b"bad"))
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "blocked.cbz", block_dangerous=False)


def test_size_budget_is_enforced_before_streaming(tmp_path):
    source = archive(tmp_path / "source.cbz")
    with pytest.raises(FileSafetyError, match=r"decompressed size.*exceeds limit"):
        writing.write_cbz_metadata(
            source, tmp_path / "target.cbz", *snapshots(), max_uncompressed_bytes=10
        )
    assert set(tmp_path.iterdir()) == {source}


@pytest.mark.parametrize("limit", [0, -1, True])
def test_invalid_budget_is_not_an_unlimited_override(tmp_path, limit):
    source = archive(tmp_path / "source.cbz")
    with pytest.raises(ValueError, match="positive"):
        writing.write_cbz_metadata(
            source, tmp_path / "target.cbz", *snapshots(), max_uncompressed_bytes=limit
        )


def test_member_reads_are_bounded_and_progress_is_monotonic(tmp_path):
    source = archive(tmp_path / "source.cbz")
    seen, progress = [], []
    read = zipfile.ZipExtFile.read

    def bounded(self, n=-1):
        seen.append(n)
        assert 0 < n <= 2 * 1024 * 1024 + 1
        return read(self, n)

    with patch.object(zipfile.ZipExtFile, "read", bounded):
        assert write(
            source, tmp_path / "target.cbz", progress_callback=lambda *v: progress.append(v)
        )
    assert len(seen) > 4
    for stage in ("transferring", "verifying"):
        samples = [(current, total) for s, current, total, unit in progress if s == stage]
        assert len(samples) > 1
        assert samples[0][0] == 0
        assert samples[-1][0] == samples[-1][1]
        assert all(0 <= current <= total for current, total in samples)
        assert [c for c, _t in samples] == sorted(c for c, _t in samples)


@pytest.mark.parametrize("stage", ["transferring", "verifying", "publishing"])
@pytest.mark.parametrize("refresh", [False, True])
def test_cancellation_at_any_stage_preserves_original(tmp_path, stage, refresh):
    source = archive(tmp_path / "source.cbz")
    original = source.read_bytes()
    target = source if refresh else tmp_path / "target.cbz"

    def cancel(current_stage, *_args):
        if stage == current_stage:
            raise JobCancelledError("Cancelled")

    with pytest.raises(JobCancelledError):
        write(source, target, replace_source=refresh, progress_callback=cancel)
    assert source.read_bytes() == original
    assert set(tmp_path.iterdir()) == {source}


def test_pre_cancelled_operation_never_opens_archive(tmp_path):
    source = archive(tmp_path / "source.cbz")

    def cancelled():
        raise JobCancelledError("Cancelled")

    with (
        patch.object(zipfile, "ZipFile", side_effect=AssertionError("should not open")),
        pytest.raises(JobCancelledError),
    ):
        write(source, tmp_path / "target.cbz", check_cancelled=cancelled)


def test_existing_target_and_temp_are_never_overwritten(tmp_path):
    source = archive(tmp_path / "source.cbz")
    target, temp = tmp_path / "target.cbz", tmp_path / "stage.cbz"
    target.write_bytes(b"existing")
    with pytest.raises(FileExistsError):
        write(source, target)
    assert target.read_bytes() == b"existing"
    target.unlink()
    temp.write_bytes(b"existing stage")
    with pytest.raises(FileExistsError):
        write(source, target, temp_path=temp)
    assert temp.read_bytes() == b"existing stage"
    assert not target.exists()


def test_concurrent_target_creation_is_not_clobbered(tmp_path):
    source = archive(tmp_path / "source.cbz")
    target = tmp_path / "target.cbz"

    def race(stage, *_args):
        if stage == "publishing":
            target.write_bytes(b"another writer")

    with pytest.raises(FileExistsError):
        write(source, target, progress_callback=race)
    assert target.read_bytes() == b"another writer"
    assert set(tmp_path.iterdir()) == {source, target}


@pytest.mark.parametrize("replace", [False, True])
def test_source_changed_before_publish_is_not_overwritten_or_used(tmp_path, replace):
    source = archive(tmp_path / "source.cbz")
    target = source if replace else tmp_path / "target.cbz"

    def race(stage, *_args):
        if stage == "publishing":
            source.write_bytes(b"changed source")

    with pytest.raises(FileSafetyError, match="changed"):
        write(source, target, replace_source=replace, progress_callback=race)
    assert source.read_bytes() == b"changed source"
    assert set(tmp_path.iterdir()) == {source}


def test_symlink_source_is_rejected(tmp_path):
    actual = archive(tmp_path / "actual.cbz")
    source = tmp_path / "source.cbz"
    source.symlink_to(actual)
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "target.cbz")
    assert source.is_symlink()


def test_temp_path_must_be_in_target_directory(tmp_path):
    source = archive(tmp_path / "source.cbz")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    with pytest.raises(ValueError, match="directory"):
        write(source, tmp_path / "target.cbz", temp_path=elsewhere / "stage.cbz")
    assert not list(elsewhere.iterdir())


def test_bad_source_crc_never_publishes(tmp_path):
    source = archive(tmp_path / "source.cbz")
    data = source.read_bytes().replace(b"page bytes", b"bad! bytes", 1)
    source.write_bytes(data)
    with pytest.raises(zipfile.BadZipFile):
        write(source, tmp_path / "target.cbz")
    assert source.read_bytes() == data
    assert set(tmp_path.iterdir()) == {source}


def test_failed_publication_preserves_source_and_cleans_stage(tmp_path):
    source = archive(tmp_path / "source.cbz")
    before = source.read_bytes()
    with (
        patch("os.replace", side_effect=OSError("publication failed")),
        pytest.raises(OSError, match="publication failed"),
    ):
        write(source, source, replace_source=True)
    assert source.read_bytes() == before
    assert set(tmp_path.iterdir()) == {source}


def test_publication_occurs_only_after_source_handle_closed(tmp_path):
    source = archive(tmp_path / "source.cbz")
    handles = []
    fdopen = os.fdopen
    replace = os.replace

    def track(fd, mode, *args, **kwargs):
        result = fdopen(fd, mode, *args, **kwargs)
        handles.append(result)
        return result

    def publish(stage, target):
        assert all(handle.closed for handle in handles)
        replace(stage, target)

    with patch("os.fdopen", track), patch("os.replace", publish):
        assert write(source, source, replace_source=True)
    assert_pair(source)


@pytest.mark.parametrize("link", [False, True])
def test_swapped_stage_is_not_published_or_deleted(tmp_path, link):
    source = archive(tmp_path / "source.cbz")
    stage, impostor = tmp_path / "stage.cbz", tmp_path / "impostor.cbz"
    before = source.read_bytes()

    def swap(current_stage, current, *_args):
        if current_stage == "verifying" and current == 0:
            stage.rename(impostor)
            if link:
                stage.symlink_to(impostor)
            else:
                stage.write_bytes(impostor.read_bytes())

    with pytest.raises(FileSafetyError, match="changed"):
        write(source, source, replace_source=True, temp_path=stage, progress_callback=swap)
    assert source.read_bytes() == before
    assert stage.exists() and impostor.exists()
    assert stage.is_symlink() is link


def test_verification_detects_corrupt_temporary_output(tmp_path):
    source = archive(tmp_path / "source.cbz")
    stage = tmp_path / "stage.cbz"
    before = source.read_bytes()

    def corrupt(current_stage, current, *_args):
        if current_stage == "verifying" and current == 0:
            stage.write_bytes(stage.read_bytes().replace(b"page bytes", b"bad! bytes", 1))

    with pytest.raises(zipfile.BadZipFile):
        write(source, source, replace_source=True, temp_path=stage, progress_callback=corrupt)
    assert source.read_bytes() == before
    assert set(tmp_path.iterdir()) == {source}


@pytest.mark.parametrize(
    "names", [("page.jpg", "PAGE.jpg"), ("folder/page.jpg", "folder\\page.jpg")]
)
def test_colliding_member_names_fail_closed(tmp_path, names):
    source = archive(tmp_path / "source.cbz", *((name, b"content") for name in names))
    with pytest.raises(FileSafetyError, match="duplicate"):
        write(source, tmp_path / "target.cbz")
    assert set(tmp_path.iterdir()) == {source}


def test_symlink_destination_is_not_replaced(tmp_path):
    source = archive(tmp_path / "source.cbz")
    target = tmp_path / "target.cbz"
    target.symlink_to(tmp_path / "absent")
    with pytest.raises(FileExistsError):
        write(source, target)
    assert target.is_symlink()


def test_metadata_bytes_are_included_in_output_budget(tmp_path):
    source = archive(tmp_path / "source.cbz")
    with zipfile.ZipFile(source) as zf:
        budget = sum(item.file_size for item in zf.infolist())
    with pytest.raises(FileSafetyError, match="after metadata"):
        writing.write_cbz_metadata(
            source, tmp_path / "target.cbz", *snapshots(), max_uncompressed_bytes=budget
        )
    assert set(tmp_path.iterdir()) == {source}


def test_published_fallback_target_remains_success_if_staging_unlink_fails(tmp_path):
    source = archive(tmp_path / "source.cbz")
    target, stage = tmp_path / "target.cbz", tmp_path / "stage.cbz"
    unlink = Path.unlink

    def deny_stage(path, *args, **kwargs):
        if path == stage:
            raise PermissionError("staging cleanup denied")
        return unlink(path, *args, **kwargs)

    with (
        patch("pullbox.core.file_publication._native_rename_without_overwrite", return_value=False),
        patch.object(Path, "unlink", deny_stage),
        capture_logs() as logs,
    ):
        assert write(source, target, temp_path=stage)
    assert_pair(target)
    assert target.stat().st_ino == stage.stat().st_ino
    assert any(log["event"] == "archive_metadata_stage_cleanup_failed" for log in logs)


def test_fsync_failure_preserves_source_and_cleans_stage(tmp_path):
    source = archive(tmp_path / "source.cbz")
    before = source.read_bytes()
    with patch("os.fsync", side_effect=OSError("disk unavailable")), pytest.raises(OSError):
        write(source, source, replace_source=True)
    assert source.read_bytes() == before
    assert set(tmp_path.iterdir()) == {source}


def test_failed_metadata_write_preserves_source_and_cleans_stage(tmp_path):
    source = archive(tmp_path / "source.cbz")
    before = source.read_bytes()
    with (
        patch.object(zipfile.ZipFile, "writestr", side_effect=OSError("disk full")),
        pytest.raises(OSError),
    ):
        write(source, source, replace_source=True)
    assert source.read_bytes() == before
    assert set(tmp_path.iterdir()) == {source}


def test_optional_platform_open_flags_do_not_block_writing(tmp_path, monkeypatch):
    source = archive(tmp_path / "source.cbz")
    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    monkeypatch.delattr(os, "O_NONBLOCK", raising=False)
    assert write(source, tmp_path / "target.cbz")
    assert_pair(tmp_path / "target.cbz")


def test_read_only_source_can_be_copied_but_not_refreshed(tmp_path):
    source = archive(tmp_path / "source.cbz")
    source.chmod(0o444)
    original = source.read_bytes()
    assert write(source, tmp_path / "copy.cbz")
    with pytest.raises(FileSafetyError, match="read-only"):
        write(source, source, replace_source=True)
    assert source.read_bytes() == original
