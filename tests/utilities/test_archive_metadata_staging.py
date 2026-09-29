"""Real workers must never publish the library artifact they are preparing."""

import asyncio
import json
import stat
import sys
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from pullbox.core.exceptions import JobCancelledError, JobPausedError
from pullbox.core.file_publication import publish_file_without_overwrite
from pullbox.core.file_safety import FileSafetyError
from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metroninfo_schema import validate_metroninfo_xml
from pullbox.schemas.metadata_snapshot import MetadataSnapshot, MetadataValues
from pullbox.utilities.executors import archive_metadata_staging as staging
from pullbox.utilities.executors import archive_subprocess


def snapshots(description=None):
    return (
        MetadataSnapshot(
            entity_kind=MetadataEntityKind.SERIES,
            identities=(
                ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "42"),
            ),
            values=MetadataValues(title="Example", description=description),
        ),
        MetadataSnapshot(
            entity_kind=MetadataEntityKind.ISSUE,
            identities=(
                ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "7"),
            ),
            values=MetadataValues(issue_number_text="50-x"),
        ),
    )


@pytest.fixture
def source(tmp_path):
    path = tmp_path / "source.cbz"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("001.jpg", b"page" * 200_000)
    return path


def stage(source, **kwargs):
    return staging.stage_cbz_metadata_interruptible(
        source, source.parent, *snapshots(), max_uncompressed_bytes=4_000_000, **kwargs
    )


def assert_pair(path):
    with zipfile.ZipFile(path) as archive:
        assert archive.testzip() is None
        assert archive.read("001.jpg") == b"page" * 200_000
        assert ET.fromstring(archive.read("ComicInfo.xml")).findtext("Number") == "50-x"
        validate_metroninfo_xml(archive.read("MetronInfo.xml"))


async def test_real_worker_yields_private_verified_pair_and_never_replaces_source(source):
    original = source.read_bytes()
    events = []
    async with stage(source, progress_callback=lambda *event: events.append(event)) as prepared:
        assert prepared.path != source
        assert prepared.path.parent.parent == source.parent
        assert stat.S_IMODE(prepared.path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(prepared.path.stat().st_mode) == 0o600
        assert prepared.source_path == source
        assert_pair(prepared.path)
        prepared.check_unchanged()
        assert source.read_bytes() == original
        assert events[-1] == ("ready", 1, 1, "files")
        assert all(event[0] != "publishing" for event in events)
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX isolated worker group cleanup")
async def test_cancel_during_completed_worker_cleanup_cannot_return_prepared_output(
    source, monkeypatch
):
    entered = asyncio.Event()
    release = asyncio.Event()
    cleanup = archive_subprocess._terminate_worker_process

    async def delayed_cleanup(proc, communicate, **kwargs):
        assert proc.returncode == 0
        entered.set()
        await release.wait()
        await cleanup(proc, communicate, **kwargs)

    async def prepare():
        async with stage(source):
            return "unexpected prepared output"

    monkeypatch.setattr(archive_subprocess, "_terminate_worker_process", delayed_cleanup)
    task = asyncio.create_task(prepare())
    try:
        await asyncio.wait_for(entered.wait(), 10)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert list(source.parent.iterdir()) == [source]
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_owner_can_publish_after_journal_boundary_without_repacking(source):
    target = source.parent / "library.cbz"
    async with stage(source) as prepared:
        prepared.check_unchanged()
        publish_file_without_overwrite(prepared.path, target)
        assert_pair(target)
    assert target.exists()
    assert set(source.parent.iterdir()) == {source, target}


async def test_failed_owner_or_progress_callback_leaves_source_unchanged(source):
    original = source.read_bytes()
    with pytest.raises(RuntimeError, match="journal unavailable"):
        async with stage(source) as prepared:
            assert prepared.path != source
            raise RuntimeError("journal unavailable")
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


async def test_callback_failure_after_worker_completion_cleans_only_private_stage(source):
    original = source.read_bytes()

    def fail(stage_name, *_args):
        if stage_name == "ready":
            raise RuntimeError("progress unavailable")

    with pytest.raises(RuntimeError, match="progress unavailable"):
        async with stage(source, progress_callback=fail):
            pytest.fail("failed progress must not hand off output")
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("control", [JobCancelledError, JobPausedError])
async def test_control_before_launch_creates_nothing(source, monkeypatch, control):
    async def fail_spawn(*_args, **_kwargs):
        pytest.fail("cancelled work must not start a child")

    async def cancel():
        raise control("stop")

    monkeypatch.setattr(archive_subprocess.asyncio, "create_subprocess_exec", fail_spawn)
    with pytest.raises(control):
        async with stage(source, cancellation_check=cancel):
            pytest.fail("cancelled work must not yield")
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("control", [JobCancelledError, JobPausedError])
async def test_control_at_completed_stage_cannot_publish(source, control):
    ready = False
    original = source.read_bytes()

    def progress(name, *_args):
        nonlocal ready
        ready = name == "ready"

    async def cancel():
        if ready:
            raise control("stop")

    with pytest.raises(control):
        async with stage(source, cancellation_check=cancel, progress_callback=progress):
            pytest.fail("pending control at handoff must win")
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("changed", ["source", "stage"])
@pytest.mark.parametrize("change", ["write", "replace", "symlink", "remove"])
async def test_owner_rechecks_changed_source_and_output(source, changed, change):
    async with stage(source) as prepared:
        path = source if changed == "source" else prepared.path
        if change == "write":
            path.write_bytes(b"changed")
        else:
            path.unlink()
            if change == "replace":
                path.write_bytes(b"replacement")
            elif change == "symlink":
                path.symlink_to("missing")
        with pytest.raises(FileSafetyError, match="changed"):
            prepared.check_unchanged()


async def test_source_changed_by_ready_callback_is_not_handed_off(source):
    def change(name, *_args):
        if name == "ready":
            source.write_bytes(b"replacement")

    with pytest.raises(FileSafetyError, match="changed"):
        async with stage(source, progress_callback=change):
            pytest.fail("changed source must not be handed off")
    assert source.read_bytes() == b"replacement"
    assert list(source.parent.iterdir()) == [source]


async def test_large_snapshot_uses_private_request_not_process_arguments(source, monkeypatch):
    spawn = asyncio.create_subprocess_exec
    launches = []
    private = "private title annotation " * 8_000

    async def capture(*args, **kwargs):
        launches.append(args)
        assert private not in repr(args)
        request = Path(json.loads(args[-1])["request_path"])
        assert stat.S_IMODE(request.stat().st_mode) == 0o600
        assert private in request.read_text()
        return await spawn(*args, **kwargs)

    monkeypatch.setattr(archive_subprocess.asyncio, "create_subprocess_exec", capture)
    async with staging.stage_cbz_metadata_interruptible(
        source, source.parent, *snapshots(private), max_uncompressed_bytes=4_000_000
    ) as prepared:
        assert_pair(prepared.path)
    assert len(launches) == 1
    assert len(repr(launches[0])) < 2_000
    assert list(source.parent.iterdir()) == [source]


async def test_request_budget_fails_before_worker_and_cleans(source, monkeypatch):
    monkeypatch.setattr(staging, "_MAX_REQUEST_BYTES", 1, raising=False)
    with pytest.raises(ValueError, match=r"request.*limit"):
        async with stage(source):
            pytest.fail("oversized request must be refused")
    assert list(source.parent.iterdir()) == [source]


async def test_worker_failure_does_not_echo_private_metadata_or_leave_stage(source):
    private = "DO NOT ECHO THIS PRIVATE VALUE"
    with zipfile.ZipFile(source, "a") as archive:
        archive.writestr("ComicInfo.xml", f"<ComicInfo><Series>{private}</Series></ComicInfo>")
    original = source.read_bytes()
    with pytest.raises(staging.ArchiveMetadataStagingError) as error:
        async with stage(source):
            pytest.fail("conflicting metadata must not yield")
    assert private not in str(error.value)
    assert str(source) not in str(error.value)
    assert "metadata_conflict" in str(error.value)
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


async def test_real_worker_failure_enforces_archive_safety(source):
    with zipfile.ZipFile(source, "a") as archive:
        archive.writestr("../bad.jpg", b"bad")
    original = source.read_bytes()
    with pytest.raises(staging.ArchiveMetadataStagingError, match="unsafe_archive"):
        async with stage(source):
            pytest.fail("unsafe archive must not yield")
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]


async def test_read_only_source_can_stage_without_mutation(source):
    source.chmod(0o444)
    before = source.read_bytes()
    async with stage(source) as prepared:
        assert prepared.path != source
        assert_pair(prepared.path)
    assert source.read_bytes() == before
    assert stat.S_IMODE(source.stat().st_mode) == 0o444


async def test_source_symlink_is_rejected(source):
    linked = source.parent / "linked.cbz"
    linked.symlink_to(source)
    with pytest.raises(FileSafetyError, match="regular"):
        async with stage(linked):
            pytest.fail("link is not independent file ownership")
    assert linked.is_symlink()


async def test_stage_cleanup_does_not_follow_replaced_workspace(source):
    outside = source.parent / "unrelated"
    outside.mkdir()
    sentinel = outside / "keep"
    sentinel.write_text("keep")
    async with stage(source) as prepared:
        assert prepared.path != source
        directory = prepared.path.parent
        moved = directory.with_name("moved")
        directory.rename(moved)
        directory.symlink_to(outside, target_is_directory=True)
    assert sentinel.read_text() == "keep"
    assert directory.is_symlink()
    assert (moved / "metadata.cbz").exists()


async def test_worker_output_changed_before_parent_observes_it_is_rejected(source, monkeypatch):
    run = staging._run_archive_operation

    async def changed(operation, payload, **kwargs):
        result = await run(operation, payload, **kwargs)
        output = Path(payload["request_path"]).parent / "metadata.cbz"
        output.write_bytes(b"changed after verification")
        return result

    monkeypatch.setattr(staging, "_run_archive_operation", changed)
    with pytest.raises(FileSafetyError, match="changed"):
        async with stage(source):
            pytest.fail("must use the worker's verified fingerprint, not a new one")
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("output", [b"\xffPRIVATE", b"PRIVATE", b"[1]", b"null"])
async def test_unreadable_worker_result_has_fixed_error(source, monkeypatch, output):
    class Process:
        returncode = 0

        async def communicate(self):
            return output, b""

    async def launch(*_args, **_kwargs):
        return Process()

    async def cleanup(proc, communicate, **_kwargs):
        assert isinstance(proc, Process)
        await communicate

    monkeypatch.setattr(archive_subprocess.asyncio, "create_subprocess_exec", launch)
    monkeypatch.setattr(archive_subprocess, "_terminate_worker_process", cleanup)
    with pytest.raises(staging.ArchiveMetadataStagingError, match="worker_failed") as error:
        async with stage(source):
            pytest.fail("invalid worker result must not be accepted")
    assert "PRIVATE" not in str(error.value)
    assert list(source.parent.iterdir()) == [source]


@pytest.mark.parametrize("control", ["task", "cancel", "pause", "crash"])
async def test_real_interrupted_worker_cannot_mutate_library_and_cleans_workspace(
    source, monkeypatch, control
):
    original = source.read_bytes()
    spawn = asyncio.create_subprocess_exec
    launched = asyncio.Event()
    children = []
    marker = source.parent / "worker-ready"

    async def launch(*args, **kwargs):
        request_path = json.loads(args[-1])["request_path"]
        script = (
            "import pathlib,sys,os,time; "
            "root=pathlib.Path(sys.argv[1]).parent; "
            "(root/'constructing.cbz').write_bytes(b'partial'); "
            f"pathlib.Path({str(marker)!r}).touch(); "
            + ("os._exit(23)" if control == "crash" else "time.sleep(60)")
        )
        child = await spawn(sys.executable, "-c", script, request_path, **kwargs)
        children.append(child)
        launched.set()
        return child

    async def check():
        if marker.exists() and control in {"cancel", "pause"}:
            raise (JobCancelledError if control == "cancel" else JobPausedError)("stop")

    async def prepare():
        async with stage(source, cancellation_check=check):
            pytest.fail("interrupted worker must not yield")

    monkeypatch.setattr(archive_subprocess.asyncio, "create_subprocess_exec", launch)
    task = asyncio.create_task(prepare())
    expected = {
        "task": asyncio.CancelledError,
        "cancel": JobCancelledError,
        "pause": JobPausedError,
        "crash": staging.ArchiveMetadataStagingError,
    }[control]
    try:
        await asyncio.wait_for(launched.wait(), 5)
        async with asyncio.timeout(5):
            while not marker.exists():
                await asyncio.sleep(0.01)
        if control == "task":
            task.cancel()
        with pytest.raises(expected):
            await asyncio.wait_for(task, 5)
        assert children[0].returncode is not None
        assert source.read_bytes() == original
        assert set(source.parent.iterdir()) == {source, marker}
    finally:
        if children and children[0].returncode is None:
            children[0].kill()
            await children[0].wait()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", ["symlink", "missing", "invalid", "oversized"])
def test_worker_request_is_bounded_regular_and_private(tmp_path, monkeypatch, kind):
    request = tmp_path / "request.json"
    if kind == "symlink":
        request.symlink_to(tmp_path / "absent")
    elif kind == "invalid":
        request.write_text('{"secret": "DO NOT ECHO"}')
    elif kind == "oversized":
        monkeypatch.setattr(staging, "_MAX_REQUEST_BYTES", 10)
        request.write_bytes(b" " * 11)
    with pytest.raises(staging.ArchiveMetadataStagingError) as error:
        staging.worker_stage_metadata({"request_path": str(request)})
    assert str(error.value) == "invalid_plan"


def test_worker_rejects_plan_with_source_changed_since_parent_check(source):
    request = staging._StagingRequest(
        source_path=source,
        source_fingerprint=staging._fingerprint(source),
        series=snapshots()[0],
        issue=snapshots()[1],
        max_uncompressed_bytes=4_000_000,
    )
    path = source.parent / "request.json"
    path.write_text(request.model_dump_json())
    source.write_bytes(b"new file")
    with pytest.raises(staging.ArchiveMetadataStagingError, match="source_changed"):
        staging.worker_stage_metadata({"request_path": str(path)})
    assert not (source.parent / "metadata.cbz").exists()


@pytest.mark.parametrize("details", [b"PRIVATE", b'{"message": "PRIVATE"}', b'{"message": []}'])
def test_arbitrary_worker_failure_message_is_not_reflected(details):
    with pytest.raises(staging.ArchiveMetadataStagingError) as error:
        archive_subprocess._raise_worker_error(
            "paired_stage", {"request_path": "PRIVATE"}, b"PRIVATE", details, returncode=1
        )
    assert str(error.value) == "worker_failed"
