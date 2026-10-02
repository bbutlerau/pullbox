"""PDF conversion must build the reconciled archive once and preserve its source."""

import asyncio
import io
import os
import shutil
import signal
import sys
import zipfile
from contextlib import suppress
from unittest.mock import patch

import pytest
from PIL import Image

from pullbox.core.exceptions import JobCancelledError
from pullbox.core.file_safety import FileSafetyError
from tests.unit.test_archive_metadata_writing import assert_pair, snapshots, write

native_pdf = pytest.mark.skipif(
    not (shutil.which("pdfinfo") and shutil.which("pdftoppm")),
    reason="Native Poppler check; mandatory in production-image runtime validation",
)


def pdf_source(path, *, count=3):
    pages = [Image.new("RGB", (72, 90), ("red", "green", "blue")[i % 3]) for i in range(count)]
    try:
        pages[0].save(path, format="PDF", save_all=True, append_images=pages[1:])
    finally:
        for page in pages:
            page.close()
    return path


@native_pdf
def test_pdf_renders_one_consistent_cbz_in_reading_order(tmp_path):
    source = pdf_source(tmp_path / "input.pdf")
    original = source.read_bytes()
    target = tmp_path / "result.cbz"
    modes = []
    events = []
    init = zipfile.ZipFile.__init__

    def tracked(self, file, mode="r", *args, **kwargs):
        modes.append(mode)
        init(self, file, mode, *args, **kwargs)

    with patch.object(zipfile.ZipFile, "__init__", tracked):
        assert write(source, target, progress_callback=lambda *event: events.append(event))
    assert [mode for mode in modes if mode in {"w", "a", "x"}] == ["w"]
    assert source.read_bytes() == original
    assert_pair(target)
    with zipfile.ZipFile(target) as archive:
        pages = [name for name in archive.namelist() if name.endswith(".jpg")]
        assert pages == ["page_0000.jpg", "page_0001.jpg", "page_0002.jpg"]
        for name, channel in zip(pages, (0, 1, 2), strict=True):
            with Image.open(io.BytesIO(archive.read(name))) as page:
                assert page.size == (200, 250)
                pixel = page.convert("RGB").getpixel((100, 100))
                assert pixel[channel] > max(pixel[(channel + 1) % 3], pixel[(channel + 2) % 3])
    assert [event for event in events if event[0] == "rendering"] == [
        ("rendering", value, 3, "pages") for value in range(4)
    ]
    assert set(tmp_path.iterdir()) == {source, target}


@pytest.mark.parametrize("stage_name", ["rendering", "transferring", "verifying", "publishing"])
@native_pdf
def test_pdf_cancel_preserves_source_and_cleans_workspace(tmp_path, stage_name):
    source = pdf_source(tmp_path / "input.pdf")
    original = source.read_bytes()

    def cancel(stage, current, *_args):
        if stage == stage_name and (current or stage == "publishing"):
            raise JobCancelledError("stop")

    with pytest.raises(JobCancelledError):
        write(source, tmp_path / "result.cbz", progress_callback=cancel)
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@native_pdf
@pytest.mark.parametrize("quality,extension", [("medium", "jpg"), ("high", "png")])
async def test_real_worker_stages_pdf_pair_privately(tmp_path, quality, extension):
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    source = pdf_source(tmp_path / "input.pdf")
    original = source.read_bytes()
    async with stage_cbz_metadata_interruptible(
        source, tmp_path, *snapshots(), max_uncompressed_bytes=1_000_000, pdf_quality=quality
    ) as staged:
        assert_pair(staged.path)
        staged.check_unchanged()
        assert staged.path.parent.parent == tmp_path
        assert source.read_bytes() == original
        with zipfile.ZipFile(staged.path) as archive:
            assert f"page_0000.{extension}" in archive.namelist()
    assert list(tmp_path.iterdir()) == [source]


def test_missing_pdf_renderer_has_a_fixed_error(monkeypatch):
    from pullbox.core import metadata_pdf_source as pdf

    def unavailable(*_args, **_kwargs):
        raise FileNotFoundError("private executable path")

    monkeypatch.setattr(pdf.subprocess, "Popen", unavailable)
    with pytest.raises(FileSafetyError, match="renderer is unavailable") as caught:
        pdf._run_bounded(
            ["pdfinfo"], io.BytesIO(), limit=100, timeout=1, check_cancelled=lambda: None
        )
    assert "private executable path" not in str(caught.value)


def test_pdf_size_limit_stops_before_publication(tmp_path):
    from pullbox.services.archive_metadata_writing import write_cbz_metadata

    source = pdf_source(tmp_path / "input.pdf")
    original = source.read_bytes()
    with pytest.raises(FileSafetyError, match=r"size|limit"):
        write_cbz_metadata(source, tmp_path / "result.cbz", *snapshots(), max_uncompressed_bytes=10)
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize(
    "quality,extension,size", [("low", "jpg", (150, 188)), ("high", "png", (300, 375))]
)
@native_pdf
def test_pdf_quality_preserves_requested_resolution(tmp_path, quality, extension, size):
    source = pdf_source(tmp_path / "input.pdf", count=1)
    target = tmp_path / "result.cbz"
    assert write(source, target, pdf_quality=quality)
    assert_pair(target)
    with (
        zipfile.ZipFile(target) as archive,
        Image.open(io.BytesIO(archive.read(f"page_0000.{extension}"))) as image,
    ):
        assert image.size == size


@pytest.mark.parametrize(
    "details",
    [
        "Pages: 0\nEncrypted: no\n",
        "Pages: 10001\nEncrypted: no\n",
        "Pages: 1\nEncrypted: yes\nPage 1 size: 72 x 90 pts\n",
        "Pages: 1\nEncrypted: no\nPage 1 size: nan x 90 pts\n",
        "Pages: 1\nEncrypted: no\nPage 1 size: 100000 x 100000 pts\n",
        "Pages: 1\nEncrypted: no\nPage 2 size: 72 x 90 pts\n",
        "Pages: 1\nEncrypted: no\nPage 1 size: 72 x 90 pts\nPages: 10001\n",
        "Pages: 1\nEncrypted: no\nPage 1 size: 72 x 90 pts\nEncrypted: yes\n",
    ],
)
def test_pdf_rejects_unverifiable_inspection_before_rendering(tmp_path, monkeypatch, details):
    from pullbox.core import metadata_pdf_source as pdf

    source = pdf_source(tmp_path / "input.pdf", count=1)
    original = source.read_bytes()
    calls = []

    def inspect(command, output, **_kwargs):
        calls.append(command[0])
        if command[0] != "pdfinfo":
            pytest.fail("Unverifiable PDF must not reach the renderer")
        return output.write(details.encode())

    monkeypatch.setattr(pdf, "_run_bounded", inspect)
    with pytest.raises(FileSafetyError):
        write(source, tmp_path / "result.cbz")
    assert calls == ["pdfinfo"]
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@pytest.mark.parametrize("boundary", ["budget", "timeout", "cancel", "exit", "closed_pipe"])
def test_pdf_child_is_reaped_after_boundary_failure(monkeypatch, boundary):
    from pullbox.core import metadata_pdf_source as pdf

    spawned = []
    popen = pdf.subprocess.Popen

    def spawn(*args, **kwargs):
        child = popen(*args, **kwargs)
        spawned.append(child)
        return child

    monkeypatch.setattr(pdf.subprocess, "Popen", spawn)
    program = {
        "budget": "import os; os.write(1, b'x' * 1000000)",
        "timeout": "import time; time.sleep(30)",
        "cancel": "import time; time.sleep(30)",
        "exit": "import sys; sys.stderr.write('private metadata'); sys.exit(2)",
        "closed_pipe": "import os,time; os.close(1); time.sleep(30)",
    }[boundary]
    checks = 0

    def cancel():
        nonlocal checks
        checks += 1
        if boundary == "cancel" and checks > 2:
            raise JobCancelledError("stop")

    output = io.BytesIO()
    with pytest.raises(JobCancelledError if boundary == "cancel" else FileSafetyError) as caught:
        pdf._run_bounded(
            [sys.executable, "-c", program], output, limit=100, timeout=0.5, check_cancelled=cancel
        )
    assert len(output.getvalue()) <= 100
    assert "private metadata" not in str(caught.value)
    assert len(spawned) == 1 and spawned[0].poll() is not None
    assert spawned[0].stdout.closed


@native_pdf
def test_corrupt_pdf_does_not_publish_or_leak_inspection_error(tmp_path):
    source = tmp_path / "corrupt.pdf"
    source.write_bytes(b"%PDF-1.5\nprivate invalid content")
    with pytest.raises(FileSafetyError) as caught:
        write(source, tmp_path / "result.cbz")
    assert "private invalid content" not in str(caught.value)
    assert list(tmp_path.iterdir()) == [source]


@native_pdf
async def test_worker_cancellation_while_pdf_renders_cleans_all_private_pages(tmp_path):
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    source = pdf_source(tmp_path / "input.pdf", count=100)
    original = source.read_bytes()
    rendering = False

    def progress(stage, *_args):
        nonlocal rendering
        rendering |= stage == "rendering"

    async def cancel():
        if rendering:
            raise JobCancelledError("stop during rendering")

    with pytest.raises(JobCancelledError):
        async with stage_cbz_metadata_interruptible(
            source,
            tmp_path,
            *snapshots(),
            max_uncompressed_bytes=10_000_000,
            progress_callback=progress,
            cancellation_check=cancel,
        ):
            pytest.fail("Cancelled rendering must not hand off a stage")
    assert rendering
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


@native_pdf
def test_fractional_pdf_page_sizes_are_not_rejected_by_rounded_inspection(tmp_path):
    source = tmp_path / "fractional.pdf"
    with Image.new("RGB", (72, 90), "red") as page:
        page.save(source, format="PDF", resolution=71.99999999)
    target = tmp_path / "result.cbz"
    assert write(source, target)
    assert_pair(target)
    with (
        zipfile.ZipFile(target) as archive,
        Image.open(io.BytesIO(archive.read("page_0000.jpg"))) as page,
    ):
        assert page.size == (201, 251)


@native_pdf
@pytest.mark.skipif(os.name == "nt", reason="POSIX worker signal/child reaping contract")
async def test_pdf_worker_cancel_reaps_an_active_poppler_child(tmp_path, monkeypatch):
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    source = pdf_source(tmp_path / "input.pdf", count=1)
    original = source.read_bytes()
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    marker = tmp_path / "renderer.pid"
    fake = binary_dir / "pdftoppm"
    fake.write_text(
        f"#!{sys.executable}\nimport os, pathlib, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    fake.chmod(0o700)
    monkeypatch.setenv("PATH", f"{binary_dir}{os.pathsep}{os.environ['PATH']}")

    async def convert():
        async with stage_cbz_metadata_interruptible(
            source, tmp_path, *snapshots(), max_uncompressed_bytes=1_000_000
        ):
            pytest.fail("stopped renderer must not produce an archive")

    task = asyncio.create_task(convert())
    pid = None
    try:
        async with asyncio.timeout(10):
            while not marker.exists():
                await asyncio.sleep(0.01)
        pid = int(marker.read_text())
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert source.read_bytes() == original
        assert not list(tmp_path.glob(".pullbox-*"))
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if pid is not None:
            with suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
