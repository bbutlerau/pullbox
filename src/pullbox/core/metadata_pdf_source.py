"""Bounded PDF page spools for the single-construction paired archive writer."""

import io
import math
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO, Literal

from PIL import Image

from pullbox.core.file_safety import FileSafetyError
from pullbox.core.metadata_archive_source import MetadataArchiveSource, _entry

type PdfQuality = Literal["high", "medium", "low"]
_PRESETS = {"high": (300, "png", 0), "medium": (200, "jpg", 90), "low": (150, "jpg", 80)}
_MAX_PAGES = 10_000
_MAX_PAGE_PIXELS = 80_000_000
_INFO_BYTES = 4 * 1024 * 1024
_CHUNK_BYTES = 64 * 1024


def _run_bounded(
    command: list[str],
    output: BinaryIO,
    *,
    limit: int,
    timeout: float,
    check_cancelled: Callable[[], None],
) -> int:
    """Bound pipe memory/disk and reap Poppler even on callback failure or timeout."""
    check_cancelled()
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "LC_ALL": "C"},
        )
    except OSError as exc:
        raise FileSafetyError("PDF renderer is unavailable") from exc
    assert process.stdout is not None
    pipe = process.stdout
    chunks: queue.Queue[bytes | OSError | None] = queue.Queue(maxsize=2)
    stop = threading.Event()

    def send(chunk: bytes | OSError | None) -> None:
        while not stop.is_set():
            try:
                chunks.put(chunk, timeout=0.05)
                return
            except queue.Full:
                continue

    def read() -> None:
        try:
            while not stop.is_set():
                chunk = pipe.read(_CHUNK_BYTES)
                if not chunk:
                    break
                send(chunk)
        except OSError as exc:
            send(exc)
        finally:
            send(None)

    reader = threading.Thread(target=read, name="pdf-output", daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout
    written = 0
    eof = False
    try:
        while not eof or process.poll() is None:
            check_cancelled()
            if time.monotonic() >= deadline:
                raise FileSafetyError("PDF rendering exceeded its time limit")
            if eof:
                stop.wait(0.05)
                continue
            try:
                chunk = chunks.get(timeout=0.05)
            except queue.Empty:
                continue
            if chunk is None:
                eof = True
            elif isinstance(chunk, OSError):
                raise FileSafetyError("PDF renderer output could not be read") from chunk
            else:
                if written + len(chunk) > limit:
                    raise FileSafetyError("PDF rendered size exceeds limit")
                output.write(chunk)
                written += len(chunk)
        if process.returncode != 0:
            raise FileSafetyError("PDF could not be inspected or rendered")
        check_cancelled()
        return written
    finally:
        stop.set()
        if process.poll() is None:
            process.kill()
        process.wait()
        reader.join()
        pipe.close()


def open_pdf_pages(
    stream: BinaryIO,
    stack: ExitStack,
    *,
    limit: int,
    scratch_parent: Path,
    quality: PdfQuality,
    check_cancelled: Callable[[], None],
    progress: Callable[[int, int], None],
) -> MetadataArchiveSource:
    """Render one page at a time; never reopen the caller's mutable source path."""
    dpi, extension, jpeg_quality = _PRESETS[quality]
    directory = Path(
        stack.enter_context(tempfile.TemporaryDirectory(prefix=".pullbox-pdf-", dir=scratch_parent))
    )
    private_input = directory / "input.pdf"
    with private_input.open("xb") as copied:
        total = 0
        while chunk := stream.read(_CHUNK_BYTES):
            check_cancelled()
            total += len(chunk)
            if total > limit:
                raise FileSafetyError("PDF source size exceeds limit")
            copied.write(chunk)

    info = io.BytesIO()
    _run_bounded(
        ["pdfinfo", "-f", "1", "-l", str(_MAX_PAGES), str(private_input)],
        info,
        limit=_INFO_BYTES,
        timeout=15,
        check_cancelled=check_cancelled,
    )
    details = info.getvalue().decode("utf-8", errors="replace")
    counts = re.findall(r"^Pages:[ \t]+(\d+)[ \t]*$", details, re.MULTILINE)
    if len(counts) != 1 or len(counts[0]) > 5:
        raise FileSafetyError("PDF page count could not be verified")
    count = int(counts[0])
    if count < 1 or count > _MAX_PAGES:
        raise FileSafetyError("PDF page count exceeds limit or is empty")
    if re.findall(r"^Encrypted:[ \t]+(.+)$", details, re.MULTILINE) != ["no"]:
        raise FileSafetyError("Encrypted PDF cannot be converted safely")
    sizes = re.findall(r"^Page\s+(\d+) size:\s+(\S+) x (\S+) pts.*$", details, re.MULTILINE)
    if len(sizes) != count:
        raise FileSafetyError("PDF page dimensions could not be verified")
    dimensions = []
    for expected, (number, width, height) in enumerate(sizes, 1):
        try:
            pixels = (float(width) * dpi / 72, float(height) * dpi / 72)
            if int(number) != expected or any(not math.isfinite(p) or p <= 0 for p in pixels):
                raise ValueError
            size = (math.ceil(pixels[0]), math.ceil(pixels[1]))
        except (ValueError, OverflowError) as exc:
            raise FileSafetyError("PDF page dimensions could not be verified") from exc
        # pdfinfo rounds physical sizes; allow one raster pixel without widening the cap.
        if (size[0] + 1) * (size[1] + 1) > _MAX_PAGE_PIXELS:
            raise FileSafetyError("PDF page pixel count exceeds limit")
        dimensions.append(size)

    progress(0, count)
    entries = []
    written = 0
    for number, expected_size in enumerate(dimensions, 1):
        check_cancelled()
        name = f"page_{number - 1:04d}.{extension}"
        path = directory / name
        command = ["pdftoppm", "-f", str(number), "-l", str(number), "-singlefile", "-r", str(dpi)]
        command += (
            ["-png"] if extension == "png" else ["-jpeg", "-jpegopt", f"quality={jpeg_quality}"]
        )
        command.append(str(private_input))
        with path.open("xb") as output:
            page_bytes = _run_bounded(
                command, output, limit=limit - written, timeout=120, check_cancelled=check_cancelled
            )
        try:
            with Image.open(path) as image:
                if image.width * image.height > _MAX_PAGE_PIXELS or not any(
                    all(
                        abs(actual - expected) <= 1
                        for actual, expected in zip(image.size, size, strict=True)
                    )
                    for size in (expected_size, expected_size[::-1])
                ):
                    raise FileSafetyError("PDF rendered page dimensions disagree with inspection")
                image.verify()
        except (OSError, ValueError, Image.DecompressionBombError) as exc:
            raise FileSafetyError("PDF rendered page could not be verified") from exc
        written += page_bytes
        entries.append(_entry(name, page_bytes))
        progress(number, count)
    private_input.unlink()
    return MetadataArchiveSource(
        entries, lambda entry: (directory / entry.filename).open("rb"), stack.close
    )
