"""Provider-independent downloading of public metadata artwork."""

import asyncio
import hashlib
import io
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx
import structlog
from PIL import Image

from pullbox.core.issue_numbers import normalize_issue_number_text

logger = structlog.get_logger(__name__)
MAX_ARTWORK_BYTES = 16 * 1024 * 1024
MAX_ARTWORK_PIXELS = 20_000_000


def allowed_artwork_url(url: str) -> bool:
    """Accept only supported public provider artwork, for browser and server use."""
    if len(url) > 4096 or any(char.isspace() or ord(char) < 32 for char in url):
        return False
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.port not in {None, 443}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            return False
        path = unquote(parsed.path)
        if "\\" in path or any(ord(char) < 32 for char in path) or ".." in path.split("/"):
            return False
        host = parsed.hostname or ""
        if host in {"metron.cloud", "static.metron.cloud"}:
            return path.startswith("/media/")
        return host in {"comicvine.gamespot.com", "comicvine.com"} or host.endswith(
            ".cbsistatic.com"
        )
    except ValueError:
        return False


def _jpeg_payload(data: bytes) -> bytes:
    with Image.open(io.BytesIO(data)) as image:
        if (
            image.format not in {"JPEG", "PNG", "WEBP"}
            or image.width * image.height > MAX_ARTWORK_PIXELS
        ):
            raise ValueError("Unsupported artwork format or dimensions")
        image.verify()
    with Image.open(io.BytesIO(data)) as image:
        image.load()
        if image.format == "JPEG":
            return data
        output = io.BytesIO()
        with image.convert("RGB") as converted:
            converted.save(output, format="JPEG", quality=90)
        result = output.getvalue()
        if len(result) > MAX_ARTWORK_BYTES:
            raise ValueError("Artwork exceeds cache size limit")
        return result


def _stage_cover(destination: Path, data: bytes) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            prefix=".cover-",
            delete=False,
        ) as file:
            stage = Path(file.name)
            file.write(data)
    except BaseException:
        if stage is not None:
            stage.unlink(missing_ok=True)
        raise
    return stage


async def _publish_cover(destination: Path, data: bytes) -> None:
    # A cancelled thread may still finish its write. Join it, then remove its stage.
    worker = asyncio.create_task(asyncio.to_thread(_stage_cover, destination, data))
    stage: Path | None = None
    try:
        stage = await asyncio.shield(worker)
        stage.replace(destination)
    except asyncio.CancelledError:
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not worker.cancelled() and worker.exception() is None:
            stage = worker.result()
        raise
    finally:
        if stage is not None:
            stage.unlink(missing_ok=True)


class ProviderArtworkClient:
    """Reuse an unauthenticated HTTP client for allowlisted public CDN artwork."""

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._client = httpx.AsyncClient(
            timeout=10.0,
            transport=transport,
            follow_redirects=False,
            headers={"User-Agent": "Pullbox", "Accept-Encoding": "identity"},
        )

    async def __aenter__(self) -> "ProviderArtworkClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._client.aclose()

    async def download_cover(self, url: str, destination: Path) -> bool:
        """Publish a verified JPEG atomically, leaving any old cover on failure."""
        if not allowed_artwork_url(url):
            logger.warning("provider_artwork_url_rejected")
            return False
        try:
            self._client.cookies.clear()
            async with asyncio.timeout(20):
                async with self._client.stream("GET", url) as response:
                    response.raise_for_status()
                    # Refuse HTTP decompression before a decoded chunk can exceed our cap.
                    if (
                        response.headers.get("content-encoding", "identity").strip().lower()
                        != "identity"
                    ):
                        raise ValueError("Unsupported artwork content encoding")
                    if response.headers.get("content-type", "").split(";", 1)[
                        0
                    ].strip().lower() not in {
                        "image/jpeg",
                        "image/png",
                        "image/webp",
                        "application/octet-stream",
                    }:
                        raise ValueError("Unsupported artwork content type")
                    length = response.headers.get("content-length")
                    if length is not None and not 0 <= int(length) <= MAX_ARTWORK_BYTES:
                        raise ValueError("Artwork exceeds download size limit")
                    content = bytearray()
                    async for chunk in response.aiter_bytes(64 * 1024):
                        if len(content) + len(chunk) > MAX_ARTWORK_BYTES:
                            raise ValueError("Artwork exceeds download size limit")
                        content.extend(chunk)
            payload = await asyncio.to_thread(_jpeg_payload, bytes(content))
            await _publish_cover(destination, payload)
        except (
            httpx.HTTPError,
            OSError,
            ValueError,
            TimeoutError,
            Image.DecompressionBombError,
        ) as exc:
            logger.warning("provider_artwork_download_failed", error_type=type(exc).__name__)
            return False
        return True


@asynccontextmanager
async def pending_provider_cover(
    client: ProviderArtworkClient,
    url: str,
    destination: Path,
) -> AsyncIterator[Path | None]:
    """Keep new artwork invisible until the caller revalidates its DB snapshot."""
    pending = destination.with_name(f".{uuid4().hex}.{destination.name}")
    try:
        yield pending if await client.download_cover(url, pending) else None
    finally:
        pending.unlink(missing_ok=True)


def issue_cover_stem(issue_number: float, issue_number_text: str | None = None) -> str:
    """Retain unambiguous legacy names; distinguish suffixes and precise decimals."""
    raw = issue_number_text or str(issue_number)
    try:
        exact = normalize_issue_number_text(raw)
        number = (
            f"{int(issue_number):03d}"
            if issue_number == int(issue_number)
            else f"{issue_number:06.1f}"
        )
        if normalize_issue_number_text(number) == exact:
            return f"issue_{number}"
    except (ValueError, OverflowError):
        exact = raw
    # Cache identity only, not a credential; bounded for arbitrary issue labels.
    return "issue_exact_" + hashlib.sha256(exact.encode("utf-8")).hexdigest()
