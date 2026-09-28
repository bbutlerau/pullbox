"""Artwork is credential-free, bounded, validated and exact-issue-safe."""

import asyncio
import io

import httpx
import pytest
from PIL import Image

from pullbox.services.provider_artwork import ProviderArtworkClient, issue_cover_stem


def cover_bytes(format="JPEG"):
    output = io.BytesIO()
    with Image.new("RGB", (2, 3), "red") as image:
        image.save(output, format=format)
    return output.getvalue()


@pytest.mark.parametrize(
    "url",
    [
        "https://comicvine.gamespot.com/a.jpg",
        "https://comicvine.com/a.jpg",
        "https://comicvine.gamespot.com/a%20b.jpg",
        "https://comicvine.gamespot.com:443/a.jpg",
        "https://comicvine.gamespot.com/a",
        "https://comicvine.gamespot.com/a.jpeg",
        "https://comicvine.gamespot.com/a.png",
        "https://static.metron.cloud/media/issue/test.jpg",
        "https://metron.cloud/media/series/test.jpg",
        "https://images.cbsistatic.com/cover.jpg",
        "https://comicvine.gamespot.com/a.webp",
        "https://comicvine.gamespot.com/a.jpg#",
        "https://comicvine.gamespot.com/a.jpg?",
    ],
)
async def test_public_cover_download_without_provider_credentials(tmp_path, url):
    payload = cover_bytes()
    requests = []

    def handle(request):
        requests.append(request)
        assert "authorization" not in request.headers and "cookie" not in request.headers
        assert "api_key" not in request.url.params
        return httpx.Response(200, content=payload, headers={"content-type": "image/jpeg"})

    destination = tmp_path / "cache" / "series.jpg"
    async with ProviderArtworkClient(transport=httpx.MockTransport(handle)) as client:
        assert await client.download_cover(url, destination)
    assert len(requests) == 1 and destination.read_bytes() == payload
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize(
    "url",
    [
        "http://comicvine.gamespot.com/a.jpg",
        "https://127.0.0.1/cover.jpg",
        "https://static.metron.cloud.evil.test/media/a.jpg",
        "https://user:pass@static.metron.cloud/media/a.jpg",
        "https://static.metron.cloud:8443/media/a.jpg",
        "https://static.metron.cloud/api/token/",
        "https://static.metron.cloud/media/../api/token/",
        "https://static.metron.cloud/media/%2e%2e/api/token/",
        "https://comicvine.gamespot.com/a.jpg?api_key=secret",
        "https://example.test/a.jpg",
    ],
)
async def test_rejects_untrusted_artwork_before_network(tmp_path, url):
    def handle(request):
        pytest.fail("untrusted URL must not be requested")

    async with ProviderArtworkClient(transport=httpx.MockTransport(handle)) as client:
        assert not await client.download_cover(url, tmp_path / "cover.jpg")
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("failure", ["redirect", "html", "corrupt", "oversize", "timeout"])
async def test_failed_download_never_replaces_existing_cover(tmp_path, failure):
    destination = tmp_path / "cover.jpg"
    destination.write_bytes(b"existing")
    calls = []

    def handle(request):
        calls.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("provider timed out", request=request)
        if failure == "redirect":
            return httpx.Response(302, headers={"location": "http://127.0.0.1/private"})
        if failure == "oversize":
            return httpx.Response(
                200,
                headers={"content-length": str(100 * 1024 * 1024), "content-type": "image/jpeg"},
                content=b"x",
            )
        if failure == "html":
            return httpx.Response(
                200, content=b"<html>login</html>", headers={"content-type": "text/html"}
            )
        return httpx.Response(200, content=b"corrupt", headers={"content-type": "image/jpeg"})

    async with ProviderArtworkClient(transport=httpx.MockTransport(handle)) as client:
        assert not await client.download_cover(
            "https://static.metron.cloud/media/issue/a.jpg", destination
        )
    assert len(calls) == 1
    assert destination.read_bytes() == b"existing" and list(tmp_path.iterdir()) == [destination]


async def test_cancellation_propagates_and_leaves_no_partial_file(tmp_path):
    async def handle(request):
        raise asyncio.CancelledError

    async with ProviderArtworkClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(asyncio.CancelledError):
            await client.download_cover(
                "https://static.metron.cloud/media/a.jpg", tmp_path / "cover.jpg"
            )
    assert not list(tmp_path.iterdir())


def test_exact_issue_designations_have_distinct_bounded_cover_names():
    designations = [
        (13, "13"),
        (13, "13A"),
        (13, "13B"),
        (50, "50-X"),
        (50, "50X"),
        (50, "50-O"),
        (1.25, "1.25"),
        (1.2, "1.2"),
        (1.5, "1.5"),
        (13, "13" + "A" * 318),
    ]
    stems = [issue_cover_stem(number, exact) for number, exact in designations]
    assert len(set(stems)) == len(stems)
    assert all(len(stem) < 200 and "/" not in stem and "\\" not in stem for stem in stems)
    assert issue_cover_stem(1, None) == "issue_001"
    assert issue_cover_stem(1.5, None) == "issue_0001.5"
    assert issue_cover_stem(13, "013a") == issue_cover_stem(13, "13A")


@pytest.mark.parametrize("format", ["PNG", "WEBP"])
async def test_image_format_is_normalized_to_destination_jpeg(tmp_path, format):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=cover_bytes(format),
            headers={"content-type": f"image/{format.lower()}"},
        )
    )
    destination = tmp_path / "cover.jpg"
    async with ProviderArtworkClient(transport=transport) as client:
        assert await client.download_cover("https://metron.cloud/media/a.jpg", destination)
    with Image.open(destination) as image:
        assert image.format == "JPEG" and image.size == (2, 3)


async def test_response_cookies_are_never_reused_for_artwork(tmp_path):
    def handle(request):
        assert "cookie" not in request.headers
        return httpx.Response(
            200,
            content=cover_bytes(),
            headers={
                "content-type": "image/jpeg",
                "set-cookie": "session=remote; Path=/",
            },
        )

    async with ProviderArtworkClient(transport=httpx.MockTransport(handle)) as client:
        for number in range(2):
            assert await client.download_cover(
                "https://metron.cloud/media/a.jpg", tmp_path / f"{number}.jpg"
            )


async def test_unbounded_stream_stops_at_byte_budget(tmp_path, monkeypatch):
    monkeypatch.setattr("pullbox.services.provider_artwork.MAX_ARTWORK_BYTES", 65536)
    read = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for number in range(100):
                read.append(number)
                yield b"x" * 65536

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            stream=Stream(),
            headers={"content-type": "image/jpeg"},
        )
    )
    async with ProviderArtworkClient(transport=transport) as client:
        assert not await client.download_cover(
            "https://metron.cloud/media/a.jpg", tmp_path / "cover.jpg"
        )
    assert read == [0, 1] and not list(tmp_path.iterdir())


async def test_cancel_during_staging_joins_writer_without_publishing(tmp_path, monkeypatch):
    import threading

    from pullbox.services import provider_artwork as artwork

    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finish = threading.Event()
    original = artwork._stage_cover

    def stage(destination, data):
        path = original(destination, data)
        loop.call_soon_threadsafe(started.set)
        assert finish.wait(5)
        return path

    monkeypatch.setattr(artwork, "_stage_cover", stage)
    destination = tmp_path / "cover.jpg"
    destination.write_bytes(b"previous")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=cover_bytes(),
            headers={"content-type": "image/jpeg"},
        )
    )
    async with ProviderArtworkClient(transport=transport) as client:
        task = asyncio.create_task(
            client.download_cover("https://metron.cloud/media/a.jpg", destination)
        )
        try:
            await asyncio.wait_for(started.wait(), 5)
            task.cancel()
        finally:
            finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert destination.read_bytes() == b"previous"
    assert list(tmp_path.iterdir()) == [destination]


async def test_pixel_budget_rejection_preserves_existing_cover(tmp_path, monkeypatch):
    monkeypatch.setattr("pullbox.services.provider_artwork.MAX_ARTWORK_PIXELS", 5)
    destination = tmp_path / "cover.jpg"
    destination.write_bytes(b"existing")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=cover_bytes(),
            headers={"content-type": "image/jpeg"},
        )
    )
    async with ProviderArtworkClient(transport=transport) as client:
        assert not await client.download_cover("https://metron.cloud/media/a.jpg", destination)
    assert destination.read_bytes() == b"existing"
    assert client._client.is_closed


async def test_stage_close_failure_cleans_up_partial_file(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from pullbox.services import provider_artwork as artwork

    original = artwork.tempfile.NamedTemporaryFile

    @contextmanager
    def failure(**kwargs):
        with original(**kwargs) as file:
            yield file
        raise OSError("flush failed")

    monkeypatch.setattr(artwork.tempfile, "NamedTemporaryFile", failure)
    destination = tmp_path / "cover.jpg"
    destination.write_bytes(b"existing")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            content=cover_bytes(),
            headers={"content-type": "image/jpeg"},
        )
    )
    async with ProviderArtworkClient(transport=transport) as client:
        assert not await client.download_cover("https://metron.cloud/media/a.jpg", destination)
    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]
