"""Tests for NZBGet download client implementation (DCE-C.1).

Covers add_nzb, get_download_status, get_queue, remove_download,
test_connection, status mapping, and edge cases. Uses mocked XML-RPC
responses to avoid real network calls.

Run:
    pytest tests/providers/test_nzbget.py -v
"""

from __future__ import annotations

import io
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pullbox.providers.download.nzbget import (
    NZBGetClient,
    NZBGetError,
    _map_group_status,
    _map_history_status,
    _TimeoutTransport,
)

# Fake NZB content that passes the content-type / body validation
_FAKE_NZB_CONTENT = b"<?xml version='1.0'?><nzb>fake</nzb>"


def test_xmlrpc_transport_normalizes_response_before_expat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pullbox.core.xml_security import normalize_xml_for_expat
    from pullbox.providers.download import nzbget

    response = (
        b"<?xml version='1.0'?><methodResponse><params><param>"
        b"<value><string>ok</string></value></param></params></methodResponse>"
    )
    guarded_payloads: list[bytes | str] = []

    def recording_guard(payload: bytes | str) -> bytes | str:
        guarded_payloads.append(payload)
        return normalize_xml_for_expat(payload)

    monkeypatch.setattr(nzbget, "normalize_xml_for_expat", recording_guard, raising=False)

    transport = _TimeoutTransport()
    transport.verbose = False

    assert transport.parse_response(io.BytesIO(response)) == ("ok",)
    assert guarded_payloads == [response]


def _make_client(**kwargs: Any) -> NZBGetClient:
    """Create an NZBGet client with defaults."""
    defaults: dict[str, Any] = {
        "url": "http://localhost:6789",
        "username": "nzbget",
        "password": "secret",
    }
    defaults.update(kwargs)
    return NZBGetClient(**defaults)


def _mock_httpx_for_nzb() -> MagicMock:
    """Create a mock httpx.AsyncClient context manager that returns NZB content."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = _FAKE_NZB_CONTENT
    mock_response.headers = {"content-type": "application/x-nzb"}
    mock_response.raise_for_status = MagicMock()

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=mock_response)
    mock_http.__aenter__ = AsyncMock(return_value=mock_http)
    mock_http.__aexit__ = AsyncMock(return_value=False)
    return mock_http


# ── add_nzb ──────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestAddNzb:
    """Tests for NZBGetClient.add_nzb()."""

    async def test_add_nzb_returns_nzbid(self) -> None:
        client = _make_client()
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = 12345
            result = await client.add_nzb("http://example.com/test.nzb", "Test NZB")

        assert result == "12345"

    async def test_add_nzb_with_category(self) -> None:
        client = _make_client(category="comics")
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = 100
            await client.add_nzb("http://example.com/test.nzb", "Test NZB")

        call_args = mock_call.call_args
        assert call_args[0][0] == "append"
        # Category is the 3rd positional param to append
        params = call_args[0][1:]
        assert "comics" in params

    async def test_add_nzb_with_priority(self) -> None:
        client = _make_client(priority="high")
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = 101
            await client.add_nzb("http://example.com/test.nzb", "Test NZB")

        assert mock_call.called

    async def test_add_nzb_returns_zero_raises(self) -> None:
        """append() returning 0 means failure."""
        client = _make_client()
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = 0
            with pytest.raises(NZBGetError, match="Failed to add NZB"):
                await client.add_nzb("http://example.com/test.nzb", "Test NZB")

    async def test_add_nzb_negative_id_raises(self) -> None:
        """Negative NZBID is a failure mode."""
        client = _make_client()
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = -1
            with pytest.raises(NZBGetError, match="Failed to add NZB"):
                await client.add_nzb("http://example.com/test.nzb", "Test NZB")

    async def test_add_nzb_with_post_processing(self) -> None:
        client = _make_client(post_processing="pp3")
        with (
            patch("httpx.AsyncClient", return_value=_mock_httpx_for_nzb()),
            patch.object(client, "_call", new_callable=AsyncMock) as mock_call,
        ):
            mock_call.return_value = 102
            await client.add_nzb("http://example.com/test.nzb", "Test NZB")

        assert mock_call.called


# ── add_torrent ──────────────────────────────────────────────────


@pytest.mark.asyncio
class TestAddTorrent:
    """Verify add_torrent raises NotImplementedError."""

    async def test_add_torrent_raises(self) -> None:
        client = _make_client()
        with pytest.raises(NotImplementedError, match="does not support torrent"):
            await client.add_torrent("magnet:?xt=urn:btih:abc", "Test")


# ── get_download_status ──────────────────────────────────────────


@pytest.mark.asyncio
class TestGetDownloadStatus:
    """Tests for NZBGetClient.get_download_status()."""

    async def test_found_in_queue(self) -> None:
        client = _make_client()
        group = {
            "NZBID": 100,
            "NZBFilename": "Batman 001.nzb",
            "Status": "DOWNLOADING",
            "FileSizeMB": 150.0,
            "DownloadedSizeMB": 75.0,
            "DownloadRate": 1048576,
            "RemainingSizeMB": 75.0,
            "RemainingTimeSec": 120,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                [group],  # listgroups
            ]
            status = await client.get_download_status("100")

        assert status.external_id == "100"
        assert status.title == "Batman 001.nzb"
        assert status.state == "downloading"
        assert status.progress == pytest.approx(0.5)
        assert status.speed_bytes == 1048576
        assert status.eta_seconds == 120

    async def test_found_in_history_completed(self) -> None:
        client = _make_client()
        history_item = {
            "NZBID": 200,
            "NZBFilename": "Batman 002.nzb",
            "Status": "SUCCESS",
            "FileSizeMB": 100.0,
            "DestDir": "/downloads/completed/Batman 002",
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                [],  # listgroups (empty)
                [history_item],  # history
            ]
            status = await client.get_download_status("200")

        assert status.state == "completed"
        assert status.progress == 1.0
        assert status.downloaded_path == "/downloads/completed/Batman 002"

    async def test_found_in_history_failed(self) -> None:
        client = _make_client()
        history_item = {
            "NZBID": 300,
            "NZBFilename": "Batman 003.nzb",
            "Status": "FAILURE",
            "FileSizeMB": 100.0,
            "HealthStatus": "DAMAGED",
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                [],  # listgroups
                [history_item],  # history
            ]
            status = await client.get_download_status("300")

        assert status.state == "failed"
        assert status.error_message is not None
        assert "DAMAGED" in status.error_message

    async def test_not_found_raises(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                [],  # listgroups
                [],  # history
            ]
            with pytest.raises(NZBGetError, match="not found"):
                await client.get_download_status("999")

    async def test_external_id_string_int_comparison(self) -> None:
        """external_id is a string but NZBID is an int — must match correctly."""
        client = _make_client()
        group = {
            "NZBID": 42,
            "NZBFilename": "Test.nzb",
            "Status": "QUEUED",
            "FileSizeMB": 50.0,
            "DownloadedSizeMB": 0.0,
            "DownloadRate": 0,
            "RemainingSizeMB": 50.0,
            "RemainingTimeSec": -1,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            status = await client.get_download_status("42")

        assert status.external_id == "42"
        assert status.state == "queued"


# ── get_queue ────────────────────────────────────────────────────


@pytest.mark.asyncio
class TestGetQueue:
    """Tests for NZBGetClient.get_queue()."""

    async def test_get_queue_returns_mapped_list(self) -> None:
        client = _make_client()
        groups = [
            {
                "NZBID": 1,
                "NZBFilename": "File1.nzb",
                "Status": "DOWNLOADING",
                "FileSizeMB": 100.0,
                "DownloadedSizeMB": 50.0,
                "DownloadRate": 500000,
                "RemainingSizeMB": 50.0,
                "RemainingTimeSec": 60,
            },
            {
                "NZBID": 2,
                "NZBFilename": "File2.nzb",
                "Status": "QUEUED",
                "FileSizeMB": 200.0,
                "DownloadedSizeMB": 0.0,
                "DownloadRate": 0,
                "RemainingSizeMB": 200.0,
                "RemainingTimeSec": -1,
            },
        ]
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = groups
            result = await client.get_queue()

        assert len(result) == 2
        assert result[0].state == "downloading"
        assert result[1].state == "queued"

    async def test_get_queue_empty(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = []
            result = await client.get_queue()

        assert result == []

    async def test_get_queue_large(self) -> None:
        """100+ items in queue all mapped correctly."""
        client = _make_client()
        groups = [
            {
                "NZBID": i,
                "NZBFilename": f"File{i}.nzb",
                "Status": "QUEUED",
                "FileSizeMB": 10.0,
                "DownloadedSizeMB": 0.0,
                "DownloadRate": 0,
                "RemainingSizeMB": 10.0,
                "RemainingTimeSec": -1,
            }
            for i in range(120)
        ]
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = groups
            result = await client.get_queue()

        assert len(result) == 120


# ── remove_download ──────────────────────────────────────────────


@pytest.mark.asyncio
class TestRemoveDownload:
    """Tests for NZBGetClient.remove_download()."""

    async def test_remove_from_queue(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = True
            result = await client.remove_download("100")

        assert result is True
        mock_call.assert_called_once_with("editqueue", "GroupDelete", 0, "", [100])

    async def test_remove_from_queue_falls_back_to_history(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                NZBGetError("not found"),  # GroupDelete fails
                True,  # HistoryDelete succeeds
            ]
            result = await client.remove_download("200")

        assert result is True
        assert mock_call.call_count == 2

    async def test_remove_not_found(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [
                NZBGetError("not found"),
                NZBGetError("not found"),
            ]
            result = await client.remove_download("999")

        assert result is False


# ── test_connection ──────────────────────────────────────────────


@pytest.mark.asyncio
class TestTestConnection:
    """Tests for NZBGetClient.test_connection()."""

    async def test_healthy(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = "21.1"
            result = await client.test_connection()

        assert result.healthy is True
        assert "21.1" in result.message
        assert result.response_time_ms >= 0

    async def test_connection_refused(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = NZBGetError("Connection refused")
            result = await client.test_connection()

        assert result.healthy is False
        assert "Connection refused" in result.message

    async def test_unexpected_exception(self) -> None:
        client = _make_client()
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = RuntimeError("something broke")
            result = await client.test_connection()

        assert result.healthy is False


# ── Status mapping ───────────────────────────────────────────────


class TestQueueStatusMapping:
    """Tests for _map_group_status — queue item status mapping."""

    def test_downloading(self) -> None:
        assert _map_group_status("DOWNLOADING") == "downloading"

    def test_queued(self) -> None:
        assert _map_group_status("QUEUED") == "queued"

    def test_paused(self) -> None:
        assert _map_group_status("PAUSED") == "paused"

    def test_pp_queued(self) -> None:
        assert _map_group_status("PP_QUEUED") == "finalizing"

    def test_loading_pars(self) -> None:
        assert _map_group_status("LOADING_PARS") == "finalizing"

    def test_verifying_sources(self) -> None:
        assert _map_group_status("VERIFYING_SOURCES") == "finalizing"

    def test_repairing(self) -> None:
        assert _map_group_status("REPAIRING") == "finalizing"

    def test_verifying_repaired(self) -> None:
        assert _map_group_status("VERIFYING_REPAIRED") == "finalizing"

    def test_renaming(self) -> None:
        assert _map_group_status("RENAMING") == "finalizing"

    def test_unpacking(self) -> None:
        assert _map_group_status("UNPACKING") == "finalizing"

    def test_moving(self) -> None:
        assert _map_group_status("MOVING") == "finalizing"

    def test_executing_script(self) -> None:
        assert _map_group_status("EXECUTING_SCRIPT") == "finalizing"

    def test_unknown_status_lowercased(self) -> None:
        assert _map_group_status("SOME_NEW_STATUS") == "some_new_status"


class TestHistoryStatusMapping:
    """Tests for _map_history_status — history item status mapping."""

    def test_success(self) -> None:
        assert _map_history_status("SUCCESS") == "completed"

    def test_failure(self) -> None:
        assert _map_history_status("FAILURE") == "failed"

    def test_deleted(self) -> None:
        assert _map_history_status("DELETED") == "failed"

    def test_unknown_lowercased(self) -> None:
        assert _map_history_status("UNKNOWN_STATUS") == "unknown_status"


# ── Post-processing edge cases ───────────────────────────────────


@pytest.mark.asyncio
class TestPostProcessingPhases:
    """PP phases should map to finalizing with progress=1.0."""

    @pytest.mark.parametrize(
        "status",
        [
            "PP_QUEUED",
            "LOADING_PARS",
            "VERIFYING_SOURCES",
            "REPAIRING",
            "VERIFYING_REPAIRED",
            "RENAMING",
            "UNPACKING",
            "MOVING",
            "EXECUTING_SCRIPT",
        ],
    )
    async def test_pp_phase_progress_pinned(self, status: str) -> None:
        client = _make_client()
        group = {
            "NZBID": 50,
            "NZBFilename": "Test.nzb",
            "Status": status,
            "FileSizeMB": 100.0,
            "DownloadedSizeMB": 100.0,
            "DownloadRate": 0,
            "RemainingSizeMB": 0.0,
            "RemainingTimeSec": 0,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("50")

        assert result.state == "finalizing"
        assert result.progress == 1.0
        assert result.client_state is not None


# ── Size and speed edge cases ────────────────────────────────────


@pytest.mark.asyncio
class TestSizeAndSpeedEdgeCases:
    """Edge cases for size, speed, and ETA mapping."""

    async def test_file_size_zero(self) -> None:
        """FileSizeMB=0 for tiny NZBs → size_bytes=0, not None."""
        client = _make_client()
        group = {
            "NZBID": 60,
            "NZBFilename": "Tiny.nzb",
            "Status": "DOWNLOADING",
            "FileSizeMB": 0.0,
            "DownloadedSizeMB": 0.0,
            "DownloadRate": 100,
            "RemainingSizeMB": 0.0,
            "RemainingTimeSec": 0,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("60")

        assert result.size_bytes == 0

    async def test_remaining_time_negative_means_unknown(self) -> None:
        """RemainingTimeSec=-1 → eta_seconds=None."""
        client = _make_client()
        group = {
            "NZBID": 61,
            "NZBFilename": "Test.nzb",
            "Status": "DOWNLOADING",
            "FileSizeMB": 100.0,
            "DownloadedSizeMB": 50.0,
            "DownloadRate": 1000,
            "RemainingSizeMB": 50.0,
            "RemainingTimeSec": -1,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("61")

        assert result.eta_seconds is None

    async def test_remaining_time_zero_means_none(self) -> None:
        """RemainingTimeSec=0 → eta_seconds=None."""
        client = _make_client()
        group = {
            "NZBID": 62,
            "NZBFilename": "Test.nzb",
            "Status": "PAUSED",
            "FileSizeMB": 100.0,
            "DownloadedSizeMB": 0.0,
            "DownloadRate": 0,
            "RemainingSizeMB": 100.0,
            "RemainingTimeSec": 0,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("62")

        assert result.eta_seconds is None

    async def test_download_rate_maps_directly_to_speed_bytes(self) -> None:
        """NZBGet DownloadRate is already in bytes/sec."""
        client = _make_client()
        group = {
            "NZBID": 63,
            "NZBFilename": "Test.nzb",
            "Status": "DOWNLOADING",
            "FileSizeMB": 100.0,
            "DownloadedSizeMB": 10.0,
            "DownloadRate": 2097152,
            "RemainingSizeMB": 90.0,
            "RemainingTimeSec": 45,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("63")

        assert result.speed_bytes == 2097152


# ── Properties ───────────────────────────────────────────────────


class TestProperties:
    """Tests for NZBGetClient properties."""

    def test_name(self) -> None:
        assert _make_client().name == "nzbget"

    def test_client_type(self) -> None:
        assert _make_client().client_type == "nzbget"


# ── Unicode edge case ────────────────────────────────────────────


@pytest.mark.asyncio
class TestUnicodeHandling:
    """NZB filenames with unicode characters."""

    async def test_unicode_filename(self) -> None:
        client = _make_client()
        group = {
            "NZBID": 70,
            "NZBFilename": "バットマン 001.nzb",
            "Status": "QUEUED",
            "FileSizeMB": 50.0,
            "DownloadedSizeMB": 0.0,
            "DownloadRate": 0,
            "RemainingSizeMB": 50.0,
            "RemainingTimeSec": -1,
        }
        with patch.object(client, "_call", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = [[group]]
            result = await client.get_download_status("70")

        assert result.title == "バットマン 001.nzb"
