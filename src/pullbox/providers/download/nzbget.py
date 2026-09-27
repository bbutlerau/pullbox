"""NZBGet download client implementation.

Integrates with the NZBGet XML-RPC API to manage Usenet NZB downloads.
Implements the DownloadClient protocol with HTTP Basic authentication,
category/priority support, and structured logging.

NZBGet API docs: https://nzbget.com/info/api/
"""

from __future__ import annotations

import asyncio
import time
import xmlrpc.client  # nosec B411
from typing import Any

import structlog
from defusedxml.xmlrpc import monkey_patch as _defusedxml_xmlrpc_monkey_patch

from pullbox.core.config_resolver import resolve_runtime_service_url
from pullbox.core.xml_security import normalize_xml_for_expat
from pullbox.providers.base import ClientOptions, DownloadStatus, ProviderHealthResult

# NZBGet exposes XML-RPC only; defusedxml patches stdlib XML-RPC parsing.
_defusedxml_xmlrpc_monkey_patch()

logger = structlog.get_logger(__name__)

_REQUEST_TIMEOUT = 10.0
_MAX_XML_RPC_RESPONSE_BYTES = 30 * 1024 * 1024

# NZBGet priority string → integer mapping
_PRIORITY_MAP: dict[str, int] = {
    "force": 900,
    "very-high": 100,
    "high": 50,
    "normal": 0,
    "low": -50,
    "very-low": -100,
}

# Post-processing phases — download is complete, PP in progress
_PP_PHASES = {
    "PP_QUEUED",
    "LOADING_PARS",
    "VERIFYING_SOURCES",
    "REPAIRING",
    "VERIFYING_REPAIRED",
    "RENAMING",
    "UNPACKING",
    "MOVING",
    "EXECUTING_SCRIPT",
}


class NZBGetError(Exception):
    """Raised when the NZBGet API returns an error."""


class NZBGetClient:
    """NZBGet implementation of DownloadClient.

    Uses XML-RPC API with HTTP Basic auth embedded in the URL.
    All XML-RPC calls are run in a thread executor to maintain
    async compatibility.

    Args:
        url: Base URL of the NZBGet instance (e.g. ``http://localhost:6789``).
        username: NZBGet control username (default: nzbget).
        password: NZBGet control password.
        category: Default download category (optional).
        priority: Default priority (force/very-high/high/normal/low/very-low).
        post_processing: Default PP level (pp0-pp3).
    """

    def __init__(
        self,
        url: str,
        username: str = "nzbget",
        password: str = "",
        category: str | None = None,
        priority: str | None = None,
        post_processing: str | None = None,
    ) -> None:
        self._base_url = resolve_runtime_service_url(url).rstrip("/")
        self._username = username
        self._password = password
        self._default_category = category or ""
        self._default_priority = priority
        self._default_post_processing = post_processing

        # Build XML-RPC URL with Basic auth embedded
        # http://username:password@host:port/xmlrpc
        from urllib.parse import urlparse

        parsed = urlparse(self._base_url)
        self._rpc_url = (
            f"{parsed.scheme}://{self._username}:{self._password}"
            f"@{parsed.hostname}:{parsed.port or 6789}{parsed.path}/xmlrpc"
        )

    @property
    def name(self) -> str:
        return "nzbget"

    @property
    def client_type(self) -> str:
        return "nzbget"

    # -- internal RPC plumbing -------------------------------------------------

    async def _call(self, method: str, *args: Any) -> Any:
        """Make an XML-RPC call to NZBGet, running in a thread executor."""
        log = logger.bind(method=method)
        log.debug("nzbget_rpc_call")

        def _sync_call() -> Any:
            try:
                proxy = xmlrpc.client.ServerProxy(
                    self._rpc_url,
                    transport=_TimeoutTransport(timeout=_REQUEST_TIMEOUT),
                )
                rpc_method = getattr(proxy, method)
                return rpc_method(*args)
            except xmlrpc.client.ProtocolError as exc:
                if exc.errcode == 401:
                    raise NZBGetError("Authentication failed — check username/password") from None
                raise NZBGetError(f"HTTP {exc.errcode}: {exc.errmsg}") from None
            except xmlrpc.client.Fault as exc:
                raise NZBGetError(f"XML-RPC fault: {exc.faultString}") from None
            except OSError as exc:
                raise NZBGetError(f"Connection failed: {exc}") from None
            except Exception as exc:
                raise NZBGetError(f"Request failed: {exc}") from None

        try:
            return await asyncio.to_thread(_sync_call)
        except NZBGetError:
            raise
        except Exception as exc:
            log.error("nzbget_call_failed", error=str(exc))
            raise NZBGetError(f"Unexpected error: {exc}") from None

    # -- DownloadClient implementation -----------------------------------------

    async def add_nzb(
        self,
        url: str,
        title: str,
        category: str | None = None,
    ) -> str:
        """Add an NZB download by URL. Returns the NZBID as a string.

        Downloads the NZB content from the URL first, then sends it to
        NZBGet as base64-encoded data. This is more reliable than URL
        mode because NZBGet may not be able to reach the indexer directly
        (e.g. different Docker networks, auth requirements).
        """
        import base64

        import httpx as _httpx

        log = logger.bind(title=title, category=category)
        log.info("nzbget_add_nzb")

        # Fetch the NZB content from the indexer URL
        try:
            async with _httpx.AsyncClient(timeout=_REQUEST_TIMEOUT, follow_redirects=True) as http:
                resp = await http.get(url)
                resp.raise_for_status()

                # Validate we got actual NZB content, not an error page
                content_type = resp.headers.get("content-type", "")
                if "xml" not in content_type and "nzb" not in content_type:
                    # Check if body looks like XML/NZB (starts with <? or <nzb)
                    body_start = resp.content[:100].lstrip()
                    if not body_start.startswith((b"<?xml", b"<nzb", b"<!DOCTYPE nzb")):
                        raise NZBGetError(
                            f"URL did not return NZB content (content-type: {content_type})"
                        )

                nzb_content = base64.standard_b64encode(resp.content).decode("ascii")
        except NZBGetError:
            raise
        except Exception as exc:
            raise NZBGetError(f"Failed to download NZB from URL: {exc}") from None

        cat = category or self._default_category
        priority_int = _PRIORITY_MAP.get(self._default_priority or "", 0)

        # Ensure title ends with .nzb for NZBGet to recognize it
        nzb_filename = title if title.lower().endswith(".nzb") else f"{title}.nzb"

        # append(nzbfilename, nzbcontent, category, priority, addtotop,
        #        addpaused, dupekey, dupescore, dupemode, ppparameters)
        nzb_id: int = await self._call(
            "append",
            nzb_filename,  # NZBFilename — display name
            nzb_content,  # NZBContent — base64-encoded NZB data
            cat,  # Category
            priority_int,  # Priority
            False,  # AddToTop
            False,  # AddPaused
            "",  # DupeKey
            0,  # DupeScore
            "score",  # DupeMode
            [],  # PPParameters
        )

        if not isinstance(nzb_id, int) or nzb_id <= 0:
            raise NZBGetError(f"Failed to add NZB: NZBGet returned ID {nzb_id}")

        log.info("nzbget_nzb_added", nzb_id=nzb_id)
        return str(nzb_id)

    async def add_torrent(
        self,
        url: str,
        title: str,
        category: str | None = None,
    ) -> str:
        """Not supported — NZBGet is Usenet-only."""
        raise NotImplementedError("NZBGet does not support torrent downloads")

    async def add_torrent_data(
        self,
        content: bytes,
        title: str,
        category: str | None = None,
    ) -> str | None:
        """Not supported - NZBGet is Usenet-only."""
        raise NotImplementedError("NZBGet does not support torrent downloads")

    async def get_download_status(self, external_id: str) -> DownloadStatus:
        """Get status of a specific download by NZBID.

        Checks the active queue first, then falls back to history.
        """
        log = logger.bind(external_id=external_id)
        log.debug("nzbget_get_status")

        nzb_id = int(external_id)

        # Check active queue
        groups: list[dict[str, Any]] = await self._call("listgroups", 0)
        for group in groups:
            if group.get("NZBID") == nzb_id:
                return _map_group(group)

        # Check history
        history: list[dict[str, Any]] = await self._call("history", False)
        for item in history:
            if item.get("NZBID") == nzb_id:
                return _map_history(item)

        raise NZBGetError(f"Download not found: {external_id}")

    async def get_queue(self) -> list[DownloadStatus]:
        """Get all active downloads from the NZBGet queue."""
        logger.debug("nzbget_get_queue")

        groups: list[dict[str, Any]] = await self._call("listgroups", 0)
        results = [_map_group(g) for g in groups]
        logger.debug("nzbget_queue_fetched", count=len(results))
        return results

    async def remove_download(
        self,
        external_id: str,
        delete_files: bool = False,
    ) -> bool:
        """Remove a download from queue or history."""
        log = logger.bind(external_id=external_id, delete_files=delete_files)
        log.info("nzbget_remove_download")

        nzb_id = int(external_id)

        # Try removing from queue first
        try:
            await self._call("editqueue", "GroupDelete", 0, "", [nzb_id])
            log.info("nzbget_removed_from_queue")
            return True
        except NZBGetError:
            pass

        # Try removing from history
        try:
            await self._call("editqueue", "HistoryDelete", 0, "", [nzb_id])
            log.info("nzbget_removed_from_history")
            return True
        except NZBGetError:
            log.warning("nzbget_remove_failed", external_id=external_id)
            return False

    async def test_connection(self) -> ProviderHealthResult:
        """Verify connectivity by calling the NZBGet version endpoint."""
        start = time.monotonic()
        try:
            version: str = await self._call("version")
            elapsed_ms = (time.monotonic() - start) * 1000
            return ProviderHealthResult(
                healthy=True,
                message=f"NZBGet v{version}",
                response_time_ms=elapsed_ms,
                details={"version": str(version)},
            )
        except NZBGetError as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            return ProviderHealthResult(
                healthy=False,
                message=f"NZBGet error: {exc}",
                response_time_ms=elapsed_ms,
            )
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.exception("nzbget_test_connection_failed")
            return ProviderHealthResult(
                healthy=False,
                message=f"Connection failed: {exc}",
                response_time_ms=elapsed_ms,
            )

    async def get_options(self) -> ClientOptions:
        """Fetch available categories from NZBGet config."""
        config: list[dict[str, Any]] = await self._call("config")
        categories = [
            item["Value"]
            for item in config
            if item.get("Name", "").startswith("Category")
            and item.get("Name", "").endswith(".Name")
            and item.get("Value")
        ]
        return ClientOptions(categories=categories)

    async def close(self) -> None:
        """No persistent connection to close for XML-RPC."""


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------


def _map_group(group: dict[str, Any]) -> DownloadStatus:
    """Map an NZBGet queue group to a DownloadStatus DTO."""
    raw_status = group.get("Status", "")
    state = _map_group_status(raw_status)

    file_size_mb = group.get("FileSizeMB", 0.0)
    downloaded_mb = group.get("DownloadedSizeMB", 0.0)
    size_bytes = int(file_size_mb * 1024 * 1024)

    # Progress: for PP phases, pin to 1.0
    if raw_status in _PP_PHASES:
        progress = 1.0
    elif file_size_mb > 0:
        progress = downloaded_mb / file_size_mb
    else:
        progress = 0.0

    # NZBGet DownloadRate is already in bytes/sec
    speed_bytes = group.get("DownloadRate", 0) or None
    if speed_bytes == 0:
        speed_bytes = None

    # ETA
    remaining_sec = group.get("RemainingTimeSec", -1)
    eta_seconds = remaining_sec if remaining_sec and remaining_sec > 0 else None

    # Client state for PP phases
    client_state = _human_readable_status(raw_status) if raw_status in _PP_PHASES else None

    return DownloadStatus(
        external_id=str(group.get("NZBID", "")),
        title=group.get("NZBFilename", "Unknown"),
        state=state,
        progress=progress,
        size_bytes=size_bytes,
        speed_bytes=speed_bytes,
        eta_seconds=eta_seconds,
        error_message=None,
        client_state=client_state,
    )


def _map_history(item: dict[str, Any]) -> DownloadStatus:
    """Map an NZBGet history item to a DownloadStatus DTO."""
    raw_status = item.get("Status", "")
    state = _map_history_status(raw_status)

    file_size_mb = item.get("FileSizeMB", 0.0)
    size_bytes = int(file_size_mb * 1024 * 1024)

    progress = 1.0 if state == "completed" else 0.0

    # Error message from health status
    error_message = None
    if state == "failed":
        health = item.get("HealthStatus", "")
        error_message = f"Download failed: {health}" if health else "Download failed"

    return DownloadStatus(
        external_id=str(item.get("NZBID", "")),
        title=item.get("NZBFilename", "Unknown"),
        state=state,
        progress=progress,
        size_bytes=size_bytes,
        speed_bytes=None,
        eta_seconds=None,
        error_message=error_message,
        downloaded_path=item.get("DestDir") or None,
    )


def _map_group_status(status: str) -> str:
    """Map NZBGet queue status string to normalized state."""
    mapping = {
        "DOWNLOADING": "downloading",
        "QUEUED": "queued",
        "PAUSED": "paused",
        "FETCHING": "downloading",
    }
    # All PP phases happen after transfer and before Pullbox post-processing.
    if status in _PP_PHASES:
        return "finalizing"
    return mapping.get(status, status.lower())


def _map_history_status(status: str) -> str:
    """Map NZBGet history status string to normalized state.

    NZBGet history statuses include a detail suffix after a slash:
    ``SUCCESS/ALL``, ``SUCCESS/HEALTH``, ``FAILURE/BAD``, ``DELETED/MANUAL``, etc.
    We match on the prefix before the slash.
    """
    prefix = status.split("/")[0] if "/" in status else status
    mapping = {
        "SUCCESS": "completed",
        "FAILURE": "failed",
        "DELETED": "failed",
    }
    return mapping.get(prefix, status.lower())


def _human_readable_status(status: str) -> str:
    """Map NZBGet status to a human-readable label for the UI."""
    labels: dict[str, str] = {
        "PP_QUEUED": "Post-Processing Queued",
        "LOADING_PARS": "Loading Pars",
        "VERIFYING_SOURCES": "Verifying",
        "REPAIRING": "Repairing",
        "VERIFYING_REPAIRED": "Verifying Repair",
        "RENAMING": "Renaming",
        "UNPACKING": "Unpacking",
        "MOVING": "Moving",
        "EXECUTING_SCRIPT": "Running Script",
    }
    return labels.get(status, status)


class _TimeoutTransport(xmlrpc.client.Transport):
    """XML-RPC transport with configurable timeout."""

    def __init__(self, timeout: float = _REQUEST_TIMEOUT) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(
        self,
        host: tuple[str, dict[str, str]] | str,
    ) -> Any:
        conn = super().make_connection(host)
        conn.timeout = self._timeout
        return conn

    def parse_response(self, response: Any) -> Any:
        """Normalize the bounded XML-RPC response before Expat parses it."""
        stream = response
        if hasattr(response, "getheader") and response.getheader("Content-Encoding", "") == "gzip":
            stream = xmlrpc.client.GzipDecodedResponse(response)

        try:
            payload = stream.read(_MAX_XML_RPC_RESPONSE_BYTES + 1)
        finally:
            if stream is not response:
                stream.close()
        if len(payload) > _MAX_XML_RPC_RESPONSE_BYTES:
            raise ValueError("XML-RPC response exceeded the 30 MiB safety limit")

        normalized = normalize_xml_for_expat(payload)
        parser, unmarshaller = self.getparser()
        parser.feed(normalized)
        parser.close()
        return unmarshaller.close()
