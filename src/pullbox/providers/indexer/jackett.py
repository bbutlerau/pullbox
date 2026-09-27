"""Jackett manager discovery client.

Jackett remains responsible for tracker-specific browser challenge handling.
Pullbox discovers configured trackers and registers each individual Torznab
feed so local priority, health, and attribution remain independent.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from xml.etree import ElementTree

import httpx
import structlog
from defusedxml.common import DefusedXmlException

from pullbox.core.url_validation import normalize_peer_base_url
from pullbox.core.xml_security import parse_untrusted_xml
from pullbox.providers.base import ProviderHealthResult

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = structlog.get_logger(__name__)

_REQUEST_TIMEOUT_SECONDS = 15.0
_MAX_INDEXER_RESPONSE_BYTES = 1_048_576
_TRACKER_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class JackettError(Exception):
    """Raised when Jackett discovery cannot be completed safely."""


@dataclass(frozen=True)
class JackettIndexerDefinition:
    """One configured Jackett tracker and its advertised capabilities."""

    id: str
    name: str
    description: str | None
    categories: tuple[str, ...]
    search_modes: tuple[str, ...]


class JackettClient:
    """Discover configured trackers through Jackett's Torznab API."""

    def __init__(
        self,
        *,
        url: str,
        api_key: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = normalize_peer_base_url(url, reject_query_or_fragment=True)
        self._api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=f"{self._base_url}/",
            timeout=_REQUEST_TIMEOUT_SECONDS,
            transport=transport,
        )

    async def get_configured_indexers(self) -> list[JackettIndexerDefinition]:
        """Return configured trackers without using Jackett's aggregate feed."""
        try:
            response = await self._client.get(
                "api/v2.0/indexers/all/results/torznab/api",
                params={
                    "apikey": self._api_key,
                    "t": "indexers",
                    "configured": "true",
                },
            )
            response.raise_for_status()
        except httpx.TimeoutException:
            raise JackettError("Jackett request timed out") from None
        except httpx.HTTPStatusError as exc:
            raise JackettError(f"Jackett returned HTTP {exc.response.status_code}") from None
        except httpx.HTTPError as exc:
            raise JackettError(f"Jackett request failed: {type(exc).__name__}") from None

        content = response.content
        if len(content) > _MAX_INDEXER_RESPONSE_BYTES:
            raise JackettError("Jackett response exceeded the 1 MiB safety limit")

        try:
            root = parse_untrusted_xml(content)
        except (ElementTree.ParseError, DefusedXmlException):
            raise JackettError("Jackett returned an invalid indexer response") from None

        definitions: list[JackettIndexerDefinition] = []
        seen_ids: set[str] = set()
        for element in root.findall("indexer"):
            tracker_id = str(element.get("id") or "").strip()
            if (
                not _TRACKER_ID_PATTERN.fullmatch(tracker_id)
                or tracker_id in seen_ids
                or str(element.get("configured", "true")).casefold() == "false"
            ):
                continue
            seen_ids.add(tracker_id)
            title = _element_text(element, "title") or tracker_id
            description = _element_text(element, "description")
            definitions.append(
                JackettIndexerDefinition(
                    id=tracker_id,
                    name=title[:255],
                    description=description,
                    categories=tuple(
                        _capability_values(element, "categories", ("category", "subcat"))
                    ),
                    search_modes=tuple(_search_modes(element)),
                )
            )
        return definitions

    async def test_connection(self) -> ProviderHealthResult:
        """Test Jackett and report its configured tracker count."""
        started = time.monotonic()
        try:
            indexers = await self.get_configured_indexers()
        except JackettError as exc:
            return ProviderHealthResult(
                healthy=False,
                message=f"Jackett error: {exc}",
                response_time_ms=(time.monotonic() - started) * 1000,
            )
        return ProviderHealthResult(
            healthy=True,
            message=f"Jackett: {len(indexers)} configured tracker(s)",
            response_time_ms=(time.monotonic() - started) * 1000,
            details={"indexer_count": str(len(indexers))},
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()


def _element_text(element: ElementTree.Element, child_name: str) -> str | None:
    child = element.find(child_name)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def _capability_values(
    element: ElementTree.Element,
    parent_name: str,
    child_names: tuple[str, ...],
) -> Iterable[str]:
    parent = element.find(f"caps/{parent_name}")
    if parent is None:
        return ()
    values: list[str] = []
    for child in parent.iter():
        if child.tag not in child_names:
            continue
        value = str(child.get("id") or "").strip()
        if value and value not in values:
            values.append(value)
    return values


def _search_modes(element: ElementTree.Element) -> Iterable[str]:
    parent = element.find("caps/searching")
    if parent is None:
        return ()
    return (child.tag for child in parent if str(child.get("available", "no")).casefold() == "yes")
