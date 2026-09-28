"""Executable ComicVine adapters; keep legacy import/cache consumers unchanged."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit

from pullbox.config import get_settings
from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind, MetadataSource
from pullbox.providers.metadata.comicvine import ComicVineError, ComicVineProvider
from pullbox.schemas.metadata_sources import (
    ProviderSeriesRead,
    SeriesDiscoveryQuery,
    SourceCapability,
    SourceStatus,
)
from pullbox.services.catalog.contract import CatalogError
from pullbox.services.catalog.reader import get_catalog_reader
from pullbox.services.catalog.storage import disk_work
from pullbox.services.metadata_discovery import MetadataSourceError, SourcePage, SourceRegistration

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pullbox.providers.base import SeriesSearchResult
    from pullbox.services.catalog.reader import CatalogReader
    from pullbox.services.metadata_sources import SourceRuntime


_catalog_reads: dict[asyncio.AbstractEventLoop, set[asyncio.Task[object]]] = {}


async def _catalog_read[T](operation: Callable[[], Awaitable[T]]) -> T:
    """Return promptly on cancellation without spawning unbounded disk workers."""
    loop = asyncio.get_running_loop()
    reads = _catalog_reads.setdefault(loop, set())
    if len(reads) >= 2:
        raise MetadataSourceError(SourceStatus.UNAVAILABLE, retry_after_seconds=1)

    async def owned_read() -> object:
        return await operation()

    def finished(task: asyncio.Task[object]) -> None:
        reads.discard(task)
        if not task.cancelled():
            task.exception()
        if not reads:
            _catalog_reads.pop(loop, None)

    task = asyncio.create_task(owned_read(), name="metadata-catalog-read")
    reads.add(task)
    task.add_done_callback(finished)
    try:
        return cast("T", await asyncio.shield(task))
    except asyncio.CancelledError:
        # disk_work joins its thread on cancellation. Retain the slot and task
        # until that owned read finishes; never abandon a database/file handle.
        task.cancel()
        raise


def _image_url(value: str | None) -> str | None:
    if not value or len(value) > 4096 or any(char.isspace() for char in value):
        return None
    try:
        parsed = urlsplit(value)
        host = parsed.hostname or ""
        if (
            parsed.scheme == "https"
            and parsed.port in {None, 443}
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
            and (
                host in {"comicvine.gamespot.com", "comicvine.com"}
                or host.endswith(".cbsistatic.com")
            )
        ):
            return value
    except ValueError:
        pass
    return None


def _page(
    source: MetadataSource,
    rows: list[SeriesSearchResult],
    *,
    total: int | None,
    offset: int,
    limit: int,
    has_more: bool,
) -> SourcePage:
    results = []
    rejected = 0
    for row in rows[:limit]:
        try:
            identity = ExternalIdentityRef(
                source.identity_namespace, MetadataEntityKind.SERIES, row.provider_id
            )
            if not row.title or not row.title.strip():
                raise ValueError("A series title is required")
            results.append(
                ProviderSeriesRead(
                    source=source,
                    identity_namespace=source.identity_namespace,
                    external_id=identity.external_id,
                    title=row.title[:500],
                    year_start=row.year_start,
                    publisher=row.publisher[:500] if row.publisher else None,
                    issue_count=row.issue_count,
                    description=row.description[:20000] if row.description else None,
                    resource_url=f"https://comicvine.gamespot.com/volume/4050-{identity.external_id}/",
                    image_url=_image_url(row.cover_url),
                )
            )
        except (ValueError, TypeError, AttributeError):
            rejected += 1
    next_offset = offset + limit if has_more and offset + limit <= 10000 else None
    return SourcePage(results, total, next_offset, rejected, has_more and next_offset is None)


def _api_error(exc: ComicVineError) -> MetadataSourceError:
    if exc.timed_out or exc.status_code in {408, 504}:
        status = SourceStatus.TIMEOUT
    elif exc.status_code in {100, 401, 403}:
        status = SourceStatus.AUTHENTICATION_FAILED
    elif exc.status_code in {107, 420, 429}:
        status = SourceStatus.RATE_LIMITED
    else:
        status = SourceStatus.UNAVAILABLE
    return MetadataSourceError(status, exc.retry_after_seconds)


class ComicVineApiSource:
    def __init__(self, provider: ComicVineProvider) -> None:
        self.provider = provider

    async def search(self, query: SeriesDiscoveryQuery, offset: int) -> SourcePage:
        try:
            rows, total = await self.provider.search_series_page(
                query.query,
                query.year,
                limit=query.limit_per_source,
                offset=offset,
                suppress_errors=False,
                strict_response=True,
            )
            if total < len(rows) or len(rows) > query.limit_per_source:
                raise ValueError("Inconsistent source page")
            return _page(
                MetadataSource.COMICVINE_API,
                rows,
                total=total,
                offset=offset,
                limit=query.limit_per_source,
                has_more=offset + query.limit_per_source < total,
            )
        except ComicVineError as exc:
            raise _api_error(exc) from None
        except (ValueError, TypeError, AttributeError, KeyError):
            raise MetadataSourceError(SourceStatus.INCOMPATIBLE_RESPONSE) from None

    async def check(self) -> None:
        page = await self.search(SeriesDiscoveryQuery(query="test", limit_per_source=1), 0)
        if page.rejected_results:
            raise MetadataSourceError(SourceStatus.INCOMPATIBLE_RESPONSE)

    async def close(self) -> None:
        await self.provider.close()


class ComicVineLocalSource:
    def __init__(self, reader: CatalogReader) -> None:
        self.reader = reader

    async def _available(self) -> None:
        if not await disk_work(lambda: self.reader.available):
            raise MetadataSourceError(SourceStatus.UNCONFIGURED)

    async def search(self, query: SeriesDiscoveryQuery, offset: int) -> SourcePage:
        return await _catalog_read(lambda: self._search(query, offset))

    async def _search(self, query: SeriesDiscoveryQuery, offset: int) -> SourcePage:
        await self._available()
        try:
            rows = await self.reader.search(
                query.query, query.year, query.limit_per_source + 1, offset
            )
        except (CatalogError, OSError, ValueError):
            raise MetadataSourceError(SourceStatus.UNAVAILABLE) from None
        return _page(
            MetadataSource.COMICVINE_LOCAL,
            rows,
            total=None,
            offset=offset,
            limit=query.limit_per_source,
            has_more=len(rows) > query.limit_per_source,
        )

    async def check(self) -> None:
        await _catalog_read(self._check)

    async def _check(self) -> None:
        await self._available()
        try:
            # Validate/open the active generation, even if it has no matching series.
            await self.reader.series(1)
        except (CatalogError, OSError, ValueError):
            raise MetadataSourceError(SourceStatus.UNAVAILABLE) from None

    async def close(self) -> None:
        return None


def _api(runtime: SourceRuntime) -> ComicVineApiSource:
    if runtime.credential is None or not runtime.credential.get_secret_value():
        raise MetadataSourceError(SourceStatus.UNCONFIGURED)
    return ComicVineApiSource(
        ComicVineProvider(
            runtime.credential.get_secret_value(), rate_limit=get_settings().comicvine_rate_limit
        )
    )


def comicvine_sources() -> dict[MetadataSource, SourceRegistration]:
    return {
        MetadataSource.COMICVINE_API: SourceRegistration(
            frozenset({SourceCapability.SERIES_SEARCH}), _api
        ),
        MetadataSource.COMICVINE_LOCAL: SourceRegistration(
            frozenset({SourceCapability.SERIES_SEARCH, SourceCapability.OFFLINE}),
            lambda runtime: ComicVineLocalSource(get_catalog_reader()),
        ),
    }
