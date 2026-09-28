"""Source-aware metadata discovery; identity attachment is a separate operation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import structlog

from pullbox.core.metadata_identity import MetadataSource
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    ProviderSeriesRead,
    SeriesDiscoveryQuery,
    SeriesDiscoveryRead,
    SourceCapability,
    SourceOutcome,
    SourceStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from pullbox.services.metadata_sources import SourceRuntime

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SourcePage:
    results: list[ProviderSeriesRead]
    total: int | None = None
    next_offset: int | None = None
    rejected_results: int = 0
    truncated: bool = False


class MetadataSourceAdapter(Protocol):
    async def search(self, query: SeriesDiscoveryQuery, offset: int) -> SourcePage: ...
    async def check(self) -> None: ...
    async def close(self) -> None: ...


class MetadataSourceError(Exception):
    def __init__(self, status: SourceStatus, retry_after_seconds: int | None = None) -> None:
        self.status = status
        self.retry_after_seconds = retry_after_seconds
        super().__init__(status.value)


@dataclass(frozen=True)
class SourceRegistration:
    capabilities: frozenset[SourceCapability]
    factory: Callable[[SourceRuntime], MetadataSourceAdapter]


class MetadataSourceRegistry:
    def __init__(
        self,
        runtime: Sequence[SourceRuntime],
        *,
        factories: Mapping[MetadataSource, SourceRegistration] | None = None,
        gcd_api_enabled: bool = False,
        per_source_timeout: float = 8,
        total_timeout: float = 15,
        concurrency: int = 3,
    ) -> None:
        if factories is None:
            from pullbox.providers.metadata.sources import comicvine_sources

            factories = comicvine_sources()
        self.runtime = {item.policy.source: item for item in runtime}
        self.factories = factories
        self.gcd_api_enabled = gcd_api_enabled
        if not 0 < per_source_timeout <= 30 or not 0 < total_timeout <= 60:
            raise ValueError("Metadata discovery deadlines must be bounded")
        if not 1 <= concurrency <= 5:
            raise ValueError("Metadata discovery concurrency must be between one and five")
        self.per_source_timeout = per_source_timeout
        self.total_timeout = total_timeout
        self.concurrency = concurrency

    def _unavailable(self, source: MetadataSource, *, search: bool) -> SourceStatus | None:
        runtime = self.runtime.get(source)
        if source is MetadataSource.GCD_API_V2 and not self.gcd_api_enabled:
            return SourceStatus.FEATURE_DISABLED
        if runtime is None:
            return SourceStatus.UNCONFIGURED
        if runtime.unavailable is not None:
            return runtime.unavailable
        if not runtime.policy.enabled:
            return SourceStatus.DISABLED
        registration = self.factories.get(source)
        if registration is None:
            return SourceStatus.NOT_IMPLEMENTED
        if search and SourceCapability.SERIES_SEARCH not in registration.capabilities:
            return SourceStatus.UNSUPPORTED
        return None

    async def _run(
        self,
        source: MetadataSource,
        query: SeriesDiscoveryQuery | None,
        semaphore: asyncio.Semaphore,
        deadline: float,
    ) -> tuple[SourcePage, SourceOutcome]:
        unavailable = self._unavailable(source, search=query is not None)
        if unavailable is not None:
            return SourcePage([]), SourceOutcome(source=source, status=unavailable)
        if asyncio.get_running_loop().time() >= deadline:
            return SourcePage([]), SourceOutcome(source=source, status=SourceStatus.TIMEOUT)
        adapter = None
        page = SourcePage([])
        try:
            async with asyncio.timeout_at(deadline), semaphore:
                async with asyncio.timeout(self.per_source_timeout):
                    adapter = self.factories[source].factory(self.runtime[source])
                    if query is None:
                        await adapter.check()
                    else:
                        page = await adapter.search(query, query.offsets.get(source, 0))
                    if any(
                        item.source != source
                        or item.identity_namespace != source.identity_namespace
                        for item in page.results
                    ):
                        raise MetadataSourceError(SourceStatus.INCOMPATIBLE_RESPONSE)
                    if query is not None and len(page.results) > query.limit_per_source:
                        raise MetadataSourceError(SourceStatus.INCOMPATIBLE_RESPONSE)
                    status = (
                        SourceStatus.INCOMPATIBLE_RESPONSE
                        if page.rejected_results
                        else SourceStatus.OK
                        if page.results or query is None
                        else SourceStatus.EMPTY
                    )
                    outcome = SourceOutcome(
                        source=source,
                        status=status,
                        total=page.total,
                        next_offset=page.next_offset,
                        rejected_results=page.rejected_results,
                        truncated=page.truncated,
                    )
        except TimeoutError:
            outcome = SourceOutcome(source=source, status=SourceStatus.TIMEOUT)
            page = SourcePage([])
        except MetadataSourceError as exc:
            outcome = SourceOutcome(
                source=source, status=exc.status, retry_after_seconds=exc.retry_after_seconds
            )
            page = SourcePage([])
        except Exception:
            # A provider fault must not leak credentials or hide other source results.
            logger.warning("metadata_source_operation_failed", source=source.value)
            outcome = SourceOutcome(source=source, status=SourceStatus.UNAVAILABLE)
            page = SourcePage([])
        finally:
            if adapter is not None:
                try:
                    async with asyncio.timeout(2):
                        await adapter.close()
                except Exception:
                    logger.warning("metadata_source_close_failed", source=source.value)
        return page, outcome

    async def discover(
        self,
        query: SeriesDiscoveryQuery,
        *,
        satisfied_by: Callable[[SourcePage], bool] | None = None,
    ) -> SeriesDiscoveryRead:
        """Cascade only stops when the caller proves its requirements are met."""
        sources = sorted(
            query.sources if query.sources is not None else self.runtime,
            key=lambda source: (
                self.runtime[source].policy.domain_priorities.get(
                    MetadataDomain.CORE, self.runtime[source].policy.priority
                )
                if source in self.runtime
                else 1001,
                source.value,
            ),
        )
        semaphore = asyncio.Semaphore(self.concurrency)
        deadline = asyncio.get_running_loop().time() + self.total_timeout
        pages = []
        if query.mode == "automatic":
            satisfied = False
            for source in sources:
                if satisfied:
                    status = self._unavailable(source, search=True) or SourceStatus.NOT_QUERIED
                    pages.append((SourcePage([]), SourceOutcome(source=source, status=status)))
                else:
                    page, outcome = await self._run(source, query, semaphore, deadline)
                    pages.append((page, outcome))
                    satisfied = (
                        bool(page.results) and satisfied_by is not None and satisfied_by(page)
                    )
        else:
            tasks = [
                asyncio.create_task(self._run(source, query, semaphore, deadline))
                for source in sources
            ]
            try:
                pages = list(await asyncio.gather(*tasks))
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        grouped = {}
        for page, _ in pages:
            for item in page.results:
                key = (item.identity_namespace, item.external_id)
                if key not in grouped:
                    grouped[key] = item.model_copy(deep=True)
                elif (
                    item.source != grouped[key].source and item.source not in grouped[key].also_from
                ):
                    grouped[key].also_from.append(item.source)
        return SeriesDiscoveryRead(
            results=list(grouped.values()), sources=[outcome for _, outcome in pages]
        )

    async def check(self, source: MetadataSource) -> SourceOutcome:
        _, outcome = await self._run(
            source,
            None,
            asyncio.Semaphore(1),
            asyncio.get_running_loop().time() + self.total_timeout,
        )
        return outcome
