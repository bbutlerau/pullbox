"""Bounded source-bound reads for selection, preview, and catalog hydration."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import structlog

from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    MetadataPage,
    ProviderIssueRead,
    ProviderSeriesRead,
    SourceCapability,
    SourceStatus,
)
from pullbox.services.metadata_discovery import MetadataSourceError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pullbox.services.metadata_discovery import MetadataSourceAdapter, MetadataSourceRegistry

logger = structlog.get_logger(__name__)
PAGE_SIZE = 100
MAX_PAGE = 10000


@runtime_checkable
class SeriesDetailAdapter(Protocol):
    async def series(
        self, external_id: str, *, validator: str | None = None
    ) -> MetadataFetch[ProviderSeriesRead]: ...


@runtime_checkable
class IssueDetailAdapter(Protocol):
    async def issue(
        self, external_id: str, *, validator: str | None = None
    ) -> MetadataFetch[ProviderIssueRead]: ...


@runtime_checkable
class IssueListAdapter(Protocol):
    async def issues(
        self, external_id: str, *, page: int = 1, validator: str | None = None
    ) -> MetadataFetch[MetadataPage[ProviderIssueRead]]: ...


def source_id(source: MetadataSource, kind: MetadataEntityKind, value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected a source identity string")
    return ExternalIdentityRef(source.identity_namespace, kind, value).external_id


def page_number(page: int) -> None:
    if type(page) is not int or not 1 <= page <= MAX_PAGE:
        raise ValueError("Metadata page must be between one and ten thousand")


def _identity(
    row: ProviderSeriesRead | ProviderIssueRead, source: MetadataSource, kind: MetadataEntityKind
) -> None:
    if (
        row.source is not source
        or row.identity_namespace is not source.identity_namespace
        or source_id(source, kind, row.external_id) != row.external_id
    ):
        raise ValueError("Source returned a different identity")


def _issue(row: ProviderIssueRead, source: MetadataSource) -> None:
    _identity(row, source, MetadataEntityKind.ISSUE)
    if (
        source_id(source, MetadataEntityKind.SERIES, row.series_external_id)
        != row.series_external_id
        or not row.issue_number_text.strip()
        or len(row.issue_number_text) > 320
    ):
        raise ValueError("Source returned an invalid issue")


async def _read[T](
    registry: MetadataSourceRegistry,
    source: MetadataSource,
    capability: SourceCapability,
    operation: Callable[[MetadataSourceAdapter], Awaitable[MetadataFetch[T]]],
    validate: Callable[[T], None],
    validator: str | None,
) -> MetadataFetch[T]:
    if validator is not None and (
        not isinstance(validator, str)
        or not 1 <= len(validator) <= 128
        or any(ord(char) < 32 or ord(char) > 126 for char in validator)
    ):
        raise ValueError("Invalid source validator")
    unavailable = registry._unavailable(source, capability=capability)
    if unavailable is not None:
        return MetadataFetch(status=unavailable)
    adapter = None
    try:
        async with asyncio.timeout(registry.total_timeout), registry.read_slots:
            async with asyncio.timeout(registry.per_source_timeout):
                adapter = registry.factories[source].factory(registry.runtime[source])
                result = await operation(adapter)
                if result.status is SourceStatus.OK:
                    if result.data is None:
                        raise ValueError("Missing source metadata")
                    validate(result.data)
                elif (
                    result.data is not None
                    or result.status not in {SourceStatus.NOT_FOUND, SourceStatus.NOT_MODIFIED}
                    or (result.status is SourceStatus.NOT_MODIFIED and validator is None)
                ):
                    raise ValueError("Invalid source read outcome")
                return result
    except TimeoutError:
        return MetadataFetch(status=SourceStatus.TIMEOUT)
    except MetadataSourceError as exc:
        return MetadataFetch(status=exc.status, retry_after_seconds=exc.retry_after_seconds)
    except (ValueError, TypeError, AttributeError):
        return MetadataFetch(status=SourceStatus.INCOMPATIBLE_RESPONSE)
    except Exception:
        logger.warning("metadata_source_read_failed", source=source.value, capability=capability)
        return MetadataFetch(status=SourceStatus.UNAVAILABLE)
    finally:
        if adapter is not None:
            try:
                async with asyncio.timeout(2):
                    await adapter.close()
            except Exception:
                logger.warning("metadata_source_close_failed", source=source.value)


async def read_series(
    registry: MetadataSourceRegistry,
    source: MetadataSource,
    external_id: str,
    *,
    validator: str | None,
) -> MetadataFetch[ProviderSeriesRead]:
    identifier = source_id(source, MetadataEntityKind.SERIES, external_id)

    async def operation(adapter: MetadataSourceAdapter) -> MetadataFetch[ProviderSeriesRead]:
        if not isinstance(adapter, SeriesDetailAdapter):
            raise MetadataSourceError(SourceStatus.UNSUPPORTED)
        return await adapter.series(identifier, validator=validator)

    def validate(row: ProviderSeriesRead) -> None:
        _identity(row, source, MetadataEntityKind.SERIES)
        if row.external_id != identifier or not row.title.strip() or len(row.title) > 500:
            raise ValueError("Source returned a different series")

    return await _read(
        registry,
        source,
        SourceCapability.SERIES_DETAILS,
        operation,
        validate,
        validator,
    )


async def read_issue(
    registry: MetadataSourceRegistry,
    source: MetadataSource,
    external_id: str,
    *,
    validator: str | None,
) -> MetadataFetch[ProviderIssueRead]:
    identifier = source_id(source, MetadataEntityKind.ISSUE, external_id)

    async def operation(adapter: MetadataSourceAdapter) -> MetadataFetch[ProviderIssueRead]:
        if not isinstance(adapter, IssueDetailAdapter):
            raise MetadataSourceError(SourceStatus.UNSUPPORTED)
        return await adapter.issue(identifier, validator=validator)

    def validate(row: ProviderIssueRead) -> None:
        _issue(row, source)
        if row.external_id != identifier:
            raise ValueError("Source returned a different issue")

    return await _read(
        registry,
        source,
        SourceCapability.ISSUE_DETAILS,
        operation,
        validate,
        validator,
    )


async def read_issues(
    registry: MetadataSourceRegistry,
    source: MetadataSource,
    external_id: str,
    *,
    page: int,
    validator: str | None,
) -> MetadataFetch[MetadataPage[ProviderIssueRead]]:
    identifier = source_id(source, MetadataEntityKind.SERIES, external_id)
    page_number(page)

    async def operation(
        adapter: MetadataSourceAdapter,
    ) -> MetadataFetch[MetadataPage[ProviderIssueRead]]:
        if not isinstance(adapter, IssueListAdapter):
            raise MetadataSourceError(SourceStatus.UNSUPPORTED)
        return await adapter.issues(identifier, page=page, validator=validator)

    def validate(result: MetadataPage[ProviderIssueRead]) -> None:
        remaining = max(0, result.total - (page - 1) * PAGE_SIZE)
        more = remaining > PAGE_SIZE
        if (
            not 0 <= result.total <= 1_000_000_000
            or len(result.results) != min(remaining, PAGE_SIZE)
            or result.next_page != (page + 1 if more and page < MAX_PAGE else None)
            or result.truncated != (more and page == MAX_PAGE)
            or result.order_is_reading_order
            or len({row.external_id for row in result.results}) != len(result.results)
        ):
            raise ValueError("Source returned an incomplete issue page")
        for row in result.results:
            _issue(row, source)
            if row.series_external_id != identifier:
                raise ValueError("Source returned an issue from a different series")

    return await _read(
        registry,
        source,
        SourceCapability.ISSUE_LIST,
        operation,
        validate,
        validator,
    )
