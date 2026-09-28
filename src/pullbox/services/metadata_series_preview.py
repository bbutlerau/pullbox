"""Source-specific series previews; returned metadata is not an adoption proof."""

import asyncio

from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_sources import MetadataFetch, SeriesPreviewRead, SourceStatus
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_source_reads import source_id


async def preview_source_series(
    registry: MetadataSourceRegistry, source: MetadataSource, external_id: str
) -> SeriesPreviewRead:
    identifier = source_id(source, MetadataEntityKind.SERIES, external_id)
    runtime = registry.runtime.get(source)
    preview = SeriesPreviewRead(
        source=source,
        external_id=identifier,
        source_revision=runtime.policy.revision if runtime else 0,
        series=MetadataFetch(status=SourceStatus.NOT_QUERIED),
        issues=MetadataFetch(status=SourceStatus.NOT_QUERIED),
    )
    try:
        async with asyncio.timeout(registry.total_timeout):
            preview.series = await registry.series(source, identifier)
            if preview.series.status is SourceStatus.OK:
                preview.issues = await registry.issues(source, identifier)
    except TimeoutError:
        if preview.series.status is SourceStatus.NOT_QUERIED:
            preview.series = MetadataFetch(status=SourceStatus.TIMEOUT)
        else:
            preview.issues = MetadataFetch(status=SourceStatus.TIMEOUT)
    return preview
