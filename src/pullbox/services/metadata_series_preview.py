"""Source-specific series previews; returned metadata is not an adoption proof."""

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.library_naming import build_series_relative_path
from pullbox.core.library_policy import load_effective_library_ingest_policy
from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind, MetadataSource
from pullbox.core.naming import classify_series_type
from pullbox.models.library import LibraryRoot
from pullbox.models.publisher import Publisher
from pullbox.models.series import Series, SeriesType
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    MetadataPage,
    ProviderSeriesRead,
    SeriesPreviewRead,
    SourceStatus,
)
from pullbox.services.metadata_catalog_review import catalog_review
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_series_adoption import fetch_source_series_bundle
from pullbox.services.metadata_series_artwork import with_representative_cover
from pullbox.services.metadata_source_reads import source_id


async def preview_series_folder(
    session: AsyncSession, profile: ProviderSeriesRead, root: LibraryRoot
) -> str:
    """Render a naming suggestion without adopting metadata or touching the filesystem."""
    policy = await load_effective_library_ingest_policy(session, root)
    series = Series(
        title=profile.title,
        year_start=profile.year_start,
        publisher=Publisher(name=profile.publisher) if profile.publisher else None,
        comicvine_id=int(profile.external_id)
        if profile.identity_namespace is IdentityNamespace.COMICVINE
        else None,
        series_type=SeriesType(profile.series_type)
        if profile.series_type is not None and profile.series_type in SeriesType
        else SeriesType(
            classify_series_type(
                profile.title,
                description=profile.description,
                issue_count=profile.issue_count or 0,
                year_start=profile.year_start,
            )
        ),
    )
    return build_series_relative_path(series, policy).as_posix()


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
        if source is MetadataSource.GCD_LOCAL:
            bundle = await fetch_source_series_bundle(
                registry, source, identifier, source_revision=preview.source_revision
            )
            preview.series = MetadataFetch(status=SourceStatus.OK, data=bundle.series)
            preview.issues = MetadataFetch(
                status=SourceStatus.OK,
                data=MetadataPage(
                    results=list(bundle.issues[:100]),
                    total=bundle.catalog_total,
                    next_page=2 if len(bundle.issues) > 100 else None,
                ),
            )
            preview.catalog_review = catalog_review(bundle)
            return preview
        async with asyncio.timeout(registry.total_timeout):
            preview.series = await registry.series(source, identifier)
            if preview.series.status is SourceStatus.OK:
                preview.issues = await registry.issues(source, identifier)
                if preview.series.data is not None and preview.issues.data is not None:
                    preview.series.data = with_representative_cover(
                        preview.series.data, preview.issues.data.results
                    )
    except TimeoutError:
        if preview.series.status is SourceStatus.NOT_QUERIED:
            preview.series = MetadataFetch(status=SourceStatus.TIMEOUT)
        else:
            preview.issues = MetadataFetch(status=SourceStatus.TIMEOUT)
    return preview
