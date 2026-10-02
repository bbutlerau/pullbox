"""Source-bound arc previews, never trusted adoption or curated order proofs."""

import asyncio

from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_sources import MetadataFetch, SourceStatus, StoryArcPreviewRead
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_source_reads import source_id


async def preview_source_arc(
    registry: MetadataSourceRegistry, source: MetadataSource, external_id: str
) -> StoryArcPreviewRead:
    identifier = source_id(source, MetadataEntityKind.STORY_ARC, external_id)
    runtime = registry.runtime.get(source)
    preview = StoryArcPreviewRead(
        source=source,
        external_id=identifier,
        source_revision=runtime.policy.revision if runtime else 0,
        arc=MetadataFetch(status=SourceStatus.NOT_QUERIED),
        issues=MetadataFetch(status=SourceStatus.NOT_QUERIED),
    )
    try:
        async with asyncio.timeout(registry.total_timeout):
            preview.arc = await registry.story_arc(source, identifier)
            if preview.arc.status is SourceStatus.OK:
                arc = preview.arc.data
                assert arc is not None
                if arc.issue_external_ids is not None and not arc.membership_complete:
                    preview.issues = MetadataFetch(status=SourceStatus.INCOMPATIBLE_RESPONSE)
                else:
                    preview.issues = await registry.story_arc_issues(source, identifier)
                    page = preview.issues.data
                    if (
                        page is not None
                        and arc.issue_external_ids is not None
                        and (
                            page.total != len(arc.issue_external_ids)
                            or [row.external_id for row in page.results]
                            != arc.issue_external_ids[:100]
                        )
                    ):
                        preview.issues = MetadataFetch(status=SourceStatus.INCOMPATIBLE_RESPONSE)
    except TimeoutError:
        if preview.arc.status is SourceStatus.NOT_QUERIED:
            preview.arc = MetadataFetch(status=SourceStatus.TIMEOUT)
        else:
            preview.issues = MetadataFetch(status=SourceStatus.TIMEOUT)
    return preview
