"""Source-scoped ownership checks for the existing arc adoption transaction."""

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind, MetadataSource
from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.services.story_arc_catalog_types import (
    StoryArcCatalogError,
    StoryArcCatalogPreview,
    catalog_provider_id,
)


async def catalog_owners(
    session: AsyncSession,
    kind: MetadataEntityKind,
    provider_ids: Sequence[str],
    source: MetadataSource,
) -> dict[str, int]:
    """Include retained ownership and legacy CV keys; never match by title or number."""
    mappings: dict[
        MetadataEntityKind,
        tuple[
            type[Series] | type[Issue] | type[StoryArc],
            type[SeriesExternalIdentity]
            | type[IssueExternalIdentity]
            | type[StoryArcExternalIdentity],
            str,
        ],
    ] = {
        MetadataEntityKind.SERIES: (Series, SeriesExternalIdentity, "series_id"),
        MetadataEntityKind.ISSUE: (Issue, IssueExternalIdentity, "issue_id"),
        MetadataEntityKind.STORY_ARC: (StoryArc, StoryArcExternalIdentity, "story_arc_id"),
    }
    model, active_model, owner_key = mappings[kind]
    active = active_model.__table__
    namespace = source.identity_namespace
    scope = (
        (active.c.source == namespace.value, active.c.namespace == "story_arc")
        if (kind is MetadataEntityKind.STORY_ARC)
        else (active.c.identity_namespace == namespace,)
    )
    ids = list(dict.fromkeys(catalog_provider_id(value, source) for value in provider_ids))
    result: dict[str, int] = {}
    for offset in range(0, len(ids), 200):
        batch = ids[offset : offset + 200]
        rows = list(
            await session.execute(
                select(active.c.external_id, active.c[owner_key]).where(
                    *scope, active.c.external_id.in_(batch)
                )
            )
        )
        if namespace is IdentityNamespace.COMICVINE:
            rows.extend(
                await session.execute(
                    select(model.comicvine_id, model.id).where(
                        model.comicvine_id.in_([int(value) for value in batch])
                    )
                )
            )
        for value, owner in rows:
            key = str(value)
            if key in result and result[key] != owner:
                raise StoryArcCatalogError(
                    "identity_conflict", "Provider identity has conflicting local owners"
                )
            result[key] = owner
    return result


async def require_catalog_source_revision(
    session: AsyncSession, preview: StoryArcCatalogPreview
) -> None:
    # Legacy CV callers predate source settings. New source-bound snapshots always
    # carry a revision and hold a shared policy lock through the caller's commit.
    if preview.source_revision is None and preview.source is MetadataSource.COMICVINE_API:
        return
    if type(preview.source_revision) is not int or preview.source_revision < 1:
        raise StoryArcCatalogError(
            "source_changed", "Preview the story arc with current source settings"
        )
    config = await session.scalar(
        select(MetadataSourceConfig)
        .where(MetadataSourceConfig.source == preview.source.value)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if config is None or not config.enabled or config.revision != preview.source_revision:
        raise StoryArcCatalogError(
            "source_changed", "Metadata source settings changed; preview the story arc again"
        )
