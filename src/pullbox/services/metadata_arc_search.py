"""Bounded complete candidate collection for interactive Story Arc pagination."""

import asyncio

from pullbox.core.metadata_identity import MetadataSource
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    ProviderStoryArcRead,
    SourceStatus,
    StoryArcDiscoveryQuery,
    StoryArcDiscoveryRead,
    StoryArcSourceOutcome,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_source_reads import read_story_arcs


async def collect_arc_candidates(
    registry: MetadataSourceRegistry, query: StoryArcDiscoveryQuery
) -> StoryArcDiscoveryRead:
    if query.mode != "interactive" or query.pages:
        raise ValueError("Interactive collection starts at the first page")
    deadline = asyncio.get_running_loop().time() + registry.total_timeout

    async def collect(
        source: MetadataSource,
    ) -> tuple[list[ProviderStoryArcRead], StoryArcSourceOutcome]:
        rows: list[ProviderStoryArcRead] = []
        seen: set[str] = set()
        outcome = StoryArcSourceOutcome(source=source, status=SourceStatus.EMPTY)
        for page_number in range(1, 11):
            result = await read_story_arcs(
                registry, source, query.query, page=page_number, deadline=deadline
            )
            page = result.data
            outcome.status = result.status
            outcome.retry_after_seconds = result.retry_after_seconds
            if page is None:
                outcome.truncated = bool(rows)
                break
            outcome.total, outcome.next_page = page.total, page.next_page
            if any(row.external_id in seen for row in page.results):
                outcome.status, outcome.truncated = SourceStatus.INCOMPATIBLE_RESPONSE, True
                break
            rows.extend(page.results)
            seen.update(row.external_id for row in page.results)
            outcome.status = SourceStatus.OK if rows else SourceStatus.EMPTY
            outcome.truncated = page.truncated or (page_number == 10 and page.next_page is not None)
            if page.next_page is None or outcome.truncated:
                break
            if page.next_page != page_number + 1:
                outcome.status, outcome.truncated = SourceStatus.INCOMPATIBLE_RESPONSE, True
                break
        return rows, outcome

    tasks = [
        asyncio.create_task(collect(source))
        for source in registry._ordered_sources(query, domain=MetadataDomain.STORY_ARCS)
    ]
    try:
        pages = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    grouped = {}
    for rows, _ in pages:
        for row in rows:
            key = (row.identity_namespace, row.external_id)
            if key not in grouped:
                grouped[key] = row.model_copy(deep=True)
            elif row.source not in grouped[key].also_from and row.source != grouped[key].source:
                grouped[key].also_from.append(row.source)
    return StoryArcDiscoveryRead(results=list(grouped.values()), sources=[o for _, o in pages])
