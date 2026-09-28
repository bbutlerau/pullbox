"""Daily, bounded source membership discovery for monitored story arcs."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import SQLAlchemyError

from pullbox.config import get_settings
from pullbox.core.metadata_identity import IdentityNamespace, MetadataSource
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.core.scheduler import scheduled_task
from pullbox.database import get_session_factory
from pullbox.models.library import LibraryRoot
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity, StoryArcLifecycle
from pullbox.schemas.metadata_arc_catalog import ArcCatalogSelection
from pullbox.schemas.metadata_sources import MetadataDomain, SourceCapability
from pullbox.services.import_activity import has_active_import_scheduler_protection
from pullbox.services.metadata_arc_catalog import StoryArcSourceError, fetch_source_arc_catalog
from pullbox.services.metadata_arc_commands import catalog_writer
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_sources import load_source_runtime
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from pullbox.services.story_arc_service import StoryArcServiceError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from sqlalchemy.sql.elements import ColumnElement

logger = structlog.get_logger(__name__)
_PAGE_SIZE = 25
_NAMESPACES = tuple({source.identity_namespace.value for source in MetadataSource})


def _active_monitored() -> tuple[ColumnElement[bool], ...]:
    verified = (
        select(StoryArcExternalIdentity.id)
        .where(
            StoryArcExternalIdentity.story_arc_id == StoryArc.id,
            StoryArcExternalIdentity.namespace == "story_arc",
            StoryArcExternalIdentity.source.in_(_NAMESPACES),
            StoryArcExternalIdentity.verification_state == IdentityVerificationState.VERIFIED,
        )
        .exists()
    )
    return (
        StoryArc.monitored.is_(True),
        StoryArc.lifecycle == StoryArcLifecycle.ACTIVE,
        or_(StoryArc.comicvine_id.is_not(None), verified),
    )


async def _targets(
    session: AsyncSession, arc: StoryArc, registry: MetadataSourceRegistry
) -> tuple[ArcCatalogSelection, ...]:
    claims = list(
        await session.scalars(
            select(StoryArcExternalIdentity).where(
                StoryArcExternalIdentity.story_arc_id == arc.id,
                StoryArcExternalIdentity.namespace == "story_arc",
                StoryArcExternalIdentity.source.in_(_NAMESPACES),
            )
        )
    )
    identities = {
        claim.source: claim.external_id
        for claim in claims
        if claim.verification_state is IdentityVerificationState.VERIFIED
    }
    # A retained canonical claim takes precedence over a legacy compatibility
    # key, including a stale/conflicted claim that blocks automatic refresh.
    if arc.comicvine_id and not any(
        claim.source == IdentityNamespace.COMICVINE for claim in claims
    ):
        identities[IdentityNamespace.COMICVINE] = str(arc.comicvine_id)
    return tuple(
        ArcCatalogSelection(
            source=source,
            external_id=identities[source.identity_namespace],
            source_revision=registry.runtime[source].policy.revision,
        )
        for source in registry.ordered_sources(domain=MetadataDomain.STORY_ARCS)
        if source.identity_namespace in identities
        and registry.runtime[source].policy.revision > 0
        and all(
            registry.source_availability(source, capability) is None
            for capability in (
                SourceCapability.STORY_ARC_DETAILS,
                SourceCapability.STORY_ARC_ISSUES,
                SourceCapability.SERIES_DETAILS,
            )
        )
    )


async def _registry(session: AsyncSession) -> MetadataSourceRegistry:
    flag = get_settings().metadata_gcd_api_v2_enabled
    return MetadataSourceRegistry(
        await load_source_runtime(session, gcd_api_enabled=flag), gcd_api_enabled=flag
    )


async def _record_failure(
    factory: async_sessionmaker[AsyncSession],
    arc_id: int,
    revision: int,
    targets: tuple[ArcCatalogSelection, ...],
    code: str,
) -> None:
    async with factory() as session:
        arc = await session.scalar(
            select(StoryArc)
            .where(StoryArc.id == arc_id, StoryArc.revision == revision, *_active_monitored())
            .with_for_update()
        )
        if arc is None or await _targets(session, arc, await _registry(session)) != targets:
            return
        await session.execute(
            update(StoryArc)
            .where(StoryArc.id == arc_id, StoryArc.revision == revision, *_active_monitored())
            .values(
                diagnostics={
                    **arc.diagnostics,
                    "provider_refresh_error": {
                        "code": code,
                        "checked_at": datetime.now(UTC).isoformat(),
                    },
                }
            )
        )
        await session.commit()


async def _refresh_arc(factory: async_sessionmaker[AsyncSession], arc_id: int) -> int | None:
    """Read exact ownership, release the reader, fetch, then recheck consent."""
    async with factory() as session:
        arc = await session.scalar(
            select(StoryArc).where(StoryArc.id == arc_id, *_active_monitored())
        )
        if arc is None:
            return None
        revision = arc.revision
        registry = await _registry(session)
        targets = await _targets(session, arc, registry)
    if not targets:
        return None
    try:
        deadline = asyncio.get_running_loop().time() + 120
        failure = None
        preview = None
        for target in targets:
            if await has_active_import_scheduler_protection(factory):
                return None
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise StoryArcCatalogError("source_timeout", "Story arc update timed out")
            try:
                preview = await fetch_source_arc_catalog(
                    registry,
                    target.source,
                    target.external_id,
                    source_revision=target.source_revision,
                    timeout=min(120, remaining),
                )
                break
            except StoryArcSourceError as exc:
                failure = exc
        if preview is None:
            assert failure is not None
            raise failure
        if await has_active_import_scheduler_protection(factory):
            return None
        async with factory() as session:
            arc = await session.scalar(
                select(StoryArc).where(StoryArc.id == arc_id, *_active_monitored())
            )
            if arc is None or arc.revision != revision:
                return None
            if await _targets(session, arc, await _registry(session)) != targets:
                return None
            # Only the explicit managed default may fill a legacy missing root.
            # Never infer it from a source or separate arc-copy folder.
            default_roots = list(
                await session.scalars(
                    select(LibraryRoot.id)
                    .where(LibraryRoot.is_default_managed_destination.is_(True))
                    .limit(2)
                )
            )
            result = await catalog_writer(preview).refresh(
                session,
                arc_id,
                preview,
                expected_revision=revision,
                library_root_id=default_roots[0] if len(default_roots) == 1 else None,
            )
            await session.commit()
            # The wanted sweep rechecks monitoring, skips, dates and duplicates.
            return len(result.added_membership_ids)
    except (StoryArcServiceError, SQLAlchemyError) as exc:
        await _record_failure(
            factory, arc_id, revision, targets, getattr(exc, "code", "provider_unavailable")
        )
        raise


async def sync_story_arc_metadata() -> None:
    """Refresh each eligible arc once, isolating source failures per arc."""
    factory = get_session_factory()
    if await has_active_import_scheduler_protection(factory):
        return
    async with factory() as session:
        ceiling = await session.scalar(select(func.max(StoryArc.id)).where(*_active_monitored()))
    if ceiling is None:
        return
    cursor = refreshed = added = failed = skipped = 0
    while cursor < ceiling:
        async with factory() as session:
            ids = list(
                await session.scalars(
                    select(StoryArc.id)
                    .where(StoryArc.id > cursor, StoryArc.id <= ceiling, *_active_monitored())
                    .order_by(StoryArc.id)
                    .limit(_PAGE_SIZE)
                )
            )
        if not ids:
            break
        for arc_id in ids:
            if await has_active_import_scheduler_protection(factory):
                return
            cursor = arc_id
            try:
                result = await _refresh_arc(factory, arc_id)
                if result is None:
                    skipped += 1
                else:
                    added += result
                    refreshed += 1
            except (StoryArcServiceError, SQLAlchemyError) as exc:
                failed += 1
                logger.warning(
                    "story_arc_metadata_refresh_failed",
                    story_arc_id=arc_id,
                    category=getattr(exc, "code", "provider_unavailable"),
                )
    logger.info(
        "story_arc_metadata_refresh_done",
        refreshed=refreshed,
        added=added,
        failed=failed,
        skipped=skipped,
    )


@scheduled_task(
    task_id="sync_story_arc_metadata",
    trigger="cron",
    display_name="Sync Story Arc Members",
    hour=1,
    minute=30,
)
async def scheduled_sync_story_arc_metadata() -> None:
    await sync_story_arc_metadata()
