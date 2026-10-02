"""Exact-identity descriptive reads separated from Story Arc catalog writes."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.config import get_settings
from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_identity import StoryArcIdentityEvent
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.publisher import Publisher
from pullbox.schemas.metadata_snapshot import MetadataValues
from pullbox.schemas.metadata_sources import SourcePolicyRead
from pullbox.services.metadata_assembly import MetadataAssemblyError
from pullbox.services.metadata_baselines import (
    MetadataBaselineConflictError,
    load_metadata_baseline,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_read_cache import source_read_cache
from pullbox.services.metadata_refresh_snapshot import (
    MetadataRefreshSnapshot,
    fetch_metadata_snapshot,
)
from pullbox.services.metadata_series_refresh_state import RefreshEntityState
from pullbox.services.metadata_sources import load_source_runtime, read_source_policies
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError, StoryArcCatalogPreview
from pullbox.services.story_arc_service import StoryArcService

ARC_FIELDS = frozenset({"title", "description", "publisher", "image_url"})


@dataclass(frozen=True)
class ArcRefreshState:
    entity: RefreshEntityState
    revision: int
    lifecycle: str
    monitored: bool
    placed: bool
    policies: tuple[SourcePolicyRead, ...]


@dataclass(frozen=True)
class ArcMetadataRefresh:
    before: ArcRefreshState
    fetched: MetadataRefreshSnapshot


async def read_arc_refresh_state(session: AsyncSession, arc_id: int) -> ArcRefreshState:
    arc = await session.scalar(
        select(StoryArc).where(StoryArc.id == arc_id).execution_options(populate_existing=True)
    )
    if arc is None:
        raise StoryArcCatalogError("arc_unavailable", "Story arc is unavailable")
    try:
        saved = await load_metadata_baseline(session, MetadataEntityKind.STORY_ARC, arc_id)
    except MetadataBaselineConflictError as exc:
        raise StoryArcCatalogError(
            "metadata_baseline_conflict", "Metadata baseline needs review; preview the arc again"
        ) from exc
    values = saved.snapshot.values.model_dump() if saved else {}
    values.update(
        title=arc.name,
        description=arc.description,
        image_url=arc.cover_url,
        publisher=await session.scalar(
            select(Publisher.name).where(Publisher.id == arc.publisher_id)
        ),
    )
    claims = tuple(
        (
            ExternalIdentityRef(
                IdentityNamespace(row.source), MetadataEntityKind.STORY_ARC, row.external_id
            ),
            row.verification_state,
            row.revision,
        )
        for row in await session.scalars(
            select(StoryArcExternalIdentity)
            .where(
                StoryArcExternalIdentity.story_arc_id == arc_id,
                StoryArcExternalIdentity.namespace == "story_arc",
                StoryArcExternalIdentity.source.in_([item.value for item in IdentityNamespace]),
            )
            .order_by(StoryArcExternalIdentity.source)
            .execution_options(populate_existing=True)
        )
    )
    if arc.comicvine_id and not any(
        ref.namespace is IdentityNamespace.COMICVINE for ref, _, _ in claims
    ):
        claims += (
            (
                ExternalIdentityRef(
                    IdentityNamespace.COMICVINE, MetadataEntityKind.STORY_ARC, str(arc.comicvine_id)
                ),
                IdentityVerificationState.VERIFIED,
                0,
            ),
        )
    event_id = await session.scalar(
        select(func.max(StoryArcIdentityEvent.id)).where(
            StoryArcIdentityEvent.story_arc_id == arc_id
        )
    )
    policies = tuple(
        item.model_copy(
            update={
                "last_tested_at": None,
                "last_success_at": None,
                "last_status": None,
            }
        )
        for item in await read_source_policies(session)
    )
    return ArcRefreshState(
        RefreshEntityState(
            arc_id,
            MetadataValues.model_validate(values),
            saved.snapshot if saved else None,
            saved.revision if saved else 0,
            claims,
            event_id or 0,
            arc.comicvine_id,
        ),
        arc.revision,
        arc.lifecycle.value,
        arc.monitored,
        await StoryArcService._has_managed_placements(session, story_arc_id=arc_id),
        policies,
    )


async def fetch_arc_metadata(
    registry: MetadataSourceRegistry,
    before: ArcRefreshState,
    preview: StoryArcCatalogPreview,
    *,
    replace_managed: bool,
) -> ArcMetadataRefresh:
    evidence = preview.source_evidence
    if evidence is None:
        raise StoryArcCatalogError("snapshot_changed", "Preview current source metadata again")
    try:
        fetched = await fetch_metadata_snapshot(
            registry,
            MetadataEntityKind.STORY_ARC,
            before.entity.identities,
            now=datetime.now(UTC),
            requested_fields=ARC_FIELDS - {"title"} if before.placed else ARC_FIELDS,
            current=before.entity.values,
            previous=before.entity.baseline,
            replace_managed=replace_managed,
            initial_candidates=[evidence.arc],
        )
    except MetadataAssemblyError as exc:
        raise StoryArcCatalogError(
            "metadata_conflict", "Source metadata needs review before refresh"
        ) from exc
    return ArcMetadataRefresh(before, fetched)


async def prepare_arc_metadata(
    session: AsyncSession,
    arc_id: int,
    preview: StoryArcCatalogPreview,
    *,
    replace_managed: bool = True,
) -> ArcMetadataRefresh:
    if session.new or session.dirty or session.deleted:
        raise ValueError("Finish pending edits before refreshing Story Arc metadata")
    try:
        before = await read_arc_refresh_state(session, arc_id)
        enabled = get_settings().metadata_gcd_api_v2_enabled
        runtime = await load_source_runtime(session, gcd_api_enabled=enabled)
        registry = MetadataSourceRegistry(
            runtime,
            gcd_api_enabled=enabled,
            read_cache=source_read_cache(session),
            revalidate_reads=True,
            total_timeout=60,
        )
    finally:
        await session.rollback()
    return await fetch_arc_metadata(registry, before, preview, replace_managed=replace_managed)


async def require_arc_refresh_state(session: AsyncSession, refresh: ArcMetadataRefresh) -> None:
    # Match the shared policy-first, then parent-first writer lock order.
    await session.execute(
        select(MetadataSourceConfig)
        .order_by(MetadataSourceConfig.source)
        .with_for_update(read=True)
    )
    await session.execute(
        select(StoryArc.id).where(StoryArc.id == refresh.before.entity.local_id).with_for_update()
    )
    current = await read_arc_refresh_state(session, refresh.before.entity.local_id)
    if current != refresh.before:
        raise StoryArcCatalogError(
            "metadata_changed",
            "Story arc metadata, identities or source settings changed; refresh the review",
        )
