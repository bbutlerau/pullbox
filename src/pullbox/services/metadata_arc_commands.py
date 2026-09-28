"""Source catalog command boundaries shared by API and browser adapters."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.library_policy import load_search_on_add_default
from pullbox.models.story_arc import StoryArc
from pullbox.schemas.metadata_arc_catalog import (
    ArcCatalogAdd,
    ArcCatalogChanges,
    ArcCatalogPreviewRead,
    ArcCatalogRefresh,
    ArcCatalogSelection,
)
from pullbox.services.metadata_arc_catalog import fetch_source_arc_catalog
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_sources import load_source_runtime
from pullbox.services.story_arc_catalog import StoryArcCatalogService
from pullbox.services.story_arc_catalog_identity import require_catalog_source_revision
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError, StoryArcCatalogPreview
from pullbox.services.story_arc_file_defaults import load_story_arc_file_defaults


@dataclass(frozen=True, slots=True)
class ArcCommandResult:
    arc: StoryArc
    search_on_add: bool
    initial_placements: bool = False


async def fetch_current_arc_catalog(
    session: AsyncSession, selection: ArcCatalogSelection, *, gcd_api_enabled: bool
) -> StoryArcCatalogPreview:
    """Release the request's read snapshot before bounded provider work."""
    runtime = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
    await session.rollback()
    return await fetch_source_arc_catalog(
        MetadataSourceRegistry(runtime, gcd_api_enabled=gcd_api_enabled),
        selection.source,
        selection.external_id,
        source_revision=selection.source_revision,
    )


def catalog_writer(preview: StoryArcCatalogPreview) -> StoryArcCatalogService:
    return StoryArcCatalogService(source=preview.source, source_revision=preview.source_revision)


async def describe_arc_catalog(
    session: AsyncSession, preview: StoryArcCatalogPreview, *, story_arc_id: int | None = None
) -> ArcCatalogPreviewRead:
    await require_catalog_source_revision(session, preview)
    evidence = preview.source_evidence
    if evidence is None or preview.source_revision is None:
        raise StoryArcCatalogError("snapshot_changed", "Preview this source again")
    result = ArcCatalogPreviewRead(
        source=preview.source,
        external_id=preview.metadata.provider_id,
        source_revision=preview.source_revision,
        fingerprint=preview.fingerprint,
        arc=evidence.arc,
        issues=list(evidence.issues),
        series=list(evidence.series),
        order_basis=preview.order_basis,
    )
    if story_arc_id is None:
        defaults = await load_story_arc_file_defaults(session)
        result.file_defaults_fingerprint = defaults.fingerprint
        result.file_summary = defaults.summary_label
    else:
        delta = await catalog_writer(preview).preview_refresh(session, story_arc_id, preview)
        result.changes = ArcCatalogChanges(
            revision=delta.revision,
            added_issue_ids=list(delta.added_issue_provider_ids),
            removed_issue_ids=list(delta.removed_issue_provider_ids),
        )
    return result


def _require_decision(
    session: AsyncSession,
    preview: StoryArcCatalogPreview,
    decision: ArcCatalogAdd | ArcCatalogRefresh,
) -> None:
    if session.in_transaction():
        raise ValueError("Source arc command requires a session without an active transaction")
    if (
        preview.source is not decision.source
        or preview.metadata.provider_id != decision.external_id
        or preview.source_revision != decision.source_revision
        or preview.fingerprint != decision.fingerprint
        or preview.source_evidence is None
    ):
        raise StoryArcCatalogError("snapshot_changed", "Source metadata changed; preview again")


@asynccontextmanager
async def source_arc_add_transaction(
    session: AsyncSession, preview: StoryArcCatalogPreview, decision: ArcCatalogAdd
) -> AsyncIterator[ArcCommandResult]:
    """Commit one graph only after the caller has successfully built its response."""
    _require_decision(session, preview, decision)
    async with session.begin():
        defaults = await load_story_arc_file_defaults(session)
        if defaults.fingerprint != decision.file_defaults_fingerprint:
            raise StoryArcCatalogError("file_defaults_changed", "Review current file defaults")
        arc = await catalog_writer(preview).add(
            session,
            preview,
            ordered_issue_provider_ids=decision.ordered_issue_ids,
            skipped_issue_provider_ids=decision.skipped_issue_ids,
            library_root_id=decision.library_root_id,
            monitored=decision.monitored,
            search_missing=decision.monitored,
            include_upcoming=decision.monitored,
            placement_policy=defaults.proposal(),
        )
        marker = arc.diagnostics.get("catalog_initial_placements")
        pending = isinstance(marker, dict) and any(
            type(value := marker.get(key)) is int and value > 0 for key in ("pending", "failed")
        )
        yield ArcCommandResult(
            arc, arc.monitored and await load_search_on_add_default(session), pending
        )


@asynccontextmanager
async def source_arc_refresh_transaction(
    session: AsyncSession,
    story_arc_id: int,
    preview: StoryArcCatalogPreview,
    decision: ArcCatalogRefresh,
) -> AsyncIterator[ArcCommandResult]:
    _require_decision(session, preview, decision)
    async with session.begin():
        result = await catalog_writer(preview).refresh(
            session,
            story_arc_id,
            preview,
            expected_revision=decision.expected_revision,
            library_root_id=decision.library_root_id,
        )
        yield ArcCommandResult(
            result.story_arc,
            result.story_arc.monitored and await load_search_on_add_default(session),
        )
