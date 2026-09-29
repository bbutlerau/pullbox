"""Canonical provenance inside the existing Story Arc catalog transaction."""

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.core.story_arc_identity import normalize_story_arc_name
from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.publisher import Publisher
from pullbox.schemas.metadata_snapshot import (
    FieldOrigin,
    MetadataValues,
    field_domain,
)
from pullbox.services.metadata_assembly import assemble_metadata
from pullbox.services.metadata_baselines import (
    MetadataBaselineWrite,
    load_metadata_baseline,
    save_metadata_baselines,
)
from pullbox.services.metadata_series_adoption import persist_adoption_series_baseline
from pullbox.services.metadata_sources import read_source_policies
from pullbox.services.story_arc_catalog_types import StoryArcCatalogPreview
from pullbox.services.story_arc_service import StoryArcService


async def persist_arc_metadata(
    session: AsyncSession,
    arc: StoryArc,
    preview: StoryArcCatalogPreview,
    *,
    created: bool,
    replace_managed: bool = False,
) -> None:
    """Apply descriptive fields only; membership and placements retain their own rules."""
    evidence = preview.source_evidence
    if evidence is None:
        return
    kind = MetadataEntityKind.STORY_ARC
    saved = await load_metadata_baseline(session, kind, arc.id)
    identities = tuple(
        ExternalIdentityRef(IdentityNamespace(row.source), kind, row.external_id)
        for row in await session.scalars(
            select(StoryArcExternalIdentity).where(
                StoryArcExternalIdentity.story_arc_id == arc.id,
                StoryArcExternalIdentity.namespace == "story_arc",
                StoryArcExternalIdentity.source.in_([item.value for item in IdentityNamespace]),
                StoryArcExternalIdentity.verification_state == IdentityVerificationState.VERIFIED,
            )
        )
    )
    current = None
    if not created:
        # The caller has claimed the arc revision. Discard a stale identity-map
        # view before comparing user edits with the last committed baseline.
        await session.refresh(arc, ["name", "description", "publisher_id", "cover_url"])
        values = saved.snapshot.values.model_dump() if saved else {}
        values.update(
            title=arc.name,
            description=arc.description,
            image_url=arc.cover_url,
            publisher=await session.scalar(
                select(Publisher.name).where(Publisher.id == arc.publisher_id)
            ),
        )
        current = MetadataValues.model_validate(values)
    now = datetime.now(UTC)
    retain_name = not created and await StoryArcService._has_managed_placements(
        session, story_arc_id=arc.id
    )
    snapshot = assemble_metadata(
        kind,
        identities,
        [evidence.arc],
        await read_source_policies(session),
        now=now,
        current=current,
        previous=saved.snapshot if saved else None,
        replace_managed=replace_managed,
        fields=frozenset(MetadataValues.model_fields) - {"title"} if retain_name else None,
    )
    if retain_name and evidence.arc.title.strip() != arc.name:
        snapshot = snapshot.model_copy(
            update={"diagnostics": (*snapshot.diagnostics, "title_retained_for_managed_placements")}
        )
    title = (snapshot.values.title or "").strip()
    if not title:
        raise ValueError("Story arc metadata requires a title")
    if title != snapshot.values.title:
        snapshot = snapshot.model_copy(
            update={
                "values": snapshot.values.model_copy(update={"title": title}),
                "origins": (
                    *(item for item in snapshot.origins if item.field != "title"),
                    FieldOrigin(
                        field="title",
                        domain=field_domain(kind, "title"),
                        observed_at=now,
                        derivation="normalization",
                    ),
                ),
            }
        )
    arc.name = title
    arc.normalized_name = normalize_story_arc_name(title)
    arc.description = snapshot.values.description
    arc.cover_url = snapshot.values.image_url
    # Reuse the catalog's publisher creation and caller-owned transaction.
    from pullbox.services.story_arc_catalog_persistence import publisher_id

    arc.publisher_id = await publisher_id(session, snapshot.values.publisher)
    await save_metadata_baselines(
        session, [MetadataBaselineWrite(arc.id, snapshot, saved.revision if saved else 0)]
    )


async def persist_seeded_metadata(
    session: AsyncSession,
    preview: StoryArcCatalogPreview,
    parents: Sequence[Series],
    issues: Sequence[Issue],
) -> None:
    """Only newly seeded rows receive provider baselines; Add is not Refresh."""
    evidence = preview.source_evidence
    if evidence is None or not (parents or issues):
        return
    policies = await read_source_policies(session)
    now = datetime.now(UTC)
    parent_profiles = {row.external_id: row for row in evidence.series}
    issue_profiles = {row.external_id: row for row in evidence.issues}
    # Caller-created rows have just acquired their one native provider identity.
    from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity

    parent_keys = (
        dict(
            (
                await session.execute(
                    select(
                        SeriesExternalIdentity.series_id, SeriesExternalIdentity.external_id
                    ).where(
                        SeriesExternalIdentity.series_id.in_([row.id for row in parents]),
                        SeriesExternalIdentity.identity_namespace
                        == preview.source.identity_namespace,
                    )
                )
            )
            .tuples()
            .all()
        )
        if parents
        else {}
    )
    issue_keys = (
        dict(
            (
                await session.execute(
                    select(IssueExternalIdentity.issue_id, IssueExternalIdentity.external_id).where(
                        IssueExternalIdentity.issue_id.in_([row.id for row in issues]),
                        IssueExternalIdentity.identity_namespace
                        == preview.source.identity_namespace,
                    )
                )
            )
            .tuples()
            .all()
        )
        if issues
        else {}
    )
    for parent in parents:
        profile = parent_profiles[parent_keys[parent.id]]
        snapshot = assemble_metadata(
            MetadataEntityKind.SERIES,
            [
                ExternalIdentityRef(
                    profile.identity_namespace, MetadataEntityKind.SERIES, profile.external_id
                )
            ],
            [profile],
            policies,
            now=now,
        )
        parent.cover_url = snapshot.values.image_url
        await persist_adoption_series_baseline(session, parent, snapshot)
    writes = []
    for issue in issues:
        issue_profile = issue_profiles[issue_keys[issue.id]]
        snapshot = assemble_metadata(
            MetadataEntityKind.ISSUE,
            [
                ExternalIdentityRef(
                    issue_profile.identity_namespace,
                    MetadataEntityKind.ISSUE,
                    issue_profile.external_id,
                )
            ],
            [
                issue_profile.model_copy(
                    update={"issue_number_text": issue.effective_issue_number_text}
                )
            ],
            policies,
            now=now,
            parent_identities=[
                ExternalIdentityRef(
                    issue_profile.identity_namespace,
                    MetadataEntityKind.SERIES,
                    issue_profile.series_external_id,
                )
            ],
        )
        issue.cover_url = snapshot.values.image_url
        writes.append(MetadataBaselineWrite(issue.id, snapshot))
    for offset in range(0, len(writes), 200):
        await save_metadata_baselines(session, writes[offset : offset + 200])
