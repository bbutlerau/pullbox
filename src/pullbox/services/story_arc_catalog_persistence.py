"""Targeted canonical seeding for arcs without whole-series adoption side effects."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid5

from sqlalchemy import func, select

from pullbox.core.exceptions import ValidationError
from pullbox.core.issue_numbers import parse_issue_number_text
from pullbox.core.library_naming import build_series_relative_path
from pullbox.core.library_policy import load_effective_library_ingest_policy
from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
)
from pullbox.core.naming import classify_series_type, detect_issue_type_from_metadata_title
from pullbox.core.type_semantics import canonical_issue_type_for_series_type
from pullbox.models.issue import Issue, IssueStatus, IssueType
from pullbox.models.library import LibraryRoot
from pullbox.models.publisher import Publisher
from pullbox.models.series import IssueCatalogState, Series, SeriesStatus, SeriesType
from pullbox.models.story_arc import StoryArc, StoryArcExternalIdentity
from pullbox.services.library_root_management import validate_managed_library_root
from pullbox.services.metadata_identity_attachment import (
    IdentityAttachmentConflictError,
    attach_verified_identities,
)
from pullbox.services.story_arc_catalog_identity import catalog_owners
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError, exact_provider_id

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.providers.base import SeriesMetadata
    from pullbox.schemas.metadata_sources import ProviderSeriesRead
    from pullbox.services.story_arc_catalog_types import StoryArcCatalogPreview


async def canonical_root(session: AsyncSession, root_id: int) -> LibraryRoot:
    if isinstance(root_id, bool) or not isinstance(root_id, int) or root_id < 1:
        raise StoryArcCatalogError("canonical_root_required", "Select a canonical library root")
    root = await session.get(LibraryRoot, root_id)
    if root is None:
        raise StoryArcCatalogError(
            "canonical_root_unavailable", "Canonical library root is unavailable"
        )
    try:
        await validate_managed_library_root(root)
    except ValidationError as exc:
        raise StoryArcCatalogError(
            "canonical_root_unavailable", "Canonical library root is unavailable"
        ) from exc
    return root


async def publisher_id(session: AsyncSession, name: str | None) -> int | None:
    if not name:
        return None
    publisher = await session.scalar(select(Publisher).where(Publisher.name == name))
    if publisher is None:
        publisher = Publisher(name=name)
        session.add(publisher)
        await session.flush()
    return publisher.id


async def seed_members(
    session: AsyncSession,
    preview: StoryArcCatalogPreview,
    root: LibraryRoot,
    provider_ids: Sequence[str],
) -> dict[str, Issue]:
    """Create only absent exact identities; all existing rows remain untouched.

    The caller's savepoint makes identity/path conflicts atomic. Paths are reserved
    in the database only; normal acquisition creates canonical folders later.
    """
    metadata_by_id = {issue.provider_id: issue for issue in preview.issues}
    series_metadata = {series.provider_id: series for series in preview.series}
    result: dict[str, Issue] = {}
    parents: dict[str, Series] = {}
    new_parents: list[Series] = []
    new_issues: list[Issue] = []
    parent_owners = await catalog_owners(
        session,
        MetadataEntityKind.SERIES,
        [metadata_by_id[key].series_provider_id for key in provider_ids],
        preview.source,
    )
    issue_owners = await catalog_owners(
        session, MetadataEntityKind.ISSUE, provider_ids, preview.source
    )
    for provider_id in provider_ids:
        metadata = metadata_by_id[provider_id]
        parent_key = metadata.series_provider_id
        parent = parents.get(parent_key)
        if parent is None:
            owner_id = parent_owners.get(parent_key)
            parent = await session.get(Series, owner_id) if owner_id is not None else None
            if parent is None:
                parent_metadata = series_metadata.get(parent_key)
                if parent_metadata is None:
                    raise StoryArcCatalogError(
                        "parent_metadata_missing", "Canonical parent changed; refresh the preview"
                    )
                profile = (
                    next(
                        (
                            row
                            for row in preview.source_evidence.series
                            if row.external_id == parent_key
                        ),
                        None,
                    )
                    if preview.source_evidence
                    else None
                )
                parent = await _new_series(
                    session, parent_metadata, root, preview.source, profile=profile
                )
                new_parents.append(parent)
            parents[parent_key] = parent
        number, exact_number = parse_issue_number_text(
            metadata.issue_number_text or metadata.issue_number
        )
        owner_id = issue_owners.get(provider_id)
        issue = await session.get(Issue, owner_id) if owner_id is not None else None
        if issue is not None:
            if issue.series_id != parent.id or issue.effective_issue_number_text != exact_number:
                raise StoryArcCatalogError(
                    "identity_conflict",
                    "Existing issue disagrees with the provider's exact identity",
                )
        else:
            sibling = await session.scalar(
                select(Issue.id).where(
                    Issue.series_id == parent.id, Issue.issue_number_text == exact_number
                )
            )
            if sibling is not None:
                raise StoryArcCatalogError(
                    "identity_conflict",
                    "An issue with a different identity already uses this exact number",
                )
            detected_type = IssueType(detect_issue_type_from_metadata_title(metadata.title))
            if detected_type is IssueType.ISSUE:
                detected_type = canonical_issue_type_for_series_type(parent.series_type)
            issue = Issue(
                series_id=parent.id,
                comicvine_id=exact_provider_id(provider_id)
                if preview.source.identity_namespace is IdentityNamespace.COMICVINE
                else None,
                issue_number=number,
                issue_number_text=exact_number,
                title=metadata.title,
                description=metadata.description,
                release_date=_date(metadata.release_date),
                store_date=_date(metadata.store_date),
                cover_url=metadata.cover_url,
                comicvine_url=metadata.comicvine_url
                if preview.source.identity_namespace is IdentityNamespace.COMICVINE
                else None,
                page_count=metadata.page_count,
                metadata_source="comicvine"
                if preview.source is MetadataSource.COMICVINE_API
                else preview.source.value,
                issue_type=detected_type,
                status=IssueStatus.SKIPPED,
                manual_skip=False,
            )
            session.add(issue)
            await session.flush()
            new_issues.append(issue)
        result[provider_id] = issue
    await _attach_member_identities(session, preview, parents, result)
    from pullbox.services.metadata_arc_baselines import persist_seeded_metadata

    await persist_seeded_metadata(session, preview, new_parents, new_issues)
    return result


async def _attach_member_identities(
    session: AsyncSession,
    preview: StoryArcCatalogPreview,
    parents: dict[str, Series],
    issues: dict[str, Issue],
) -> None:
    """Persist verified preview evidence without another provider or file read."""
    operation_id = uuid5(UUID("e5975ab0-13f6-4e1f-b7af-c3a00ccab88b"), preview.fingerprint)
    parent_ids = {row.id: key for key, row in parents.items()}
    requests = []
    for kind, rows in ((MetadataEntityKind.SERIES, parents), (MetadataEntityKind.ISSUE, issues)):
        for provider_id, row in rows.items():
            identity = ExternalIdentityRef(preview.source.identity_namespace, kind, provider_id)
            parent = (
                ExternalIdentityRef(
                    preview.source.identity_namespace,
                    MetadataEntityKind.SERIES,
                    parent_ids[row.series_id],
                )
                if isinstance(row, Issue)
                else None
            )
            requests.append(
                IdentityEventRequest(
                    operation_id,
                    row.id,
                    IdentityVerificationAction.VERIFY,
                    IdentityEventEvidence(
                        ExactIdentityEvidence(
                            identity,
                            IdentityEvidenceKind.PROVIDER_RESULT,
                            preview.source,
                        ),
                        preview.fingerprint,
                        source_identity=identity,
                        parent_identity=parent,
                    ),
                )
            )
    try:
        await attach_verified_identities(session, requests, require_current_ownership=True)
    except (IdentityAttachmentConflictError, IdentityReviewRequiredError) as exc:
        raise StoryArcCatalogError(
            "identity_conflict", "Canonical identity needs review before adding these members"
        ) from exc


async def attach_arc_identity(
    session: AsyncSession, arc: StoryArc, preview: StoryArcCatalogPreview
) -> None:
    """Record the validated preview without treating an old receipt as current proof."""
    identity = ExternalIdentityRef(
        preview.source.identity_namespace,
        MetadataEntityKind.STORY_ARC,
        preview.metadata.provider_id,
    )
    request = IdentityEventRequest(
        uuid5(UUID("e5975ab0-13f6-4e1f-b7af-c3a00ccab88b"), preview.fingerprint),
        arc.id,
        IdentityVerificationAction.VERIFY,
        IdentityEventEvidence(
            ExactIdentityEvidence(identity, IdentityEvidenceKind.PROVIDER_RESULT, preview.source),
            preview.fingerprint,
            source_identity=identity,
        ),
    )
    try:
        await attach_verified_identities(session, [request], require_current_ownership=True)
    except (IdentityAttachmentConflictError, IdentityReviewRequiredError) as exc:
        raise StoryArcCatalogError(
            "identity_conflict", "Story arc identity needs review before updating its catalog"
        ) from exc
    owner = await session.scalar(
        select(StoryArcExternalIdentity).where(
            StoryArcExternalIdentity.story_arc_id == arc.id,
            StoryArcExternalIdentity.source == preview.source.identity_namespace.value,
            StoryArcExternalIdentity.namespace == "story_arc",
        )
    )
    assert owner is not None
    if (
        preview.source.identity_namespace is IdentityNamespace.COMICVINE
        and preview.metadata.comicvine_url is not None
    ):
        owner.source_url = preview.metadata.comicvine_url
    if preview.source_evidence is not None and preview.source_evidence.arc.resource_url:
        owner.source_url = preview.source_evidence.arc.resource_url
    owner.evidence = {**owner.evidence, "snapshot_fingerprint": preview.fingerprint}
    await session.refresh(arc, ["comicvine_id"])


async def _new_series(
    session: AsyncSession,
    metadata: SeriesMetadata,
    root: LibraryRoot,
    source: MetadataSource,
    *,
    profile: ProviderSeriesRead | None = None,
) -> Series:
    series = Series(
        comicvine_id=exact_provider_id(metadata.provider_id)
        if source.identity_namespace is IdentityNamespace.COMICVINE
        else None,
        title=metadata.title,
        sort_title=metadata.sort_title or metadata.title,
        year_start=metadata.year_start,
        year_end=metadata.year_end,
        description=metadata.description,
        cover_url=metadata.cover_url,
        comicvine_url=metadata.comicvine_url
        if source.identity_namespace is IdentityNamespace.COMICVINE
        else None,
        issue_count=metadata.issue_count or 0,
        status=SeriesStatus.ENDED if metadata.status == "ended" else SeriesStatus.CONTINUING,
        metadata_source="comicvine_partial"
        if source is MetadataSource.COMICVINE_API
        else f"{source.value}_partial",
        monitored=False,
        issue_catalog_state=IssueCatalogState.PARTIAL,
        series_type=SeriesType(
            classify_series_type(
                metadata.title,
                description=metadata.description,
                issue_count=metadata.issue_count or 0,
                year_start=metadata.year_start,
            )
        ),
        library_root_id=root.id,
    )
    if profile is not None:
        series.status = (
            SeriesStatus(profile.status)
            if profile.status is not None and profile.status in SeriesStatus
            else SeriesStatus.UNKNOWN
        )
        if profile.series_type is not None and profile.series_type in SeriesType:
            series.series_type = SeriesType(profile.series_type)
    identifier = await publisher_id(session, metadata.publisher)
    series.publisher = await session.get(Publisher, identifier) if identifier is not None else None
    session.add(series)
    policy = await load_effective_library_ingest_policy(session, root)
    path = Path(root.path) / build_series_relative_path(series, policy)
    if await _path_claimed(session, path):
        label = (
            "cv"
            if source.identity_namespace is IdentityNamespace.COMICVINE
            else source.identity_namespace.value
        )
        path = path.with_name(f"{path.name} [{label}-{metadata.provider_id}]")
        if await _path_claimed(session, path):
            raise StoryArcCatalogError(
                "canonical_path_collision", "Canonical series path is already in use"
            )
    if not path.resolve().is_relative_to(Path(root.path).resolve()):
        raise StoryArcCatalogError(
            "canonical_path_unsafe", "Canonical series path escapes its library root"
        )
    if len(str(path)) > 1000:
        raise StoryArcCatalogError("canonical_path_unsafe", "Canonical series path is too long")
    series.path = str(path)
    session.add(series)
    await session.flush()
    return series


async def _path_claimed(session: AsyncSession, path: Path) -> bool:
    return (
        path.exists()
        or path.is_symlink()
        or bool(
            await session.scalar(
                select(Series.id).where(func.lower(Series.path) == str(path).lower()).limit(1)
            )
        )
    )


def _date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None
