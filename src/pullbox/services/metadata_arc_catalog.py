"""Complete server-owned Story Arc snapshots for the existing catalog saver."""

import asyncio
from dataclasses import replace

from pullbox.core.issue_numbers import parse_issue_number_text
from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.providers.base import IssueMetadata, SeriesMetadata
from pullbox.providers.story_arcs import StoryArcMetadata
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    ProviderIssueRead,
    ProviderSeriesRead,
    ProviderStoryArcRead,
    SourceStatus,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_source_reads import source_id
from pullbox.services.story_arc_catalog_types import (
    SourceArcCatalogEvidence,
    StoryArcCatalogError,
    StoryArcCatalogPreview,
    catalog_provider_id,
    snapshot_fingerprint,
)

# These are command limits, not the larger browse-only API limit.
MAX_CATALOG_MEMBERS = 2000
MAX_CATALOG_PARENTS = 200


class StoryArcSourceError(StoryArcCatalogError):
    """Safe typed source failure, retaining retry advice without provider text."""

    def __init__(self, status: SourceStatus, retry_after_seconds: int | None = None) -> None:
        self.source_status = status
        self.retry_after_seconds = retry_after_seconds
        super().__init__(
            "source_unavailable",
            f"Could not finish the story arc: {status.value}. Retry the preview.",
        )


def _required[T](result: MetadataFetch[T]) -> T:
    if result.status is not SourceStatus.OK or result.data is None:
        raise StoryArcSourceError(result.status, result.retry_after_seconds)
    return result.data


async def fetch_source_arc_catalog(
    registry: MetadataSourceRegistry,
    source: MetadataSource,
    external_id: str,
    *,
    source_revision: int,
    timeout: float = 120,
) -> StoryArcCatalogPreview:
    """Fetch all members and parents without opening a database transaction."""
    identifier = source_id(source, MetadataEntityKind.STORY_ARC, external_id)
    runtime = registry.runtime.get(source)
    if (
        type(source_revision) is not int
        or runtime is None
        or runtime.policy.revision != source_revision
    ):
        raise StoryArcCatalogError(
            "source_changed", "Metadata source settings changed; preview the story arc again"
        )
    if not 0 < timeout <= 120:
        raise ValueError("Invalid catalog timeout")
    try:
        async with asyncio.timeout(timeout):
            arc = _required(await registry.story_arc(source, identifier))
            if arc.issue_external_ids is not None and not arc.membership_complete:
                raise StoryArcCatalogError(
                    "incomplete_membership", "Provider membership is incomplete; retry the preview"
                )
            if (
                arc.declared_issue_count is not None
                and arc.declared_issue_count > MAX_CATALOG_MEMBERS
            ):
                raise StoryArcCatalogError(
                    "catalog_limit_exceeded", "This arc exceeds the supported membership limit"
                )
            members: list[ProviderIssueRead] = []
            seen: set[str] = set()
            page = 1
            total: int | None = None
            while True:
                current = _required(await registry.story_arc_issues(source, identifier, page=page))
                if total is None:
                    total = current.total
                if current.total != total or (
                    arc.declared_issue_count is not None and arc.declared_issue_count != total
                ):
                    raise StoryArcCatalogError(
                        "incomplete_membership",
                        "The arc membership count changed; retry the preview",
                    )
                if total > MAX_CATALOG_MEMBERS or current.truncated:
                    raise StoryArcCatalogError(
                        "catalog_limit_exceeded", "This arc exceeds the supported membership limit"
                    )
                for issue in current.results:
                    if issue.external_id in seen:
                        raise StoryArcCatalogError(
                            "identity_conflict",
                            "The arc repeats a member identity; retry the preview",
                        )
                    seen.add(issue.external_id)
                    members.append(issue.model_copy(deep=True))
                if current.next_page is None:
                    break
                page = current.next_page
            if len(members) != total or (
                arc.issue_external_ids is not None
                and arc.issue_external_ids != [row.external_id for row in members]
            ):
                raise StoryArcCatalogError(
                    "incomplete_membership",
                    "The arc membership changed or is incomplete; retry the preview",
                )
            parent_ids = tuple(dict.fromkeys(issue.series_external_id for issue in members))
            if len(parent_ids) > MAX_CATALOG_PARENTS:
                raise StoryArcCatalogError(
                    "catalog_limit_exceeded", "This arc exceeds the supported parent-series limit"
                )
            parents = []
            for parent_id in parent_ids:
                parent = _required(await registry.series(source, parent_id))
                parents.append(parent.model_copy(deep=True))
            return project_source_arc_catalog(
                SourceArcCatalogEvidence(arc.model_copy(deep=True), tuple(members), tuple(parents)),
                source_revision,
            )
    except TimeoutError as exc:
        raise StoryArcCatalogError(
            "source_timeout", "Story arc preview timed out; retry the preview"
        ) from exc


def source_record_identities(
    record: ProviderStoryArcRead | ProviderSeriesRead | ProviderIssueRead,
    kind: MetadataEntityKind,
) -> tuple[ExternalIdentityRef, ...]:
    native = ExternalIdentityRef(record.source.identity_namespace, kind, record.external_id)
    catalog_provider_id(record.external_id, record.source)
    if native.namespace != record.identity_namespace:
        raise StoryArcCatalogError(
            "identity_conflict", "Source metadata uses a different namespace"
        )
    identities = {native.namespace: native}
    for identity in record.cross_identities:
        if identity.entity_kind is not kind or (
            identity.namespace in identities and identities[identity.namespace] != identity
        ):
            raise StoryArcCatalogError(
                "identity_conflict", "Provider cross-identities disagree; review the match"
            )
        if identity.namespace is IdentityNamespace.COMICVINE and int(identity.external_id) >= 2**63:
            raise StoryArcCatalogError(
                "identity_conflict", "ComicVine identity exceeds its supported range"
            )
        identities[identity.namespace] = identity
    return tuple(identities.values())


def project_source_arc_catalog(
    evidence: SourceArcCatalogEvidence, revision: int
) -> StoryArcCatalogPreview:
    """Project without losing the normalized evidence needed by later consumers."""
    arc = evidence.arc
    source_record_identities(arc, MetadataEntityKind.STORY_ARC)
    ids = tuple(issue.external_id for issue in evidence.issues)
    if (
        len(ids) != len(set(ids))
        or len(ids) > MAX_CATALOG_MEMBERS
        or (arc.declared_issue_count is not None and arc.declared_issue_count != len(ids))
        or (
            arc.issue_external_ids is not None
            and (not arc.membership_complete or tuple(arc.issue_external_ids) != ids)
        )
    ):
        raise StoryArcCatalogError(
            "incomplete_membership", "Source evidence does not contain one complete member set"
        )
    parents = {parent.external_id: parent for parent in evidence.series}
    if (
        len(parents) != len(evidence.series)
        or len(parents) > MAX_CATALOG_PARENTS
        or set(parents) != {issue.series_external_id for issue in evidence.issues}
    ):
        raise StoryArcCatalogError(
            "parent_metadata_missing", "Source evidence does not contain the exact parent set"
        )
    issues = []
    numbers: set[tuple[str, str]] = set()
    for issue in evidence.issues:
        source_record_identities(issue, MetadataEntityKind.ISSUE)
        if issue.source is not arc.source:
            raise StoryArcCatalogError("identity_conflict", "Arc members belong to another source")
        try:
            number, exact = parse_issue_number_text(issue.issue_number_text)
        except ValueError as exc:
            raise StoryArcCatalogError(
                "invalid_issue_number", "Arc member designation needs review"
            ) from exc
        key = (issue.series_external_id, exact)
        if key in numbers:
            raise StoryArcCatalogError(
                "identity_conflict", "Two arc members use the same issue designation"
            )
        numbers.add(key)
        issues.append(
            IssueMetadata(
                issue.external_id,
                issue.series_external_id,
                number,
                issue.title,
                issue.description,
                issue.cover_date.isoformat() if issue.cover_date else None,
                issue.store_date.isoformat() if issue.store_date else None,
                issue.image_url,
                issue.page_count,
                issue.resource_url
                if arc.identity_namespace is IdentityNamespace.COMICVINE
                else None,
                issue_number_text=issue.issue_number_text,
            )
        )
    series = []
    for parent in evidence.series:
        source_record_identities(parent, MetadataEntityKind.SERIES)
        if parent.source is not arc.source or not parent.title.strip():
            raise StoryArcCatalogError("identity_conflict", "Arc parents belong to another source")
        series.append(
            SeriesMetadata(
                parent.external_id,
                parent.title,
                parent.sort_title,
                parent.year_start,
                parent.year_end,
                parent.status,
                parent.publisher,
                parent.description,
                parent.image_url,
                parent.issue_count,
                parent.resource_url
                if arc.identity_namespace is IdentityNamespace.COMICVINE
                else None,
            )
        )
    preview = StoryArcCatalogPreview(
        metadata=StoryArcMetadata(
            provider_id=arc.external_id,
            title=arc.title,
            issue_provider_ids=ids,
            description=arc.description,
            publisher=arc.publisher,
            cover_url=arc.image_url,
            comicvine_url=arc.resource_url
            if arc.identity_namespace is IdentityNamespace.COMICVINE
            else None,
            declared_issue_count=len(ids),
            membership_complete=True,
            order_basis=arc.order_basis,
            warnings=tuple(arc.warnings),
        ),
        issues=tuple(issues),
        series=tuple(series),
        fingerprint="",
        source=arc.source,
        source_revision=revision,
        source_evidence=evidence,
    )
    return replace(preview, fingerprint=snapshot_fingerprint(preview))
