"""Immutable provider-catalog review contracts; never persisted as new schema."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.services.story_arc_service import StoryArcValidationError

if TYPE_CHECKING:
    from pullbox.models.story_arc import StoryArc
    from pullbox.providers.base import IssueMetadata, SeriesMetadata
    from pullbox.providers.story_arcs import StoryArcMetadata


class StoryArcCatalogError(StoryArcValidationError):
    """A safe, actionable validation error at the catalog boundary."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class StoryArcCatalogPreview:
    """A fully fetched snapshot; no ORM objects or open network work."""

    metadata: StoryArcMetadata
    issues: tuple[IssueMetadata, ...]
    series: tuple[SeriesMetadata, ...]
    fingerprint: str
    source: MetadataSource = MetadataSource.COMICVINE_API
    source_revision: int | None = None

    @property
    def membership_complete(self) -> bool:
        return self.metadata.membership_complete

    @property
    def order_basis(self) -> str:
        return self.metadata.order_basis


@dataclass(frozen=True, slots=True)
class StoryArcCatalogRefreshPreview:
    story_arc_id: int
    revision: int
    added_issue_provider_ids: tuple[str, ...]
    removed_issue_provider_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StoryArcCatalogRefreshResult:
    story_arc: StoryArc
    added_membership_ids: tuple[int, ...]
    removed_issue_provider_ids: tuple[str, ...]


def catalog_snapshot(preview: StoryArcCatalogPreview) -> dict[str, object]:
    """Only provider-public data is stored; no credentials or local file paths."""
    metadata = preview.metadata
    snapshot: dict[str, object] = {
        "provider": preview.source.identity_namespace.value,
        "provider_id": metadata.provider_id,
        "title": metadata.title,
        "description": metadata.description,
        "publisher": metadata.publisher,
        "cover_url": metadata.cover_url,
        "comicvine_url": metadata.comicvine_url,
        "issue_provider_ids": list(metadata.issue_provider_ids),
        "declared_issue_count": metadata.declared_issue_count,
        "membership_complete": metadata.membership_complete,
        "order_basis": metadata.order_basis,
        "warnings": list(metadata.warnings),
        "issues": [asdict(issue) for issue in preview.issues],
        "series": [asdict(series) for series in preview.series],
    }
    if preview.source is not MetadataSource.COMICVINE_API or preview.source_revision is not None:
        snapshot.update(source=preview.source.value, source_revision=preview.source_revision)
    return snapshot


def snapshot_fingerprint(preview: StoryArcCatalogPreview) -> str:
    payload = json.dumps(catalog_snapshot(preview), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def exact_provider_id(value: str) -> int:
    """Accept a canonical positive numeric CV ID, never a fuzzy or coerced key."""
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise StoryArcCatalogError("invalid_identity", "Provider identity must be a positive ID")
    number = int(value)
    if number < 1 or str(number) != value or number > 2**63 - 1:
        raise StoryArcCatalogError("invalid_identity", "Provider identity must be a positive ID")
    return number


def catalog_provider_id(value: str, source: MetadataSource) -> str:
    """Keep native IDs as text; only compatibility CV IDs have an integer bound."""
    try:
        identity = ExternalIdentityRef(
            source.identity_namespace, MetadataEntityKind.STORY_ARC, value
        )
        if identity.external_id != value:
            raise ValueError
        if identity.namespace is IdentityNamespace.COMICVINE:
            exact_provider_id(value)
    except ValueError as exc:
        raise StoryArcCatalogError(
            "invalid_identity", "Provider identity must be a canonical positive ID"
        ) from exc
    return value
