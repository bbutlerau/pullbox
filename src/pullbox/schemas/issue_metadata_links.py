"""Bounded operator commands for an existing issue's metadata providers."""

from datetime import date
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field

from pullbox.core.metadata_identity import MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_identity_review import IdentityReviewRead
from pullbox.schemas.metadata_snapshot import FieldOrigin
from pullbox.schemas.metadata_sources import ProviderIssueRead, SeriesPreviewQuery, SourceOutcome
from pullbox.schemas.series_metadata_links import SeriesLinkIdentity, SeriesLinkSource


class IssueLinkCurrent(BaseModel):
    series_id: int
    series_title: str
    issue_number_text: str
    title: str | None
    cover_date: date | None
    page_count: int | None


class IssueLinkSource(SeriesLinkSource):
    series_external_id: str


class IssueLinksRead(BaseModel):
    current: IssueLinkCurrent
    identities: list[SeriesLinkIdentity]
    sources: list[IssueLinkSource]
    origins: list[FieldOrigin]
    can_refresh: bool
    syncing: bool


class IssueLinkQuery(SeriesPreviewQuery):
    entity_kind: ClassVar[MetadataEntityKind] = MetadataEntityKind.ISSUE


class IssueCandidatesQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: MetadataSource
    page: int = Field(default=1, ge=1, le=10000, strict=True)


class IssueLinkPreview(BaseModel):
    current: IssueLinkCurrent
    candidate: ProviderIssueRead
    source_revision: int
    review: IdentityReviewRead


class IssueRefreshRead(BaseModel):
    issue_id: int
    outcomes: list[SourceOutcome]
