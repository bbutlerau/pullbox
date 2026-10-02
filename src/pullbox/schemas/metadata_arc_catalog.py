"""Source-bound catalog decisions, never client-supplied provider metadata."""

from typing import Annotated

from pydantic import BaseModel, Field

from pullbox.schemas.metadata_sources import (
    ProviderIssueRead,
    ProviderSeriesRead,
    ProviderStoryArcRead,
    StoryArcPreviewQuery,
)

_Fingerprint = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$", strict=True)]
_MemberId = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,254}$", strict=True)]


class ArcCatalogSelection(StoryArcPreviewQuery):
    source_revision: int = Field(ge=1, lt=2**63, strict=True)


class ArcCatalogAdd(ArcCatalogSelection):
    fingerprint: _Fingerprint
    file_defaults_fingerprint: _Fingerprint
    ordered_issue_ids: list[_MemberId] = Field(min_length=1, max_length=2000)
    skipped_issue_ids: list[_MemberId] = Field(default_factory=list, max_length=2000)
    library_root_id: int = Field(gt=0, lt=2**63, strict=True)
    monitored: bool = Field(default=False, strict=True)


class ArcCatalogRefresh(ArcCatalogSelection):
    fingerprint: _Fingerprint
    expected_revision: int = Field(ge=1, lt=2**63, strict=True)
    library_root_id: int | None = Field(default=None, gt=0, lt=2**63, strict=True)


class ArcCatalogChanges(BaseModel):
    revision: int
    added_issue_ids: list[str]
    removed_issue_ids: list[str]


class ArcCatalogPreviewRead(ArcCatalogSelection):
    fingerprint: str
    arc: ProviderStoryArcRead
    issues: list[ProviderIssueRead]
    series: list[ProviderSeriesRead]
    order_basis: str
    file_defaults_fingerprint: str | None = None
    file_summary: str | None = None
    changes: ArcCatalogChanges | None = None
