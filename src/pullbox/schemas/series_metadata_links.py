"""Interactive, series-only provider linking contracts."""

from pydantic import BaseModel, ConfigDict, Field

from pullbox.core.metadata_identity import IdentityNamespace, MetadataSource
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.schemas.metadata_identity_review import IdentityReviewRead
from pullbox.schemas.metadata_sources import ProviderSeriesRead


class SeriesLinkCurrent(BaseModel):
    title: str
    year_start: int | None
    publisher: str | None
    issue_count: int


class SeriesLinkIdentity(BaseModel):
    namespace: IdentityNamespace
    external_id: str
    state: IdentityVerificationState


class SeriesLinkSource(BaseModel):
    source: MetadataSource
    label: str


class SeriesLinksRead(BaseModel):
    current: SeriesLinkCurrent
    identities: list[SeriesLinkIdentity]
    sources: list[SeriesLinkSource]


class SeriesLinkPreview(BaseModel):
    current: SeriesLinkCurrent
    candidate: ProviderSeriesRead
    source_revision: int
    review: IdentityReviewRead


class SeriesLinkConfirm(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: int = Field(gt=0, lt=2**63, strict=True)
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    review_revision: int = Field(gt=0, lt=2**63, strict=True)
    source_revision: int = Field(ge=0, lt=2**63, strict=True)
