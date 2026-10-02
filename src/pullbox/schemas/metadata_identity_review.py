"""Sanitized saved-claim review contracts."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from pullbox.core.metadata_identity import IdentityNamespace, MetadataEntityKind
from pullbox.core.metadata_identity_state import IdentityVerificationState


class IdentityClaimRead(BaseModel):
    event_id: int
    identity_namespace: IdentityNamespace
    external_id: str
    verification_state: IdentityVerificationState


class IdentityReviewRead(IdentityClaimRead):
    entity_kind: MetadataEntityKind
    local_id: int
    current_external_id: str | None
    owner_local_id: int | None
    review_revision: int
    fingerprint: str


class IdentityReviewWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["confirm", "reject"]
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    review_revision: int = Field(gt=0, lt=2**63, strict=True)


class IdentityReviewReceiptRead(BaseModel):
    event_id: int
    replayed: bool
