"""Interactive review of saved identity claims; no provider or file mutation."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query

from pullbox.api.deps import DbSession, InteractiveOperatorUser
from pullbox.core.metadata_identity import MetadataEntityKind
from pullbox.core.metadata_identity_events import IdentityEventReplayConflictError
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
)
from pullbox.schemas.metadata_identity_review import (
    IdentityClaimRead,
    IdentityReviewRead,
    IdentityReviewReceiptRead,
    IdentityReviewWrite,
)
from pullbox.schemas.pagination import PaginatedResponse
from pullbox.services.metadata_identity_attachment import IdentityAttachmentConflictError
from pullbox.services.metadata_identity_review import (
    IdentityReviewNotFoundError,
    apply_identity_review,
    list_identity_claims,
    preview_identity_review,
)

router = APIRouter(prefix="/metadata-identities", tags=["metadata-identities"])
LocalId = Annotated[int, Path(gt=0, lt=2**63)]


def _error(exc: ValueError) -> HTTPException:
    if isinstance(exc, IdentityReviewNotFoundError):
        return HTTPException(404, str(exc))
    return HTTPException(409, str(exc))


@router.get("/{kind}/{local_id}/claims", response_model=PaginatedResponse[IdentityClaimRead])
async def identity_claims(
    kind: MetadataEntityKind,
    local_id: LocalId,
    session: DbSession,
    _user: InteractiveOperatorUser,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PaginatedResponse[IdentityClaimRead]:
    try:
        items, total = await list_identity_claims(
            session, kind, local_id, limit=limit, offset=offset
        )
    except IdentityAttachmentConflictError as exc:
        raise _error(exc) from exc
    return PaginatedResponse(
        items=[IdentityClaimRead.model_validate(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
        has_more=offset + len(items) < total,
    )


@router.get("/{kind}/{local_id}/review/{event_id}", response_model=IdentityReviewRead)
async def identity_review_preview(
    kind: MetadataEntityKind,
    local_id: LocalId,
    event_id: LocalId,
    session: DbSession,
    _user: InteractiveOperatorUser,
) -> IdentityReviewRead:
    try:
        result = await preview_identity_review(session, kind, local_id, event_id)
    except (IdentityReviewNotFoundError, IdentityAttachmentConflictError) as exc:
        raise _error(exc) from exc
    return IdentityReviewRead.model_validate(result)


@router.post("/{kind}/{local_id}/review/{event_id}", response_model=IdentityReviewReceiptRead)
async def identity_review_apply(
    kind: MetadataEntityKind,
    local_id: LocalId,
    event_id: LocalId,
    body: IdentityReviewWrite,
    session: DbSession,
    user: InteractiveOperatorUser,
) -> IdentityReviewReceiptRead:
    try:
        receipt = await apply_identity_review(
            session,
            kind,
            local_id,
            event_id,
            action=IdentityVerificationAction(body.action),
            fingerprint=body.fingerprint,
            review_revision=body.review_revision,
            actor_user_id=user.id,
        )
    except (
        IdentityReviewNotFoundError,
        IdentityAttachmentConflictError,
        IdentityReviewRequiredError,
        IdentityEventReplayConflictError,
    ) as exc:
        raise _error(exc) from exc
    return IdentityReviewReceiptRead(event_id=receipt.event_id, replayed=receipt.replayed)
