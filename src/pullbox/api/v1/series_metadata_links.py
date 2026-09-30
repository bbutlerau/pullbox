"""Operator review of additional identities for an existing series."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path

from pullbox.api.deps import DbSession, InteractiveOperatorUser, Settings
from pullbox.core.exceptions import ValidationError
from pullbox.schemas.metadata_identity_review import IdentityReviewReceiptRead
from pullbox.schemas.metadata_sources import SeriesPreviewQuery
from pullbox.schemas.series_metadata_links import (
    SeriesLinkConfirm,
    SeriesLinkPreview,
    SeriesLinksRead,
)
from pullbox.services.series_metadata_links import (
    confirm_series_link,
    preview_series_link,
    read_series_links,
)

router = APIRouter(prefix="/series", tags=["series"])
LocalId = Annotated[int, Path(gt=0, lt=2**63)]


@router.get("/{series_id}/metadata-links", response_model=SeriesLinksRead)
async def links(
    series_id: LocalId, session: DbSession, _user: InteractiveOperatorUser, settings: Settings
) -> SeriesLinksRead:
    return await read_series_links(
        session, series_id, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
    )


@router.post("/{series_id}/metadata-links/preview", response_model=SeriesLinkPreview)
async def preview(
    series_id: LocalId,
    body: SeriesPreviewQuery,
    session: DbSession,
    _user: InteractiveOperatorUser,
    settings: Settings,
) -> SeriesLinkPreview:
    try:
        return await preview_series_link(
            session, series_id, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/{series_id}/metadata-links/confirm", response_model=IdentityReviewReceiptRead)
async def confirm(
    series_id: LocalId,
    body: SeriesLinkConfirm,
    session: DbSession,
    user: InteractiveOperatorUser,
    settings: Settings,
) -> IdentityReviewReceiptRead:
    try:
        return await confirm_series_link(
            session,
            series_id,
            body,
            user_id=user.id,
            gcd_api_enabled=settings.metadata_gcd_api_v2_enabled,
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc
