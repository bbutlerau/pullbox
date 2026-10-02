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
from pullbox.schemas.series_sidecar import (
    SeriesSidecarApproval,
    SeriesSidecarPreview,
    SeriesSidecarWriteRead,
)
from pullbox.services.series_metadata_links import (
    confirm_series_link,
    preview_series_link,
    read_series_links,
)
from pullbox.services.series_sidecar import prepare_series_sidecar, write_series_sidecar

router = APIRouter(prefix="/series", tags=["series"])
LocalId = Annotated[int, Path(gt=0, lt=2**63)]


@router.post("/{series_id}/sidecar/preview", response_model=SeriesSidecarPreview)
async def sidecar_preview(
    series_id: LocalId, session: DbSession, _user: InteractiveOperatorUser
) -> SeriesSidecarPreview:
    try:
        return (await prepare_series_sidecar(session, series_id)).preview
    except (ValueError, ValidationError):
        raise HTTPException(
            409,
            "Series metadata is not ready for file output. Finish metadata syncing "
            "and verify its provider links, then preview again.",
        ) from None


@router.post("/{series_id}/sidecar/write", response_model=SeriesSidecarWriteRead)
async def sidecar_write(
    series_id: LocalId,
    body: SeriesSidecarApproval,
    session: DbSession,
    _user: InteractiveOperatorUser,
) -> SeriesSidecarWriteRead:
    try:
        return await write_series_sidecar(session, series_id, body.review_key)
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from None


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
