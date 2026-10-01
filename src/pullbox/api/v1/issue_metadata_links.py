"""Operator review and descriptive refresh for existing issues."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path

from pullbox.api.deps import DbSession, InteractiveOperatorUser, Settings
from pullbox.core.exceptions import ValidationError
from pullbox.schemas.issue_metadata_links import (
    IssueCandidatesQuery,
    IssueLinkPreview,
    IssueLinkQuery,
    IssueLinksRead,
    IssueRefreshRead,
)
from pullbox.schemas.metadata_identity_review import IdentityReviewReceiptRead
from pullbox.schemas.metadata_sources import MetadataFetch, MetadataPage, ProviderIssueRead
from pullbox.schemas.series_metadata_links import SeriesLinkConfirm
from pullbox.services.issue_metadata_links import (
    confirm_issue_link,
    issue_link_candidates,
    preview_issue_link,
    read_issue_links,
)
from pullbox.services.metadata_issue_refresh import refresh_issue_from_sources

router = APIRouter(prefix="/issues", tags=["issues"])
LocalId = Annotated[int, Path(gt=0, lt=2**63)]


@router.get("/{issue_id}/metadata-links", response_model=IssueLinksRead)
async def links(
    issue_id: LocalId, session: DbSession, _user: InteractiveOperatorUser, settings: Settings
) -> IssueLinksRead:
    try:
        return await read_issue_links(
            session, issue_id, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post(
    "/{issue_id}/metadata-links/candidates",
    response_model=MetadataFetch[MetadataPage[ProviderIssueRead]],
)
async def candidates(
    issue_id: LocalId,
    body: IssueCandidatesQuery,
    session: DbSession,
    _user: InteractiveOperatorUser,
    settings: Settings,
) -> MetadataFetch[MetadataPage[ProviderIssueRead]]:
    try:
        return await issue_link_candidates(
            session, issue_id, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/{issue_id}/metadata-links/preview", response_model=IssueLinkPreview)
async def preview(
    issue_id: LocalId,
    body: IssueLinkQuery,
    session: DbSession,
    _user: InteractiveOperatorUser,
    settings: Settings,
) -> IssueLinkPreview:
    try:
        return await preview_issue_link(
            session, issue_id, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/{issue_id}/metadata-links/confirm", response_model=IdentityReviewReceiptRead)
async def confirm(
    issue_id: LocalId,
    body: SeriesLinkConfirm,
    session: DbSession,
    user: InteractiveOperatorUser,
    settings: Settings,
) -> IdentityReviewReceiptRead:
    try:
        return await confirm_issue_link(
            session,
            issue_id,
            body,
            user_id=user.id,
            gcd_api_enabled=settings.metadata_gcd_api_v2_enabled,
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/{issue_id}/refresh-metadata", response_model=IssueRefreshRead)
async def refresh(
    issue_id: LocalId, session: DbSession, _user: InteractiveOperatorUser, settings: Settings
) -> IssueRefreshRead:
    try:
        return await refresh_issue_from_sources(
            session, issue_id, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except (ValueError, ValidationError) as exc:
        raise HTTPException(409, str(exc)) from exc
