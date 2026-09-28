"""Source-bound Story Arc catalog commands."""

from typing import Annotated, NoReturn

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Request
from sqlalchemy.exc import IntegrityError

from pullbox.api.deps import AuthenticatedUser, DbSession, Settings, get_request_session_factory
from pullbox.api.v1.story_arcs import _load_arc_response
from pullbox.schemas.metadata_arc_catalog import (
    ArcCatalogAdd,
    ArcCatalogPreviewRead,
    ArcCatalogRefresh,
    ArcCatalogSelection,
)
from pullbox.schemas.story_arc import StoryArcResponse
from pullbox.services.metadata_arc_catalog import StoryArcSourceError
from pullbox.services.metadata_arc_commands import (
    ArcCommandResult,
    describe_arc_catalog,
    fetch_current_arc_catalog,
    source_arc_add_transaction,
    source_arc_refresh_transaction,
)
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError
from pullbox.services.story_arc_placement_integration import StoryArcPlacementIntegrationError
from pullbox.services.story_arc_service import StoryArcServiceError

router = APIRouter(prefix="/metadata/story-arcs/catalog", tags=["metadata"])


_ArcId = Annotated[int, Path(gt=0, lt=2**63)]


def _failure(exc: Exception) -> NoReturn:
    detail = {
        "code": "catalog_conflict",
        "message": (
            "The arc was not changed. Review the current source and storage settings, then retry."
        ),
    }
    headers = None
    if isinstance(exc, StoryArcCatalogError):
        detail["code"] = exc.code
        detail["message"] = str(exc)
    if isinstance(exc, StoryArcSourceError):
        detail["source_status"] = exc.source_status.value
        if exc.retry_after_seconds is not None:
            headers = {"Retry-After": str(exc.retry_after_seconds)}
    raise HTTPException(409, detail, headers=headers) from exc


def _schedule(result: ArcCommandResult, request: Request, tasks: BackgroundTasks) -> None:
    # Called only after the owned transaction exits successfully.
    if result.search_on_add:
        from pullbox.tasks.story_arc_search_task import schedule_story_arc_search

        schedule_story_arc_search(result.arc.id)
    if result.initial_placements:
        from pullbox.services.story_arc_catalog_placement import run_catalog_initial_placements

        tasks.add_task(
            run_catalog_initial_placements,
            result.arc.id,
            session_factory=get_request_session_factory(request),
        )


@router.post("/preview", response_model=ArcCatalogPreviewRead)
async def preview_catalog(
    body: ArcCatalogSelection, session: DbSession, _user: AuthenticatedUser, settings: Settings
) -> ArcCatalogPreviewRead:
    try:
        preview = await fetch_current_arc_catalog(
            session, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
        return await describe_arc_catalog(session, preview)
    except (StoryArcServiceError, StoryArcPlacementIntegrationError) as exc:
        _failure(exc)


@router.post("", response_model=StoryArcResponse, status_code=201)
async def add_catalog(
    body: ArcCatalogAdd,
    session: DbSession,
    _user: AuthenticatedUser,
    settings: Settings,
    request: Request,
    background_tasks: BackgroundTasks,
) -> StoryArcResponse:
    try:
        preview = await fetch_current_arc_catalog(
            session, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
        async with source_arc_add_transaction(session, preview, body) as result:
            response = await _load_arc_response(session, result.arc.id)
    except (StoryArcServiceError, StoryArcPlacementIntegrationError, IntegrityError) as exc:
        _failure(exc)
    _schedule(result, request, background_tasks)
    return response


@router.post("/{story_arc_id}/preview", response_model=ArcCatalogPreviewRead)
async def preview_catalog_refresh(
    story_arc_id: _ArcId,
    body: ArcCatalogSelection,
    session: DbSession,
    _user: AuthenticatedUser,
    settings: Settings,
) -> ArcCatalogPreviewRead:
    try:
        preview = await fetch_current_arc_catalog(
            session, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
        return await describe_arc_catalog(session, preview, story_arc_id=story_arc_id)
    except (StoryArcServiceError, StoryArcPlacementIntegrationError) as exc:
        _failure(exc)


@router.post("/{story_arc_id}", response_model=StoryArcResponse)
async def refresh_catalog(
    story_arc_id: _ArcId,
    body: ArcCatalogRefresh,
    session: DbSession,
    _user: AuthenticatedUser,
    settings: Settings,
    request: Request,
    background_tasks: BackgroundTasks,
) -> StoryArcResponse:
    try:
        preview = await fetch_current_arc_catalog(
            session, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
        async with source_arc_refresh_transaction(session, story_arc_id, preview, body) as result:
            response = await _load_arc_response(session, result.arc.id)
    except (StoryArcServiceError, StoryArcPlacementIntegrationError, IntegrityError) as exc:
        _failure(exc)
    _schedule(result, request, background_tasks)
    return response
