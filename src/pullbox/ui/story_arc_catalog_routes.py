"""Compatibility URLs and shared presentation for source-bound Story Arc commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Path, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import ValidationError
from starlette.responses import Response

from pullbox.api.deps import AuthenticatedUser, DbSession  # noqa: TC001
from pullbox.core.issue_numbers import format_issue_number
from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.story_arc import StoryArc, StoryArcLifecycle
from pullbox.schemas.metadata_sources import StoryArcPreviewQuery
from pullbox.services.metadata_arc_commands import saved_arc_source
from pullbox.ui.metadata_arc_search import arc_search_context
from pullbox.ui.story_arc_catalog_forms import StoryArcCatalogAddForm  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastapi.templating import Jinja2Templates

    from pullbox.services.story_arc_catalog import StoryArcCatalogPreview

router = APIRouter()
_get_templates: Callable[[], Jinja2Templates] | None = None
_build_context: Callable[..., dict[str, object]] | None = None
_ProviderId = Annotated[str, Path(pattern=r"^[1-9][0-9]{0,18}$")]


@dataclass(frozen=True)
class _TemplateUser:
    username: str


def configure_story_arc_catalog_routes(
    *, get_templates: Callable[[], Jinja2Templates], build_context: Callable[..., dict[str, object]]
) -> None:
    global _get_templates, _build_context
    _get_templates, _build_context = get_templates, build_context


def _render(request: Request, username: str, template: str, **values: object) -> Response:
    if _get_templates is None or _build_context is None:
        raise RuntimeError("Story Arc catalog routes are not configured")
    context = _build_context(request, _TemplateUser(username), **values)
    return _get_templates().TemplateResponse(request, template, context)


def _redirect(request: Request, url: str) -> Response:
    if request.headers.get("HX-Request"):
        return Response(status_code=204, headers={"HX-Redirect": url})
    # Callers build fixed /story-arcs routes from validated IDs/source enums.
    # codeql[py/url-redirection]
    return RedirectResponse(url, status_code=303)


def _members(preview: StoryArcCatalogPreview) -> list[dict[str, str]]:
    titles = {series.provider_id: series.title for series in preview.series}
    return [
        {
            "provider_id": issue.provider_id,
            "series_name": titles.get(
                issue.series_provider_id, f"Series {issue.series_provider_id}"
            ),
            "issue_number": issue.issue_number_text or format_issue_number(issue.issue_number),
            "title": issue.title or "Untitled issue",
        }
        for issue in preview.issues
    ]


@router.get("/story-arcs/catalog", include_in_schema=False)
async def story_arc_catalog_search(
    request: Request,
    user: AuthenticatedUser,
    session: DbSession,
    q: Annotated[str, Query(max_length=300)] = "",
    page: Annotated[int, Query(ge=1, le=100)] = 1,
) -> Response:
    username = user.username
    context = await arc_search_context(
        request,
        session,
        q=q,
        page=page,
        source=MetadataSource.COMICVINE_API,
        base_url="/story-arcs/catalog",
    )
    return _render(
        request,
        username,
        "partials/story_arc_catalog_results.html"
        if request.headers.get("HX-Request")
        else "pages/story_arc_catalog.html",
        **context,
    )


@router.get("/story-arcs/catalog/{provider_id}", include_in_schema=False)
async def story_arc_catalog_preview(
    provider_id: _ProviderId,
    request: Request,
    user: AuthenticatedUser,
    session: DbSession,
    error: str = Query(""),
) -> Response:
    from pullbox.ui.story_arc_source_routes import source_arc_preview

    if int(provider_id) > 2**63 - 1:
        raise HTTPException(status_code=404, detail="Story Arc provider identity not found")
    return await source_arc_preview(
        MetadataSource.COMICVINE_API, provider_id, request, user, session, error
    )


@router.post("/story-arcs/catalog/{provider_id}", include_in_schema=False)
async def story_arc_catalog_add(
    provider_id: _ProviderId,
    request: Request,
    _user: AuthenticatedUser,
    session: DbSession,
    background_tasks: BackgroundTasks,
    form: Annotated[StoryArcCatalogAddForm, Form()],
) -> Response:
    from pullbox.ui.story_arc_source_routes import SourceArcAddForm, source_arc_add

    if int(provider_id) > 2**63 - 1:
        raise HTTPException(status_code=404, detail="Story Arc provider identity not found")
    try:
        decision = SourceArcAddForm.model_validate(form.model_dump())
    except ValidationError:
        # A pre-upgrade form has no source revision and must not bypass review.
        return _redirect(request, f"/story-arcs/catalog/{provider_id}?error=review")
    return await source_arc_add(
        MetadataSource.COMICVINE_API,
        provider_id,
        request,
        _user,
        session,
        background_tasks,
        decision,
    )


def _refresh_selection(arc: StoryArc) -> StoryArcPreviewQuery | None:
    selected = saved_arc_source(arc)
    if selected is None and arc.comicvine_id is not None:
        selected = StoryArcPreviewQuery(
            source=MetadataSource.COMICVINE_API, external_id=str(arc.comicvine_id)
        )
    return selected


async def _provider_arc(session: DbSession, arc_id: int) -> StoryArc:
    arc = await session.get(StoryArc, arc_id)
    if (
        arc is None
        or _refresh_selection(arc) is None
        or arc.lifecycle is not StoryArcLifecycle.ACTIVE
    ):
        raise HTTPException(status_code=404, detail="Active provider Story Arc not found")
    return arc


def _has_initial_work(arc: StoryArc) -> bool:
    marker = (arc.diagnostics or {}).get("catalog_initial_placements")
    if not isinstance(marker, dict):
        return False
    return any(type(value := marker.get(key)) is int and value > 0 for key in ("pending", "failed"))


@router.post("/story-arcs/{story_arc_id}/initial-placements/retry", include_in_schema=False)
async def story_arc_catalog_initial_placements_retry(
    story_arc_id: int,
    request: Request,
    _user: AuthenticatedUser,
    session: DbSession,
    background_tasks: BackgroundTasks,
) -> Response:
    """Resume only the frozen creation work, never migrate an established policy."""
    from pullbox.services.story_arc_catalog_placement import run_catalog_initial_placements

    arc = await _provider_arc(session, story_arc_id)
    if not _has_initial_work(arc):
        return _redirect(request, f"/story-arcs/{story_arc_id}")
    await session.commit()
    background_tasks.add_task(
        run_catalog_initial_placements,
        story_arc_id,
        retry_failed=True,
        session_factory=request.app.state.db_session_factory,
    )
    return _redirect(request, f"/story-arcs/{story_arc_id}?notice=catalog-placements-started")


@router.get("/story-arcs/{story_arc_id}/catalog-refresh", include_in_schema=False)
async def story_arc_catalog_refresh_preview(
    story_arc_id: int,
    request: Request,
    user: AuthenticatedUser,
    session: DbSession,
    error: str = Query(""),
) -> Response:
    from pullbox.ui.story_arc_source_routes import source_refresh_preview

    username = user.username
    arc = await _provider_arc(session, story_arc_id)
    selection = _refresh_selection(arc)
    assert selection is not None
    return await source_refresh_preview(arc, selection, request, username, session, error)


@router.post("/story-arcs/{story_arc_id}/catalog-refresh", include_in_schema=False)
async def story_arc_catalog_refresh(
    story_arc_id: int,
    request: Request,
    _user: AuthenticatedUser,
    session: DbSession,
    expected_revision: Annotated[int, Form(ge=1)],
    fingerprint: Annotated[str, Form(max_length=128)],
    confirm_refresh: bool = Form(False),
    library_root_id: Annotated[int | None, Form(ge=1)] = None,
    source_revision: Annotated[int | None, Form(ge=1)] = None,
) -> Response:
    from pullbox.ui.story_arc_source_routes import source_refresh

    arc = await _provider_arc(session, story_arc_id)
    selection = _refresh_selection(arc)
    assert selection is not None
    return await source_refresh(
        story_arc_id,
        selection,
        request,
        session,
        source_revision=source_revision,
        expected_revision=expected_revision,
        fingerprint=fingerprint,
        confirm_refresh=confirm_refresh,
        library_root_id=library_root_id,
    )
