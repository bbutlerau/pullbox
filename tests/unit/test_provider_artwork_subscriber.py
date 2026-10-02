"""Native source additions use real artwork transport and isolated DB phases."""

from pathlib import Path

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from pullbox.api.v1.covers import get_issue_cover
from pullbox.core import subscribers
from pullbox.core.events import SeriesAdded
from pullbox.models import Issue, Series
from pullbox.services.provider_artwork import ProviderArtworkClient
from tests.unit.test_provider_artwork import cover_bytes
from tests.unit.test_subscriber_transaction_boundaries import _SessionTracker, _TrackingFactory


async def test_native_artwork_round_trip_keeps_exact_issue_covers_separate(
    async_engine,
    tmp_path,
    monkeypatch,
):
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with maker.begin() as session:
        series = Series(
            title="Native",
            sort_title="native",
            cover_url="https://static.metron.cloud/media/series/1.jpg",
        )
        session.add(series)
        await session.flush()
        series_id = series.id
        issues = [
            Issue(
                series_id=series_id,
                issue_number=number,
                issue_number_text=exact,
                cover_url=f"https://static.metron.cloud/media/issue/{exact}.jpg",
            )
            for number, exact in [(13, "13A"), (13, "13B"), (1.25, "1.25"), (1.2, "1.2")]
        ]
        session.add_all(issues)
        await session.flush()
        ids = [issue.id for issue in issues]

    async def forbidden_key(_session):
        pytest.fail("Public artwork must not require a ComicVine credential")

    async def covers(_session):
        return tmp_path

    async def sleep(_delay):
        pass

    calls = []

    def handle(request):
        assert _SessionTracker.active_sessions == 0
        calls.append(request.url.path)
        return httpx.Response(200, content=cover_bytes(), headers={"content-type": "image/jpeg"})

    monkeypatch.setattr("pullbox.core.comicvine_key.get_comicvine_api_key", forbidden_key)
    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", covers)
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: _TrackingFactory(maker))
    monkeypatch.setattr(subscribers.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        "pullbox.services.provider_artwork.ProviderArtworkClient",
        lambda: ProviderArtworkClient(transport=httpx.MockTransport(handle)),
    )
    await subscribers._download_covers_for_series(SeriesAdded(series_id=series_id))
    assert len(calls) == 5
    async with maker() as session:
        saved = list(await session.scalars(select(Issue).order_by(Issue.id)))
        assert all(issue.cover_path == f"/api/v1/issues/{issue.id}/cover" for issue in saved)
        paths = [
            Path((await get_issue_cover(issue_id, object(), session)).path) for issue_id in ids
        ]
    assert len(set(paths)) == 4
    assert all(path.read_bytes() == cover_bytes() for path in paths)


async def test_exact_issue_never_uses_another_issues_numeric_cover(
    async_engine, tmp_path, monkeypatch
):
    maker = async_sessionmaker(async_engine, expire_on_commit=False)

    async def covers(_session):
        return tmp_path

    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", covers)
    async with maker.begin() as session:
        series = Series(title="Exact", sort_title="exact", path=str(tmp_path / "series"))
        session.add(series)
        await session.flush()
        issue = Issue(series_id=series.id, issue_number=13, issue_number_text="13B")
        session.add(issue)
        await session.flush()
        for directory in (Path(series.path), tmp_path / str(series.id)):
            directory.mkdir()
            (directory / "issue_013.jpg").write_bytes(cover_bytes())
        response = await get_issue_cover(issue.id, object(), session)
        assert response.status_code == 404


@pytest.mark.parametrize("change", ["url", "designation", "series", "delete"])
async def test_stale_download_does_not_publish_artwork(async_engine, tmp_path, monkeypatch, change):
    maker = async_sessionmaker(async_engine, expire_on_commit=False)
    async with maker.begin() as session:
        series = Series(title="Native", sort_title="native")
        other = Series(title="Other", sort_title="other")
        session.add_all([series, other])
        await session.flush()
        series_id, other_id = series.id, other.id
        issue = Issue(
            series_id=series_id,
            issue_number=13,
            issue_number_text="13A",
            cover_url="https://metron.cloud/media/a.jpg",
        )
        session.add(issue)
        await session.flush()
        issue_id = issue.id

    async def covers(_session):
        return tmp_path

    async def sleep(_delay):
        pass

    async def handle(request):
        async with maker.begin() as session:
            issue = await session.get(Issue, issue_id)
            if change == "delete":
                await session.delete(issue)
            elif change == "series":
                issue.series_id = other_id
            elif change == "designation":
                issue.issue_number_text = "13B"
            else:
                issue.cover_url = "https://metron.cloud/media/new.jpg"
        return httpx.Response(200, content=cover_bytes(), headers={"content-type": "image/jpeg"})

    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", covers)
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: maker)
    monkeypatch.setattr(subscribers.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        "pullbox.services.provider_artwork.ProviderArtworkClient",
        lambda: ProviderArtworkClient(transport=httpx.MockTransport(handle)),
    )
    await subscribers._download_covers_for_series(SeriesAdded(series_id=series_id))
    assert not list(tmp_path.rglob("*.jpg"))
    async with maker() as session:
        issue = await session.get(Issue, issue_id)
        assert issue is None or issue.cover_path is None
