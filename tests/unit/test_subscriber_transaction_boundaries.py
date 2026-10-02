"""Transaction-boundary tests for event subscriber side effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pullbox.core.events import SeriesAdded
from pullbox.core.subscribers import _download_covers_for_series
from pullbox.models import Base
from pullbox.models.series import Series

if TYPE_CHECKING:
    from pathlib import Path


class _SessionTracker:
    active_sessions = 0


class _TrackingFactory:
    def __init__(self, maker: async_sessionmaker[AsyncSession]) -> None:
        self._maker = maker

    def __call__(self) -> _TrackingSession:
        return _TrackingSession(self._maker())


class _TrackingSession:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def __aenter__(self) -> AsyncSession:
        _SessionTracker.active_sessions += 1
        return await self._session.__aenter__()

    async def __aexit__(self, *exc_info: object) -> None:
        try:
            await self._session.__aexit__(*exc_info)
        finally:
            _SessionTracker.active_sessions -= 1


@pytest.mark.asyncio
async def test_series_cover_download_happens_outside_database_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        series = Series(
            title="Absolute Wonder Woman",
            sort_title="absolute wonder woman",
            year_start=2024,
            cover_url="https://example.test/series.jpg",
        )
        session.add(series)
        await session.commit()
        series_id = series.id

    async def fake_sleep(_delay: float) -> None:
        return None

    async def fake_api_key(_session: AsyncSession) -> str:
        return "test-key"

    async def fake_covers_dir(_session: AsyncSession) -> Path:
        return tmp_path / ".covers"

    class FakeProvider:
        def __init__(self, **_kwargs: Any) -> None:
            return None

    class FakeMetadataService:
        def __init__(self, **_kwargs: Any) -> None:
            return None

        async def __aenter__(self) -> FakeMetadataService:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def download_cover(self, _url: str, destination: Path) -> bool:
            assert _SessionTracker.active_sessions == 0
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"cover")
            return True

    _SessionTracker.active_sessions = 0
    monkeypatch.setattr("pullbox.core.subscribers.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(
        "pullbox.core.subscribers.get_session_factory", lambda: _TrackingFactory(maker)
    )
    monkeypatch.setattr("pullbox.core.comicvine_key.get_comicvine_api_key", fake_api_key)
    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", fake_covers_dir)
    monkeypatch.setattr("pullbox.providers.metadata.comicvine.ComicVineProvider", FakeProvider)
    monkeypatch.setattr(
        "pullbox.services.provider_artwork.ProviderArtworkClient", FakeMetadataService
    )

    await _download_covers_for_series(SeriesAdded(series_id=series_id, comicvine_id=12345))

    async with maker() as session:
        saved_cover_path = await session.scalar(
            select(Series.cover_path).where(Series.id == series_id)
        )

    assert saved_cover_path == f"/api/v1/series/{series_id}/cover"
    await engine.dispose()
