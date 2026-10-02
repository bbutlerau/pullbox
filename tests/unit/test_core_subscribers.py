"""Tests for application event subscribers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pullbox.core import subscribers
from pullbox.core.events import DownloadCompleted, DownloadFailed, FileMatched, SeriesAdded
from pullbox.models.download import DownloadClientType, DownloadHistory, DownloadState
from pullbox.models.issue import Issue, IssueStatus
from pullbox.models.library import FileFormat, LibraryFile, LibraryRoot, MatchConfidence
from pullbox.models.search_log import SearchLog, SearchType
from pullbox.models.series import IssueCatalogState, Series, SeriesStatus

if TYPE_CHECKING:
    from pathlib import Path


class _FakeTask:
    def add_done_callback(self, callback: object) -> None:
        if callable(callback):
            callback(self)


async def _seed_series_issue_root(
    factory: async_sessionmaker[AsyncSession],
    root_path: Path,
) -> tuple[int, int, int]:
    async with factory() as session:
        root = LibraryRoot(name="Library", path=str(root_path), enabled=True)
        series = Series(
            title="Subscriber Test",
            sort_title="subscriber test",
            year_start=2026,
            status=SeriesStatus.CONTINUING,
            monitored=True,
        )
        session.add_all([root, series])
        await session.flush()
        issue = Issue(
            series_id=series.id,
            issue_number=1.0,
            status=IssueStatus.DOWNLOADING,
            cover_url="https://example.test/issue-001.jpg",
        )
        session.add(issue)
        await session.commit()
        return root.id, series.id, issue.id


@pytest.mark.asyncio
async def test_download_completed_updates_downloaded_path(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    _root_id, _series_id, issue_id = await _seed_series_issue_root(factory, tmp_path)
    async with factory() as session:
        download = DownloadHistory(
            issue_id=issue_id,
            title="Subscriber Test 001",
            download_url="https://example.test/download",
            download_client=DownloadClientType.SABNZBD,
            state=DownloadState.COMPLETED,
            downloaded_path="/downloads/old.cbz",
        )
        session.add(download)
        await session.commit()
        download_id = download.id

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)

    await subscribers.on_download_completed(
        DownloadCompleted(
            download_id=download_id,
            issue_id=issue_id,
            file_path="/downloads/new.cbz",
        )
    )

    async with factory() as session:
        saved_path = await session.scalar(
            select(DownloadHistory.downloaded_path).where(DownloadHistory.id == download_id)
        )
    assert saved_path == "/downloads/new.cbz"


@pytest.mark.asyncio
async def test_download_failed_reverts_downloading_issue_to_wanted(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    _root_id, _series_id, issue_id = await _seed_series_issue_root(factory, tmp_path)
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)

    await subscribers.on_download_failed(
        DownloadFailed(download_id=123, issue_id=issue_id, error="client failed")
    )

    async with factory() as session:
        status = await session.scalar(select(Issue.status).where(Issue.id == issue_id))
    assert status == IssueStatus.WANTED


@pytest.mark.asyncio
async def test_file_matched_links_library_file_and_marks_high_confidence_issue_owned(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    root_id, _series_id, issue_id = await _seed_series_issue_root(factory, tmp_path)
    file_path = tmp_path / "Subscriber Test 001.cbz"
    file_path.write_bytes(b"comic")
    async with factory() as session:
        library_file = LibraryFile(
            library_root_id=root_id,
            file_path=str(file_path),
            file_name=file_path.name,
            file_size=file_path.stat().st_size,
            file_format=FileFormat.CBZ,
            file_modified_at=datetime.now(tz=UTC),
            match_confidence=MatchConfidence.UNMATCHED,
        )
        session.add(library_file)
        await session.commit()
        library_file_id = library_file.id

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)

    await subscribers.on_file_matched(
        FileMatched(
            library_file_id=library_file_id,
            issue_id=issue_id,
            confidence=MatchConfidence.HIGH,
        )
    )

    async with factory() as session:
        saved_file = await session.get(LibraryFile, library_file_id)
        saved_issue = await session.get(Issue, issue_id)
    assert saved_file is not None
    assert saved_issue is not None
    assert saved_file.issue_id == issue_id
    assert saved_issue.status == IssueStatus.OWNED


@pytest.mark.asyncio
async def test_file_matched_low_confidence_links_file_without_marking_owned(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    root_id, _series_id, issue_id = await _seed_series_issue_root(factory, tmp_path)
    async with factory() as session:
        library_file = LibraryFile(
            library_root_id=root_id,
            file_path=str(tmp_path / "low.cbz"),
            file_name="low.cbz",
            file_size=1,
            file_format=FileFormat.CBZ,
            file_modified_at=datetime.now(tz=UTC),
            match_confidence=MatchConfidence.UNMATCHED,
        )
        session.add(library_file)
        await session.commit()
        library_file_id = library_file.id

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)

    await subscribers.on_file_matched(
        FileMatched(
            library_file_id=library_file_id,
            issue_id=issue_id,
            confidence=MatchConfidence.LOW,
        )
    )

    async with factory() as session:
        saved_file = await session.get(LibraryFile, library_file_id)
        status = await session.scalar(select(Issue.status).where(Issue.id == issue_id))
    assert saved_file is not None
    assert saved_file.issue_id == issue_id
    assert status == IssueStatus.DOWNLOADING


@pytest.mark.asyncio
async def test_series_added_schedules_cover_and_search_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_coroutines: list[str] = []

    def fake_create_task(coro: Any) -> _FakeTask:
        created_coroutines.append(coro.__name__)
        coro.close()
        return _FakeTask()

    async def fake_download(_event: SeriesAdded) -> None:
        return None

    async def fake_search(_event: SeriesAdded) -> None:
        return None

    monkeypatch.setattr(subscribers.asyncio, "create_task", fake_create_task)
    monkeypatch.setattr(subscribers, "_download_covers_for_series", fake_download)
    monkeypatch.setattr(subscribers, "_search_new_series", fake_search)

    await subscribers.on_series_added(SeriesAdded(series_id=1, comicvine_id=2))

    assert created_coroutines == ["fake_download", "fake_search"]
    assert not subscribers._background_tasks


@pytest.mark.asyncio
async def test_series_added_search_retries_until_catalog_is_complete(
    async_engine: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        series = Series(
            title="Delayed Search",
            sort_title="delayed search",
            monitored=True,
            issue_catalog_state=IssueCatalogState.HYDRATING,
        )
        session.add(series)
        await session.flush()
        issue = Issue(
            series_id=series.id,
            issue_number=1.0,
            status=IssueStatus.WANTED,
        )
        session.add(issue)
        await session.commit()
        series_id = series.id

    sleep_calls = 0

    async def fake_sleep(_delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 2:
            async with factory() as session:
                series = await session.get(Series, series_id)
                assert series is not None
                series.issue_catalog_state = IssueCatalogState.COMPLETE
                await session.commit()

    search_mock = AsyncMock(return_value={"wanted": 1, "sent": 0, "queued": 0})
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)
    monkeypatch.setattr(subscribers.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(subscribers, "SEARCH_ON_ADD_MAX_READINESS_ATTEMPTS", 3)
    monkeypatch.setattr("pullbox.tasks.search_task.search_series_issues", search_mock)

    await subscribers._search_new_series(SeriesAdded(series_id=series_id, comicvine_id=123))

    assert sleep_calls == 2
    search_mock.assert_awaited_once_with(series_id)


@pytest.mark.asyncio
async def test_load_recent_search_on_add_misses_finds_unsearched_recent_series(
    async_engine: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        missed = Series(
            title="Missed Search",
            sort_title="missed search",
            monitored=True,
            issue_catalog_state=IssueCatalogState.COMPLETE,
        )
        already_searched = Series(
            title="Already Searched",
            sort_title="already searched",
            monitored=True,
            issue_catalog_state=IssueCatalogState.COMPLETE,
        )
        session.add_all([missed, already_searched])
        await session.flush()
        missed_issue = Issue(
            series_id=missed.id,
            issue_number=1.0,
            status=IssueStatus.WANTED,
        )
        searched_issue = Issue(
            series_id=already_searched.id,
            issue_number=1.0,
            status=IssueStatus.WANTED,
        )
        session.add_all([missed_issue, searched_issue])
        await session.flush()
        session.add(
            SearchLog(
                issue_id=searched_issue.id,
                series_title=already_searched.title,
                issue_number=1.0,
                search_type=SearchType.BULK,
            )
        )
        await session.commit()
        missed_id = missed.id

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)

    recovered = await subscribers.load_recent_search_on_add_misses()

    assert recovered == [missed_id]


@pytest.mark.asyncio
async def test_download_covers_returns_when_series_missing(
    async_engine: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)
    monkeypatch.setattr(subscribers.asyncio, "sleep", AsyncNoop())

    await subscribers._download_covers_for_series(SeriesAdded(series_id=999, comicvine_id=123))


class AsyncNoop:
    async def __call__(self, _delay: float) -> None:
        return None


class BrokenSession:
    async def __aenter__(self) -> BrokenSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def get(self, *_args: object) -> object:
        raise RuntimeError("database failed")

    async def rollback(self) -> None:
        return None


class BrokenSubscriberFactory:
    def __call__(self) -> BrokenSession:
        return BrokenSession()


@pytest.mark.asyncio
async def test_subscriber_database_failures_are_rolled_back_and_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: BrokenSubscriberFactory())

    await subscribers.on_download_completed(
        DownloadCompleted(download_id=1, issue_id=1, file_path="/downloads/file.cbz")
    )
    await subscribers.on_download_failed(DownloadFailed(download_id=1, issue_id=1, error="failed"))
    await subscribers.on_file_matched(
        FileMatched(
            library_file_id=1,
            issue_id=1,
            confidence=MatchConfidence.HIGH,
        )
    )


@pytest.mark.asyncio
async def test_download_covers_handles_load_and_cover_dir_failures(
    async_engine: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    monkeypatch.setattr(subscribers.asyncio, "sleep", AsyncNoop())

    class BrokenFactory:
        def __call__(self) -> BrokenSessionContext:
            return BrokenSessionContext()

    class BrokenSessionContext:
        async def __aenter__(self) -> object:
            raise RuntimeError("db unavailable")

        async def __aexit__(self, *exc_info: object) -> None:
            return None

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: BrokenFactory())
    await subscribers._download_covers_for_series(SeriesAdded(series_id=1, comicvine_id=123))

    async with factory() as session:
        series = Series(title="Broken Covers", sort_title="broken covers", cover_url=None)
        session.add(series)
        await session.commit()
        series_id = series.id

    async def fake_api_key(_session: AsyncSession) -> str:
        return "test-key"

    async def failing_covers_dir(_session: AsyncSession) -> Path:
        raise RuntimeError("covers unavailable")

    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)
    monkeypatch.setattr("pullbox.core.comicvine_key.get_comicvine_api_key", fake_api_key)
    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", failing_covers_dir)

    await subscribers._download_covers_for_series(
        SeriesAdded(series_id=series_id, comicvine_id=123)
    )


@pytest.mark.asyncio
async def test_download_covers_continues_after_series_cover_failure(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        series = Series(
            title="Series Cover Failure",
            sort_title="series cover failure",
            cover_url="https://example.test/series.jpg",
        )
        session.add(series)
        await session.commit()
        series_id = series.id

    async def fake_api_key(_session: AsyncSession) -> str:
        return "test-key"

    async def fake_covers_dir(_session: AsyncSession) -> Path:
        return tmp_path / ".covers"

    class FakeProvider:
        def __init__(self, **_kwargs: object) -> None:
            return None

    class FailingMetadataService:
        def __init__(self, **_kwargs: object) -> None:
            return None

        async def __aenter__(self) -> FailingMetadataService:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def download_cover(self, _url: str, _destination: Path) -> bool:
            raise RuntimeError("cdn failed")

    monkeypatch.setattr(subscribers.asyncio, "sleep", AsyncNoop())
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)
    monkeypatch.setattr("pullbox.core.comicvine_key.get_comicvine_api_key", fake_api_key)
    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", fake_covers_dir)
    monkeypatch.setattr("pullbox.providers.metadata.comicvine.ComicVineProvider", FakeProvider)
    monkeypatch.setattr(
        "pullbox.services.provider_artwork.ProviderArtworkClient",
        FailingMetadataService,
    )

    await subscribers._download_covers_for_series(
        SeriesAdded(series_id=series_id, comicvine_id=123)
    )

    async with factory() as session:
        cover_path = await session.scalar(select(Series.cover_path).where(Series.id == series_id))
    assert cover_path is None


@pytest.mark.asyncio
async def test_download_covers_downloads_issue_covers_and_continues_after_failures(
    async_engine: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = async_sessionmaker(async_engine, expire_on_commit=False)
    async with factory() as session:
        series = Series(
            title="Issue Covers",
            sort_title="issue covers",
            cover_url="https://example.test/series.jpg",
        )
        session.add(series)
        await session.flush()
        issue_one = Issue(
            series_id=series.id,
            issue_number=1.0,
            cover_url="https://example.test/issue-001.jpg",
            status=IssueStatus.WANTED,
        )
        issue_half = Issue(
            series_id=series.id,
            issue_number=1.5,
            cover_url="https://example.test/issue-001.5.jpg",
            status=IssueStatus.WANTED,
        )
        session.add_all([issue_one, issue_half])
        await session.commit()
        series_id = series.id
        issue_one_id = issue_one.id
        issue_half_id = issue_half.id

    downloaded_destinations: list[str] = []

    async def fake_sleep(_delay: float) -> None:
        return None

    async def fake_api_key(_session: AsyncSession) -> str:
        return "test-key"

    async def fake_covers_dir(_session: AsyncSession) -> Path:
        return tmp_path / ".covers"

    class FakeProvider:
        def __init__(self, **_kwargs: object) -> None:
            return None

    class FakeMetadataService:
        def __init__(self, **_kwargs: object) -> None:
            return None

        async def __aenter__(self) -> FakeMetadataService:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def download_cover(self, url: str, destination: Path) -> bool:
            assert destination.name.startswith(".")
            downloaded_destinations.append(destination.name.split(".", 2)[2])
            if "001.5" in url:
                raise RuntimeError("cdn failed")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"cover")
            return True

    monkeypatch.setattr(subscribers.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(subscribers, "get_session_factory", lambda: factory)
    monkeypatch.setattr("pullbox.core.comicvine_key.get_comicvine_api_key", fake_api_key)
    monkeypatch.setattr("pullbox.services.cover_resolver.resolve_covers_dir", fake_covers_dir)
    monkeypatch.setattr("pullbox.providers.metadata.comicvine.ComicVineProvider", FakeProvider)
    monkeypatch.setattr(
        "pullbox.services.provider_artwork.ProviderArtworkClient", FakeMetadataService
    )

    await subscribers._download_covers_for_series(
        SeriesAdded(series_id=series_id, comicvine_id=123)
    )

    async with factory() as session:
        saved_series = await session.get(Series, series_id)
        saved_issue_one = await session.get(Issue, issue_one_id)
        saved_issue_half = await session.get(Issue, issue_half_id)

    assert saved_series is not None
    assert saved_issue_one is not None
    assert saved_issue_half is not None
    assert saved_series.cover_path == f"/api/v1/series/{series_id}/cover"
    assert saved_issue_one.cover_path == f"/api/v1/issues/{issue_one_id}/cover"
    assert saved_issue_half.cover_path is None
    assert downloaded_destinations == ["series.jpg", "issue_001.jpg", "issue_0001.5.jpg"]
