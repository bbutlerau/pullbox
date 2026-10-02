"""Deletion must select literal paths and retain neighboring registrations."""

import pytest

from pullbox.api.v1.library import _folder_has_tracked_descendants
from pullbox.models import Issue, LibraryFile, LibraryRoot, Series
from pullbox.models.issue import IssueStatus
from pullbox.models.library import LibraryFileStorageMode
from pullbox.services.library_delete_service import (
    LibraryDeleteContext,
    build_delete_context,
    delete_library_entry,
)
from tests.unit.test_library_delete_service import _seed_root, _seed_series_issue_file


@pytest.mark.parametrize("name,neighbor", [("Comics_", "ComicsX"), ("Comics%", "ComicsExtra")])
@pytest.mark.parametrize("referenced", [False, True])
@pytest.mark.parametrize("check_preview", [False, True])
async def test_folder_delete_selects_only_literal_descendants(
    identity_probe_db, tmp_path, name, neighbor, referenced, check_preview
):
    _, factory, _ = identity_probe_db
    target, sibling = tmp_path / name, tmp_path / neighbor
    target.mkdir()
    sibling.mkdir()
    selected_path, untouched_path = target / "issue.cbz", sibling / "issue.cbz"
    selected_path.write_bytes(b"selected")
    untouched_path.write_bytes(b"neighbor")
    async with factory.begin() as session:
        root = await _seed_root(session, tmp_path)
        selected = await _seed_series_issue_file(
            session, root, selected_path, series_path=target / "nested"
        )
        untouched = await _seed_series_issue_file(
            session,
            root,
            untouched_path,
            series_path=sibling / "nested",
            storage_mode=(
                LibraryFileStorageMode.REFERENCED if referenced else LibraryFileStorageMode.MANAGED
            ),
        )
        root_id = root.id
        selected_ids = tuple(row.id for row in selected)
        untouched_ids = tuple(row.id for row in untouched)

    async with factory.begin() as session:
        context = LibraryDeleteContext(mode="folder", trash_enabled=False)
        if check_preview:
            context = await build_delete_context(
                session, target=target, kind="folder", trash_enabled=False
            )
            assert context.tracked_file_count == 1
            assert context.tracked_series_count == 1
            assert context.managed_file_count == 1
            assert context.referenced_file_count == 0
        root = await session.get(LibraryRoot, root_id)
        outcome = await delete_library_entry(
            session, target=target, root=root, kind="folder", delete_context=context
        )
        assert outcome.managed_files_deleted == 1
        assert outcome.referenced_files_detached == 0

    assert not target.exists()
    assert untouched_path.read_bytes() == b"neighbor"
    async with factory() as session:
        assert (await session.get(Series, selected_ids[0])).path is None
        assert (await session.get(Issue, selected_ids[1])).status == IssueStatus.WANTED
        assert await session.get(LibraryFile, selected_ids[2]) is None
        assert (await session.get(Series, untouched_ids[0])).path == str(sibling / "nested")
        assert (await session.get(Issue, untouched_ids[1])).status == IssueStatus.OWNED
        assert (await session.get(LibraryFile, untouched_ids[2])).file_path == str(untouched_path)


@pytest.mark.parametrize("name,neighbor", [("Comics_", "ComicsX"), ("Comics%", "ComicsExtra")])
@pytest.mark.parametrize("with_file", [False, True])
async def test_browser_tracking_does_not_include_wildcard_neighbors(
    identity_probe_db, tmp_path, name, neighbor, with_file
):
    _, factory, _ = identity_probe_db
    target, sibling = tmp_path / name, tmp_path / neighbor
    target.mkdir()
    sibling.mkdir()
    async with factory.begin() as session:
        if with_file:
            root = await _seed_root(session, tmp_path)
            await _seed_series_issue_file(session, root, sibling / "nested" / "issue.cbz")
        else:
            session.add(
                Series(title="Neighbor", sort_title="Neighbor", path=str(sibling / "nested"))
            )
    async with factory() as session:
        assert not await _folder_has_tracked_descendants(session, target)
        assert await _folder_has_tracked_descendants(session, sibling)
