"""Benchmark Library navigation against disposable metadata-only scale fixtures.

The named profiles mirror two real-world support libraries without creating
comic payloads or scanning the filesystem:

* ``ethan``: 650 series and 8,000 registered files.
* ``bigredone``: 20,000 series and 120,000 registered files.

Usage:
  .venv/bin/python scripts/benchmark_library_navigation.py --profile ethan
  .venv/bin/python scripts/benchmark_library_navigation.py --profile bigredone
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi.templating import Jinja2Templates
from sqlalchemy import event, insert, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from pullbox.models import Base
from pullbox.models.library import (
    FileFormat,
    LibraryFile,
    LibraryFileStorageMode,
    LibraryRoot,
    MatchConfidence,
)
from pullbox.models.series import Series
from pullbox.performance.baseline import current_process_peak_rss_bytes
from pullbox.ui.library_routes import (
    build_library_browser_snapshot,
    configure_library_routes,
    load_library_browser_catalog_entries,
    load_library_file_summary,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

NAVIGATION_SCALE_PROFILES: dict[str, tuple[int, int]] = {
    "ethan": (650, 8_000),
    "bigredone": (20_000, 120_000),
}


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _configure_presenters(repo_root: Path) -> None:
    templates = Jinja2Templates(directory=repo_root / "src/pullbox/ui/templates")

    async def load_config_values(
        _session: AsyncSession,
        _keys: Sequence[str],
    ) -> Mapping[str, str]:
        return {}

    def format_localtime(value: date | datetime | None, fmt: str | None = None) -> str:
        return value.strftime(fmt or "%Y-%m-%d") if value is not None else "never"

    configure_library_routes(
        get_templates=lambda: templates,
        build_context=lambda _request, _user=None, **kwargs: dict(kwargs),
        load_system_config_values=load_config_values,
        build_rename_templates=lambda _configs: {},
        resolve_utility_browse_paths=lambda _configs: {},
        format_filesize=lambda value: f"{value} bytes",
        format_localtime=format_localtime,
        dashboard_gauge_offset=lambda value: round(100.0 - (value * 100.0), 2),
    )


async def _seed_navigation_fixture(
    session: AsyncSession,
    *,
    root: Path,
    series_count: int,
    file_count: int,
    insert_batch_size: int,
) -> None:
    await session.execute(
        insert(LibraryRoot),
        [
            {
                "id": 1,
                "name": "Navigation Benchmark",
                "path": str(root),
                "enabled": True,
                "allow_referenced_registrations": True,
                "allow_managed_writes": True,
                "is_default_managed_destination": True,
            }
        ],
    )

    for start in range(1, series_count + 1, insert_batch_size):
        stop = min(start + insert_batch_size, series_count + 1)
        await session.execute(
            insert(Series),
            [
                {
                    "id": series_id,
                    "title": f"Synthetic Series {series_id:06d}",
                    "sort_title": f"synthetic series {series_id:06d}",
                    "path": str(root / f"Series {series_id:06d}"),
                    "library_root_id": 1,
                }
                for series_id in range(start, stop)
            ],
        )

    formats = tuple(FileFormat)
    now = datetime(2026, 9, 26, tzinfo=UTC)
    for start in range(1, file_count + 1, insert_batch_size):
        stop = min(start + insert_batch_size, file_count + 1)
        rows: list[dict[str, object]] = []
        for file_id in range(start, stop):
            series_id = ((file_id - 1) % series_count) + 1
            file_format = formats[(file_id - 1) % len(formats)]
            file_name = f"Issue {file_id:07d}.{file_format.value}"
            rows.append(
                {
                    "id": file_id,
                    "file_path": str(root / f"Series {series_id:06d}" / file_name),
                    "file_name": file_name,
                    "file_size": 25_000_000 + file_id,
                    "file_format": file_format,
                    "file_modified_at": now,
                    "match_confidence": (
                        MatchConfidence.UNMATCHED if file_id % 10 == 0 else MatchConfidence.HIGH
                    ),
                    "naming_snapshot": {},
                    "storage_mode": LibraryFileStorageMode.REFERENCED,
                    "source_signature": {},
                    "library_root_id": 1,
                }
            )
        await session.execute(insert(LibraryFile), rows)
    await session.commit()


def _install_select_counter(
    engine: AsyncEngine,
    counts: dict[str, int],
    phase: list[str],
) -> None:
    def record_statement(*args: object) -> None:
        if str(args[2]).lstrip().upper().startswith("SELECT"):
            counts[phase[0]] = counts.get(phase[0], 0) + 1

    event.listen(engine.sync_engine, "before_cursor_execute", record_statement)


async def _run(args: argparse.Namespace) -> dict[str, object]:
    profile_counts = NAVIGATION_SCALE_PROFILES.get(args.profile)
    if profile_counts is None:
        if args.series_count is None or args.file_count is None:
            raise ValueError("custom profile requires --series-count and --file-count")
        series_count, file_count = args.series_count, args.file_count
    else:
        series_count = args.series_count or profile_counts[0]
        file_count = args.file_count or profile_counts[1]

    total_started = time.monotonic()
    repo_root = Path(__file__).resolve().parents[1]
    _configure_presenters(repo_root)

    with tempfile.TemporaryDirectory(prefix="pullbox-library-navigation-") as tmp:
        fixture_root = Path(tmp)
        library_root_path = fixture_root / "library"
        library_root_path.mkdir()
        database_path = fixture_root / "navigation.db"
        engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        select_counts: dict[str, int] = {}
        phase = ["seed"]
        _install_select_counter(engine, select_counts, phase)
        async with session_factory() as session:
            seed_started = time.monotonic()
            await _seed_navigation_fixture(
                session,
                root=library_root_path,
                series_count=series_count,
                file_count=file_count,
                insert_batch_size=args.insert_batch_size,
            )
            seed_elapsed_ms = round((time.monotonic() - seed_started) * 1000, 2)

            phase[0] = "summary"
            summary_started = time.monotonic()
            (
                total_files,
                matched_files,
                total_size_bytes,
                format_counts,
            ) = await load_library_file_summary(session)
            summary_elapsed_ms = round((time.monotonic() - summary_started) * 1000, 2)

            phase[0] = "setup"
            library_root = (
                await session.execute(select(LibraryRoot).where(LibraryRoot.id == 1))
            ).scalar_one()
            phase[0] = "catalog"
            catalog_started = time.monotonic()
            resolved_paths: dict[str, Path] = {}
            catalog_entries = await load_library_browser_catalog_entries(
                session,
                (library_root,),
                resolved_paths=resolved_paths,
            )
            catalog_elapsed_ms = round((time.monotonic() - catalog_started) * 1000, 2)

            phase[0] = "snapshot"
            snapshot_started = time.monotonic()
            snapshot = await asyncio.to_thread(
                build_library_browser_snapshot,
                library_root_path,
                active_root=library_root_path,
                library_roots=(library_root,),
                series_metrics={},
                catalog_entries=catalog_entries,
                total_size_bytes=total_size_bytes,
                browser_sort="name",
                resolved_catalog_paths=resolved_paths,
            )
            snapshot_elapsed_ms = round((time.monotonic() - snapshot_started) * 1000, 2)

        database_bytes = database_path.stat().st_size
        await engine.dispose()

    return {
        "profile": args.profile,
        "series_count": series_count,
        "file_count": file_count,
        "summary_total_files": total_files,
        "summary_matched_files": matched_files,
        "summary_total_size_bytes": total_size_bytes,
        "summary_format_counts": format_counts,
        "catalog_entry_count": len(catalog_entries),
        "canonical_path_count": len(resolved_paths),
        "browser_row_count": len(snapshot[5]),
        "library_summary_select_count": select_counts.get("summary", 0),
        "catalog_select_count": select_counts.get("catalog", 0),
        "filesystem_scan_count": 0,
        "archive_payload_count": 0,
        "seed_elapsed_ms": seed_elapsed_ms,
        "summary_elapsed_ms": summary_elapsed_ms,
        "catalog_elapsed_ms": catalog_elapsed_ms,
        "snapshot_elapsed_ms": snapshot_elapsed_ms,
        "total_elapsed_ms": round((time.monotonic() - total_started) * 1000, 2),
        "peak_rss_bytes": current_process_peak_rss_bytes(),
        "database_bytes": database_bytes,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=(*NAVIGATION_SCALE_PROFILES, "custom"),
        default="ethan",
    )
    parser.add_argument("--series-count", type=_positive_int)
    parser.add_argument("--file-count", type=_positive_int)
    parser.add_argument("--insert-batch-size", type=_positive_int, default=5_000)
    args = parser.parse_args()
    try:
        report = await _run(args)
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
