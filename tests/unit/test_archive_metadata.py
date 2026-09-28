"""Synthetic paired-metadata probes, independent of matching and live providers."""

from __future__ import annotations

import io
import random
import tarfile
import zipfile
from stat import S_IFLNK
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

import py7zr
import pytest

from pullbox.core.archive import ArchiveError, ArchiveReader, ArchiveResourceLimitError
from pullbox.core.archive_metadata import MAX_METADATA_BYTES, MetadataReadDiagnostic
from pullbox.core.comicinfo import parse_comicinfo
from pullbox.core.metroninfo import parse_metroninfo

if TYPE_CHECKING:
    from pathlib import Path

COMICINFO = b"<ComicInfo><Series>Harbor Lights</Series><Number>13a</Number></ComicInfo>"
METRONINFO = b'<MetronInfo><IDS><ID source="Metron">101</ID></IDS><Number>13a</Number></MetronInfo>'


def _archive(path: Path, entries: list[tuple[str, bytes]]) -> Path:
    if path.suffix in {".cbz", ".cbr"}:
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in entries:
                archive.writestr(name, payload)
    elif path.suffix == ".cbt":
        with tarfile.open(path, "w") as archive:
            for name, payload in entries:
                member = tarfile.TarInfo(name)
                member.size = len(payload)
                archive.addfile(member, io.BytesIO(payload))
    else:
        with py7zr.SevenZipFile(path, "w") as archive:
            for name, payload in entries:
                archive.writestr(payload, name)
    return path


@pytest.mark.parametrize("suffix", [".cbz", ".cbr", ".cbt", ".cb7"])
@pytest.mark.parametrize("nested", [False, True])
def test_reads_both_metadata_files_without_changing_archive(
    tmp_path: Path, suffix: str, nested: bool
) -> None:
    prefix = "metadata/" if nested else ""
    path = _archive(
        tmp_path / f"issue{suffix}",
        [
            (prefix + "cOmIcInFo.XmL", COMICINFO),
            (prefix + "mEtRoNiNfO.xMl", METRONINFO),
            ("pages/001.jpg", b"synthetic page"),
        ],
    )
    original = path.read_bytes()
    before = set(tmp_path.rglob("*"))
    result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.payload == COMICINFO and result.metroninfo.payload == METRONINFO
    assert result.comicinfo.entry_count == result.metroninfo.entry_count == 1
    assert result.comicinfo.diagnostics == result.metroninfo.diagnostics == ()
    assert path.read_bytes() == original and set(tmp_path.rglob("*")) == before


@pytest.mark.parametrize(
    "suffix,backend",
    [
        (".cbz", "zipfile.ZipFile"),
        (".cbt", "tarfile.open"),
        (".cb7", "py7zr.SevenZipFile"),
    ],
)
def test_probe_opens_archive_once(tmp_path: Path, suffix: str, backend: str) -> None:
    path = _archive(
        tmp_path / f"issue{suffix}",
        [
            ("ComicInfo.xml", COMICINFO),
            ("MetronInfo.xml", METRONINFO),
        ],
    )
    module, name = backend.split(".")
    original = getattr({"zipfile": zipfile, "tarfile": tarfile, "py7zr": py7zr}[module], name)
    with patch(backend, wraps=original) as opened:
        result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload == METRONINFO
    assert opened.call_count == 1


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7"])
def test_missing_optional_metadata_is_not_a_failure(tmp_path: Path, suffix: str) -> None:
    result = ArchiveReader(
        _archive(tmp_path / f"issue{suffix}", [("001.jpg", b"page")])
    ).read_metadata_files()
    assert result.comicinfo.payload is result.metroninfo.payload is None
    assert result.comicinfo.entry_count == result.metroninfo.entry_count == 0
    assert result.comicinfo.diagnostics == result.metroninfo.diagnostics == ()


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7"])
def test_duplicate_metadata_selects_root_deterministically_and_reports_ambiguity(
    tmp_path: Path, suffix: str
) -> None:
    path = _archive(
        tmp_path / f"issue{suffix}",
        [
            ("nested/MetronInfo.xml", b"other"),
            ("MetronInfo.xml", METRONINFO),
            ("ComicInfo.xml", COMICINFO),
        ],
    )
    result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload == METRONINFO
    assert result.metroninfo.entry_count == 2
    assert result.metroninfo.diagnostics == (MetadataReadDiagnostic.DUPLICATE_ENTRIES,)


@pytest.mark.parametrize("suffix", [".cbz", ".cbt"])
def test_exact_duplicate_names_read_first_member_not_backend_last_wins(
    tmp_path: Path, suffix: str
) -> None:
    if suffix == ".cbz":
        with pytest.warns(UserWarning, match="Duplicate name"):
            path = _archive(
                tmp_path / f"issue{suffix}",
                [("MetronInfo.xml", METRONINFO), ("MetronInfo.xml", b"other")],
            )
    else:
        path = _archive(
            tmp_path / f"issue{suffix}",
            [("MetronInfo.xml", METRONINFO), ("MetronInfo.xml", b"other")],
        )
    result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload == METRONINFO
    assert MetadataReadDiagnostic.DUPLICATE_ENTRIES in result.metroninfo.diagnostics


@pytest.mark.parametrize(
    "name", ["../MetronInfo.xml", "/MetronInfo.xml", r"C:\MetronInfo.xml", r"..\MetronInfo.xml"]
)
def test_unsafe_metadata_member_is_never_read(tmp_path: Path, name: str) -> None:
    path = _archive(tmp_path / "unsafe.cbz", [(name, METRONINFO), ("ComicInfo.xml", COMICINFO)])
    original = zipfile.ZipFile.open
    reads: list[str] = []

    def tracked(
        archive: zipfile.ZipFile, member: object, *args: object, **kwargs: object
    ) -> object:
        reads.append(member.filename if isinstance(member, zipfile.ZipInfo) else str(member))
        return original(archive, member, *args, **kwargs)

    with patch("zipfile.ZipFile.open", tracked):
        result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.payload == COMICINFO
    assert result.metroninfo.payload is None
    assert MetadataReadDiagnostic.UNSAFE_ENTRY in result.metroninfo.diagnostics
    assert reads == ["ComicInfo.xml"]


@pytest.mark.parametrize("suffix", [".cbz", ".cbt"])
def test_metadata_links_are_not_followed(tmp_path: Path, suffix: str) -> None:
    path = tmp_path / f"link{suffix}"
    if suffix == ".cbz":
        with zipfile.ZipFile(path, "w") as archive:
            member = zipfile.ZipInfo("MetronInfo.xml")
            member.create_system = 3
            member.external_attr = (S_IFLNK | 0o777) << 16
            archive.writestr(member, "private-secret")
    else:
        with tarfile.open(path, "w") as archive:
            member = tarfile.TarInfo("MetronInfo.xml")
            member.type = tarfile.SYMTYPE
            member.linkname = "private-secret"
            archive.addfile(member)
    result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload is None
    assert result.metroninfo.diagnostics == (MetadataReadDiagnostic.UNSAFE_ENTRY,)
    assert "private-secret" not in repr(result)


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7"])
def test_oversized_metadata_does_not_discard_the_other_file(tmp_path: Path, suffix: str) -> None:
    path = _archive(
        tmp_path / f"large{suffix}",
        [
            ("ComicInfo.xml", COMICINFO),
            ("MetronInfo.xml", b"x" * (MAX_METADATA_BYTES + 1)),
        ],
    )
    result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.payload == COMICINFO
    assert result.metroninfo.payload is None
    assert result.metroninfo.diagnostics == (MetadataReadDiagnostic.SIZE_LIMIT,)


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7"])
def test_malformed_optional_xml_does_not_hide_other_metadata(tmp_path: Path, suffix: str) -> None:
    path = _archive(
        tmp_path / f"bad-xml{suffix}",
        [("ComicInfo.xml", COMICINFO), ("MetronInfo.xml", b"<broken>")],
    )
    result = ArchiveReader(path).read_metadata_files()
    assert parse_comicinfo(result.comicinfo.payload).series == "Harbor Lights"
    assert parse_metroninfo(result.metroninfo.payload).diagnostics


def test_cb7_uses_bounded_memory_factory_not_disk_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _archive(
        tmp_path / "memory.cb7", [("ComicInfo.xml", COMICINFO), ("MetronInfo.xml", METRONINFO)]
    )
    original = py7zr.SevenZipFile.extract
    factories: list[object] = []

    def tracked(archive: py7zr.SevenZipFile, *args: object, **kwargs: object) -> None:
        assert kwargs.get("factory") is not None and kwargs.get("path") is None
        assert kwargs.get("recursive") is False
        factories.append(kwargs["factory"])
        original(archive, *args, **kwargs)

    monkeypatch.setattr(py7zr.SevenZipFile, "extract", tracked)
    assert ArchiveReader(path).read_metadata_files().metroninfo.payload == METRONINFO
    assert len(factories) == 1


def test_solid_scan_budget_prevents_decompressing_large_cb7_for_tiny_metadata(
    tmp_path: Path,
) -> None:
    path = _archive(
        tmp_path / "solid.cb7", [("page.jpg", b"x" * 4096), ("MetronInfo.xml", METRONINFO)]
    )
    with patch("py7zr.SevenZipFile.extract") as extract:
        result = ArchiveReader(path).read_metadata_files(max_solid_scan_bytes=1024)
    extract.assert_not_called()
    assert result.metroninfo.diagnostics == (MetadataReadDiagnostic.SOLID_SCAN_LIMIT,)


@pytest.mark.parametrize("limit", [0, -1, True])
def test_invalid_scan_budget_is_rejected_before_open(tmp_path: Path, limit: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        ArchiveReader(tmp_path / "missing.cbz").read_metadata_files(max_solid_scan_bytes=limit)


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7", ".cbr", ".pdf"])
def test_unreadable_archive_has_safe_error(tmp_path: Path, suffix: str) -> None:
    path = tmp_path / f"secret{suffix}"
    path.write_bytes(b"broken")
    with pytest.raises(ArchiveError) as error:
        ArchiveReader(path).read_metadata_files()
    assert "secret" not in str(error.value) and str(tmp_path) not in str(error.value)


def test_rar_configures_backend_once_and_streams_members_without_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rarfile

    from pullbox.core import rar_backend

    path = tmp_path / "real.cbr"
    path.write_bytes(b"Rar!\x1a\x07\x00fake")
    calls: list[str] = []
    payloads = {"ComicInfo.xml": COMICINFO, "MetronInfo.xml": METRONINFO}

    class FakeRar:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            calls.append("open")

        def __enter__(self) -> FakeRar:
            return self

        def __exit__(self, *_args: object) -> None:
            calls.append("close")

        def infolist(self) -> list[SimpleNamespace]:
            return [
                SimpleNamespace(
                    filename=name,
                    file_size=len(data),
                    compress_size=len(data),
                    is_file=lambda: True,
                    is_symlink=lambda: False,
                )
                for name, data in payloads.items()
            ]

        def is_solid(self) -> bool:
            return False

        def open(self, info: SimpleNamespace, *_args: object) -> io.BytesIO:
            calls.append(info.filename)
            return io.BytesIO(payloads[info.filename])

    monkeypatch.setattr(rar_backend, "configure_rarfile_backend", lambda: calls.append("configure"))
    monkeypatch.setattr(rarfile, "RarFile", FakeRar)
    result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.payload == COMICINFO and result.metroninfo.payload == METRONINFO
    assert calls == ["configure", "open", "ComicInfo.xml", "MetronInfo.xml", "close"]


def test_cb7_packed_block_size_is_not_the_metadata_member_compressed_size(tmp_path: Path) -> None:
    path = _archive(
        tmp_path / "large-block.cb7",
        [
            ("MetronInfo.xml", METRONINFO),
            ("page.jpg", random.Random(1234).randbytes(MAX_METADATA_BYTES + 1024)),
        ],
    )
    result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload == METRONINFO and not result.metroninfo.diagnostics


def test_cb7_exact_duplicate_names_are_reported_without_name_based_overwrite(
    tmp_path: Path,
) -> None:
    path = _archive(
        tmp_path / "duplicates.cb7",
        [
            ("MetronInfo.xml", METRONINFO),
            ("MetronInfo.xml", b"different"),
            ("ComicInfo.xml", COMICINFO),
        ],
    )
    result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.payload == COMICINFO
    assert result.metroninfo.payload is None
    assert result.metroninfo.diagnostics == (
        MetadataReadDiagnostic.DUPLICATE_ENTRIES,
        MetadataReadDiagnostic.UNREADABLE,
    )


@pytest.mark.parametrize("suffix", [".cbz", ".cbt", ".cb7"])
def test_relative_current_directory_metadata_is_read(tmp_path: Path, suffix: str) -> None:
    path = _archive(tmp_path / f"relative{suffix}", [("./MetronInfo.xml", METRONINFO)])
    assert ArchiveReader(path).read_metadata_files().metroninfo.payload == METRONINFO


@pytest.mark.parametrize(
    "payload,diagnostic",
    [
        (b"x", MetadataReadDiagnostic.UNREADABLE),
        (b"x" * (MAX_METADATA_BYTES + 1), MetadataReadDiagnostic.SIZE_LIMIT),
    ],
    ids=["truncated", "oversized"],
)
def test_zip_actual_stream_length_is_bounded_and_checked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
    diagnostic: MetadataReadDiagnostic,
) -> None:
    path = _archive(tmp_path / "length.cbz", [("MetronInfo.xml", METRONINFO)])
    reads: list[int] = []

    class FakeStream(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            reads.append(size)
            return super().read(size)

    monkeypatch.setattr(zipfile.ZipFile, "open", lambda *args, **kwargs: FakeStream(payload))
    result = ArchiveReader(path).read_metadata_files()
    assert result.metroninfo.payload is None and diagnostic in result.metroninfo.diagnostics
    assert reads == [MAX_METADATA_BYTES + 1]


def test_unreadable_member_does_not_erase_other_document_or_echo_error(tmp_path: Path) -> None:
    path = _archive(
        tmp_path / "errors.cbz", [("ComicInfo.xml", COMICINFO), ("MetronInfo.xml", METRONINFO)]
    )
    original = zipfile.ZipFile.open

    def fail_one(
        archive: zipfile.ZipFile, member: zipfile.ZipInfo, *args: object, **kwargs: object
    ) -> object:
        if member.filename == "ComicInfo.xml":
            raise RuntimeError("private-token-and-source-path")
        return original(archive, member, *args, **kwargs)

    with patch("zipfile.ZipFile.open", fail_one):
        result = ArchiveReader(path).read_metadata_files()
    assert result.comicinfo.diagnostics == (MetadataReadDiagnostic.UNREADABLE,)
    assert result.metroninfo.payload == METRONINFO
    assert "private-token" not in repr(result)


def test_metadata_reader_does_not_swallow_cancellation(tmp_path: Path) -> None:
    path = _archive(tmp_path / "cancel.cbz", [("MetronInfo.xml", METRONINFO)])
    with (
        patch("zipfile.ZipFile.open", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        ArchiveReader(path).read_metadata_files()


def test_open_zip_probe_reuses_the_callers_archive(tmp_path: Path) -> None:
    from pullbox.core.archive_metadata import read_open_zip_metadata

    path = _archive(
        tmp_path / "reuse.cbz", [("ComicInfo.xml", COMICINFO), ("MetronInfo.xml", METRONINFO)]
    )
    with (
        zipfile.ZipFile(path) as archive,
        patch("zipfile.ZipFile", side_effect=AssertionError("reopened")),
    ):
        assert read_open_zip_metadata(archive).metroninfo.payload == METRONINFO


def test_memory_factory_rejects_unknown_targets_and_growth_before_allocation() -> None:
    from pullbox.core.archive_metadata import _MetadataFactory

    factory = _MetadataFactory(["MetronInfo.xml"])
    with pytest.raises(ArchiveError, match="Unexpected"):
        factory.create("page.jpg")
    writer = factory.create("MetronInfo.xml")
    writer.write(b"ok")
    with pytest.raises(ArchiveResourceLimitError):
        writer.write(b"x" * MAX_METADATA_BYTES)
    assert writer.size() == 2
    with pytest.raises(ArchiveResourceLimitError):
        writer.seek(MAX_METADATA_BYTES + 1)
    writer.seek(0)
    assert writer.read() == b"ok"
