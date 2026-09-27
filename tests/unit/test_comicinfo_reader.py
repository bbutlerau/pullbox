"""Unit tests for ComicInfo.xml reader."""

from __future__ import annotations

import io
import zipfile
from typing import TYPE_CHECKING

from pullbox.core.comicinfo_reader import read_comicinfo

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import MonkeyPatch


def _make_cbz(
    path: Path,
    xml_content: bytes | str | None = None,
    filename: str = "ComicInfo.xml",
) -> None:
    """Create a CBZ file with optional ComicInfo.xml."""
    with zipfile.ZipFile(path, "w") as zf:
        if xml_content is not None:
            zf.writestr(filename, xml_content)
        zf.writestr("page001.jpg", b"fake image")


_FULL_XML = """\
<?xml version="1.0"?>
<ComicInfo>
  <Series>Batman</Series>
  <Volume>2016</Volume>
  <Publisher>DC Comics</Publisher>
  <Notes>Tagged with ComicTagger 1.6.0 using ComicVine [cvid:47050]</Notes>
</ComicInfo>
"""


class TestBasicExtraction:
    """Full ComicInfo.xml with all fields."""

    def test_all_fields_extracted(self, tmp_path: Path) -> None:
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, _FULL_XML)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.series_name == "Batman"
        assert result.volume_year == 2016
        assert result.publisher == "DC Comics"
        assert result.comicvine_id == 47050


class TestPartialFields:
    """ComicInfo.xml with only some fields populated."""

    def test_series_only(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Saga</Series></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.series_name == "Saga"
        assert result.volume_year is None
        assert result.publisher is None
        assert result.comicvine_id is None

    def test_publisher_only(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Publisher>Image Comics</Publisher></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.publisher == "Image Comics"


class TestCvIdPatterns:
    """ComicVine ID extraction from Notes field."""

    def test_cvid_bracket_pattern(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Notes>[cvid:12345]</Notes></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.comicvine_id == 12345

    def test_cv_vol_id_bracket_pattern(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Notes>[cv_vol_id:12345] [cv_issue_id:67890]</Notes></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.comicvine_id == 12345

    def test_cvid_colon_pattern(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Notes>CVID: 67890</Notes></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.comicvine_id == 67890

    def test_comicvine_url_pattern(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Notes>ComicVine volume/4050-47050</Notes></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.comicvine_id == 4050 or result.comicvine_id == 47050

    def test_retailer_notes_do_not_supply_comicvine_id(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Batman</Series><Notes>Comixology [cvid:12345]</Notes></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.comicvine_id is None


class TestNoComicInfo:
    """Archives without ComicInfo.xml."""

    def test_missing_comicinfo(self, tmp_path: Path) -> None:
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml_content=None)

        result = read_comicinfo(cbz)

        assert result is None


class TestRarComicInfo:
    """CBR metadata reads should use the central RAR backend."""

    def test_cbr_comicinfo_read_configures_rar_backend(
        self,
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
    ) -> None:
        import rarfile

        from pullbox.core import comicinfo_reader

        cbr = tmp_path / "test.cbr"
        cbr.write_bytes(b"Rar!\x1a\x07\x00fake")
        backend_calls: list[bool] = []

        class FakeRarFile:
            def __init__(self, *_args: object, **_kwargs: object) -> None:
                pass

            def __enter__(self) -> FakeRarFile:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def namelist(self) -> list[str]:
                return ["ComicInfo.xml", "page_001.jpg"]

            def open(self, _name: str):
                return io.BytesIO(b"<ComicInfo><Series>Batman</Series></ComicInfo>")

        monkeypatch.setattr(
            comicinfo_reader,
            "configure_rarfile_backend",
            lambda: backend_calls.append(True),
        )
        monkeypatch.setattr(rarfile, "RarFile", FakeRarFile)

        result = read_comicinfo(cbr)

        assert result is not None
        assert result.series_name == "Batman"
        assert backend_calls == [True]


class TestEmptyFields:
    """ComicInfo.xml with no useful fields."""

    def test_empty_xml(self, tmp_path: Path) -> None:
        xml = "<ComicInfo></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is None


class TestMalformedXml:
    """Malformed XML content."""

    def test_invalid_xml(self, tmp_path: Path) -> None:
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, "<<<not valid xml>>>")

        result = read_comicinfo(cbz)

        assert result is None

    def test_xml_entities_are_rejected(self, tmp_path: Path) -> None:
        cbz = tmp_path / "entity.cbz"
        _make_cbz(
            cbz,
            """\
<!DOCTYPE ComicInfo [
  <!ENTITY injected "Injected Series">
]>
<ComicInfo><Series>&injected;</Series></ComicInfo>
""",
        )

        result = read_comicinfo(cbz)

        assert result is None

    def test_malformed_utf16_surrogate_is_rejected(self, tmp_path: Path) -> None:
        cbz = tmp_path / "malformed-utf16.cbz"
        payload = (
            b"\xff\xfe<\x00C\x00o\x00m\x00i\x00c\x00I\x00n\x00f\x00o\x00>\x00"
            b"\x00\xd8<\x00/\x00C\x00o\x00m\x00i\x00c\x00I\x00n\x00f\x00o\x00>\x00"
        )
        _make_cbz(cbz, payload)

        assert read_comicinfo(cbz) is None

    def test_valid_utf16_comicinfo_remains_supported(self, tmp_path: Path) -> None:
        cbz = tmp_path / "valid-utf16.cbz"
        xml = (
            '<?xml version="1.0" encoding="UTF-16"?>'
            "<ComicInfo><Series>Fritzi Ritz</Series></ComicInfo>"
        )
        _make_cbz(cbz, b"\xff\xfe" + xml.encode("utf-16-le"))

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.series_name == "Fritzi Ritz"


class TestVolumeYear:
    """Volume field parsing."""

    def test_non_four_digit_volume(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Test</Series><Volume>2</Volume></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.volume_year is None

    def test_non_numeric_volume(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Test</Series><Volume>Vol 2</Volume></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.volume_year is None


class TestUnsupportedFormats:
    """Unsupported archive formats."""

    def test_pdf_returns_none(self, tmp_path: Path) -> None:
        pdf = tmp_path / "test.pdf"
        pdf.write_bytes(b"fake pdf")

        result = read_comicinfo(pdf)

        assert result is None

    def test_epub_returns_none(self, tmp_path: Path) -> None:
        epub = tmp_path / "test.epub"
        epub.write_bytes(b"fake epub")

        result = read_comicinfo(epub)

        assert result is None


class TestCorruptedArchive:
    """Corrupted archive file."""

    def test_corrupt_cbz(self, tmp_path: Path) -> None:
        cbz = tmp_path / "test.cbz"
        cbz.write_bytes(b"not a zip file")

        result = read_comicinfo(cbz)

        assert result is None

    def test_corrupt_cbr(self, tmp_path: Path) -> None:
        cbr = tmp_path / "test.cbr"
        cbr.write_bytes(b"not a rar file")

        result = read_comicinfo(cbr)

        assert result is None


class TestCaseInsensitiveFilename:
    """ComicInfo.xml filename is case-insensitive."""

    def test_uppercase_filename(self, tmp_path: Path) -> None:
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, _FULL_XML, filename="COMICINFO.XML")

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.series_name == "Batman"


class TestIssueNumber:
    """Issue number extraction from Number field."""

    def test_issue_number_extracted(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Batman</Series><Number>42</Number></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.issue_number == "42"

    def test_issue_number_absent(self, tmp_path: Path) -> None:
        xml = "<ComicInfo><Series>Batman</Series></ComicInfo>"
        cbz = tmp_path / "test.cbz"
        _make_cbz(cbz, xml)

        result = read_comicinfo(cbz)

        assert result is not None
        assert result.issue_number is None
