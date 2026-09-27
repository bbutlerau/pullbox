"""ComicInfo.xml embedded metadata reader for collection import.

Extracts series name, volume year, publisher, and ComicVine ID from
ComicInfo.xml files embedded in CBZ (ZIP) and CBR (RAR) comic archives.
Best-effort: returns None on any failure without raising.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

import structlog
from defusedxml.common import DefusedXmlException

from pullbox.core.comicinfo_sanitizer import scrub_stale_retailer_value
from pullbox.core.rar_backend import configure_rarfile_backend
from pullbox.core.xml_security import parse_untrusted_xml

logger = structlog.get_logger(__name__)

# ComicVine ID extraction patterns (checked in order)
_CVID_PATTERNS = [
    re.compile(r"\[cv_vol_id:(\d+)\]", re.IGNORECASE),
    re.compile(r"\[cvid:(\d+)\]", re.IGNORECASE),
    re.compile(r"\bCVID[:\s]+(\d+)\b", re.IGNORECASE),
    re.compile(r"ComicVine[^\d]*(\d{4,})", re.IGNORECASE),
]


@dataclass
class ComicInfoData:
    """Data extracted from a ComicInfo.xml file.

    All fields optional — absent fields stay None.
    """

    series_name: str | None = None
    volume_year: int | None = None
    publisher: str | None = None
    comicvine_id: int | None = None
    issue_number: str | None = None


def read_comicinfo(archive_path: str | Path) -> ComicInfoData | None:
    """Extract ComicInfo.xml from a comic archive and parse relevant fields.

    Supports .cbz (ZIP) and .cbr (RAR, via rarfile library).
    Returns None if the archive cannot be opened, contains no ComicInfo.xml,
    or the XML is malformed. Never raises.
    """
    path = Path(archive_path)
    suffix = path.suffix.lower()

    xml_content: bytes | None = None

    if suffix == ".cbz":
        xml_content = _read_from_zip(path)
    elif suffix == ".cbr":
        xml_content = _read_from_rar(path)
    else:
        return None

    if not xml_content:
        return None

    return _parse_comicinfo_xml(xml_content, archive_path=str(path))


def _read_from_zip(path: Path) -> bytes | None:
    """Read ComicInfo.xml from a ZIP archive."""
    try:
        with zipfile.ZipFile(path, "r") as zf:
            # Case-insensitive lookup
            names = {n.lower(): n for n in zf.namelist()}
            key = names.get("comicinfo.xml")
            if not key:
                return None
            with zf.open(key) as f:
                return f.read(1024 * 64)
    except Exception:
        logger.debug("comicinfo_read_failed", path=str(path), suffix=".cbz")
        return None


def _read_from_rar(path: Path) -> bytes | None:
    """Read ComicInfo.xml from a RAR archive. Requires rarfile library."""
    try:
        import rarfile  # type: ignore[import-untyped]

        configure_rarfile_backend()
        with rarfile.RarFile(str(path), "r") as rf:
            names: dict[str, str] = {n.lower(): n for n in rf.namelist()}
            key = names.get("comicinfo.xml")
            if not key:
                return None
            with rf.open(key) as f:
                content: bytes = f.read(1024 * 64)
                return content
    except ImportError:
        return None
    except Exception:
        logger.debug("comicinfo_read_failed", path=str(path), suffix=".cbr")
        return None


def _parse_comicinfo_xml(xml_content: bytes | str, archive_path: str) -> ComicInfoData | None:
    """Parse ComicInfo.xml string into ComicInfoData."""
    try:
        root = parse_untrusted_xml(xml_content)
    except (ElementTree.ParseError, DefusedXmlException):
        logger.debug("comicinfo_xml_malformed", path=archive_path)
        return None

    def get_text(tag: str) -> str | None:
        el = root.find(tag)
        return el.text.strip() if el is not None and el.text else None

    series_name = get_text("Series")
    publisher = get_text("Publisher")
    notes_value = scrub_stale_retailer_value(get_text("Notes"))
    notes = notes_value if isinstance(notes_value, str) else None
    issue_number = get_text("Number")

    # Volume field stores series start year
    volume_year: int | None = None
    vol_text = get_text("Volume")
    if vol_text and vol_text.isdigit() and len(vol_text) == 4:
        volume_year = int(vol_text)

    # Extract ComicVine ID from Notes
    comicvine_id: int | None = None
    if notes:
        for pattern in _CVID_PATTERNS:
            m = pattern.search(notes)
            if m:
                comicvine_id = int(m.group(1))
                break

    if not any([series_name, volume_year, publisher, comicvine_id]):
        return None

    return ComicInfoData(
        series_name=series_name,
        volume_year=volume_year,
        publisher=publisher,
        comicvine_id=comicvine_id,
        issue_number=issue_number,
    )
