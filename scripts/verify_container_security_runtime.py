"""Verify security-sensitive libraries embedded in the production runtime."""

from __future__ import annotations

import asyncio
import io
import pyexpat
import tempfile
import zipfile
from importlib.util import find_spec
from pathlib import Path
from xml.etree import ElementTree

from PIL import Image

from pullbox.core.xml_security import parse_untrusted_xml

MINIMUM_EXPAT_VERSION = (2, 8, 1)


def verify_expat_version(version: tuple[int, int, int]) -> None:
    """Reject runtimes whose Python XML parser lacks reviewed Expat fixes."""
    if version < MINIMUM_EXPAT_VERSION:
        actual = ".".join(str(part) for part in version)
        required = ".".join(str(part) for part in MINIMUM_EXPAT_VERSION)
        raise SystemExit(f"Expat {required} or newer is required; found {actual}")


def verify_utf16_xml_boundary() -> None:
    """Require malformed UTF-16 to fail before it reaches Expat."""
    malformed = (
        b"\xff\xfe<\x00C\x00o\x00m\x00i\x00c\x00I\x00n\x00f\x00o\x00>\x00"
        b"\x00\xd8<\x00/\x00C\x00o\x00m\x00i\x00c\x00I\x00n\x00f\x00o\x00>\x00"
    )
    try:
        parse_untrusted_xml(malformed)
    except ElementTree.ParseError:
        return
    raise SystemExit("Container must reject malformed UTF-16 XML before Expat parsing")


def verify_paired_pdf_boundary() -> None:
    """Require real bounded Poppler rendering and private paired staging in the image."""
    from pullbox.core.metadata_identity import (
        ExternalIdentityRef,
        IdentityNamespace,
        MetadataEntityKind,
    )
    from pullbox.core.metroninfo import parse_metroninfo
    from pullbox.core.metroninfo_schema import validate_metroninfo_xml
    from pullbox.schemas.metadata_snapshot import MetadataSnapshot, MetadataValues
    from pullbox.utilities.executors.archive_metadata_staging import (
        stage_cbz_metadata_interruptible,
    )

    series = MetadataSnapshot(
        entity_kind=MetadataEntityKind.SERIES,
        identities=(
            ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, "42"),
        ),
        values=MetadataValues(title="Runtime fixture"),
    )
    issue = MetadataSnapshot(
        entity_kind=MetadataEntityKind.ISSUE,
        identities=(
            ExternalIdentityRef(IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE, "7"),
        ),
        values=MetadataValues(issue_number_text="50-x"),
    )

    async def probe(directory: Path) -> None:
        source = directory / "fixture.pdf"
        with (
            Image.new("RGB", (72, 90), "red") as first,
            Image.new("RGB", (72, 90), "blue") as second,
        ):
            first.save(source, format="PDF", save_all=True, append_images=[second])
        original = source.read_bytes()
        async with stage_cbz_metadata_interruptible(
            source, directory, series, issue, max_uncompressed_bytes=2_000_000
        ) as staged:
            staged.check_unchanged()
            with zipfile.ZipFile(staged.path) as archive:
                assert archive.testzip() is None
                assert archive.namelist() == [
                    "page_0000.jpg",
                    "page_0001.jpg",
                    "ComicInfo.xml",
                    "MetronInfo.xml",
                ]
                ci = parse_untrusted_xml(archive.read("ComicInfo.xml"))
                mi_payload = archive.read("MetronInfo.xml")
                validate_metroninfo_xml(mi_payload)
                mi = parse_metroninfo(mi_payload)
                assert ci.findtext("Series") == mi.series == "Runtime fixture"
                assert ci.findtext("Number") == mi.number == "50-x"
                for name, channel in (("page_0000.jpg", 0), ("page_0001.jpg", 2)):
                    with Image.open(io.BytesIO(archive.read(name))) as page:
                        assert page.size == (200, 250)
                        pixel = page.getpixel((100, 100))
                        assert pixel[channel] > 200 and pixel[1] < 20
        assert source.read_bytes() == original
        assert list(directory.iterdir()) == [source]

    with tempfile.TemporaryDirectory(prefix="pullbox-pdf-probe-") as directory:
        asyncio.run(probe(Path(directory)))


def main() -> None:
    """Run all container runtime security assertions."""
    for package in ("safety", "nltk"):
        if find_spec(package) is not None:
            raise SystemExit(f"Development-only package {package} must not ship in production")
    print("Container excludes development-only Safety/NLTK dependencies")
    verify_expat_version(pyexpat.version_info)
    print(f"Container Expat runtime verified: {pyexpat.EXPAT_VERSION}")
    verify_utf16_xml_boundary()
    print("Container malformed UTF-16 XML guard verified")
    verify_paired_pdf_boundary()
    print("Container paired PDF staging and source-preservation boundary verified")


if __name__ == "__main__":
    main()
