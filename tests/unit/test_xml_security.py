"""Security regressions for XML normalization before Expat parsing."""

from __future__ import annotations

import pytest

from pullbox.core.xml_security import normalize_xml_for_expat


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be"])
def test_normalize_xml_for_expat_preserves_valid_utf16(encoding: str) -> None:
    declaration = '<?xml version="1.0" encoding="UTF-16"?>'
    xml_text = f"{declaration}<ComicInfo><Series>Fritzi Ritz</Series></ComicInfo>"
    bom = b"\xff\xfe" if encoding == "utf-16-le" else b"\xfe\xff"

    normalized = normalize_xml_for_expat(bom + xml_text.encode(encoding))

    assert isinstance(normalized, bytes)
    assert normalized.decode("utf-8").endswith(
        "<ComicInfo><Series>Fritzi Ritz</Series></ComicInfo>"
    )
    assert b'encoding="utf-8"' in normalized


@pytest.mark.parametrize(
    "payload",
    [
        b"\xff\xfe<\x00?\x00x\x00m\x00l\x00>\x00\x00\xd8<\x00/\x00?\x00x\x00m\x00l\x00>\x00",
        "<ComicInfo>\ud800</ComicInfo>",
        b'<?xml version="1.0" encoding="UTF-16"?><ComicInfo />',
    ],
)
def test_normalize_xml_for_expat_rejects_unsafe_utf16(payload: bytes | str) -> None:
    with pytest.raises(ValueError, match="unsafe UTF-16 XML"):
        normalize_xml_for_expat(payload)


def test_normalize_xml_for_expat_leaves_utf8_bytes_unchanged() -> None:
    payload = b'<?xml version="1.0" encoding="UTF-8"?><ComicInfo />'

    assert normalize_xml_for_expat(payload) == payload


def test_normalize_xml_for_expat_detects_utf16_after_leading_whitespace() -> None:
    payload = "\n\t<ComicInfo><Series>Saga</Series></ComicInfo>".encode("utf-16-le")

    normalized = normalize_xml_for_expat(payload)

    assert isinstance(normalized, bytes)
    assert normalized.decode("utf-8") == "\n\t<ComicInfo><Series>Saga</Series></ComicInfo>"


def test_normalize_xml_for_expat_rejects_unpaired_surrogate_after_utf16_whitespace() -> None:
    payload = b"\n\x00<\x00C\x00o\x00m\x00i\x00c\x00I\x00n\x00f\x00o\x00>\x00\x00\xd8"

    with pytest.raises(ValueError, match="unsafe UTF-16 XML"):
        normalize_xml_for_expat(payload)


def test_normalize_xml_for_expat_does_not_misclassify_utf32() -> None:
    payload = b"\xff\xfe\x00\x00" + "<ComicInfo />".encode("utf-32-le")

    assert normalize_xml_for_expat(payload) == payload
