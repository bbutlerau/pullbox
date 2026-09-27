"""Shared pre-parse validation for XML handled by Expat."""

from __future__ import annotations

import re
from xml.etree import ElementTree

from defusedxml import ElementTree as DefusedElementTree


class UnsafeXmlError(ValueError):
    """Raised when XML cannot be represented safely for Expat."""


_XML_ENCODING_DECLARATION = re.compile(
    r"(<\?xml\b[^>]{0,256}?\bencoding\s*=\s*)(['\"])UTF-16(?:LE|BE)?\2",
    re.IGNORECASE,
)
_ASCII_UTF16_DECLARATION = re.compile(
    rb"<\?xml\b[^>]{0,256}?\bencoding\s*=\s*(['\"])UTF-16(?:LE|BE)?\1",
    re.IGNORECASE,
)


def _contains_surrogate(value: str) -> bool:
    return any("\ud800" <= character <= "\udfff" for character in value)


def _utf16_encoding(payload: bytes) -> str | None:
    if payload.startswith(
        (
            b"\xff\xfe\x00\x00",
            b"\x00\x00\xfe\xff",
            b"<\x00\x00\x00",
            b"\x00\x00\x00<",
        )
    ):
        return None
    if payload.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"

    little_endian_offset = 0
    while payload[little_endian_offset : little_endian_offset + 2] in {
        b" \x00",
        b"\t\x00",
        b"\n\x00",
        b"\r\x00",
    }:
        little_endian_offset += 2
    if payload[little_endian_offset : little_endian_offset + 2] == b"<\x00":
        return "utf-16-le"

    big_endian_offset = 0
    while payload[big_endian_offset : big_endian_offset + 2] in {
        b"\x00 ",
        b"\x00\t",
        b"\x00\n",
        b"\x00\r",
    }:
        big_endian_offset += 2
    if payload[big_endian_offset : big_endian_offset + 2] == b"\x00<":
        return "utf-16-be"
    return None


def normalize_xml_for_expat(payload: bytes | str) -> bytes | str:
    """Normalize UTF-16 XML strictly before passing it to Expat."""
    if isinstance(payload, str):
        if _contains_surrogate(payload):
            raise UnsafeXmlError("unsafe UTF-16 XML: unpaired surrogate code point")
        return payload

    encoding = _utf16_encoding(payload)
    if encoding is None:
        if _ASCII_UTF16_DECLARATION.search(payload[:512]):
            raise UnsafeXmlError("unsafe UTF-16 XML: declaration has no UTF-16 byte order")
        return payload

    try:
        decoded = payload.decode(encoding, errors="strict")
    except UnicodeDecodeError as exc:
        raise UnsafeXmlError("unsafe UTF-16 XML: malformed code unit sequence") from exc
    if _contains_surrogate(decoded):
        raise UnsafeXmlError("unsafe UTF-16 XML: unpaired surrogate code point")

    normalized = _XML_ENCODING_DECLARATION.sub(
        lambda match: f"{match.group(1)}{match.group(2)}utf-8{match.group(2)}",
        decoded,
        count=1,
    )
    return normalized.encode("utf-8")


def parse_untrusted_xml(payload: bytes | str) -> ElementTree.Element:
    """Parse XML after enforcing the representation boundary."""
    try:
        normalized = normalize_xml_for_expat(payload)
    except UnsafeXmlError as exc:
        raise ElementTree.ParseError(str(exc)) from exc
    return DefusedElementTree.fromstring(normalized)
