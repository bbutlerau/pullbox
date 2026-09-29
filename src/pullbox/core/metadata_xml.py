"""Shared bounded XML envelope for embedded metadata, with no external I/O."""

from __future__ import annotations

import io
import re
from xml.etree import ElementTree

from defusedxml import ElementTree as DefusedElementTree
from defusedxml.common import DefusedXmlException

from pullbox.core.xml_security import UnsafeXmlError, normalize_xml_for_expat

MAX_METADATA_XML_BYTES = 2 * 1024 * 1024
MAX_METADATA_XML_DEPTH = 32
MAX_METADATA_XML_NODES = 4096
MAX_METADATA_XML_ATTRIBUTES = 16


class MetadataXmlError(ValueError):
    """Safe diagnostic code, never the untrusted XML body."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def parse_metadata_xml(payload: bytes | str, *, root_name: str) -> ElementTree.Element:
    if len(payload) > MAX_METADATA_XML_BYTES:
        raise MetadataXmlError("too_large")
    try:
        if isinstance(payload, str) and len(payload.encode("utf-8")) > MAX_METADATA_XML_BYTES:
            raise MetadataXmlError("too_large")
        normalized = normalize_xml_for_expat(payload)
        header = normalized[:512]
        if isinstance(header, bytes):
            header = header.decode("ascii", errors="ignore")
        encoding = re.search(r"<\?xml\b[^>]*\bencoding\s*=\s*['\"]([^'\"]+)", header)
        if encoding and encoding[1].lower() not in {
            "utf-8",
            "utf-16",
            "utf-16le",
            "utf-16be",
            "us-ascii",
        }:
            raise MetadataXmlError("unsupported_encoding")
        stream = io.StringIO(normalized) if isinstance(normalized, str) else io.BytesIO(normalized)
        depth = nodes = 0
        root: ElementTree.Element | None = None
        iterator = DefusedElementTree.iterparse(
            stream,
            events=("start", "end"),
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
        for event, node in iterator:
            if event == "start":
                if root is None:
                    root = node
                depth += 1
                nodes += 1
                if (
                    depth > MAX_METADATA_XML_DEPTH
                    or nodes > MAX_METADATA_XML_NODES
                    or len(node.attrib) > MAX_METADATA_XML_ATTRIBUTES
                ):
                    raise MetadataXmlError("complexity_limit")
                if node.tag.startswith("{http://www.w3.org/2001/XInclude}"):
                    raise MetadataXmlError("unsafe_xml")
            else:
                depth -= 1
    except DefusedXmlException as exc:
        raise MetadataXmlError("unsafe_xml") from exc
    except (ElementTree.ParseError, UnsafeXmlError, UnicodeError, LookupError) as exc:
        raise MetadataXmlError("invalid_xml") from exc
    if root is None or root.tag != root_name:
        raise MetadataXmlError("invalid_root")
    return root
