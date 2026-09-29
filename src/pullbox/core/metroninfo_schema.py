"""Offline MetronInfo 1.1 validation before paired archive metadata writes."""

from functools import lru_cache
from importlib.resources import files

from xmlschema import XMLResource, XMLSchema11

from pullbox.core.metadata_xml import MetadataXmlError, parse_metadata_xml


@lru_cache(maxsize=1)
def _schema() -> XMLSchema11:
    payload = files("pullbox").joinpath("core/metadata_schemas/MetronInfo-1.1.xsd").read_bytes()
    # The pinned schema bytes retain namespace declarations used by QName attributes.
    return XMLSchema11(
        payload,
        allow="none",
        defuse="always",
        use_fallback=False,
        use_cache=False,
        use_location_hints=False,
    )


def validate_metroninfo_xml(payload: bytes | str) -> None:
    """Reject unsafe or schema-invalid output without exposing its contents."""
    root = parse_metadata_xml(payload, root_name="MetronInfo")
    # xmlschema 4.3's assertion context treats explicit false attributes as true.
    # In this pinned XSD, an absent optional primary flag is equivalent to false.
    # Normalize only valid false lexemes on this private tree; never alter the XML
    # payload, schema, true flags, other attributes, or other validation rules.
    for path in ("IDS/ID", "URLs/URL"):
        for node in root.findall(path):
            flag = node.get("primary")
            if flag is None:
                continue
            normalized = flag.strip(" \t\r\n")
            if normalized not in {"false", "0", "true", "1"}:
                raise MetadataXmlError("invalid_schema")
            if normalized in {"false", "0"}:
                del node.attrib["primary"]
    # Only a bounded, defused tree reaches the validator, never a caller-provided URI.
    resource = XMLResource(root, allow="none", defuse="always")
    if not _schema().is_valid(resource, use_defaults=False, use_location_hints=False):
        raise MetadataXmlError("invalid_schema")
