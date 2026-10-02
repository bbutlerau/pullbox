"""ComicVine Story Arc identifiers at legacy import boundaries."""

from __future__ import annotations


def normalize_comicvine_arc_id(value: object) -> str:
    """Normalize an exact numeric ID or typed 4045 resource ID, never a URL."""
    if type(value) is int:
        if 0 < value < 2**63:
            return str(value)
    elif isinstance(value, str) and len(value) <= 255:
        text = value.strip(" ").removeprefix("4045-")
        if text.isascii() and text.isdecimal():
            normalized = text.lstrip("0")
            if normalized and int(normalized) < 2**63:
                return normalized
    raise ValueError("Invalid ComicVine Story Arc ID")
