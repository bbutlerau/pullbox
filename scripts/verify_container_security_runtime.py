"""Verify security-sensitive libraries embedded in the production runtime."""

from __future__ import annotations

import pyexpat
from importlib.util import find_spec
from xml.etree import ElementTree

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


if __name__ == "__main__":
    main()
