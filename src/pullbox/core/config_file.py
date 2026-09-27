"""Config file provider — persistent host secret storage.

Pullbox treats ``config.xml`` as a narrow host-secret store rather than a
general host/network configuration file. Runtime wiring such as bind address,
port, and filesystem paths come from bootstrap settings, while editable
application settings live in ``system_config``.
"""

from __future__ import annotations

import secrets
import stat
from typing import TYPE_CHECKING
from xml.etree import ElementTree as ET

from defusedxml.common import DefusedXmlException

if TYPE_CHECKING:
    from pathlib import Path

import structlog

from pullbox.config import get_settings
from pullbox.core.secret_validation import WeakApplicationSecretError, validate_application_secret
from pullbox.core.xml_security import parse_untrusted_xml

logger = structlog.get_logger(__name__)


class ConfigFileError(Exception):
    """Raised when config.xml is malformed or unreadable."""


# Keys actively stored in config.xml.
_KEY_MAP: dict[str, str] = {
    "secret_key": "SecretKey",
}
_REVERSE_MAP: dict[str, str] = {v: k for k, v in _KEY_MAP.items()}
CONFIG_XML_KEYS: frozenset[str] = frozenset(_KEY_MAP.keys())


def is_config_xml_key(key: str) -> bool:
    """Check if a setting key belongs in config.xml."""
    return key in CONFIG_XML_KEYS


def _to_pascal(key: str) -> str:
    """Convert a snake_case key to its PascalCase XML element name."""
    return _KEY_MAP.get(key, key)


def _to_snake(pascal: str) -> str:
    """Convert a PascalCase XML element name to its snake_case key."""
    return _REVERSE_MAP.get(pascal, pascal)


class ConfigFileProvider:
    """Reads and writes config.xml — the persistent host secret store."""

    def __init__(self, config_path: Path) -> None:
        """Initialize with path to config.xml."""
        self._config_path = config_path

    @property
    def config_path(self) -> Path:
        """Return the path to config.xml."""
        return self._config_path

    def _read_tree(self) -> ET.Element:
        """Parse config.xml and return the root <Config> element.

        Raises ConfigFileError on malformed XML or missing <Config> root.
        Returns None-like behavior is handled by callers checking file existence.
        """
        try:
            root = parse_untrusted_xml(self._config_path.read_bytes())
        except (ET.ParseError, DefusedXmlException) as exc:
            raise ConfigFileError(f"Malformed config.xml at {self._config_path}: {exc}") from exc

        if root.tag != "Config":
            raise ConfigFileError(
                f"Expected <Config> root element, found <{root.tag}> in {self._config_path}"
            )
        return root

    def get(self, key: str, default: str | None = None) -> str | None:
        """Read a value directly from config.xml (no env var check).

        Returns the default if the key is not found or the file doesn't exist.
        """
        if not self._config_path.exists():
            return default

        root = self._read_tree()
        pascal_key = _to_pascal(key)
        elem = root.find(pascal_key)
        if elem is None:
            return default

        text = elem.text
        if text is None:
            return ""
        return text.strip()

    def resolve(self, key: str) -> str | None:
        """Resolve a config value from the supported source for that key."""
        if key == "secret_key":
            settings_secret = get_settings().secret_key.strip()
            if settings_secret:
                return settings_secret
        return self.get(key)

    def _write_tree(self, root: ET.Element) -> None:
        """Write the XML tree to config.xml with proper formatting and permissions."""
        self._config_path.parent.mkdir(parents=True, exist_ok=True)

        ET.indent(root, space="  ")
        tree = ET.ElementTree(root)
        tree.write(
            self._config_path,
            encoding="utf-8",
            xml_declaration=True,
        )
        # Append trailing newline for cleanliness
        with open(self._config_path, "a", encoding="utf-8") as f:
            f.write("\n")

        # chmod 600 — owner read/write only (contains secret key)
        self._config_path.chmod(stat.S_IRUSR | stat.S_IWUSR)

    def set(self, key: str, value: str) -> None:
        """Write a single key-value pair to config.xml."""
        self.save_dict({key: value})

    def save_dict(self, values: dict[str, str]) -> None:
        """Write multiple key-value pairs to config.xml."""
        if not values:
            return

        # Read existing or create new root
        root = self._read_tree() if self._config_path.exists() else ET.Element("Config")

        for key, value in values.items():
            pascal_key = _to_pascal(key)
            elem = root.find(pascal_key)
            if elem is None:
                elem = ET.SubElement(root, pascal_key)
            elem.text = value

        self._write_tree(root)

    def secret_key(self) -> str:
        """Return the resolved secret key. Raises ConfigFileError if empty/missing."""
        value = self.resolve("secret_key")
        if not value:
            raise ConfigFileError(
                "No secret key found. Ensure config.xml exists with a "
                "<SecretKey> element, or set the bootstrap secret key "
                "environment variable."
            )
        try:
            validate_application_secret(value)
        except WeakApplicationSecretError as exc:
            raise ConfigFileError(str(exc)) from exc
        return value

    def ensure_config_exists(self, db_path: Path | None = None) -> bool:
        """Generate config.xml on first run. Returns True if file was created."""
        if self._config_path.exists():
            return False

        bootstrap_secret = get_settings().secret_key.strip()
        if bootstrap_secret:
            try:
                validate_application_secret(bootstrap_secret)
            except WeakApplicationSecretError as exc:
                raise ConfigFileError(str(exc)) from exc
            secret = bootstrap_secret
            logger.info("config_xml_seed_secret_key", source="bootstrap")
        else:
            secret = secrets.token_hex(64)
            logger.info("config_xml_generated_secret_key")

        self.save_dict({"secret_key": secret})
        logger.info("config_xml_generated", path=str(self._config_path))
        return True

    def all_resolved(self) -> dict[str, str | None]:
        """Return all config.xml keys with their resolved values."""
        return {key: self.resolve(key) for key in CONFIG_XML_KEYS}


# ── Module-level singleton ───────────────────────────────────────────

_provider: ConfigFileProvider | None = None


def init_config_provider(data_dir: Path) -> ConfigFileProvider:
    """Initialize the global ConfigFileProvider. Called once at startup."""
    global _provider
    _provider = ConfigFileProvider(data_dir / "config.xml")
    return _provider


def get_config_provider() -> ConfigFileProvider:
    """Return the initialized ConfigFileProvider.

    Raises RuntimeError if called before init_config_provider().
    """
    if _provider is None:
        raise RuntimeError("ConfigFileProvider not initialized — call init_config_provider() first")
    return _provider
