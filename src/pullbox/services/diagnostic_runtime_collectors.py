"""Runtime and bootstrap collectors for diagnostic packages."""

from __future__ import annotations

import os
import platform
import resource
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog
from defusedxml import ElementTree as DefusedET

from pullbox.core.xml_security import parse_untrusted_xml
from pullbox.services.diagnostic_sanitizer import REDACTED, coerce_json_safe, redact_value

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


async def collect_system_info() -> dict[str, object]:
    """Gather system-level information."""
    import pullbox

    return {
        "pullbox_version": pullbox.__version__,
        "python_version": (
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        ),
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "os": platform.system(),
        "cpu_count": os.cpu_count(),
        "is_docker": os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"),
        "collected_at": datetime.now(UTC).isoformat(),
    }


async def collect_bootstrap_settings() -> dict[str, object]:
    """Return sanitized environment/bootstrap settings in effect at runtime."""
    from pullbox.config import get_settings

    settings = get_settings()
    return coerce_json_safe(settings.model_dump(mode="python"))  # type: ignore[return-value]


def collect_container_runtime() -> dict[str, object]:
    """Collect lightweight runtime/container metadata helpful for support."""
    from pullbox.config import get_settings

    settings = get_settings()
    db_path: str | None = None
    if ":///" in settings.db_url:
        db_path = settings.db_url.split(":///", 1)[1]

    hostname = os.environ.get("HOSTNAME") or platform.node()
    image_env = {
        key: os.environ[key]
        for key in (
            "PULLBOX_IMAGE",
            "PULLBOX_IMAGE_NAME",
            "PULLBOX_IMAGE_TAG",
            "IMAGE_NAME",
            "IMAGE_TAG",
        )
        if key in os.environ
    }

    return {
        "hostname": hostname,
        "container_id_guess": hostname if hostname and len(hostname) >= 12 else None,
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "argv": [coerce_json_safe(arg) for arg in sys.argv],
        "timezone": os.environ.get("TZ"),
        "is_docker": os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"),
        "uid": getattr(os, "getuid", lambda: None)(),
        "gid": getattr(os, "getgid", lambda: None)(),
        "db_path": db_path,
        "config_xml_path": str(settings.data_dir / "config.xml"),
        "mount_paths": coerce_json_safe(
            {
                "data_dir": settings.data_dir,
                "library_root": settings.library_root,
                "covers_dir": settings.covers_dir,
                "logs_dir": settings.logs_dir,
                "temp_dir": settings.temp_dir,
                "backup_dir": settings.backup_dir,
            }
        ),
        "image_env": image_env,
    }


def collect_config_xml_snapshot() -> tuple[str, bytes] | None:
    """Return a redacted config.xml snapshot when available."""
    from pullbox.config import get_settings

    settings = get_settings()
    config_path = settings.data_dir / "config.xml"
    if not config_path.is_file():
        return None

    try:
        root = parse_untrusted_xml(config_path.read_bytes())
        for element in root.iter():
            if element.text is None:
                continue
            if redact_value(element.tag, element.text) == REDACTED:
                element.text = REDACTED
        xml_bytes = DefusedET.tostring(root, encoding="utf-8", xml_declaration=True)
        return "config_xml.xml", xml_bytes + b"\n"
    except Exception:
        logger.warning("diagnostic_config_xml_snapshot_failed", exc_info=True)
        return None


async def collect_installed_packages() -> list[dict[str, str]]:
    """List installed Python packages and their versions."""
    from importlib.metadata import distributions

    return sorted(
        [{"name": d.metadata["Name"], "version": d.metadata["Version"]} for d in distributions()],
        key=lambda p: p["name"].lower(),
    )


def collect_scheduler_state() -> list[dict[str, object]]:
    """Collect current state of all scheduled background tasks."""
    try:
        from pullbox.core.scheduler import get_scheduler

        scheduler = get_scheduler()
        if not scheduler.running:
            return []
        return scheduler.get_scheduled_tasks()
    except Exception:
        logger.debug("diagnostic_scheduler_collect_failed", exc_info=True)
        return []


async def collect_runtime_info(session: AsyncSession) -> dict[str, object]:
    """Collect process memory, uptime, and Alembic migration version."""
    from sqlalchemy import text

    info: dict[str, object] = {}

    try:
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        rss = rusage.ru_maxrss
        if sys.platform == "darwin":
            info["process_rss_bytes"] = rss
        else:
            info["process_rss_bytes"] = rss * 1024
    except Exception:
        pass

    try:
        from pullbox.api.v1.health import _started_at

        info["started_at"] = _started_at.isoformat()
        info["uptime_seconds"] = round((datetime.now(UTC) - _started_at).total_seconds(), 1)
    except Exception:
        pass

    try:
        result = await session.execute(text("SELECT version_num FROM alembic_version"))
        row = result.scalar_one_or_none()
        info["alembic_version"] = str(row) if row else None
    except Exception:
        info["alembic_version"] = None

    return info
