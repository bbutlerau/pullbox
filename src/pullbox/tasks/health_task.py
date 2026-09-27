"""Health check background tasks.

Cheap local checks run frequently, while external integrations run on slower
cadences to avoid noisy logs and unbounded health-history growth.
"""

from __future__ import annotations

import time

import structlog

from pullbox.config import get_settings
from pullbox.core.config_resolver import get_int_setting, load_system_config_values
from pullbox.core.scheduler import scheduled_task
from pullbox.database import get_session_factory
from pullbox.models.health import HealthStatus
from pullbox.services.health_persistence import cleanup_health_history as cleanup_health_rows
from pullbox.services.health_runtime import run_health_refresh

logger = structlog.get_logger(__name__)


async def run_health_checks() -> None:
    """Execute all configured health checks and persist results."""
    await _run_health_refresh(component=None, event_name="health_checks_complete")


@scheduled_task(
    task_id="run_scheduler_health_check",
    trigger="interval",
    display_name="Check Scheduler Health",
    minutes=30,
)
async def run_scheduler_health_check() -> None:
    """Refresh scheduler health."""
    await _run_health_refresh(component="scheduler", event_name="scheduler_health_check_complete")


@scheduled_task(
    task_id="run_database_health_check",
    trigger="interval",
    display_name="Check Database Health",
    minutes=15,
)
async def run_database_health_check() -> None:
    """Refresh database health."""
    await _run_health_refresh(component="database", event_name="database_health_check_complete")


@scheduled_task(
    task_id="run_filesystem_health_check",
    trigger="interval",
    display_name="Check Filesystem Health",
    minutes=15,
)
async def run_filesystem_health_check() -> None:
    """Refresh filesystem health."""
    await _run_health_refresh(
        component="filesystem",
        event_name="filesystem_health_check_complete",
    )


@scheduled_task(
    task_id="run_system_health_check",
    trigger="interval",
    display_name="Check System Health",
    minutes=15,
)
async def run_system_health_check() -> None:
    """Refresh system resource health."""
    await _run_health_refresh(component="system", event_name="system_health_check_complete")


@scheduled_task(
    task_id="run_download_client_health_checks",
    trigger="interval",
    display_name="Check Download Client Health",
    hours=4,
)
async def run_download_client_health_checks() -> None:
    """Refresh download client health."""
    await _run_health_refresh(
        component="download_clients",
        event_name="download_client_health_checks_complete",
    )


@scheduled_task(
    task_id="run_indexer_health_checks",
    trigger="interval",
    display_name="Check Indexer Health",
    hours=8,
)
async def run_indexer_health_checks() -> None:
    """Refresh indexer health."""
    await _run_health_refresh(component="indexers", event_name="indexer_health_checks_complete")


@scheduled_task(
    task_id="run_comicvine_health_check",
    trigger="interval",
    display_name="Check ComicVine Health",
    hours=8,
)
async def run_comicvine_health_check() -> None:
    """Refresh ComicVine health."""
    await _run_health_refresh(component="comicvine", event_name="comicvine_health_check_complete")


@scheduled_task(
    task_id="cleanup_health_history",
    trigger="cron",
    display_name="Cleanup Health History",
    hour=4,
)
async def cleanup_health_history() -> None:
    """Prune health history rows past the configured retention period."""
    settings = get_settings()
    factory = get_session_factory()
    async with factory() as session:
        configs = await load_system_config_values(session, ("health_history_retention_days",))
        retention_days = max(
            1,
            get_int_setting(
                configs,
                "health_history_retention_days",
                settings.health_history_retention_days,
            ),
        )
        deleted = await cleanup_health_rows(session, retention_days)
        await session.commit()

    if deleted:
        logger.info(
            "cleanup_health_history_complete",
            pruned=deleted,
            retention_days=retention_days,
        )


async def _run_health_refresh(component: str | None, event_name: str) -> None:
    """Run a health refresh and write a compact completion log."""
    start = time.monotonic()
    outcomes = (
        await run_health_refresh(component=component)
        if component is not None
        else await run_health_refresh()
    )

    elapsed = time.monotonic() - start

    counts: dict[str, int] = {
        "healthy": 0,
        "degraded": 0,
        "unhealthy": 0,
        "unknown": 0,
    }
    for outcome in outcomes:
        counts[outcome.status.value] = counts.get(outcome.status.value, 0) + 1

    attention_statuses = {
        HealthStatus.DEGRADED,
        HealthStatus.UNHEALTHY,
        HealthStatus.UNKNOWN,
    }
    attention_checks: list[str] = []
    attention_subchecks: list[str] = []
    for outcome in outcomes:
        outcome_component = str(getattr(outcome, "component", component or "health"))
        check_name = str(getattr(outcome, "check_name", "check"))
        if outcome.status in attention_statuses:
            attention_checks.append(f"{outcome_component}.{check_name}:{outcome.status.value}")
        for sub_check in getattr(outcome, "sub_checks", ()):
            if sub_check.status not in attention_statuses:
                continue
            attention_subchecks.append(
                f"{outcome_component}.{check_name}.{sub_check.check_name}:{sub_check.status.value}"
            )

    logger.info(
        event_name,
        component=component,
        healthy=counts[HealthStatus.HEALTHY],
        degraded=counts[HealthStatus.DEGRADED],
        unhealthy=counts[HealthStatus.UNHEALTHY],
        unknown=counts[HealthStatus.UNKNOWN],
        duration_seconds=round(elapsed, 1),
        total_checks=len(outcomes),
        attention_checks=attention_checks,
        attention_subchecks=attention_subchecks,
    )
