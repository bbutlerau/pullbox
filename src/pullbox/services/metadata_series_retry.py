"""Bounded, caller-committed admission and retries for scheduled source reads."""

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from pullbox.models import MetadataSeriesRetry, Series
from pullbox.models.metadata_source_account import MetadataSourceAccount
from pullbox.models.series import IssueCatalogState
from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus
from pullbox.services.metadata_account_admission import account_key
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_sources import SourceRuntime, load_source_runtime
from pullbox.services.metadata_writer_identity import metadata_write_scope

TRANSIENT = {SourceStatus.RATE_LIMITED, SourceStatus.TIMEOUT, SourceStatus.UNAVAILABLE}


def config_key(runtime: SourceRuntime) -> str:
    # Only a one-way scope key is retained; credentials never enter retry records.
    token = runtime.credential.get_secret_value() if runtime.credential else ""
    return hashlib.sha256(f"{runtime.policy.revision}:{token}".encode()).hexdigest()


@dataclass(frozen=True)
class RetryToken:
    id: int
    source: str
    revision: int


@dataclass(frozen=True)
class RetryAdmission:
    registry: MetadataSourceRegistry
    config_keys: dict[str, str]
    tokens: tuple[RetryToken, ...]
    held: frozenset[str]


async def retry_runtime(session: AsyncSession, *, gcd_api_enabled: bool) -> list[SourceRuntime]:
    return await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)


def _changed(runtime: list[SourceRuntime]) -> ColumnElement[bool]:
    return or_(
        False,
        *(
            (MetadataSeriesRetry.source == item.policy.source.value)
            & (MetadataSeriesRetry.config_key != config_key(item))
            for item in runtime
            if item.policy.enabled and item.unavailable is None
        ),
    )


async def retry_candidates(
    session: AsyncSession, task_id: str, runtime: list[SourceRuntime], *, limit: int
) -> list[int]:
    return list(
        await session.scalars(
            select(MetadataSeriesRetry.series_id)
            .join(Series, Series.id == MetadataSeriesRetry.series_id)
            .where(
                MetadataSeriesRetry.task_id == task_id,
                Series.issue_catalog_state != IssueCatalogState.HYDRATING,
                or_(MetadataSeriesRetry.retry_at <= datetime.now(UTC), _changed(runtime)),
            )
            .group_by(MetadataSeriesRetry.series_id)
            .order_by(func.min(MetadataSeriesRetry.retry_at), MetadataSeriesRetry.series_id)
            .limit(limit)
        )
    )


async def retry_deadline(
    session: AsyncSession, task_id: str, runtime: list[SourceRuntime]
) -> datetime | None:
    if await session.scalar(
        select(MetadataSeriesRetry.id)
        .where(MetadataSeriesRetry.task_id == task_id, _changed(runtime))
        .limit(1)
    ):
        return datetime.now(UTC)
    return await session.scalar(
        select(func.min(MetadataSeriesRetry.retry_at)).where(MetadataSeriesRetry.task_id == task_id)
    )


async def prune_retries(session: AsyncSession, task_id: str, eligible: ColumnElement[bool]) -> None:
    await session.execute(
        delete(MetadataSeriesRetry).where(
            MetadataSeriesRetry.task_id == task_id,
            MetadataSeriesRetry.series_id.not_in(select(Series.id).where(eligible)),
        )
    )


async def admit_retry(
    session: AsyncSession,
    task_id: str,
    series_id: int,
    *,
    gcd_api_enabled: bool,
    retry_only: bool,
) -> RetryAdmission:
    runtime = await retry_runtime(session, gcd_api_enabled=gcd_api_enabled)
    keys = {item.policy.source.value: config_key(item) for item in runtime}
    rows = list(
        await session.scalars(
            select(MetadataSeriesRetry).where(
                MetadataSeriesRetry.task_id == task_id, MetadataSeriesRetry.series_id == series_id
            )
        )
    )
    now = datetime.now(UTC)
    held = frozenset(
        row.source
        for row in rows
        if row.source != "*"
        and row.config_key == keys.get(row.source)
        and (row.retry_at is None or row.retry_at > now)
    )
    due = {row.source for row in rows if row.source not in held}
    selected = [
        item
        for item in runtime
        if item.policy.source.value not in held
        and (not retry_only or "*" in due or item.policy.source.value in due)
    ]
    return RetryAdmission(
        MetadataSourceRegistry(
            selected, gcd_api_enabled=gcd_api_enabled, revalidate_reads=True, total_timeout=60
        ),
        keys,
        tuple(RetryToken(row.id, row.source, row.revision) for row in rows),
        held,
    )


async def settle_retry(
    session: AsyncSession,
    task_id: str,
    series_id: int,
    admission: RetryAdmission,
    *,
    outcomes: tuple[SourceOutcome, ...] = (),
    retry_seconds: float | None = None,
) -> None:
    """Settle only the read generation; newer deferred work must survive this caller."""
    failures = {
        item.source.value: item
        for item in outcomes
        if item.status in TRANSIENT | {SourceStatus.AUTHENTICATION_FAILED}
    }
    now = datetime.now(UTC)
    pending: dict[str, tuple[str, datetime | None, str]] = {}
    for source, item in failures.items():
        if source in admission.held:
            continue
        delay = item.retry_after_seconds or (
            3600 if item.status is SourceStatus.RATE_LIMITED else 300
        )
        pending[source] = (
            item.status.value,
            None
            if item.status is SourceStatus.AUTHENTICATION_FAILED
            else now + timedelta(seconds=max(60, min(delay, 7 * 86400))),
            admission.config_keys[source],
        )
    if retry_seconds is not None and not failures:
        if not math.isfinite(retry_seconds) or retry_seconds < 0:
            raise ValueError("Invalid metadata retry deadline")
        pending["*"] = (
            SourceStatus.TIMEOUT.value,
            now + timedelta(seconds=max(60, min(retry_seconds, 7 * 86400))),
            "",
        )
    async with metadata_write_scope(session):
        # A failure may settle after an explicit account probe has already recovered.
        auth_sources = {
            source
            for source, (status, _, _) in pending.items()
            if status == SourceStatus.AUTHENTICATION_FAILED.value
        }
        if auth_sources:
            current = {
                item.policy.source.value: item
                for item in await retry_runtime(
                    session, gcd_api_enabled=admission.registry.gcd_api_enabled
                )
            }
            for account in await session.scalars(
                select(MetadataSourceAccount)
                .where(MetadataSourceAccount.source.in_(auth_sources))
                .order_by(MetadataSourceAccount.source)
                .with_for_update()
            ):
                runtime = current[account.source]
                if (
                    runtime.unavailable is None
                    and account.account_key == account_key(runtime)
                    and pending[account.source][2] == config_key(runtime)
                    and account.status != SourceStatus.AUTHENTICATION_FAILED.value
                ):
                    pending[account.source] = (
                        account.status or SourceStatus.UNAVAILABLE.value,
                        account.retry_at or now,
                        pending[account.source][2],
                    )
        if (
            await session.scalar(select(Series.id).where(Series.id == series_id).with_for_update())
            is None
        ):
            return
        tokens = {item.source: item for item in admission.tokens}
        rows = {
            row.source: row
            for row in await session.scalars(
                select(MetadataSeriesRetry)
                .where(
                    MetadataSeriesRetry.task_id == task_id,
                    MetadataSeriesRetry.series_id == series_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        }
        for source in (set(tokens) | set(pending)) - admission.held:
            row, token = rows.get(source), tokens.get(source)
            if token is not None and row is None:
                continue
            if row is not None and (
                token is None or (row.id, row.revision) != (token.id, token.revision)
            ):
                continue
            if source not in pending:
                if row is not None:
                    await session.delete(row)
                continue
            status, due, key = pending[source]
            if row is None:
                row = MetadataSeriesRetry(
                    task_id=task_id, series_id=series_id, source=source, revision=0
                )
                session.add(row)
            row.status, row.retry_at, row.config_key = status, due, key
            row.revision += 1
        await session.flush()
