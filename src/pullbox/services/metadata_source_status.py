"""Read-only, bounded operator views of account holds and deferred series work."""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.metadata_identity import MetadataSource
from pullbox.models import MetadataSeriesRetry, Series
from pullbox.models.metadata_source_account import MetadataSourceAccount
from pullbox.schemas.metadata_sources import (
    DeferredMetadataRead,
    SourceAccountRead,
    SourceDescriptor,
    SourceStatus,
)
from pullbox.schemas.pagination import PaginatedResponse
from pullbox.services.metadata_account_admission import account_key
from pullbox.services.metadata_discovery import describe_source_policies
from pullbox.services.metadata_series_retry import config_key
from pullbox.services.metadata_sources import SourceRuntime, load_source_runtime


async def _accounts(
    session: AsyncSession, runtime: list[SourceRuntime]
) -> dict[str, SourceAccountRead]:
    current = {item.policy.source.value: item for item in runtime}
    result = {}
    for row in await session.scalars(select(MetadataSourceAccount)):
        source = current.get(row.source)
        if (
            source is not None
            and source.unavailable is None
            and row.account_key == account_key(source)
        ):
            result[row.source] = SourceAccountRead(
                status=row.status, retry_at=row.retry_at, probe_until=row.lease_until
            )
    return result


async def source_status(session: AsyncSession, *, gcd_api_enabled: bool) -> list[SourceDescriptor]:
    runtime = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
    accounts = await _accounts(session, runtime)
    counts = {
        row.source: (row.series_count, row.work_count)
        for row in (
            await session.execute(
                select(
                    MetadataSeriesRetry.source,
                    func.count(func.distinct(MetadataSeriesRetry.series_id)).label("series_count"),
                    func.count().label("work_count"),
                ).group_by(MetadataSeriesRetry.source)
            )
        )
    }
    result = describe_source_policies(
        [item.policy for item in runtime], gcd_api_enabled=gcd_api_enabled
    )
    for item in result:
        item.account = accounts.get(item.source.value)
        item.deferred_series, item.deferred_work = counts.get(item.source.value, (0, 0))
    return result


async def deferred_work(
    session: AsyncSession,
    *,
    source: MetadataSource | None,
    limit: int,
    offset: int,
    gcd_api_enabled: bool,
) -> PaginatedResponse[DeferredMetadataRead]:
    runtime = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
    accounts = await _accounts(session, runtime)
    current = {item.policy.source.value: item for item in runtime}
    criteria = (MetadataSeriesRetry.source == source.value,) if source is not None else ()
    total = (
        await session.scalar(select(func.count()).select_from(MetadataSeriesRetry).where(*criteria))
        or 0
    )
    rows = await session.execute(
        select(MetadataSeriesRetry, Series.title)
        .join(Series, Series.id == MetadataSeriesRetry.series_id)
        .where(*criteria)
        .order_by(MetadataSeriesRetry.series_id, MetadataSeriesRetry.id)
        .limit(limit)
        .offset(offset)
    )
    now = datetime.now(UTC)
    items = []
    for row, title in rows:
        active = current.get(row.source)
        account = accounts.get(row.source)
        state: str = "ready"
        due = row.retry_at
        if active and active.unavailable is not None:
            state, due = "source_disabled", None
        elif account and account.status is SourceStatus.AUTHENTICATION_FAILED:
            state, due = "authentication_required", None
        elif (
            account
            and account.status is not None
            and any(value and value > now for value in (account.retry_at, account.probe_until))
        ):
            state, due = (
                "waiting",
                max(value for value in (account.retry_at, account.probe_until) if value),
            )
        elif active is not None and config_key(active) != row.config_key:
            due = now
        elif row.retry_at is None:
            state = "authentication_required"
        elif row.retry_at > now:
            state = "waiting"
        items.append(
            DeferredMetadataRead.model_validate(
                dict(
                    id=row.id,
                    series_id=row.series_id,
                    series_title=title,
                    source=row.source,
                    task_id=row.task_id,
                    state=state,
                    retry_at=due,
                )
            )
        )
    return PaginatedResponse(
        items=items, total=total, limit=limit, offset=offset, has_more=offset + len(items) < total
    )
