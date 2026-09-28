"""Short independent account-state transactions around, never across, provider I/O."""

from __future__ import annotations

import asyncio
import hashlib
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.metadata_source_account import MetadataSourceAccount as Account
from pullbox.schemas.metadata_sources import SourceOutcome, SourceStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pullbox.services.metadata_sources import SourceRuntime

logger = structlog.get_logger(__name__)
_REMOTE = {MetadataSource.COMICVINE_API, MetadataSource.METRON_API, MetadataSource.GCD_API_V2}
_FAILURES = {
    SourceStatus.RATE_LIMITED,
    SourceStatus.TIMEOUT,
    SourceStatus.UNAVAILABLE,
    SourceStatus.AUTHENTICATION_FAILED,
}


def account_key(runtime: SourceRuntime) -> str:
    # Account identity only, not password verification. Priority edits cannot reset a hold.
    token = runtime.credential.get_secret_value() if runtime.credential else ""
    return hashlib.sha256(f"{runtime.policy.source.value}:{token}".encode()).hexdigest()


@dataclass
class AccountAttempt:
    blocked: SourceOutcome | None = None
    key: str = ""
    revision: int = 0
    probe: bool = False
    started: bool = False
    outcome: SourceOutcome | None = None


def source_account_admission(
    session: AsyncSession, *, gcd_api_enabled: bool
) -> MetadataAccountAdmission | None:
    if not isinstance(session.bind, AsyncEngine):
        return None
    return MetadataAccountAdmission(
        async_sessionmaker(session.bind, class_=type(session), expire_on_commit=False),
        gcd_api_enabled=gcd_api_enabled,
    )


@asynccontextmanager
async def account_request(
    runtime: SourceRuntime, *, slots: asyncio.Semaphore, deadline: float
) -> AsyncIterator[AccountAttempt]:
    async with asyncio.timeout_at(deadline):
        await slots.acquire()
    try:
        gate = runtime.account_admission
        if gate is None or runtime.policy.source not in _REMOTE or runtime.credential is None:
            yield AccountAttempt()
            return
        attempt = await gate.admit(runtime)
        try:
            yield attempt
        finally:
            if attempt.blocked is None:
                await gate.finish(runtime, attempt)
    finally:
        slots.release()


class MetadataAccountAdmission:
    """Account transport state owns its sessions, never commits a library caller's work."""

    def __init__(self, factory: async_sessionmaker[AsyncSession], *, gcd_api_enabled: bool) -> None:
        self.factory = factory
        self.gcd_api_enabled = gcd_api_enabled

    async def admit(self, runtime: SourceRuntime) -> AccountAttempt:
        from pullbox.services.metadata_sources import load_source_runtime

        source, key = runtime.policy.source, account_key(runtime)
        try:
            async with asyncio.timeout(2), self.factory.begin() as session:
                # Recheck a captured registry before it can reset another credential's state.
                current = next(
                    item
                    for item in await load_source_runtime(
                        session, gcd_api_enabled=self.gcd_api_enabled
                    )
                    if item.policy.source is source
                )
                if current.unavailable or account_key(current) != key:
                    return AccountAttempt(
                        blocked=SourceOutcome(
                            source=source,
                            status=current.unavailable or SourceStatus.UNAVAILABLE,
                            retry_after_seconds=60,
                        )
                    )
                insert = (
                    pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
                )
                await session.execute(
                    insert(Account)
                    .values(
                        source=source.value,
                        account_key=key,
                        revision=1,
                    )
                    .on_conflict_do_nothing(index_elements=[Account.source])
                )
                row = await session.scalar(
                    select(Account).where(Account.source == source.value).with_for_update()
                )
                assert row is not None
                if row.account_key != key:
                    row.account_key = key
                    row.status = row.retry_at = row.lease_until = None
                    row.revision += 1
                now = datetime.now(UTC)
                if row.status == SourceStatus.AUTHENTICATION_FAILED.value:
                    return AccountAttempt(
                        blocked=SourceOutcome(
                            source=source, status=SourceStatus.AUTHENTICATION_FAILED
                        )
                    )
                deadline = max(
                    (item for item in (row.retry_at, row.lease_until) if item), default=None
                )
                if row.status is not None and deadline is not None and deadline > now:
                    return AccountAttempt(
                        blocked=SourceOutcome(
                            source=source,
                            status=SourceStatus(row.status),
                            retry_after_seconds=max(1, math.ceil((deadline - now).total_seconds())),
                        )
                    )
                probe = row.status is not None
                if probe:
                    row.lease_until = now + timedelta(seconds=45)
                    row.revision += 1
                return AccountAttempt(key=key, revision=row.revision, probe=probe)
        except (SQLAlchemyError, TimeoutError):
            logger.warning("metadata_account_admission_unavailable", source=source.value)
            return AccountAttempt(
                blocked=SourceOutcome(
                    source=source, status=SourceStatus.UNAVAILABLE, retry_after_seconds=5
                )
            )

    async def finish(self, runtime: SourceRuntime, attempt: AccountAttempt) -> None:
        outcome = attempt.outcome if attempt.started else None
        failed = outcome is not None and outcome.status in _FAILURES
        if not failed and not attempt.probe:
            return
        try:
            async with asyncio.timeout(2), self.factory.begin() as session:
                statement = (
                    update(Account)
                    .where(
                        Account.source == runtime.policy.source.value,
                        Account.account_key == attempt.key,
                        Account.revision == attempt.revision,
                    )
                    .values(lease_until=None, revision=Account.revision + 1)
                )
                if failed and outcome is not None:
                    delay = outcome.retry_after_seconds or (
                        3600 if outcome.status is SourceStatus.RATE_LIMITED else 300
                    )
                    due = (
                        None
                        if outcome.status is SourceStatus.AUTHENTICATION_FAILED
                        else (datetime.now(UTC) + timedelta(seconds=max(60, min(delay, 7 * 86400))))
                    )
                    statement = statement.values(status=outcome.status.value, retry_at=due)
                elif outcome is not None:
                    statement = statement.values(status=None, retry_at=None)
                # Cancellation only releases the probe lease, retaining the prior failure.
                await session.execute(statement)
        except (SQLAlchemyError, TimeoutError):
            logger.warning(
                "metadata_account_outcome_store_failed", source=runtime.policy.source.value
            )
