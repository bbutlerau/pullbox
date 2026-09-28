"""Persistent normalized source reads with source-bound conditional validators."""

from __future__ import annotations

import asyncio
import hashlib
import json
import weakref
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from pullbox.models.provider_cache import MetadataProviderCacheEntry as Entry
from pullbox.schemas.metadata_sources import MetadataFetch, SourceCapability, SourceStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pullbox.core.metadata_identity import MetadataSource

logger = structlog.get_logger(__name__)
_KIND = "source-read-v1"
_MAX_BYTES = 256 * 1024
_MAX_ENTRIES = 128


@dataclass
class _Flight:
    task: asyncio.Task[bytes]
    waiters: int = 0


@dataclass
class _Coordinator:
    pending: dict[tuple[str, bool], _Flight] = field(default_factory=dict)


# Coordinators do not retain engines; active fetches own their sessions and are
# drained when their final waiter leaves. Separate application DBs never share.
_coordinators: weakref.WeakKeyDictionary[AsyncEngine, _Coordinator] = weakref.WeakKeyDictionary()


def source_read_cache(session: AsyncSession) -> MetadataReadCache | None:
    """Use the caller's database without borrowing its transaction."""
    if not isinstance(session.bind, AsyncEngine):
        return None
    return MetadataReadCache(
        async_sessionmaker(session.bind, class_=type(session), expire_on_commit=False)
    )


class MetadataReadCache:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.factory = factory
        self.now = now or (lambda: datetime.now(UTC))
        engine = factory.kw.get("bind")
        if not isinstance(engine, AsyncEngine):
            raise ValueError("Source cache requires its own engine-bound sessions")
        self.coordinator = _coordinators.setdefault(engine, _Coordinator())

    async def _load(
        self, source: MetadataSource, key: str
    ) -> tuple[datetime, dict[str, object]] | None:
        try:
            async with asyncio.timeout(2), self.factory() as session:
                row = await session.scalar(
                    select(Entry).where(
                        Entry.provider_name == source.value,
                        Entry.cache_kind == _KIND,
                        Entry.cache_key == key,
                        Entry.expires_at > self.now(),
                    )
                )
                return (row.fetched_at, row.payload) if row is not None else None
        except (SQLAlchemyError, TimeoutError):
            logger.warning("metadata_read_cache_unavailable", source=source.value)
            return None

    async def _store(
        self,
        source: MetadataSource,
        key: str,
        request: dict[str, object],
        payload: bytes,
        fetched_at: datetime,
    ) -> None:
        if len(payload) > _MAX_BYTES:
            return
        try:
            async with asyncio.timeout(2), self.factory.begin() as session:
                insert = (
                    pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
                )
                statement = insert(Entry).values(
                    provider_name=source.value,
                    cache_kind=_KIND,
                    cache_key=key,
                    request=request,
                    payload=json.loads(payload),
                    fetched_at=fetched_at,
                    expires_at=fetched_at + timedelta(days=1),
                )
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[Entry.provider_name, Entry.cache_kind, Entry.cache_key],
                        set_={
                            "payload": statement.excluded.payload,
                            "fetched_at": statement.excluded.fetched_at,
                            "expires_at": statement.excluded.expires_at,
                        },
                        where=Entry.fetched_at <= statement.excluded.fetched_at,
                    )
                )
                oldest = (
                    select(Entry.id)
                    .where(
                        Entry.cache_kind == _KIND,
                        Entry.provider_name.in_(["comicvine_api", "metron_api", "gcd_api_v2"]),
                    )
                    .order_by(Entry.fetched_at.desc(), Entry.id.desc())
                    .offset(_MAX_ENTRIES)
                )
                await session.execute(delete(Entry).where(Entry.id.in_(oldest)))
        except (SQLAlchemyError, TimeoutError):
            logger.warning("metadata_read_cache_store_failed", source=source.value)

    async def _fetch[T](
        self,
        source: MetadataSource,
        key: str,
        request: dict[str, object],
        model: type[MetadataFetch[T]],
        loader: Callable[[str | None], Awaitable[MetadataFetch[T]]],
        validate: Callable[[T], None],
        *,
        conditional: bool,
        revalidate: bool,
    ) -> bytes:
        stored = await self._load(source, key)
        cached = None
        if stored is not None:
            try:
                cached = model.model_validate(stored[1])
                if cached.status is SourceStatus.OK and cached.data is not None:
                    validate(cached.data)
                elif cached.status is not SourceStatus.NOT_FOUND or cached.data is not None:
                    raise ValueError("Invalid cached response")
                validator = cached.validator
                if validator is not None and (
                    not 1 <= len(validator) <= 128
                    or any(ord(char) < 32 or ord(char) > 126 for char in validator)
                ):
                    raise ValueError("Invalid cached validator")
            except (ValueError, TypeError, AttributeError):
                cached = None
        if cached is not None and stored is not None and not revalidate:
            ttl = timedelta(seconds=300 if cached.status is SourceStatus.OK else 15)
            if stored[0] <= self.now() < stored[0] + ttl:
                return cached.model_dump_json().encode()
        validator = (
            cached.validator
            if cached is not None and cached.data is not None and conditional
            else None
        )
        started_at = self.now()
        result = await loader(validator)
        if result.status is SourceStatus.NOT_MODIFIED:
            if cached is None or cached.data is None or validator is None:
                return model(status=SourceStatus.INCOMPATIBLE_RESPONSE).model_dump_json().encode()
            result = cached
        if result.status is SourceStatus.OK:
            if result.data is None:
                return model(status=SourceStatus.INCOMPATIBLE_RESPONSE).model_dump_json().encode()
            validate(result.data)
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise asyncio.CancelledError
        payload = result.model_dump_json().encode()
        if result.status in {SourceStatus.OK, SourceStatus.NOT_FOUND}:
            await self._store(source, key, request, payload, started_at)
        return payload

    async def get[T](
        self,
        source: MetadataSource,
        revision: int,
        capability: SourceCapability,
        identifier: str,
        page: int,
        model: type[MetadataFetch[T]],
        loader: Callable[[str | None], Awaitable[MetadataFetch[T]]],
        validate: Callable[[T], None],
        *,
        conditional: bool,
        revalidate: bool = False,
    ) -> MetadataFetch[T]:
        request: dict[str, object] = {
            "source": source.value,
            "revision": revision,
            "operation": capability.value,
            "external_id": identifier,
            "page": page,
        }
        # A public cache identity, not an authentication or password digest.
        key = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
        flight_key = (key, revalidate)
        flight = self.coordinator.pending.get(flight_key)
        if flight is not None and flight.task.cancelling():
            return model(status=SourceStatus.UNAVAILABLE, retry_after_seconds=1)
        if flight is None:
            if len(self.coordinator.pending) >= 8:
                return model(status=SourceStatus.UNAVAILABLE, retry_after_seconds=1)
            flight = _Flight(
                asyncio.create_task(
                    self._fetch(
                        source,
                        key,
                        request,
                        model,
                        loader,
                        validate,
                        conditional=conditional,
                        revalidate=revalidate,
                    )
                )
            )
            self.coordinator.pending[flight_key] = flight
        flight.waiters += 1
        try:
            return model.model_validate_json(await asyncio.shield(flight.task))
        finally:
            flight.waiters -= 1
            if not flight.waiters:
                if not flight.task.done():
                    flight.task.cancel()
                try:
                    await asyncio.gather(flight.task, return_exceptions=True)
                finally:
                    if self.coordinator.pending.get(flight_key) is flight:
                        self.coordinator.pending.pop(flight_key)
