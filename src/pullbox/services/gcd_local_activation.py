"""Atomic GCD activation; validation runs before the application write transaction."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models.config import SystemConfig
from pullbox.providers.metadata.gcd_local_database import (
    GcdDatabaseError,
    GcdSnapshot,
    validate_candidate,
)

ACTIVE_KEY = "metadata.gcd_local.active"
PREVIOUS_KEY = "metadata.gcd_local.previous"


async def validate_for_request(
    path: str | None, disconnected: Callable[[], Awaitable[bool]]
) -> GcdSnapshot:
    task = asyncio.create_task(validate_candidate(path))
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=0.25)
            if await disconnected():
                raise GcdDatabaseError("GCD validation cancelled. Saved settings are unchanged.")
            if done:
                return await task
    finally:
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError, GcdDatabaseError):
            await task


async def active_snapshot(session: AsyncSession) -> GcdSnapshot | None:
    row = await session.get(SystemConfig, ACTIVE_KEY, populate_existing=True)
    return GcdSnapshot.model_validate_json(row.value) if row else None


async def activate_snapshot(session: AsyncSession, candidate: GcdSnapshot) -> None:
    """Caller owns the policy revision lock and commits both records together."""
    current = await session.get(SystemConfig, ACTIVE_KEY, populate_existing=True)
    if current is not None and current.value != candidate.model_dump_json():
        previous = await session.get(SystemConfig, PREVIOUS_KEY, populate_existing=True)
        if previous is None:
            previous = SystemConfig(key=PREVIOUS_KEY, value=current.value, value_type="string")
            session.add(previous)
        else:
            previous.value = current.value
    if current is None:
        current = SystemConfig(key=ACTIVE_KEY, value="", value_type="string")
        session.add(current)
    current.value = candidate.model_dump_json()
    await session.flush()
