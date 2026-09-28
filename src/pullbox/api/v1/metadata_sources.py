"""Metadata source configuration, discovery and explicit connection checks."""

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, HTTPException

from pullbox.api.deps import AuthenticatedUser, DbSession, InteractiveOperatorUser, Settings
from pullbox.core.metadata_identity import MetadataSource
from pullbox.schemas.metadata_sources import (
    SeriesDiscoveryQuery,
    SeriesDiscoveryRead,
    SourceDescriptor,
    SourcePolicyRead,
    SourcePolicyWrite,
    SourcePriorityWrite,
    SourceTestRead,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry, describe_source_policies
from pullbox.services.metadata_sources import (
    SourceConfigurationConflictError,
    load_source_runtime,
    read_source_policies,
    record_source_health,
    save_source_policy,
    save_source_priorities,
)

router = APIRouter(prefix="/metadata", tags=["metadata"])
logger = structlog.get_logger(__name__)


@router.get("/sources", response_model=list[SourceDescriptor])
async def sources(
    session: DbSession, _user: InteractiveOperatorUser, settings: Settings
) -> list[SourceDescriptor]:
    return describe_source_policies(
        await read_source_policies(session), gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
    )


@router.put("/priorities", response_model=list[SourceDescriptor])
async def save_priorities(
    body: SourcePriorityWrite, session: DbSession, user: InteractiveOperatorUser, settings: Settings
) -> list[SourceDescriptor]:
    try:
        policies = await save_source_priorities(session, body)
    except SourceConfigurationConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    logger.info("metadata_source_priorities_updated", user_id=user.id)
    return describe_source_policies(policies, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled)


@router.put("/sources/{source}", response_model=SourcePolicyRead)
async def save_source(
    source: MetadataSource,
    body: SourcePolicyWrite,
    session: DbSession,
    user: InteractiveOperatorUser,
    settings: Settings,
) -> SourcePolicyRead:
    try:
        result = await save_source_policy(
            session, source, body, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
        )
    except SourceConfigurationConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    logger.info(
        "metadata_source_policy_updated",
        source=source.value,
        revision=result.revision,
        enabled=result.enabled,
        user_id=user.id,
    )
    return result


@router.post("/search", response_model=SeriesDiscoveryRead)
async def search(
    body: SeriesDiscoveryQuery, session: DbSession, _user: AuthenticatedUser, settings: Settings
) -> SeriesDiscoveryRead:
    runtime = await load_source_runtime(
        session, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
    )
    await session.rollback()
    registry = MetadataSourceRegistry(runtime, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled)
    return await registry.discover(body)


@router.post("/sources/{source}/test", response_model=SourceTestRead)
async def test_source(
    source: MetadataSource, session: DbSession, _user: InteractiveOperatorUser, settings: Settings
) -> SourceTestRead:
    runtime = await load_source_runtime(
        session, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
    )
    revision = next(item.policy.revision for item in runtime if item.policy.source is source)
    await session.rollback()
    checked_at = datetime.now(UTC)
    outcome = await MetadataSourceRegistry(
        runtime, gcd_api_enabled=settings.metadata_gcd_api_v2_enabled
    ).check(source)
    recorded = await record_source_health(session, source, revision, outcome, checked_at)
    logger.info(
        "metadata_source_test_complete",
        source=source.value,
        status=outcome.status.value,
        recorded=recorded,
    )
    return SourceTestRead(outcome=outcome, recorded=recorded)
