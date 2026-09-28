"""Persist validated source policy without executing providers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from pydantic import SecretStr, ValidationError
from sqlalchemy import false, select, update
from sqlalchemy.exc import IntegrityError

from pullbox.config import get_settings
from pullbox.core.comicvine_key import get_comicvine_api_key, save_comicvine_api_key
from pullbox.core.encryption import decrypt_secret, encrypt_secret, is_encrypted
from pullbox.core.metadata_identity import MetadataSource
from pullbox.models.config import SystemConfig
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    SourcePolicyRead,
    SourcePolicyWrite,
    SourceSettings,
    SourceStatus,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class SourceConfigurationConflictError(ValueError):
    """Source policy was edited after the displayed revision."""


@dataclass(frozen=True)
class SourceRuntime:
    policy: SourcePolicyRead
    credential: SecretStr | None = None
    unavailable: SourceStatus | None = None


def default_policy(source: MetadataSource) -> SourcePolicyRead:
    return SourcePolicyRead(
        source=source,
        identity_namespace=source.identity_namespace,
        enabled=source in {MetadataSource.COMICVINE_LOCAL, MetadataSource.COMICVINE_API},
        priority=(list(MetadataSource).index(source) + 1) * 10,
        domain_priorities={},
        settings=SourceSettings(),
        credential_configured=False,
        revision=0,
    )


async def read_source_policies(session: AsyncSession) -> list[SourcePolicyRead]:
    rows = {row.source: row for row in await session.scalars(select(MetadataSourceConfig))}
    cv_row = await session.get(SystemConfig, "comicvine_api_key")
    cv_configured = bool((cv_row and cv_row.value) or get_settings().comicvine_api_key)
    result = []
    for source in MetadataSource:
        policy = default_policy(source)
        row = rows.get(source.value)
        if row is not None:
            try:
                body = SourcePolicyWrite(
                    revision=row.revision,
                    enabled=row.enabled,
                    priority=row.priority,
                    domain_priorities=row.domain_priorities,
                    settings=row.settings,
                )
                _validate(source, body, gcd_api_enabled=True)
                policy = SourcePolicyRead(
                    **{
                        **policy.model_dump(),
                        **body.model_dump(exclude={"clear_credential"}),
                        "last_tested_at": row.last_tested_at,
                        "last_success_at": row.last_success_at,
                        "last_status": row.last_status,
                    }
                )
            except (ValueError, ValidationError):
                policy = policy.model_copy(
                    update={
                        "enabled": False,
                        "revision": row.revision,
                        "configuration_status": SourceStatus.INVALID_CONFIG,
                    }
                )
        policy.credential_configured = (
            cv_configured
            if source is MetadataSource.COMICVINE_API
            else bool(row and row.credential_secret)
        )
        result.append(policy)
    return sorted(result, key=lambda item: (item.priority, item.source.value))


def _validate(source: MetadataSource, body: SourcePolicyWrite, *, gcd_api_enabled: bool) -> None:
    if source is MetadataSource.GCD_API_V2 and body.enabled and not gcd_api_enabled:
        raise ValueError("GCD API v2 is disabled by the release feature flag")
    if any(
        type(value) is not int or not 0 <= value <= 1000
        for value in body.domain_priorities.values()
    ):
        raise ValueError("Domain priorities must be integers between 0 and 1000")
    if (
        source in {MetadataSource.GCD_LOCAL, MetadataSource.GCD_API_V2}
        and MetadataDomain.ARTWORK in body.domain_priorities
    ):
        raise ValueError("This source does not provide artwork")
    path = body.settings.database_path
    if path is not None and (
        source is not MetadataSource.GCD_LOCAL
        or not PurePosixPath(path).is_absolute()
        or ".." in PurePosixPath(path).parts
        or "\x00" in path
    ):
        raise ValueError("Only GCD Local accepts an absolute database candidate path")
    if body.credential is not None and body.clear_credential:
        raise ValueError("Choose a replacement credential or clear it, not both")
    if (body.credential is not None or body.clear_credential) and source in {
        MetadataSource.COMICVINE_LOCAL,
        MetadataSource.GCD_LOCAL,
    }:
        raise ValueError("A local source does not accept credentials")
    if body.credential is not None:
        token = body.credential.get_secret_value()
        if (
            not token
            or len(token) > 4096
            or token.startswith("enc:")
            or any(char.isspace() for char in token)
        ):
            raise ValueError("Enter a non-empty provider token, not an encrypted value")


async def save_source_policy(
    session: AsyncSession,
    source: MetadataSource,
    body: SourcePolicyWrite,
    *,
    gcd_api_enabled: bool = False,
) -> SourcePolicyRead:
    _validate(source, body, gcd_api_enabled=gcd_api_enabled)
    encrypted = (
        encrypt_secret(body.credential.get_secret_value()) if body.credential is not None else None
    )
    if session.get_bind().dialect.name == "sqlite":
        await session.execute(
            update(MetadataSourceConfig)
            .where(false())
            .values(revision=MetadataSourceConfig.revision)
        )
    try:
        async with session.begin_nested():
            row = await session.scalar(
                select(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == source.value)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (row.revision if row else 0) != body.revision:
                raise SourceConfigurationConflictError(
                    "Source settings changed; reload before saving"
                )
            if row is None:
                row = MetadataSourceConfig(source=source.value)
                session.add(row)
            row.enabled, row.priority = body.enabled, body.priority
            row.domain_priorities = {
                key.value: value for key, value in body.domain_priorities.items()
            }
            row.settings = body.settings.model_dump(exclude_none=True)
            row.revision = body.revision + 1
            row.last_status = None
            row.last_tested_at = row.last_success_at = None
            if source is MetadataSource.COMICVINE_API:
                if body.credential is not None or body.clear_credential:
                    await save_comicvine_api_key(
                        session, body.credential.get_secret_value() if body.credential else ""
                    )
            elif encrypted is not None or body.clear_credential:
                row.credential_secret = encrypted if encrypted is not None else None
            await session.flush()
    except IntegrityError as exc:
        raise SourceConfigurationConflictError(
            "Source settings changed; reload before saving"
        ) from exc
    return next(item for item in await read_source_policies(session) if item.source is source)


async def load_source_runtime(
    session: AsyncSession, *, gcd_api_enabled: bool
) -> list[SourceRuntime]:
    """Load configuration only. Disabled and flagged sources never decrypt secrets."""
    policies = await read_source_policies(session)
    rows = {row.source: row for row in await session.scalars(select(MetadataSourceConfig))}
    result = []
    for policy in policies:
        status = policy.configuration_status
        if policy.source is MetadataSource.GCD_API_V2 and not gcd_api_enabled:
            status = SourceStatus.FEATURE_DISABLED
        elif not policy.enabled:
            status = status or SourceStatus.DISABLED
        credential = None
        if status is None:
            try:
                if policy.source is MetadataSource.COMICVINE_API:
                    token = await get_comicvine_api_key(session)
                    credential = SecretStr(token) if token else None
                elif policy.source in {MetadataSource.METRON_API, MetadataSource.GCD_API_V2}:
                    row = rows.get(policy.source.value)
                    encrypted = row.credential_secret if row else None
                    if encrypted and not is_encrypted(encrypted):
                        raise ValueError("Source credential is not encrypted")
                    token = decrypt_secret(encrypted) if encrypted else ""
                    credential = SecretStr(token) if token else None
            except ValueError:
                status = SourceStatus.INVALID_CONFIG
        result.append(SourceRuntime(policy, credential, status))
    return result
