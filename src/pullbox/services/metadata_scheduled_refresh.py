"""Source eligibility and caller-owned background refresh of existing series."""

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from pullbox.core.metadata_identity import MetadataSource
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Issue, Series
from pullbox.models.issue import IssueStatus
from pullbox.models.metadata_identity import SeriesExternalIdentity
from pullbox.schemas.metadata_sources import SourceCapability, SourceOutcome
from pullbox.services.cover_resolver import resolve_covers_dir
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_series_refresh import refresh_series_catalog_from_sources
from pullbox.services.metadata_sources import load_source_runtime


async def scheduled_series_eligibility(
    session: AsyncSession, *, gcd_api_enabled: bool
) -> ColumnElement[bool]:
    """Use executable configured sources, not cached health or compatibility IDs."""
    registry = MetadataSourceRegistry(
        await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled),
        gcd_api_enabled=gcd_api_enabled,
    )
    namespaces = {
        source.identity_namespace
        for source, runtime in registry.runtime.items()
        if registry.source_availability(source, SourceCapability.SERIES_DETAILS) is None
        and registry.source_availability(source, SourceCapability.ISSUE_LIST) is None
        and (
            source in {MetadataSource.COMICVINE_LOCAL, MetadataSource.GCD_LOCAL}
            or runtime.credential is not None
        )
    }
    return exists().where(
        SeriesExternalIdentity.series_id == Series.id,
        SeriesExternalIdentity.identity_namespace.in_(namespaces),
        SeriesExternalIdentity.verification_state == IdentityVerificationState.VERIFIED,
    )


@dataclass(frozen=True)
class ScheduledSeriesRefresh:
    added: int
    search_wanted: bool
    cover_url: str | None
    covers: Path
    outcomes: tuple[SourceOutcome, ...] = ()


async def refresh_scheduled_series(
    session: AsyncSession, series_id: int, *, registry: MetadataSourceRegistry | None = None
) -> ScheduledSeriesRefresh:
    """Fill gaps and synchronize a complete catalog; the task commits progress too."""
    covers = await resolve_covers_dir(session)
    result = await refresh_series_catalog_from_sources(
        session, series_id, registry=registry, replace_managed=False
    )
    wanted = (
        await session.scalar(
            select(Issue.id)
            .where(
                Issue.series_id == series_id,
                Issue.id.in_(result.created_issue_ids),
                Issue.status == IssueStatus.WANTED,
            )
            .limit(1)
        )
        if result.created_issue_ids
        else None
    )
    return ScheduledSeriesRefresh(
        len(result.created_issue_ids),
        result.series.monitored and wanted is not None,
        result.series.cover_url,
        covers,
        result.outcomes,
    )
