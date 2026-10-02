"""Source-bound progress for catalog synchronization, separate from provenance."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind, MetadataSource
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Series, SeriesCatalogCheckpoint
from pullbox.models.metadata_identity import SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.services.metadata_writer_identity import metadata_write_scope


class CatalogCheckpointConflictError(ValueError):
    """The source, identity or previous catalog checkpoint changed."""


@dataclass(frozen=True)
class CatalogCheckpoint:
    series_id: int
    source: MetadataSource
    source_revision: int
    identity_id: int
    identity_revision: int
    external_id: str
    revision: int
    checked_at: datetime
    full_synced_at: datetime
    source_updated_at: datetime | None


def _snapshot(row: SeriesCatalogCheckpoint) -> CatalogCheckpoint:
    return CatalogCheckpoint(
        row.series_id,
        MetadataSource(row.source),
        row.source_revision,
        row.identity_id,
        row.identity_revision,
        row.external_id,
        row.revision,
        row.checked_at,
        row.full_synced_at,
        row.source_updated_at,
    )


async def read_catalog_checkpoints(
    session: AsyncSession, series_id: int
) -> tuple[CatalogCheckpoint, ...]:
    """Include invalidated rows so optimistic refresh read sets detect changes."""
    return tuple(
        _snapshot(row)
        for row in await session.scalars(
            select(SeriesCatalogCheckpoint)
            .where(SeriesCatalogCheckpoint.series_id == series_id)
            .order_by(SeriesCatalogCheckpoint.source)
            .execution_options(populate_existing=True)
        )
    )


async def load_catalog_checkpoint(
    session: AsyncSession, series_id: int, source: MetadataSource
) -> CatalogCheckpoint | None:
    """Return progress only while its exact source and identity binding is current.

    Local readers must additionally compare the window's generation before
    applying it. A changed generation requires another full synchronization.
    """
    row = await session.scalar(
        select(SeriesCatalogCheckpoint)
        .join(MetadataSourceConfig, MetadataSourceConfig.source == SeriesCatalogCheckpoint.source)
        .join(
            SeriesExternalIdentity, SeriesExternalIdentity.id == SeriesCatalogCheckpoint.identity_id
        )
        .where(
            SeriesCatalogCheckpoint.series_id == series_id,
            SeriesCatalogCheckpoint.source == source.value,
            MetadataSourceConfig.enabled.is_(True),
            MetadataSourceConfig.revision == SeriesCatalogCheckpoint.source_revision,
            SeriesExternalIdentity.series_id == series_id,
            SeriesExternalIdentity.identity_namespace == source.identity_namespace,
            SeriesExternalIdentity.external_id == SeriesCatalogCheckpoint.external_id,
            SeriesExternalIdentity.revision == SeriesCatalogCheckpoint.identity_revision,
            SeriesExternalIdentity.verification_state == IdentityVerificationState.VERIFIED,
        )
        .execution_options(populate_existing=True)
    )
    return _snapshot(row) if row else None


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CatalogCheckpointConflictError(
            "Catalog progress requires an aware request timestamp."
        )
    return value.astimezone(UTC)


async def save_full_catalog_checkpoint(
    session: AsyncSession,
    series_id: int,
    *,
    source: MetadataSource,
    source_revision: int,
    identity_revision: int,
    external_id: str,
    started_at: datetime,
    source_updated_at: datetime | None = None,
    expected_revision: int = 0,
) -> CatalogCheckpoint:
    """Save only after a complete catalog is applied, in the caller's transaction.

    Never call with a publication slice or truncated modification window. The
    boundary is the live request start, not the time the final page was saved.
    """
    return await _save_catalog_checkpoint(
        session,
        series_id,
        source=source,
        source_revision=source_revision,
        identity_revision=identity_revision,
        external_id=external_id,
        started_at=started_at,
        source_updated_at=source_updated_at,
        expected_revision=expected_revision,
    )


async def advance_catalog_checkpoint(
    session: AsyncSession,
    previous: CatalogCheckpoint,
    *,
    started_at: datetime,
    source_updated_at: datetime | None = None,
) -> CatalogCheckpoint:
    """Advance a fully applied bounded window without resetting its full-sync date.

    The caller must fall back to full reconciliation for truncated modification
    windows or changed local generations, and commit its issue writes atomically.
    """
    return await _save_catalog_checkpoint(
        session,
        previous.series_id,
        source=previous.source,
        source_revision=previous.source_revision,
        identity_revision=previous.identity_revision,
        external_id=previous.external_id,
        started_at=started_at,
        source_updated_at=source_updated_at,
        expected_revision=previous.revision,
        previous=previous,
    )


async def _save_catalog_checkpoint(
    session: AsyncSession,
    series_id: int,
    *,
    source: MetadataSource,
    source_revision: int,
    identity_revision: int,
    external_id: str,
    started_at: datetime,
    source_updated_at: datetime | None,
    expected_revision: int,
    previous: CatalogCheckpoint | None = None,
) -> CatalogCheckpoint:
    if (
        not isinstance(source, MetadataSource)
        or type(source_revision) is not int
        or source_revision <= 0
        or type(identity_revision) is not int
        or identity_revision <= 0
        or type(expected_revision) is not int
        or expected_revision < 0
    ):
        raise CatalogCheckpointConflictError("Invalid catalog source or revision.")
    identity = ExternalIdentityRef(
        source.identity_namespace, MetadataEntityKind.SERIES, external_id
    )
    if identity.external_id != external_id:
        raise CatalogCheckpointConflictError("Catalog progress requires an exact identity.")
    started_at = _utc(started_at)
    if started_at > datetime.now(UTC):
        raise CatalogCheckpointConflictError("Catalog progress cannot start in the future.")
    local = source in {MetadataSource.COMICVINE_LOCAL, MetadataSource.GCD_LOCAL}
    if local and source_updated_at is None:
        raise CatalogCheckpointConflictError("Local catalog progress requires its generation.")
    generation = _utc(source_updated_at) if local and source_updated_at else None
    try:
        async with metadata_write_scope(session):
            config = await session.scalar(
                select(MetadataSourceConfig)
                .where(MetadataSourceConfig.source == source.value)
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
            parent = await session.scalar(
                select(Series.id).where(Series.id == series_id).with_for_update()
            )
            claim = await session.scalar(
                select(SeriesExternalIdentity)
                .where(
                    SeriesExternalIdentity.series_id == series_id,
                    SeriesExternalIdentity.identity_namespace == source.identity_namespace,
                )
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
            if (
                parent is None
                or config is None
                or not config.enabled
                or config.revision != source_revision
                or claim is None
                or claim.external_id != external_id
                or claim.revision != identity_revision
                or claim.verification_state is not IdentityVerificationState.VERIFIED
            ):
                raise CatalogCheckpointConflictError("Catalog source or series identity changed.")
            row = await session.scalar(
                select(SeriesCatalogCheckpoint)
                .where(
                    SeriesCatalogCheckpoint.series_id == series_id,
                    SeriesCatalogCheckpoint.source == source.value,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (row.revision if row else 0) != expected_revision:
                raise CatalogCheckpointConflictError(
                    "Catalog progress changed. Read the catalog again."
                )
            if row is not None and started_at < row.checked_at:
                raise CatalogCheckpointConflictError(
                    "An older catalog cannot replace newer progress."
                )
            if previous is not None and (
                row is None
                or _snapshot(row) != previous
                or generation != previous.source_updated_at
            ):
                raise CatalogCheckpointConflictError(
                    "Catalog progress or generation changed. Read the full catalog again."
                )
            if row is None:
                row = SeriesCatalogCheckpoint(series_id=series_id, source=source.value)
                session.add(row)
            row.source_revision = source_revision
            row.identity_id, row.identity_revision = claim.id, claim.revision
            row.external_id = external_id
            row.revision = expected_revision + 1
            row.checked_at = started_at
            row.full_synced_at = previous.full_synced_at if previous else started_at
            row.source_updated_at = generation
            await session.flush()
            return _snapshot(row)
    except IntegrityError as exc:
        raise CatalogCheckpointConflictError(
            "Catalog progress changed. Read the catalog again."
        ) from exc
