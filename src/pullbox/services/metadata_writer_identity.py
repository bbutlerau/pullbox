"""Evidence adapters for prefetched ComicVine metadata and local import records."""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import date
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import false, select, update

from pullbox.core.exceptions import ValidationError
from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import (
    IdentityEventEvidence,
    IdentityEventRequest,
    IdentityEvidenceLocator,
    IdentityEvidenceRecordKind,
)
from pullbox.core.metadata_identity_state import (
    IdentityReviewRequiredError,
    IdentityVerificationAction,
    IdentityVerificationState,
)
from pullbox.models import Series
from pullbox.models.import_job import ImportedSeries, ImportJob, ImportSourceType
from pullbox.models.metadata_identity import SeriesExternalIdentity
from pullbox.services.catalog.reader import CatalogIssueSummary, CatalogSeriesMetadata
from pullbox.services.metadata_identity_attachment import (
    IdentityAttachmentConflictError,
    attach_verified_identities,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from pullbox.models import Issue
    from pullbox.providers.base import IssueMetadata, IssueSummary, SeriesMetadata


@dataclass(frozen=True)
class ImportIdentityOrigin:
    record_id: int | None


@dataclass(frozen=True)
class _LocalEvidence:
    kind: IdentityEvidenceKind
    locator: IdentityEvidenceLocator
    snapshot: dict[str, object]


def comicvine_identity(kind: MetadataEntityKind, external_id: str) -> ExternalIdentityRef:
    try:
        identity = ExternalIdentityRef(IdentityNamespace.COMICVINE, kind, external_id)
        if int(identity.external_id) >= 2**63:
            raise ValueError("Out of range")
        return identity
    except ValueError as exc:
        raise ValidationError("Metadata contains an invalid ComicVine identity.") from exc


@asynccontextmanager
async def metadata_write_scope(session: AsyncSession) -> AsyncIterator[None]:
    """Roll back metadata as well as ownership if the caller catches a conflict."""
    if session.get_bind().dialect.name == "sqlite":
        await session.execute(
            update(Series).where(false()).values(comicvine_id=Series.comicvine_id)
        )
    try:
        async with session.begin_nested():
            yield
    except (IdentityAttachmentConflictError, IdentityReviewRequiredError) as exc:
        raise ValidationError(
            "Metadata identity conflicts with an existing match. Review the match before retrying.",
            details={"reason": "metadata_identity_conflict"},
        ) from exc


async def _import_evidence(
    session: AsyncSession, origin: ImportIdentityOrigin | None, parent: ExternalIdentityRef
) -> _LocalEvidence | None:
    if origin is None:
        return None
    if type(origin.record_id) is not int or origin.record_id <= 0:
        raise ValidationError("Import identity evidence requires a saved review record.")
    row = (
        await session.execute(
            select(
                ImportedSeries.id,
                ImportedSeries.import_job_id,
                ImportedSeries.cv_id,
                ImportedSeries.user_selected_cv_id,
                ImportedSeries.cv_match_method,
                ImportJob.source_type,
            )
            .join(ImportJob, ImportJob.id == ImportedSeries.import_job_id)
            .where(ImportedSeries.id == origin.record_id)
        )
    ).one_or_none()
    if row is None or (row.user_selected_cv_id or row.cv_id) != int(parent.external_id):
        raise ValidationError("The saved import match changed. Reload the review before retrying.")
    trusted_mylar = (
        row.source_type == ImportSourceType.MYLAR3
        and row.cv_match_method == "mylar3_cv_id"
        and row.user_selected_cv_id is None
    )
    return _LocalEvidence(
        IdentityEvidenceKind.MYLAR_DATABASE if trusted_mylar else IdentityEvidenceKind.MIGRATION,
        IdentityEvidenceLocator(IdentityEvidenceRecordKind.IMPORTED_SERIES, row.id),
        dict(row._mapping),
    )


def _json_value(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError("Unsupported metadata snapshot value")


def _request(
    operation: UUID,
    local_id: int,
    identity: ExternalIdentityRef,
    metadata: SeriesMetadata | IssueMetadata | IssueSummary | None,
    local: _LocalEvidence | None,
    parent: ExternalIdentityRef | None = None,
) -> IdentityEventRequest:
    snapshot = {
        "metadata": asdict(metadata) if metadata is not None else None,
        "local": local.snapshot if local is not None else None,
    }
    revision = hashlib.sha256(
        json.dumps(
            snapshot,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=_json_value,
        ).encode()
    ).hexdigest()
    source = (
        None
        if local
        else (
            MetadataSource.COMICVINE_LOCAL
            if isinstance(metadata, CatalogSeriesMetadata | CatalogIssueSummary)
            else MetadataSource.COMICVINE_API
        )
    )
    return IdentityEventRequest(
        operation,
        local_id,
        IdentityVerificationAction.VERIFY,
        IdentityEventEvidence(
            ExactIdentityEvidence(
                identity, local.kind if local else IdentityEvidenceKind.PROVIDER_RESULT, source
            ),
            revision,
            locator=local.locator if local else None,
            source_identity=identity if source else None,
            parent_identity=parent,
        ),
    )


async def attach_series_metadata_identity(
    session: AsyncSession,
    series: Series,
    metadata: SeriesMetadata,
    origin: ImportIdentityOrigin | None,
) -> None:
    identity = comicvine_identity(MetadataEntityKind.SERIES, metadata.provider_id)
    local = await _import_evidence(session, origin, identity)
    await attach_verified_identities(
        session, [_request(uuid4(), series.id, identity, metadata, local)]
    )


async def attach_issue_summary_identities(
    session: AsyncSession,
    series: Series,
    members: list[tuple[Issue, IssueSummary]],
    origin: ImportIdentityOrigin | None,
) -> None:
    await _attach_issue_identities(session, series, members, origin)


async def attach_issue_metadata_identity(
    session: AsyncSession,
    series: Series,
    issue: Issue,
    metadata: IssueMetadata,
) -> None:
    parent = comicvine_identity(MetadataEntityKind.SERIES, metadata.series_provider_id)
    identity = comicvine_identity(MetadataEntityKind.ISSUE, metadata.provider_id)
    if series.comicvine_id != int(parent.external_id) or issue.comicvine_id != int(
        identity.external_id
    ):
        raise IdentityAttachmentConflictError("Full issue metadata disagrees with its local target")
    await _attach_issue_identities(session, series, [(issue, metadata)], None)


async def _attach_issue_identities(
    session: AsyncSession,
    series: Series,
    members: Sequence[tuple[Issue, IssueSummary | IssueMetadata]],
    origin: ImportIdentityOrigin | None,
) -> None:
    if not members:
        return
    parent = comicvine_identity(MetadataEntityKind.SERIES, str(series.comicvine_id))
    local = await _import_evidence(session, origin, parent)
    operation = uuid4()
    requests = []
    existing = await session.scalar(
        select(SeriesExternalIdentity).where(
            SeriesExternalIdentity.series_id == series.id,
            SeriesExternalIdentity.identity_namespace == IdentityNamespace.COMICVINE,
        )
    )
    if existing is None:
        # Existing legacy rows need an honest local backfill, not invented API evidence.
        parent_local = local or _LocalEvidence(
            IdentityEvidenceKind.LEGACY_BACKFILL,
            IdentityEvidenceLocator(IdentityEvidenceRecordKind.SERIES, series.id),
            {"id": series.id, "comicvine_id": series.comicvine_id},
        )
        requests.append(_request(operation, series.id, parent, None, parent_local))
    elif (
        existing.external_id != parent.external_id
        or existing.verification_state != IdentityVerificationState.VERIFIED
    ):
        raise IdentityAttachmentConflictError("Issue catalog requires a verified series identity")
    requests.extend(
        _request(
            operation,
            issue.id,
            comicvine_identity(MetadataEntityKind.ISSUE, summary.provider_id),
            summary,
            local,
            parent,
        )
        for issue, summary in members
    )
    await attach_verified_identities(session, requests)
