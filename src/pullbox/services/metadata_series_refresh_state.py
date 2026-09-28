"""Bounded, immutable library read set for source-aware refresh."""

from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import NotFoundError
from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import Issue, Series
from pullbox.models.metadata_baseline import IssueMetadataBaseline, SeriesMetadataBaseline
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesExternalIdentity,
    SeriesIdentityEvent,
)
from pullbox.models.publisher import Publisher
from pullbox.models.series import IssueCatalogState
from pullbox.schemas.metadata_snapshot import MetadataSnapshot, MetadataValues
from pullbox.schemas.metadata_sources import SourcePolicyRead
from pullbox.services.metadata_baselines import MetadataBaselineConflictError
from pullbox.services.metadata_sources import read_source_policies

SERIES_FIELDS = frozenset(
    {
        "title",
        "sort_title",
        "publisher",
        "description",
        "year_start",
        "year_end",
        "series_type",
        "status",
        "issue_count",
        "image_url",
        "volume",
        "language",
    }
)
ISSUE_FIELDS = frozenset(
    {
        "title",
        "description",
        "issue_number_text",
        "cover_date",
        "store_date",
        "page_count",
        "image_url",
    }
)


@dataclass(frozen=True)
class RefreshEntityState:
    local_id: int
    values: MetadataValues
    baseline: MetadataSnapshot | None
    baseline_revision: int
    claims: tuple[tuple[ExternalIdentityRef, IdentityVerificationState, int], ...]
    event_id: int
    comicvine_id: int | None
    overrides: frozenset[str] = frozenset()

    @property
    def identities(self) -> tuple[ExternalIdentityRef, ...]:
        return tuple(
            ref for ref, state, _ in self.claims if state is IdentityVerificationState.VERIFIED
        )


@dataclass(frozen=True)
class SeriesRefreshState:
    series: RefreshEntityState
    issues: tuple[RefreshEntityState, ...]
    policies: tuple[SourcePolicyRead, ...]


def _baseline(payload: str | None, kind: MetadataEntityKind) -> MetadataSnapshot | None:
    if payload is None:
        return None
    try:
        snapshot = MetadataSnapshot.model_validate_json(payload)
        if snapshot.entity_kind is not kind:
            raise ValueError("Wrong baseline kind")
        return snapshot
    except ValueError as exc:
        raise MetadataBaselineConflictError(
            "Stored metadata baseline is invalid. Review before refreshing."
        ) from exc


async def read_series_refresh_state(session: AsyncSession, series_id: int) -> SeriesRefreshState:
    row = (
        await session.execute(
            select(
                Series,
                Publisher.name,
                SeriesMetadataBaseline.revision,
                SeriesMetadataBaseline.snapshot_json,
            )
            .outerjoin(Publisher, Publisher.id == Series.publisher_id)
            .outerjoin(SeriesMetadataBaseline, SeriesMetadataBaseline.series_id == Series.id)
            .where(Series.id == series_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        raise NotFoundError("Series", series_id)
    series, publisher, revision, payload = row
    if series.issue_catalog_state is IssueCatalogState.HYDRATING:
        raise ValueError("Initial metadata sync is already in progress.")
    baseline = _baseline(payload, MetadataEntityKind.SERIES)
    values = baseline.values.model_dump() if baseline else {}
    values.update(
        title=series.title,
        sort_title=series.sort_title,
        publisher=publisher,
        description=series.description,
        year_start=series.year_start,
        year_end=series.year_end,
        series_type=series.series_type.value,
        status=series.status.value,
        issue_count=series.issue_count,
        image_url=series.cover_url,
    )
    claims = tuple(
        (
            ExternalIdentityRef(
                item.identity_namespace, MetadataEntityKind.SERIES, item.external_id
            ),
            item.verification_state,
            item.revision,
        )
        for item in await session.scalars(
            select(SeriesExternalIdentity)
            .where(SeriesExternalIdentity.series_id == series_id)
            .order_by(SeriesExternalIdentity.identity_namespace)
            .execution_options(populate_existing=True)
        )
    )
    event_id = await session.scalar(
        select(func.max(SeriesIdentityEvent.id)).where(SeriesIdentityEvent.series_id == series_id)
    )
    parent = RefreshEntityState(
        series_id,
        MetadataValues.model_validate(values),
        baseline,
        revision or 0,
        claims,
        event_id or 0,
        series.comicvine_id,
        frozenset({"status", "year_end"}) if series.status_override else frozenset(),
    )
    members = (
        await session.execute(
            select(Issue, IssueMetadataBaseline.revision, IssueMetadataBaseline.snapshot_json)
            .outerjoin(IssueMetadataBaseline, IssueMetadataBaseline.issue_id == Issue.id)
            .where(Issue.series_id == series_id)
            .order_by(Issue.id)
            .limit(10001)
            .execution_options(populate_existing=True)
        )
    ).all()
    if len(members) > 10000:
        raise ValueError("This series exceeds the bounded refresh limit of 10,000 issues.")
    issue_claims: dict[int, list[tuple[ExternalIdentityRef, IdentityVerificationState, int]]] = {}
    for claim in await session.scalars(
        select(IssueExternalIdentity)
        .join(Issue, Issue.id == IssueExternalIdentity.issue_id)
        .where(Issue.series_id == series_id)
        .order_by(IssueExternalIdentity.identity_namespace)
        .execution_options(populate_existing=True)
    ):
        issue_claims.setdefault(claim.issue_id, []).append(
            (
                ExternalIdentityRef(
                    claim.identity_namespace, MetadataEntityKind.ISSUE, claim.external_id
                ),
                claim.verification_state,
                claim.revision,
            )
        )
    events = dict(
        (
            await session.execute(
                select(IssueIdentityEvent.issue_id, func.max(IssueIdentityEvent.id))
                .join(Issue, Issue.id == IssueIdentityEvent.issue_id)
                .where(Issue.series_id == series_id)
                .group_by(IssueIdentityEvent.issue_id)
            )
        )
        .tuples()
        .all()
    )
    issues = []
    for issue, issue_revision, issue_payload in members:
        previous = _baseline(issue_payload, MetadataEntityKind.ISSUE)
        issue_values = previous.values.model_dump() if previous else {}
        issue_values.update(
            title=issue.title,
            description=issue.description,
            issue_number_text=issue.effective_issue_number_text,
            cover_date=issue.release_date,
            store_date=issue.store_date,
            page_count=issue.page_count,
            image_url=issue.cover_url,
        )
        issues.append(
            RefreshEntityState(
                issue.id,
                MetadataValues.model_validate(issue_values),
                previous,
                issue_revision or 0,
                tuple(issue_claims.get(issue.id, ())),
                events.get(issue.id, 0),
                issue.comicvine_id,
            )
        )
    # Connection-test timestamps do not affect authority. Everything that can
    # affect provider selection remains part of the read set, including revision.
    policies = tuple(
        item.model_copy(
            update={"last_tested_at": None, "last_success_at": None, "last_status": None}
        )
        for item in await read_source_policies(session)
    )
    return SeriesRefreshState(parent, tuple(issues), policies)
