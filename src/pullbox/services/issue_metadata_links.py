"""Reviewed issue links require a verified series parent and never touch files."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import NotFoundError
from pullbox.core.issue_numbers import normalize_issue_number_text
from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import IdentityVerificationAction as Action
from pullbox.core.metadata_identity_state import IdentityVerificationState as State
from pullbox.models import Issue, Series
from pullbox.models.metadata_identity import IssueIdentityEvent
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.series import IssueCatalogState
from pullbox.schemas.issue_metadata_links import (
    IssueCandidatesQuery,
    IssueLinkCurrent,
    IssueLinkPreview,
    IssueLinkQuery,
    IssueLinkSource,
    IssueLinksRead,
)
from pullbox.schemas.metadata_identity_review import IdentityReviewRead, IdentityReviewReceiptRead
from pullbox.schemas.metadata_sources import (
    MetadataFetch,
    MetadataPage,
    ProviderIssueRead,
    SourceCapability,
    SourceStatus,
)
from pullbox.schemas.series_metadata_links import SeriesLinkConfirm, SeriesLinkIdentity
from pullbox.services.metadata_assembly import assemble_metadata
from pullbox.services.metadata_discovery import MetadataSourceRegistry, describe_source_policies
from pullbox.services.metadata_identity_review import (
    apply_identity_review,
    preview_identity_review,
    record_identity_observation,
)
from pullbox.services.metadata_read_cache import source_read_cache
from pullbox.services.metadata_series_refresh_state import (
    ISSUE_FIELDS,
    SeriesRefreshState,
    read_series_refresh_state,
)
from pullbox.services.metadata_sources import load_source_runtime
from pullbox.services.metadata_writer_identity import metadata_write_scope
from pullbox.services.series_metadata_links import SOURCE_LABELS


@dataclass(frozen=True)
class IssueLinkContext:
    current: IssueLinkCurrent
    parents: tuple[tuple[ExternalIdentityRef, State | None, int], ...]
    other_claims: tuple[tuple[str, str, str, int], ...]


async def _state(session: AsyncSession, issue_id: int) -> SeriesRefreshState:
    series_id = await session.scalar(select(Issue.series_id).where(Issue.id == issue_id))
    if series_id is None:
        raise NotFoundError("Issue", issue_id)
    return await read_series_refresh_state(
        session, series_id, issue_ids=(issue_id,), allow_partial_catalog=True
    )


async def _context(
    session: AsyncSession, issue_id: int, source: MetadataSource
) -> IssueLinkContext:
    state = await _state(session, issue_id)
    return _context_from_state(state, source)


def _context_from_state(state: SeriesRefreshState, source: MetadataSource) -> IssueLinkContext:
    issue = state.issues[0]
    claims = tuple(
        (ref.namespace.value, ref.external_id, status.value if status else "unknown", revision)
        for ref, status, revision in issue.claims
        if ref.namespace is not source.identity_namespace
    )
    if issue.comicvine_id and source.identity_namespace is not IdentityNamespace.COMICVINE:
        claims += (("comicvine", str(issue.comicvine_id), "legacy", 0),)
    return IssueLinkContext(
        IssueLinkCurrent(
            series_id=state.series.local_id,
            series_title=state.series.values.title or "Untitled series",
            issue_number_text=issue.values.issue_number_text or "",
            title=issue.values.title,
            cover_date=issue.values.cover_date,
            page_count=issue.values.page_count,
        ),
        state.series.claims,
        claims,
    )


def _parent(context: IssueLinkContext, source: MetadataSource) -> ExternalIdentityRef:
    for ref, state, _ in context.parents:
        if ref.namespace is source.identity_namespace and state is State.VERIFIED:
            return ref
    raise ValueError("Link this provider to the series first, then review this issue match.")


def _revision(context: IssueLinkContext, source: MetadataSource, revision: int) -> str:
    # Only a review freshness digest, never authentication or password storage.
    payload = {
        "purpose": "issue-link-v1",
        "current": context.current.model_dump(mode="json"),
        "parents": [
            (ref.namespace.value, ref.external_id, state, rev)
            for ref, state, rev in context.parents
        ],
        "other_claims": context.other_claims,
        "source": source.value,
        "revision": revision,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


async def _lock(session: AsyncSession, context: IssueLinkContext, issue_id: int) -> None:
    await session.execute(
        select(MetadataSourceConfig)
        .order_by(MetadataSourceConfig.source)
        .with_for_update(read=True)
    )
    await session.execute(
        select(Series.id).where(Series.id == context.current.series_id).with_for_update()
    )
    await session.execute(select(Issue.id).where(Issue.id == issue_id).with_for_update())


async def _registry(session: AsyncSession, *, gcd_api_enabled: bool) -> MetadataSourceRegistry:
    return MetadataSourceRegistry(
        await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled),
        gcd_api_enabled=gcd_api_enabled,
        read_cache=source_read_cache(session),
    )


async def read_issue_links(
    session: AsyncSession, issue_id: int, *, gcd_api_enabled: bool
) -> IssueLinksRead:
    state = await _state(session, issue_id)
    issue = state.issues[0]
    owned = {ref.namespace for ref, _, _ in issue.claims}
    parents = {ref.namespace: ref for ref in state.series.identities}
    providers = describe_source_policies(list(state.policies), gcd_api_enabled=gcd_api_enabled)
    available = [
        item
        for item in providers
        if item.enabled
        and item.availability is None
        and SourceCapability.ISSUE_DETAILS in item.capabilities
    ]
    syncing = (
        await session.scalar(
            select(Series.issue_catalog_state).where(Series.id == state.series.local_id)
        )
        is IssueCatalogState.HYDRATING
    )
    snapshot = assemble_metadata(
        MetadataEntityKind.ISSUE,
        issue.identities,
        [],
        list(state.policies),
        now=datetime.now(UTC),
        current=issue.values,
        previous=issue.baseline,
        fields=ISSUE_FIELDS,
        parent_identities=state.series.identities,
    )
    return IssueLinksRead(
        current=_context_from_state(state, MetadataSource.METRON_API).current,
        identities=[
            SeriesLinkIdentity(namespace=ref.namespace, external_id=ref.external_id, state=status)
            for ref, status, _ in issue.claims
            if status is not None
        ],
        sources=[
            IssueLinkSource(
                source=item.source,
                label=SOURCE_LABELS[item.source],
                series_external_id=parents[item.source.identity_namespace].external_id,
            )
            for item in available
            if item.source.identity_namespace in parents
            and item.source.identity_namespace not in owned
            and SourceCapability.ISSUE_LIST in item.capabilities
        ],
        origins=list(snapshot.origins),
        syncing=syncing,
        can_refresh=not syncing
        and any(
            item.source.identity_namespace in {ref.namespace for ref in issue.identities}
            and item.source.identity_namespace in parents
            for item in available
        ),
    )


async def issue_link_candidates(
    session: AsyncSession, issue_id: int, query: IssueCandidatesQuery, *, gcd_api_enabled: bool
) -> MetadataFetch[MetadataPage[ProviderIssueRead]]:
    context = await _context(session, issue_id, query.source)
    parent = _parent(context, query.source)
    registry = await _registry(session, gcd_api_enabled=gcd_api_enabled)
    revision = registry.runtime[query.source].policy.revision
    await session.rollback()
    result = await registry.issues(query.source, parent.external_id, page=query.page)
    current = await _context(session, issue_id, query.source)
    updated = await _registry(session, gcd_api_enabled=gcd_api_enabled)
    if current != context or updated.runtime[query.source].policy.revision != revision:
        raise ValueError("The issue or source settings changed. Load the provider issues again.")
    return result


async def preview_issue_link(
    session: AsyncSession, issue_id: int, query: IssueLinkQuery, *, gcd_api_enabled: bool
) -> IssueLinkPreview:
    context = await _context(session, issue_id, query.source)
    parent = _parent(context, query.source)
    registry = await _registry(session, gcd_api_enabled=gcd_api_enabled)
    revision = registry.runtime[query.source].policy.revision
    await session.rollback()
    result = await registry.issue(query.source, query.external_id)
    if result.status is not SourceStatus.OK or result.data is None:
        raise ValueError(
            "This provider could not load the issue. Check Metadata settings or try again later."
        )
    candidate = result.data
    if (
        candidate.source is not query.source
        or candidate.identity_namespace is not query.source.identity_namespace
        or candidate.external_id != query.external_id
        or candidate.series_external_id != parent.external_id
        or normalize_issue_number_text(candidate.issue_number_text)
        != context.current.issue_number_text
    ):
        raise ValueError(
            "This provider issue disagrees with the series or exact issue number. "
            "Choose another match; no links were changed."
        )
    if any(
        cross.entity_kind is not MetadataEntityKind.ISSUE
        or any(
            namespace == cross.namespace.value and external_id != cross.external_id
            for namespace, external_id, *_ in context.other_claims
        )
        for cross in candidate.cross_identities
    ):
        raise ValueError(
            "This provider identifies a different issue than your existing match. "
            "No links were changed."
        )
    async with metadata_write_scope(session):
        await _lock(session, context, issue_id)
        updated = await _registry(session, gcd_api_enabled=gcd_api_enabled)
        if (
            updated.runtime[query.source].policy.revision != revision
            or updated._unavailable(query.source, capability=SourceCapability.ISSUE_DETAILS)
            or context != await _context(session, issue_id, query.source)
        ):
            raise ValueError("The issue or source settings changed. Preview the match again.")
        identity = ExternalIdentityRef(
            query.source.identity_namespace, MetadataEntityKind.ISSUE, query.external_id
        )
        saved = await record_identity_observation(
            session,
            IdentityEventRequest(
                uuid4(),
                issue_id,
                Action.OBSERVE,
                IdentityEventEvidence(
                    ExactIdentityEvidence(
                        identity, IdentityEvidenceKind.PROVIDER_RESULT, query.source
                    ),
                    _revision(context, query.source, revision),
                    source_identity=identity,
                    parent_identity=parent,
                ),
            ),
        )
        review = IdentityReviewRead.model_validate(
            await preview_identity_review(
                session, MetadataEntityKind.ISSUE, issue_id, saved.event_id
            )
        )
    return IssueLinkPreview(
        current=context.current, candidate=candidate, source_revision=revision, review=review
    )


async def confirm_issue_link(
    session: AsyncSession,
    issue_id: int,
    body: SeriesLinkConfirm,
    *,
    user_id: int,
    gcd_api_enabled: bool,
) -> IdentityReviewReceiptRead:
    state = await _state(session, issue_id)
    context = _context_from_state(state, MetadataSource.METRON_API)
    async with metadata_write_scope(session):
        await _lock(session, context, issue_id)
        event = await session.get(IssueIdentityEvent, body.event_id)
        if event is None or event.issue_id != issue_id:
            raise ValueError("This saved match no longer exists. Preview it again.")
        try:
            evidence = json.loads(event.request_json)
            source = MetadataSource(evidence["origin"]["source_instance"])
            digest = evidence["evidence_revision"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError("Preview this provider match before linking it.") from exc
        current = await _context(session, issue_id, source)
        registry = await _registry(session, gcd_api_enabled=gcd_api_enabled)
        _parent(current, source)
        if (
            state.series.local_id != current.current.series_id
            or registry.runtime[source].policy.revision != body.source_revision
            or registry._unavailable(source, capability=SourceCapability.ISSUE_DETAILS)
            or digest != _revision(current, source, body.source_revision)
        ):
            raise ValueError("The issue or source settings changed. Preview the match again.")
        receipt = await apply_identity_review(
            session,
            MetadataEntityKind.ISSUE,
            issue_id,
            body.event_id,
            action=Action.CONFIRM,
            fingerprint=body.fingerprint,
            review_revision=body.review_revision,
            actor_user_id=user_id,
        )
    return IdentityReviewReceiptRead(event_id=receipt.event_id, replayed=receipt.replayed)
