"""Explicit series links reuse saved claims; they never adopt issues or touch files."""

import hashlib
import json
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.exceptions import NotFoundError
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
from pullbox.models import Series
from pullbox.models.metadata_identity import SeriesExternalIdentity, SeriesIdentityEvent
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.publisher import Publisher
from pullbox.schemas.metadata_identity_review import IdentityReviewRead, IdentityReviewReceiptRead
from pullbox.schemas.metadata_sources import SeriesPreviewQuery, SourceCapability, SourceStatus
from pullbox.schemas.series_metadata_links import (
    SeriesLinkConfirm,
    SeriesLinkCurrent,
    SeriesLinkIdentity,
    SeriesLinkPreview,
    SeriesLinkSource,
    SeriesLinksRead,
)
from pullbox.services.metadata_discovery import MetadataSourceRegistry, describe_source_policies
from pullbox.services.metadata_identity_review import (
    apply_identity_review,
    preview_identity_review,
    record_identity_observation,
)
from pullbox.services.metadata_read_cache import source_read_cache
from pullbox.services.metadata_sources import load_source_runtime, read_source_policies
from pullbox.services.metadata_writer_identity import metadata_write_scope

SOURCE_LABELS = {
    MetadataSource.COMICVINE_LOCAL: "ComicVine Local",
    MetadataSource.COMICVINE_API: "ComicVine API",
    MetadataSource.METRON_API: "Metron",
    MetadataSource.GCD_LOCAL: "GCD Local",
    MetadataSource.GCD_API_V2: "GCD API v2",
}


async def _current(session: AsyncSession, series_id: int) -> SeriesLinkCurrent:
    row = (
        await session.execute(
            select(Series.title, Series.year_start, Publisher.name, Series.issue_count)
            .outerjoin(Publisher, Publisher.id == Series.publisher_id)
            .where(Series.id == series_id)
        )
    ).one_or_none()
    if row is None:
        raise NotFoundError("Series", series_id)
    return SeriesLinkCurrent(
        title=row.title, year_start=row.year_start, publisher=row.name, issue_count=row.issue_count
    )


async def read_series_links(
    session: AsyncSession, series_id: int, *, gcd_api_enabled: bool
) -> SeriesLinksRead:
    current = await _current(session, series_id)
    identities = [
        SeriesLinkIdentity(
            namespace=item.identity_namespace,
            external_id=item.external_id,
            state=item.verification_state,
        )
        for item in await session.scalars(
            select(SeriesExternalIdentity)
            .where(SeriesExternalIdentity.series_id == series_id)
            .order_by(SeriesExternalIdentity.identity_namespace)
        )
    ]
    owned = {item.namespace for item in identities}
    sources = describe_source_policies(
        await read_source_policies(session), gcd_api_enabled=gcd_api_enabled
    )
    return SeriesLinksRead(
        current=current,
        identities=identities,
        sources=[
            SeriesLinkSource(source=item.source, label=SOURCE_LABELS[item.source])
            for item in sources
            if item.enabled
            and item.availability is None
            and item.source.identity_namespace not in owned
            and SourceCapability.SERIES_SEARCH in item.capabilities
        ],
    )


async def _other_claims(
    session: AsyncSession, series_id: int, source: MetadataSource
) -> list[tuple[str, str, str, int]]:
    claims = [
        (row.identity_namespace.value, row.external_id, row.verification_state.value, row.revision)
        for row in await session.scalars(
            select(SeriesExternalIdentity)
            .where(
                SeriesExternalIdentity.series_id == series_id,
                SeriesExternalIdentity.identity_namespace != source.identity_namespace,
            )
            .order_by(SeriesExternalIdentity.identity_namespace)
        )
    ]
    legacy = await session.scalar(select(Series.comicvine_id).where(Series.id == series_id))
    if legacy and source.identity_namespace is not IdentityNamespace.COMICVINE:
        claims.append((IdentityNamespace.COMICVINE.value, str(legacy), "legacy", 0))
    return claims


def _revision(
    current: SeriesLinkCurrent,
    source: MetadataSource,
    revision: int,
    claims: list[tuple[str, str, str, int]],
) -> str:
    # A review-freshness digest, never an authorization token or credential.
    payload = {
        "purpose": "series-link-v1",
        "current": current.model_dump(),
        "source": source.value,
        "revision": revision,
        "other_claims": claims,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


async def _lock(session: AsyncSession, series_id: int) -> None:
    await session.execute(
        select(MetadataSourceConfig)
        .order_by(MetadataSourceConfig.source)
        .with_for_update(read=True)
    )
    await session.execute(select(Series.id).where(Series.id == series_id).with_for_update())


async def preview_series_link(
    session: AsyncSession, series_id: int, query: SeriesPreviewQuery, *, gcd_api_enabled: bool
) -> SeriesLinkPreview:
    current = await _current(session, series_id)
    runtime = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
    registry = MetadataSourceRegistry(
        runtime, gcd_api_enabled=gcd_api_enabled, read_cache=source_read_cache(session)
    )
    source = registry.runtime[query.source]
    revision = source.policy.revision
    claims = await _other_claims(session, series_id, query.source)
    await session.rollback()
    result = await registry.series(query.source, query.external_id)
    if result.status is not SourceStatus.OK or result.data is None:
        raise ValueError(
            "This provider could not load the series. Check Metadata settings or try again later."
        )
    profile = result.data
    if profile.source is not query.source or profile.external_id != query.external_id:
        raise ValueError("The provider returned a different series. Search again before linking.")
    if any(
        cross.entity_kind is not MetadataEntityKind.SERIES
        or any(
            namespace == cross.namespace.value and external_id != cross.external_id
            for namespace, external_id, *_ in claims
        )
        for cross in profile.cross_identities
    ):
        raise ValueError(
            "This provider identifies a different series than your existing match. "
            "Choose another result; no links were changed."
        )
    async with metadata_write_scope(session):
        await _lock(session, series_id)
        refreshed = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
        updated = MetadataSourceRegistry(refreshed, gcd_api_enabled=gcd_api_enabled)
        if (
            updated.runtime[query.source].policy.revision != revision
            or updated._unavailable(query.source, capability=SourceCapability.SERIES_DETAILS)
            or current != await _current(session, series_id)
            or claims != await _other_claims(session, series_id, query.source)
        ):
            raise ValueError("The series or source settings changed. Search again before linking.")
        identity = ExternalIdentityRef(
            query.source.identity_namespace, MetadataEntityKind.SERIES, query.external_id
        )
        saved = await record_identity_observation(
            session,
            IdentityEventRequest(
                uuid4(),
                series_id,
                Action.OBSERVE,
                IdentityEventEvidence(
                    ExactIdentityEvidence(
                        identity, IdentityEvidenceKind.PROVIDER_RESULT, query.source
                    ),
                    _revision(current, query.source, revision, claims),
                    source_identity=identity,
                ),
            ),
        )
        review = IdentityReviewRead.model_validate(
            await preview_identity_review(
                session, MetadataEntityKind.SERIES, series_id, saved.event_id
            )
        )
    return SeriesLinkPreview(
        current=current, candidate=profile, source_revision=revision, review=review
    )


async def confirm_series_link(
    session: AsyncSession,
    series_id: int,
    body: SeriesLinkConfirm,
    *,
    user_id: int,
    gcd_api_enabled: bool,
) -> IdentityReviewReceiptRead:
    async with metadata_write_scope(session):
        await _lock(session, series_id)
        event = await session.get(SeriesIdentityEvent, body.event_id)
        if event is None or event.series_id != series_id:
            raise ValueError("This saved match no longer exists. Search again before linking.")
        try:
            evidence = json.loads(event.request_json)
            source = MetadataSource(evidence["origin"]["source_instance"])
            digest = evidence["evidence_revision"]
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError("Search and preview this provider match before linking it.") from exc
        current = await _current(session, series_id)
        claims = await _other_claims(session, series_id, source)
        runtime = await load_source_runtime(session, gcd_api_enabled=gcd_api_enabled)
        registry = MetadataSourceRegistry(runtime, gcd_api_enabled=gcd_api_enabled)
        if (
            registry.runtime[source].policy.revision != body.source_revision
            or registry._unavailable(source, capability=SourceCapability.SERIES_DETAILS)
            or digest != _revision(current, source, body.source_revision, claims)
        ):
            raise ValueError("The series or source settings changed. Search again before linking.")
        receipt = await apply_identity_review(
            session,
            MetadataEntityKind.SERIES,
            series_id,
            body.event_id,
            action=Action.CONFIRM,
            fingerprint=body.fingerprint,
            review_revision=body.review_revision,
            actor_user_id=user_id,
        )
    return IdentityReviewReceiptRead(event_id=receipt.event_id, replayed=receipt.replayed)
