"""Retain cross-provider observations without silently acquiring foreign ownership."""

from collections import defaultdict
from collections.abc import Iterator
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import IdentityVerificationAction
from pullbox.schemas.metadata_sources import (
    ProviderIssueRead,
    ProviderSeriesRead,
    ProviderStoryArcRead,
)
from pullbox.services.metadata_arc_catalog import source_record_identities
from pullbox.services.metadata_identity_review import record_identity_observation
from pullbox.services.story_arc_catalog_identity import catalog_owners
from pullbox.services.story_arc_catalog_types import StoryArcCatalogError, StoryArcCatalogPreview


def _groups(
    preview: StoryArcCatalogPreview,
) -> Iterator[
    tuple[
        MetadataEntityKind,
        tuple[ProviderIssueRead | ProviderSeriesRead | ProviderStoryArcRead, ...],
    ]
]:
    evidence = preview.source_evidence
    if evidence is not None:
        yield MetadataEntityKind.STORY_ARC, (evidence.arc,)
        yield MetadataEntityKind.SERIES, evidence.series
        yield MetadataEntityKind.ISSUE, evidence.issues


async def require_crosswalk_ownership(
    session: AsyncSession, preview: StoryArcCatalogPreview
) -> None:
    """Unverified foreign ownership requires review, never duplicate creation."""
    for kind, records in _groups(preview):
        native = await catalog_owners(
            session, kind, [row.external_id for row in records], preview.source
        )
        claims: dict[IdentityNamespace, dict[str, str]] = defaultdict(dict)
        for row in records:
            for identity in source_record_identities(row, kind)[1:]:
                previous = claims[identity.namespace].setdefault(
                    identity.external_id, row.external_id
                )
                if previous != row.external_id:
                    raise StoryArcCatalogError(
                        "identity_conflict",
                        "Provider cross-identities repeat a target; review the match",
                    )
        for namespace, mapping in claims.items():
            owners = await catalog_owners(session, kind, list(mapping), namespace)
            if any(native.get(mapping[external]) != owner for external, owner in owners.items()):
                raise StoryArcCatalogError(
                    "identity_conflict",
                    "A provider cross-identity already has a library owner; review the match",
                )


async def record_catalog_crosswalks(session: AsyncSession, preview: StoryArcCatalogPreview) -> None:
    for kind, records in _groups(preview):
        targets = await catalog_owners(
            session, kind, [row.external_id for row in records], preview.source
        )
        for row in records:
            identities = source_record_identities(row, kind)
            for identity in identities[1:]:
                request = IdentityEventRequest(
                    uuid5(UUID("e5975ab0-13f6-4e1f-b7af-c3a00ccab88b"), preview.fingerprint),
                    targets[row.external_id],
                    IdentityVerificationAction.OBSERVE,
                    IdentityEventEvidence(
                        ExactIdentityEvidence(
                            identity, IdentityEvidenceKind.PROVIDER_CROSSWALK, preview.source
                        ),
                        preview.fingerprint,
                        source_identity=identities[0],
                    ),
                )
                await record_identity_observation(session, request)
