"""Provider identity primitives; no provider execution or persistence policy."""

from __future__ import annotations

import enum
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


class IdentityNamespace(enum.StrEnum):
    """Provider namespaces, independent of transport and configuration."""

    COMICVINE = "comicvine"
    METRON = "metron"
    GCD = "gcd"
    LOCG = "locg"


class MetadataSource(enum.StrEnum):
    """Known source names, not a registry of enabled providers."""

    COMICVINE_LOCAL = "comicvine_local"
    COMICVINE_API = "comicvine_api"
    METRON_API = "metron_api"
    GCD_LOCAL = "gcd_local"
    GCD_API_V2 = "gcd_api_v2"

    @property
    def identity_namespace(self) -> IdentityNamespace:
        return _SOURCE_NAMESPACES[self]


_SOURCE_NAMESPACES = {
    MetadataSource.COMICVINE_LOCAL: IdentityNamespace.COMICVINE,
    MetadataSource.COMICVINE_API: IdentityNamespace.COMICVINE,
    MetadataSource.METRON_API: IdentityNamespace.METRON,
    MetadataSource.GCD_LOCAL: IdentityNamespace.GCD,
    MetadataSource.GCD_API_V2: IdentityNamespace.GCD,
}


class MetadataEntityKind(enum.StrEnum):
    """Kinds of canonical metadata entities, not release variants."""

    SERIES = "series"
    ISSUE = "issue"
    STORY_ARC = "story_arc"


class IdentityEvidenceKind(enum.StrEnum):
    """Origin of an identity assertion; origin alone does not establish trust."""

    LEGACY_BACKFILL = "legacy_backfill"
    MYLAR_DATABASE = "mylar_database"
    SERIES_JSON = "series_json"
    COMICINFO_XML = "comicinfo_xml"
    METRONINFO_XML = "metroninfo_xml"
    PROVIDER_RESULT = "provider_result"
    PROVIDER_CROSSWALK = "provider_crosswalk"
    USER_SELECTION = "user_selection"
    MIGRATION = "migration"


@dataclass(frozen=True)
class ExternalIdentityRef:
    """Canonical numeric provider key, not an issue designation or release ID.

    These keys do not establish cross-provider sameness or verification. Adapters
    must validate provider URLs and extract their IDs before constructing a key.
    LOCG release/variant IDs belong to discovery references, not canonical issues.
    """

    namespace: IdentityNamespace
    entity_kind: MetadataEntityKind
    external_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, IdentityNamespace):
            raise ValueError("Unknown identity namespace")
        if not isinstance(self.entity_kind, MetadataEntityKind):
            raise ValueError("Unknown metadata entity kind")
        if not isinstance(self.external_id, str) or len(self.external_id) > 255:
            raise ValueError("Invalid external ID: expected bounded decimal text")
        value = self.external_id.strip(" ")
        if not value.isascii() or not value.isdecimal() or not value.lstrip("0"):
            raise ValueError("Invalid external ID: expected a positive decimal ID")
        object.__setattr__(self, "external_id", value.lstrip("0"))


@dataclass(frozen=True)
class ExactIdentityEvidence:
    """An exact-ID assertion, not proof of trust or authorization to attach it."""

    identity: ExternalIdentityRef
    evidence_kind: IdentityEvidenceKind
    source_instance: MetadataSource | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.evidence_kind, IdentityEvidenceKind):
            raise ValueError("Unknown identity evidence kind")
        if self.source_instance is not None and not isinstance(
            self.source_instance, MetadataSource
        ):
            raise ValueError("Unknown metadata source instance")


@dataclass(frozen=True)
class ExactIdentityConflict:
    """Competing IDs for one proposed target, with their evidence retained."""

    namespace: IdentityNamespace
    entity_kind: MetadataEntityKind
    evidence: tuple[ExactIdentityEvidence, ...]


def find_exact_identity_conflicts(
    evidence: Iterable[ExactIdentityEvidence],
) -> tuple[ExactIdentityConflict, ...]:
    """Find same-provider disagreement for one target; never select a winner.

    Call once per proposed local target, not across a scan or search result list.
    No conflicts does not mean the assertions are verified, linked, unowned, or
    safe to attach. Persistence and crosswalk resolution enforce those separately.
    """
    grouped: dict[tuple[IdentityNamespace, MetadataEntityKind], set[ExactIdentityEvidence]] = (
        defaultdict(set)
    )
    for observation in evidence:
        identity = observation.identity
        grouped[(identity.namespace, identity.entity_kind)].add(observation)

    return tuple(
        ExactIdentityConflict(namespace, kind, tuple(sorted(observations, key=_evidence_sort_key)))
        for (namespace, kind), observations in sorted(grouped.items())
        if len({item.identity.external_id for item in observations}) > 1
    )


def _evidence_sort_key(evidence: ExactIdentityEvidence) -> tuple[str, str, str]:
    return (
        evidence.identity.external_id,
        evidence.evidence_kind.value,
        evidence.source_instance.value if evidence.source_instance is not None else "",
    )
