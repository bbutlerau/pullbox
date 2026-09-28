"""Assemble one identity-bound snapshot before any database or archive mutation."""

from collections.abc import Sequence
from datetime import UTC, datetime

from pullbox.core.issue_numbers import parse_issue_number_text
from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_snapshot import (
    FieldOrigin,
    MetadataSnapshot,
    MetadataValues,
    field_domain,
)
from pullbox.schemas.metadata_sources import (
    MetadataDomain,
    ProviderIssueRead,
    ProviderSeriesRead,
    ProviderStoryArcRead,
    SourcePolicyRead,
)
from pullbox.services.provider_artwork import allowed_artwork_url

type MetadataCandidate = ProviderSeriesRead | ProviderIssueRead | ProviderStoryArcRead


class MetadataAssemblyError(ValueError):
    """Evidence cannot safely describe one canonical entity."""


def _missing(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _identity_map(
    identities: Sequence[ExternalIdentityRef], kind: MetadataEntityKind
) -> dict[str, ExternalIdentityRef]:
    result: dict[str, ExternalIdentityRef] = {}
    for identity in identities:
        if identity.entity_kind is not kind or (
            identity.namespace in result and result[identity.namespace] != identity
        ):
            raise MetadataAssemblyError("Exact identities disagree. Review the match.")
        result[identity.namespace] = identity
    return result


def _candidate_values(
    kind: MetadataEntityKind, candidate: MetadataCandidate, diagnostics: set[str]
) -> MetadataValues:
    expected = {
        MetadataEntityKind.SERIES: ProviderSeriesRead,
        MetadataEntityKind.ISSUE: ProviderIssueRead,
        MetadataEntityKind.STORY_ARC: ProviderStoryArcRead,
    }[kind]
    if not isinstance(candidate, expected):
        raise MetadataAssemblyError("Metadata belongs to a different entity kind.")
    values = {
        key: value
        for key, value in candidate.model_dump().items()
        if key in MetadataValues.model_fields
    }
    if candidate.image_url and (
        candidate.source in {MetadataSource.GCD_LOCAL, MetadataSource.GCD_API_V2}
        or not allowed_artwork_url(candidate.image_url)
    ):
        values["image_url"] = None
        diagnostics.add(f"{candidate.source.value}:invalid_artwork_url")
    return MetadataValues.model_validate(values)


def assemble_metadata(
    kind: MetadataEntityKind,
    identities: Sequence[ExternalIdentityRef],
    candidates: Sequence[MetadataCandidate],
    policies: Sequence[SourcePolicyRead],
    *,
    now: datetime,
    current: MetadataValues | None = None,
    previous: MetadataSnapshot | None = None,
    overrides: frozenset[str] = frozenset(),
    replace_managed: bool = False,
    parent_identities: Sequence[ExternalIdentityRef] = (),
    fields: frozenset[str] | None = None,
) -> MetadataSnapshot:
    """Apply authority only after identity agreement; never attach observed crosswalks.

    Unknown existing values are local, not implicitly provider-managed. A changed
    or cleared value relative to the last snapshot becomes a durable override.
    Callers own proof of the supplied identities and persistence of the result.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise MetadataAssemblyError("Assembly requires an aware observation timestamp.")
    now = now.astimezone(UTC)
    if (overrides | (fields or frozenset())) - MetadataValues.model_fields.keys():
        raise MetadataAssemblyError("Unknown metadata override field.")
    verified = _identity_map(identities, kind)
    parents = _identity_map(parent_identities, MetadataEntityKind.SERIES)
    if previous is not None and (
        previous.entity_kind is not kind or not set(previous.identities) <= set(identities)
    ):
        raise MetadataAssemblyError("The previous snapshot belongs to different identities.")
    policy = {item.source: item for item in policies}
    if len(policy) != len(policies):
        raise MetadataAssemblyError("Metadata source policies repeat a source.")
    diagnostics: set[str] = set()
    evidence = dict(verified)
    normalized: dict[MetadataSource, tuple[MetadataCandidate, MetadataValues]] = {}
    designations: set[str] = set()
    for candidate in candidates:
        source = candidate.source
        native = ExternalIdentityRef(source.identity_namespace, kind, candidate.external_id)
        if (
            candidate.identity_namespace is not native.namespace
            or candidate.external_id != native.external_id
            or verified.get(native.namespace) != native
        ):
            raise MetadataAssemblyError("Metadata requires a verified exact identity.")
        if source not in policy or not policy[source].enabled or source in normalized:
            raise MetadataAssemblyError("Metadata requires one enabled policy per source.")
        for claim in candidate.cross_identities:
            if claim.entity_kind is not kind or (
                claim.namespace in evidence and evidence[claim.namespace] != claim
            ):
                raise MetadataAssemblyError("Exact identities disagree. Review the match.")
            evidence[claim.namespace] = claim
        if isinstance(candidate, ProviderIssueRead):
            parent = ExternalIdentityRef(
                source.identity_namespace, MetadataEntityKind.SERIES, candidate.series_external_id
            )
            if parents.get(parent.namespace) != parent:
                raise MetadataAssemblyError("Issue metadata requires independent parent proof.")
            try:
                _, designation = parse_issue_number_text(candidate.issue_number_text)
            except ValueError as exc:
                raise MetadataAssemblyError("Unsupported issue designation.") from exc
            designations.add(designation.casefold())
        normalized[source] = (candidate, _candidate_values(kind, candidate, diagnostics))
    if len(designations) > 1:
        raise MetadataAssemblyError("Issue designations disagree. Review the issue match.")

    current = current or (previous.values if previous else MetadataValues())
    if current.image_url and not allowed_artwork_url(current.image_url):
        raise MetadataAssemblyError("Existing artwork requires a supported public provider URL.")
    origins = {item.field: item for item in previous.origins} if previous else {}
    values = current.model_dump()
    assembled: list[FieldOrigin] = []

    def rank(source: MetadataSource, domain: MetadataDomain) -> tuple[int, str]:
        config = policy.get(source)
        return (
            (config.domain_priorities.get(domain, config.priority), source.value)
            if (config and config.enabled)
            else (1001, source.value)
        )

    for field, existing in values.items():
        domain = field_domain(kind, field)
        prior = origins.get(field)
        edited = previous is not None and existing != getattr(previous.values, field)
        protected = field in overrides or edited or (prior is not None and prior.user_override)
        chosen = prior or FieldOrigin(field=field, domain=domain, observed_at=now)
        if protected:
            chosen = FieldOrigin(field=field, domain=domain, observed_at=now, user_override=True)
        elif fields is None or field in fields:
            for source in sorted(normalized, key=lambda source: rank(source, domain)):
                candidate, incoming = normalized[source]
                value = getattr(incoming, field)
                if _missing(value):
                    continue
                may_replace = (
                    replace_managed
                    and prior is not None
                    and (
                        prior.derivation is not None
                        or (
                            prior.source is not None
                            and rank(source, domain) <= rank(prior.source, domain)
                        )
                    )
                )
                if _missing(existing) or may_replace:
                    values[field] = value
                    chosen = FieldOrigin(
                        field=field,
                        domain=domain,
                        source=source,
                        source_updated_at=candidate.source_updated_at,
                        observed_at=now,
                    )
                break
        if not _missing(values[field]) or protected or prior is not None:
            assembled.append(chosen)
    return MetadataSnapshot(
        entity_kind=kind,
        identities=tuple(
            sorted(set(identities), key=lambda item: (item.namespace, item.external_id))
        ),
        values=MetadataValues.model_validate(values),
        origins=tuple(assembled),
        observed_identities=tuple(
            sorted(
                set(evidence.values()) - set(identities),
                key=lambda item: (item.namespace, item.external_id),
            )
        ),
        diagnostics=tuple(sorted(diagnostics)),
    )
