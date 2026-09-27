"""Provider keys and source provenance must not silently settle identity conflicts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from itertools import permutations
from typing import cast

import pytest

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
    find_exact_identity_conflicts,
)


def _identity(
    external_id: str,
    namespace: IdentityNamespace = IdentityNamespace.COMICVINE,
    kind: MetadataEntityKind = MetadataEntityKind.SERIES,
) -> ExternalIdentityRef:
    return ExternalIdentityRef(namespace, kind, external_id)


@pytest.mark.parametrize(
    ("source", "namespace"),
    [
        (MetadataSource.COMICVINE_LOCAL, IdentityNamespace.COMICVINE),
        (MetadataSource.COMICVINE_API, IdentityNamespace.COMICVINE),
        (MetadataSource.METRON_API, IdentityNamespace.METRON),
        (MetadataSource.GCD_LOCAL, IdentityNamespace.GCD),
        (MetadataSource.GCD_API_V2, IdentityNamespace.GCD),
    ],
)
def test_local_and_api_sources_share_one_namespace(source, namespace) -> None:
    assert source.identity_namespace is namespace


@pytest.mark.parametrize("namespace", list(IdentityNamespace))
@pytest.mark.parametrize("raw", ["42", "00042", " 42 "])
def test_numeric_provider_keys_are_canonical_decimal_text(namespace, raw) -> None:
    identity = _identity(raw, namespace)
    assert identity.external_id == "42"
    assert identity == _identity("42", namespace)
    assert hash(identity) == hash(_identity("42", namespace))


@pytest.mark.parametrize(
    "raw",
    [
        "",
        " ",
        "0",
        "000",
        "-1",
        "+42",
        "1.5",
        "1e3",
        "13A",
        "50-X",
        "42\n",
        "4\t2",
        "\x0042",
        "\uff14\uff12",
        "4050-42",
        "https://example.test/42",
        "1" * 256,
    ],
)
def test_invalid_provider_keys_are_not_coerced_into_identities(raw: str) -> None:
    with pytest.raises(ValueError, match="external ID"):
        _identity(raw)


@pytest.mark.parametrize("raw", [None, True, 42, 42.0, {}, []])
def test_non_text_input_requires_explicit_adapter_normalization(raw: object) -> None:
    with pytest.raises(ValueError, match="external ID"):
        _identity(cast("str", raw))


def test_identity_scope_includes_namespace_and_entity_kind() -> None:
    identities = {
        _identity("42", namespace, kind)
        for namespace in IdentityNamespace
        for kind in MetadataEntityKind
    }
    assert len(identities) == 12


def test_identity_keys_do_not_allow_unknown_namespaces_or_entity_kinds() -> None:
    with pytest.raises(ValueError, match="namespace"):
        _identity("42", cast("IdentityNamespace", "gcd_api_v2"))
    with pytest.raises(ValueError, match="entity kind"):
        _identity("42", kind=cast("MetadataEntityKind", "release_variant"))


def test_identity_is_immutable() -> None:
    identity = _identity("42")
    with pytest.raises(FrozenInstanceError):
        identity.external_id = "43"  # type: ignore[misc]


def test_locg_identity_requires_no_executable_provider() -> None:
    identity = _identity("42", IdentityNamespace.LOCG)
    evidence = ExactIdentityEvidence(identity, IdentityEvidenceKind.USER_SELECTION)
    assert evidence.source_instance is None
    assert identity.namespace is IdentityNamespace.LOCG
    assert "locg" not in {source.value for source in MetadataSource}
    assert find_exact_identity_conflicts([evidence]) == ()


def test_transport_provenance_is_retained_without_splitting_identity() -> None:
    identity = _identity("42", IdentityNamespace.GCD)
    local = ExactIdentityEvidence(
        identity, IdentityEvidenceKind.PROVIDER_RESULT, MetadataSource.GCD_LOCAL
    )
    api = ExactIdentityEvidence(
        identity, IdentityEvidenceKind.PROVIDER_RESULT, MetadataSource.GCD_API_V2
    )
    assert local != api
    assert local.identity == api.identity
    assert local.source_instance is MetadataSource.GCD_LOCAL
    assert api.source_instance is MetadataSource.GCD_API_V2
    assert find_exact_identity_conflicts([local, api]) == ()


def test_crosswalk_origin_can_differ_from_asserted_identity_namespace() -> None:
    evidence = ExactIdentityEvidence(
        _identity("42"), IdentityEvidenceKind.PROVIDER_CROSSWALK, MetadataSource.METRON_API
    )
    assert evidence.identity.namespace is IdentityNamespace.COMICVINE
    assert evidence.source_instance is MetadataSource.METRON_API


def test_evidence_rejects_unknown_origin_and_source() -> None:
    with pytest.raises(ValueError, match="evidence kind"):
        ExactIdentityEvidence(_identity("42"), cast("IdentityEvidenceKind", "title_guess"))
    with pytest.raises(ValueError, match="source instance"):
        ExactIdentityEvidence(
            _identity("42"), IdentityEvidenceKind.PROVIDER_RESULT, cast("MetadataSource", "locg")
        )


def test_evidence_is_immutable() -> None:
    evidence = ExactIdentityEvidence(_identity("42"), IdentityEvidenceKind.MYLAR_DATABASE)
    with pytest.raises(FrozenInstanceError):
        evidence.source_instance = MetadataSource.COMICVINE_API  # type: ignore[misc]


def test_exact_conflicts_keep_all_origins_and_never_choose_by_input_order() -> None:
    observations = [
        ExactIdentityEvidence(_identity("42"), IdentityEvidenceKind.MYLAR_DATABASE),
        ExactIdentityEvidence(_identity("43"), IdentityEvidenceKind.SERIES_JSON),
        ExactIdentityEvidence(
            _identity("42"), IdentityEvidenceKind.PROVIDER_CROSSWALK, MetadataSource.METRON_API
        ),
    ]
    expected = find_exact_identity_conflicts(observations)
    assert len(expected) == 1
    assert expected[0].namespace is IdentityNamespace.COMICVINE
    assert expected[0].entity_kind is MetadataEntityKind.SERIES
    assert set(expected[0].evidence) == set(observations)
    for ordered in permutations(observations):
        assert find_exact_identity_conflicts(iter(ordered)) == expected


def test_repeated_observations_are_idempotent_without_losing_distinct_provenance() -> None:
    first = ExactIdentityEvidence(_identity("42"), IdentityEvidenceKind.MYLAR_DATABASE)
    second = ExactIdentityEvidence(_identity("43"), IdentityEvidenceKind.COMICINFO_XML)
    assert find_exact_identity_conflicts([first, first, second]) == find_exact_identity_conflicts(
        [first, second]
    )


def test_series_issue_and_story_arc_conflicts_are_separate_and_stably_ordered() -> None:
    observations = [
        ExactIdentityEvidence(
            _identity(value, namespace, kind), IdentityEvidenceKind.PROVIDER_RESULT
        )
        for namespace in (IdentityNamespace.GCD, IdentityNamespace.COMICVINE)
        for kind in MetadataEntityKind
        for value in ("42", "43")
    ]
    conflicts = find_exact_identity_conflicts(observations)
    assert len(conflicts) == 6
    assert conflicts == find_exact_identity_conflicts(reversed(observations))
    assert {(item.namespace, item.entity_kind) for item in conflicts} == {
        (namespace, kind)
        for namespace in (IdentityNamespace.GCD, IdentityNamespace.COMICVINE)
        for kind in MetadataEntityKind
    }


def test_different_providers_or_kinds_are_not_same_provider_conflicts() -> None:
    observations = [
        ExactIdentityEvidence(
            _identity(str(index + 1), namespace, kind), IdentityEvidenceKind.PROVIDER_RESULT
        )
        for index, (namespace, kind) in enumerate(
            (namespace, kind) for namespace in IdentityNamespace for kind in MetadataEntityKind
        )
    ]
    assert find_exact_identity_conflicts(observations) == ()
    # Lack of disagreement is not a crosswalk or authorization to attach these keys.
    assert len({item.identity for item in observations}) == 12


def test_empty_evidence_has_no_conflicts() -> None:
    assert find_exact_identity_conflicts([]) == ()
