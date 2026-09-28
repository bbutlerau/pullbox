"""Parent agreement is part of the retained request, not a transient assertion."""

from dataclasses import replace

import pytest

from pullbox.core.metadata_identity import (
    ExternalIdentityRef,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_events import prepare_identity_event
from tests.fixtures.metadata_identity_events import identity_event_request


def test_issue_parent_changes_semantics_but_not_retry_slot():
    request = identity_event_request(MetadataEntityKind.ISSUE, 1)

    def with_parent(key):
        return replace(
            request,
            evidence=replace(
                request.evidence,
                parent_identity=ExternalIdentityRef(
                    IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES, key
                ),
            ),
        )

    first, second = [prepare_identity_event(with_parent(key)) for key in ("21", "22")]
    assert first.event_key == second.event_key
    assert first.request_fingerprint != second.request_fingerprint
    assert '"parent_identity"' in first.request_json


@pytest.mark.parametrize(
    "target,namespace,parent_kind",
    [
        (MetadataEntityKind.SERIES, IdentityNamespace.COMICVINE, MetadataEntityKind.SERIES),
        (MetadataEntityKind.ISSUE, IdentityNamespace.METRON, MetadataEntityKind.SERIES),
        (MetadataEntityKind.ISSUE, IdentityNamespace.COMICVINE, MetadataEntityKind.ISSUE),
    ],
)
def test_rejects_wrong_parent_kind_or_namespace(target, namespace, parent_kind):
    request = identity_event_request(target, 1)
    with pytest.raises(ValueError):
        replace(request.evidence, parent_identity=ExternalIdentityRef(namespace, parent_kind, "21"))
