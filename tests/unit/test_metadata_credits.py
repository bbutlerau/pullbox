"""Creator credits survive source normalization and canonical authority."""

import httpx
import pytest

from pullbox.providers.base import IssueMetadata
from pullbox.providers.metadata import comicvine_normalization, metron_normalization
from pullbox.schemas.metadata_credits import parse_credits
from pullbox.schemas.metadata_snapshot import MetadataSnapshot, MetadataValues
from pullbox.services.metadata_assembly import assemble_metadata
from tests.unit.test_comicvine_source_reads import issue_payload
from tests.unit.test_metadata_assembly import CV, METRON, NOW, Kind, identity, origin
from tests.unit.test_metadata_discovery import runtime
from tests.unit.test_metadata_source_adapters import api_adapter
from tests.unit.test_metron_source import issue_row, source

EXPECTED = [{"name": "Fixture Author", "role": "inker, writer"}]


def candidate(source_kind=METRON, credits=None):
    if source_kind is METRON:
        row = issue_row()
        row.update(id=100, series={"id": 42}, number="1")
        if credits is not None:
            row["credits"] = credits
        return metron_normalization.issue(row)
    return comicvine_normalization.issue(
        CV,
        IssueMetadata(
            provider_id="100",
            series_provider_id="42",
            issue_number=1,
            issue_number_text="1",
            creators=credits or [],
            title=None,
            description=None,
            release_date=None,
            store_date=None,
            cover_url=None,
            page_count=None,
            comicvine_url=None,
        ),
    )


def metron_credits(name="Fixture Author", role="Writer"):
    return [{"id": 77, "creator": name, "role": [{"id": 1, "name": role}]}]


def assemble(candidates, **kwargs):
    return assemble_metadata(
        Kind.ISSUE,
        [identity(CV, Kind.ISSUE, "100"), identity(METRON, Kind.ISSUE, "100")],
        candidates,
        [runtime(CV).policy, runtime(METRON).policy],
        now=NOW,
        parent_identities=[identity(CV), identity(METRON)],
        **kwargs,
    )


@pytest.mark.parametrize("provider", ["comicvine", "metron"])
async def test_http_detail_retains_all_roles_without_extra_requests(provider):
    calls = []

    def handle(request):
        calls.append(request)
        if provider == "comicvine":
            return httpx.Response(
                200,
                json={
                    "status_code": 1,
                    "results": issue_payload(
                        person_credits=[
                            {"id": 77, "name": "Fixture Author", "role": "writer, inker"}
                        ],
                    ),
                },
            )
        return httpx.Response(
            200,
            json={
                **issue_row(123),
                "credits": [
                    {
                        "id": 77,
                        "creator": "Fixture Author",
                        "role": [
                            {"id": 1, "name": "Writer"},
                            {"id": 2, "name": "Inker"},
                        ],
                    },
                ],
            },
        )

    adapter = await api_adapter(handle) if provider == "comicvine" else source(handle)
    try:
        result = await adapter.issue("123")
        assert result.data.model_dump(mode="json").get("credits") == EXPECTED
        assert len(calls) == 1
    finally:
        await adapter.close()


def test_snapshot_credit_authority_roundtrip_and_explicit_refresh():
    primary = candidate(CV, [{"name": "Primary", "role": "writer"}])
    secondary = candidate(credits=metron_credits("Secondary"))
    before = assemble([secondary, primary])
    assert before.values.model_dump(mode="json").get("credits") == [
        {"name": "Primary", "role": "writer"},
    ]
    assert origin(before, "credits").source is CV
    assert before == MetadataSnapshot.model_validate_json(before.model_dump_json())
    replacement = candidate(CV, [{"name": "Replacement", "role": "writer"}])
    assert assemble([replacement], previous=before).values == before.values
    refreshed = assemble([replacement], previous=before, replace_managed=True)
    assert refreshed.values.model_dump(mode="json")["credits"][0]["name"] == "Replacement"
    assert not origin(refreshed, "credits").user_override


def test_empty_provider_credits_allow_fallback_without_merging_lists():
    result = assemble([candidate(CV), candidate(credits=metron_credits())])
    assert result.values.model_dump(mode="json").get("credits") == [
        {"name": "Fixture Author", "role": "writer"},
    ]
    assert origin(result, "credits").source is METRON


@pytest.mark.parametrize(
    "invalid", ["not a list", [None], metron_credits(role="x" * 101), metron_credits() * 129]
)
def test_invalid_metron_credits_are_not_silently_discarded(invalid):
    with pytest.raises(ValueError):
        candidate(credits=invalid)


def test_normalized_roles_must_still_fit_database_storage():
    with pytest.raises(ValueError):
        parse_credits([{"name": "Artist", "role": ",".join(f"r{i}" for i in range(25))}])


def test_duplicate_roles_and_unknown_roles_survive_normalization():
    result = parse_credits(
        [
            {"name": "Artist", "role": "Writer, inker"},
            {"name": "Artist", "role": "WRITER, Design consultant"},
        ]
    )
    assert len(result) == 1
    assert result[0].role == "design consultant, inker, writer"


def test_metron_credit_without_assigned_role_is_retained_not_invented():
    try:
        result = candidate(credits=[{"id": 77, "creator": "Unclassified", "role": []}])
    except ValueError:
        pytest.fail("A valid unclassified creator must not reject the whole issue")
    assert result.model_dump(mode="json")["credits"] == [{"name": "Unclassified", "role": ""}]


@pytest.mark.parametrize("local", [[], [{"name": "Local", "role": "translator"}]])
def test_local_credits_and_clear_remain_overrides_across_refreshes(local):
    before = assemble([candidate(credits=metron_credits())])
    current = MetadataValues.model_validate({**before.values.model_dump(), "credits": local})
    result = assemble(
        [candidate(credits=metron_credits("New"))],
        current=current,
        previous=before,
        replace_managed=True,
    )
    assert result.values.credits == current.credits
    assert origin(result, "credits").user_override
    next_result = assemble(
        [candidate(credits=metron_credits("Other"))], previous=result, replace_managed=True
    )
    assert next_result.values.credits == current.credits


async def test_empty_credits_trigger_exact_fallback_but_local_clear_does_not():
    from pullbox.schemas.metadata_sources import MetadataFetch, SourceStatus
    from pullbox.services.metadata_refresh_snapshot import fetch_metadata_snapshot
    from tests.unit.test_metadata_refresh_snapshot import setup

    registry, adapters = setup()
    adapters[CV].result = MetadataFetch(status=SourceStatus.OK, data=candidate(CV))
    adapters[METRON].result = MetadataFetch(
        status=SourceStatus.OK, data=candidate(credits=metron_credits())
    )
    options = dict(
        now=NOW,
        requested_fields=frozenset({"credits"}),
        parent_identities=[identity(CV), identity(METRON)],
    )
    ids = [identity(CV, Kind.ISSUE, "100"), identity(METRON, Kind.ISSUE, "100")]
    result = await fetch_metadata_snapshot(
        registry, Kind.ISSUE, ids, current=MetadataValues(credits=()), **options
    )
    assert result.snapshot.values.credits[0].name == "Fixture Author"
    assert all(len(a.calls) == 1 for a in adapters.values())
    await fetch_metadata_snapshot(
        registry,
        Kind.ISSUE,
        ids,
        current=result.snapshot.values.model_copy(update={"credits": ()}),
        previous=result.snapshot,
        replace_managed=True,
        **options,
    )
    assert all(len(a.calls) == 1 for a in adapters.values())


def test_penciler_and_penciller_are_one_descriptive_role():
    assert parse_credits([{"name": "David Finch", "role": "penciler, writer"}]) == parse_credits(
        [{"name": "David Finch", "role": "penciller, writer"}]
    )
