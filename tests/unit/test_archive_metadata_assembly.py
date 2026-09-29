"""Archive values join canonical assembly without acquiring provider authority."""

from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from pullbox.core.archive_metadata import ArchiveMetadataFiles, MetadataFile
from pullbox.core.metadata_identity import ExternalIdentityRef
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.schemas.metadata_snapshot import FieldOrigin, MetadataSnapshot, MetadataValues
from pullbox.schemas.metadata_sources import MetadataDomain, ProviderIssueRead
from pullbox.services.archive_metadata_reconciliation import reconcile_archive_metadata
from pullbox.services.metadata_assembly import MetadataAssemblyError, assemble_metadata
from tests.unit.test_metadata_discovery import runtime

NOW = datetime(2026, 9, 28, 12, tzinfo=UTC)
CV = ExternalIdentityRef(Namespace.COMICVINE, Kind.ISSUE, "7")
PARENT = ExternalIdentityRef(Namespace.COMICVINE, Kind.SERIES, "42")
METRON = ExternalIdentityRef(Namespace.METRON, Kind.ISSUE, "8")


def embedded(ci="", mi=""):
    return reconcile_archive_metadata(
        ArchiveMetadataFiles(
            comicinfo=MetadataFile(
                name="ComicInfo.xml",
                payload=f"<ComicInfo>{ci}</ComicInfo>".encode() if ci is not None else None,
                entry_count=1 if ci is not None else 0,
            ),
            metroninfo=MetadataFile(
                name="MetronInfo.xml",
                payload=f"<MetronInfo>{mi}</MetronInfo>".encode() if mi is not None else None,
                entry_count=1 if mi is not None else 0,
            ),
        )
    )


def candidate(**changes):
    values = ProviderIssueRead(
        source=Source.COMICVINE_API,
        identity_namespace=Namespace.COMICVINE,
        external_id="7",
        series_external_id="42",
        issue_number_text="50-x",
        title="Provider title",
    ).model_dump()
    return ProviderIssueRead.model_validate(values | changes)


def assemble(archive=None, **kwargs):
    return assemble_metadata(
        Kind.ISSUE,
        (CV,),
        kwargs.pop("candidates", []),
        [runtime(Source.COMICVINE_API).policy],
        now=NOW,
        parent_identities=(PARENT,),
        archive=archive,
        **kwargs,
    )


def origin(snapshot, field):
    return next(item for item in snapshot.origins if item.field == field)


def test_archive_gaps_use_honest_document_provenance_not_provider_or_user_authority():
    archive = embedded(
        "<Title>Local title</Title><Number>50-x</Number><Summary>Local summary</Summary>",
        "<Stories><Story>Local title</Story></Stories><PageCount>28</PageCount>",
    )
    result = assemble(archive, candidates=[candidate()])
    assert result.values.title == "Local title"
    assert result.values.description == "Local summary"
    assert result.values.page_count == 28
    assert origin(result, "title").embedded_documents == ("ComicInfo.xml", "MetronInfo.xml")
    assert origin(result, "description").embedded_documents == ("ComicInfo.xml",)
    assert origin(result, "page_count").embedded_documents == ("MetronInfo.xml",)
    for field in ("title", "description", "page_count"):
        provenance = origin(result, field)
        assert provenance.source is None and not provenance.user_override
        assert provenance.derivation is None and provenance.source_updated_at is None
        assert provenance.observed_at == NOW
    assert MetadataSnapshot.model_validate_json(result.model_dump_json()) == result


def test_series_archive_values_flow_through_the_same_assembler():
    archive = embedded(
        "<Series>Local series</Series><Publisher>Local publisher</Publisher>",
        "<Series><Name>Local series</Name><StartYear>1986</StartYear></Series>",
    )
    result = assemble_metadata(Kind.SERIES, (PARENT,), [], [], now=NOW, archive=archive)
    assert result.values.title == "Local series" and result.values.year_start == 1986
    assert origin(result, "title").embedded_documents == ("ComicInfo.xml", "MetronInfo.xml")


@pytest.mark.parametrize(
    ("canonical", "label"),
    [
        ("standard", "Single Issue"),
        ("tpb", "Trade Paperback"),
        ("one_shot", "One-Shot"),
        ("annual", "Annual"),
        ("hardcover", "Hardcover"),
        ("omnibus", "Omnibus"),
        ("graphic_novel", "Graphic Novel"),
        ("special", "Special"),
        ("compendium", "Compendium"),
        ("deluxe", "Deluxe"),
        ("volume", "Volume"),
    ],
)
def test_written_format_labels_round_trip_without_false_disagreement(canonical, label):
    archive = embedded(f"<Format>{label}</Format>", None)
    result = assemble_metadata(
        Kind.SERIES,
        (PARENT,),
        [],
        [],
        now=NOW,
        archive=archive,
        current=MetadataValues(series_type=canonical),
    )
    assert archive.series.series_type == canonical
    assert result.values.series_type == canonical
    assert not any("series_type" in reason for reason in result.diagnostics)
    adopted = assemble_metadata(Kind.SERIES, (PARENT,), [], [], now=NOW, archive=archive)
    assert adopted.values.series_type == canonical
    assert origin(adopted, "series_type").embedded_documents == ("ComicInfo.xml",)


def test_unknown_formats_and_real_format_conflicts_are_not_normalized_away():
    archive = embedded("<Format>Unknown custom format</Format>", None)
    assert archive.series.series_type == "Unknown custom format"
    conflict = embedded("<Format>Annual</Format>", "<Series><Format>Single Issue</Format></Series>")
    assert any(item.field == "series_type" for item in conflict.differences)


@pytest.mark.parametrize("cleared", ["My title", "", None])
def test_edits_and_clears_survive_archive_adoption_and_subsequent_provider_refresh(cleared):
    before = assemble(candidates=[candidate()])
    current = before.values.model_copy(update={"title": cleared})
    result = assemble(
        embedded("<Title>XML title</Title>"),
        previous=before,
        current=current,
        candidates=[candidate()],
        replace_managed=True,
    )
    assert result.values.title == cleared and origin(result, "title").user_override
    assert origin(result, "title").embedded_documents == ()
    again = assemble(previous=result, candidates=[candidate()], replace_managed=True)
    assert again.values.title == cleared and origin(again, "title").user_override


@pytest.mark.parametrize("xml", ["<Writer/>", None])
def test_explicit_empty_credits_are_not_an_enrichment_gap(xml):
    archive = embedded(xml, "<Credits/>" if xml is None else None)
    result = assemble(
        archive, candidates=[candidate(credits=({"name": "Provider", "role": "writer"},))]
    )
    assert result.values.credits == ()
    assert origin(result, "credits").embedded_documents
    result = MetadataSnapshot.model_validate_json(result.model_dump_json())
    later = assemble(
        previous=result,
        candidates=[candidate(credits=({"name": "Provider", "role": "writer"},))],
        replace_managed=True,
    )
    assert later.values.credits == ()
    assert origin(later, "credits") == origin(result, "credits")


def test_absent_credits_do_not_prevent_provider_gap_fill():
    result = assemble(
        embedded(), candidates=[candidate(credits=({"name": "Provider", "role": "writer"},))]
    )
    assert result.values.credits[0].name == "Provider"
    assert origin(result, "credits").source is Source.COMICVINE_API


def test_archive_values_remain_local_on_explicit_provider_refresh():
    before = assemble(embedded("<Title>My embedded title</Title>"))
    later = assemble(previous=before, candidates=[candidate()], replace_managed=True)
    assert later.values.title == "My embedded title"
    assert origin(later, "title") == origin(before, "title")


def test_existing_values_are_not_relabelled_as_archive_or_silently_replaced():
    current = MetadataValues(title="Existing", description="")
    result = assemble(
        embedded("<Title>Embedded</Title><Summary>New summary</Summary>"),
        current=current,
        replace_managed=True,
    )
    assert result.values.title == "Existing"
    assert result.values.description == "New summary"
    assert not origin(result, "title").embedded_documents
    assert "archive:issue:title:current_disagreement" in result.diagnostics


def test_xml_disagreements_remain_unresolved_even_when_a_provider_could_fill_the_field():
    archive = embedded(
        "<Title>First</Title>", "<Stories><Story>Second</Story></Stories><PageCount>24</PageCount>"
    )
    result = assemble(archive, candidates=[candidate()])
    assert result.values.title is None and result.values.page_count == 24
    assert "archive:issue:title:disagreement" in result.diagnostics
    later = assemble(previous=result, candidates=[candidate()], replace_managed=True)
    assert later.values.title is None
    assert "archive:issue:title:disagreement" in later.diagnostics
    corrected = assemble(embedded("<Title>Corrected</Title>"), previous=later)
    assert corrected.values.title == "Corrected"
    assert "archive:issue:title:disagreement" not in corrected.diagnostics


def test_field_scope_does_not_erase_archive_or_local_provenance():
    result = assemble(
        embedded("<Title>Local</Title><Summary>Summary</Summary>"),
        fields=frozenset({"description"}),
    )
    assert result.values.title is None and result.values.description == "Summary"


@pytest.mark.parametrize("notes", ["[cv_issue_id:9]", "[cv_vol_id:99]"])
def test_archive_exact_conflict_stops_all_fields_even_if_outside_requested_scope(notes):
    with pytest.raises(MetadataAssemblyError, match="identit"):
        assemble(
            embedded(f"<Notes>{notes}</Notes><Summary>Should not apply</Summary>"),
            fields=frozenset({"description"}),
        )


def test_xml_internal_identity_conflict_cannot_be_hidden_by_empty_fields():
    archive = embedded(
        "<Notes>[cv_issue_id:7]</Notes>", '<IDS><ID source="Comic Vine">9</ID></IDS>'
    )
    assert archive.identity_conflicts
    with pytest.raises(MetadataAssemblyError, match="identit"):
        assemble(archive, fields=frozenset())


def test_foreign_archive_ids_are_observed_not_attached_and_survive_refresh():
    archive = embedded("<Notes>[cv_issue_id:7]</Notes>", '<IDS><ID source="Metron">8</ID></IDS>')
    result = assemble(archive)
    assert result.identities == (CV,) and result.observed_identities == (METRON,)
    later = assemble(previous=result, candidates=[candidate()])
    assert later.observed_identities == (METRON,) and later.identities == (CV,)


def test_prior_observed_identity_cannot_disappear_or_be_contradicted_on_refresh():
    prior = assemble().model_copy(update={"observed_identities": (METRON,)})
    assert assemble(previous=prior).observed_identities == (METRON,)
    conflicting = ExternalIdentityRef(Namespace.METRON, Kind.ISSUE, "99")
    with pytest.raises(MetadataAssemblyError, match="identit"):
        assemble(previous=prior, candidates=[candidate(cross_identities=[conflicting])])


@pytest.mark.parametrize("number", ["50-o", "50x", "50", "51-x"])
def test_archive_issue_number_cannot_change_the_resolved_target(number):
    with pytest.raises(MetadataAssemblyError, match="designation"):
        assemble(
            embedded(f"<Number>{number}</Number>"), current=MetadataValues(issue_number_text="50-x")
        )


def test_archive_and_provider_designations_must_agree_before_any_field_is_applied():
    with pytest.raises(MetadataAssemblyError, match="designation"):
        assemble(
            embedded("<Number>50-o</Number><Summary>Local</Summary>"), candidates=[candidate()]
        )


@pytest.mark.parametrize("number", ["50-X", "050-x"])
def test_equivalent_issue_designations_preserve_source_text(number):
    result = assemble(embedded(f"<Number>{number}</Number>"), candidates=[candidate()])
    assert result.values.issue_number_text.casefold() == "50-x"


@pytest.mark.parametrize(
    "ci,mi",
    [
        ("<PageCount>invalid</PageCount>", ""),
        ("<Title><Bad/></Title>", ""),
        ("", "<Series/><Series/>"),
    ],
)
def test_invalid_xml_stops_adoption_instead_of_using_the_other_document(ci, mi):
    archive = embedded(ci, mi)
    assert archive.diagnostics
    with pytest.raises(MetadataAssemblyError, match="review"):
        assemble(archive, candidates=[candidate()])


def test_unsupported_xml_is_retained_as_a_durable_preservation_requirement():
    result = assemble(embedded("<Title>Local</Title><Custom>Keep me</Custom>"))
    assert result.values.title == "Local"
    assert "archive:ComicInfo.xml:unmapped_content" in result.diagnostics
    later = assemble(previous=result, candidates=[candidate()])
    assert "archive:ComicInfo.xml:unmapped_content" in later.diagnostics
    assert "Keep me" not in result.model_dump_json()


def test_story_arc_membership_cannot_be_assigned_from_an_issue_archive():
    with pytest.raises(MetadataAssemblyError, match="archive"):
        assemble_metadata(Kind.STORY_ARC, (), [], [], now=NOW, archive=embedded())


@pytest.mark.parametrize(
    "attributes",
    [
        {"source": Source.COMICVINE_API},
        {"derivation": "catalog"},
        {"user_override": True},
        {"source_updated_at": NOW},
    ],
)
def test_embedded_provenance_cannot_claim_provider_user_or_derived_authority(attributes):
    with pytest.raises(ValidationError):
        MetadataSnapshot(
            entity_kind=Kind.ISSUE,
            identities=(CV,),
            values=MetadataValues(title="Archive"),
            origins=(
                FieldOrigin(
                    field="title",
                    domain=MetadataDomain.CORE,
                    observed_at=NOW,
                    embedded_documents=("ComicInfo.xml",),
                    **attributes,
                ),
            ),
        )


def test_old_snapshot_payloads_remain_compatible_and_document_origin_is_bounded():
    legacy = assemble(candidates=[candidate()]).model_dump(mode="json")
    for field in legacy["origins"]:
        field.pop("embedded_documents", None)
    restored = MetadataSnapshot.model_validate(legacy)
    assert origin(restored, "title").embedded_documents == ()
    for documents in (("ComicInfo.xml", "ComicInfo.xml"), ("/secret/path",)):
        with pytest.raises(ValidationError):
            MetadataSnapshot(
                entity_kind=Kind.ISSUE,
                identities=(CV,),
                values=MetadataValues(title="Archive"),
                origins=(
                    FieldOrigin(
                        field="title",
                        domain=MetadataDomain.CORE,
                        observed_at=NOW,
                        embedded_documents=documents,
                    ),
                ),
            )


def test_fresh_archive_read_updates_only_its_own_evidence_not_observation_clock():
    before = assemble(embedded("<Title>Original</Title>"))
    later = assemble_metadata(
        Kind.ISSUE,
        (CV,),
        [],
        [],
        now=NOW + timedelta(days=1),
        previous=before,
        archive=embedded("<Title>Original</Title>"),
        parent_identities=(PARENT,),
    )
    assert origin(later, "title") == origin(before, "title")


def test_partial_archive_date_cannot_be_silently_replaced_by_a_different_canonical_date():
    result = assemble(
        embedded("<Year>1997</Year><Month>6</Month>"),
        current=MetadataValues(cover_date=date(1998, 6, 1)),
    )
    assert result.values.cover_date == date(1998, 6, 1)
    assert "archive:issue:cover_date:current_disagreement" in result.diagnostics


def test_partial_date_agreement_does_not_invent_archive_provenance_for_a_full_date():
    result = assemble(
        embedded("<Year>1997</Year>"),
        current=MetadataValues(cover_date=date(1997, 6, 1)),
    )
    assert not result.diagnostics and not origin(result, "cover_date").embedded_documents


def test_xml_issue_number_disagreement_stops_adoption_without_a_provider_candidate():
    with pytest.raises(MetadataAssemblyError, match="designation"):
        assemble(embedded("<Number>13a</Number>", "<Number>13b</Number>"))


def test_observed_archive_id_cannot_become_a_verified_snapshot_identity():
    result = assemble_metadata(
        Kind.ISSUE,
        (),
        [],
        [],
        now=NOW,
        archive=embedded("<Notes>[cv_issue_id:7]</Notes><Title>Local</Title>"),
    )
    assert result.identities == () and result.observed_identities == (CV,)


def test_requested_field_scope_cannot_bypass_invalid_archive_metadata():
    with pytest.raises(MetadataAssemblyError, match="review"):
        assemble(embedded("<Title/><Title/>"), fields=frozenset())


@pytest.mark.parametrize("manual", [False, True])
def test_unchanged_provider_managed_xml_keeps_existing_refresh_authority(manual):
    before = assemble(candidates=[candidate(title="Original")])
    result = assemble(
        embedded("<Title>Original</Title>"),
        previous=before,
        candidates=[candidate(title="Refreshed")],
        replace_managed=manual,
    )
    assert result.values.title == ("Refreshed" if manual else "Original")
    assert origin(result, "title").source is Source.COMICVINE_API
    assert not origin(result, "title").embedded_documents and not result.diagnostics


def test_changed_provider_managed_xml_still_requires_review_before_replacement():
    before = assemble(candidates=[candidate(title="Original")])
    result = assemble(
        embedded("<Title>Edited in another application</Title>"),
        previous=before,
        candidates=[candidate(title="Refreshed")],
        replace_managed=True,
    )
    assert result.values.title == "Original"
    assert "archive:issue:title:current_disagreement" in result.diagnostics


def test_agreement_with_unknown_local_data_does_not_make_it_provider_managed():
    before = assemble(current=MetadataValues(title="Local"))
    result = assemble(
        embedded("<Title>Local</Title>"),
        previous=before,
        candidates=[candidate(title="Refreshed")],
        replace_managed=True,
    )
    assert result.values.title == "Local" and origin(result, "title").source is None
