"""The write gate enforces the complete pinned XSD 1.1, entirely offline."""

import hashlib
import socket
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files
from unittest.mock import Mock

import pytest

from pullbox.core.metadata_xml import MAX_METADATA_XML_BYTES, MetadataXmlError
from pullbox.core.metroninfo_schema import validate_metroninfo_xml


def document(content: str = "", *, series: str = "<Name>Gen13</Name>") -> str:
    return f"<MetronInfo><Series>{series}</Series>{content}</MetronInfo>"


@pytest.mark.parametrize(
    "content",
    [
        "",
        "<Number>50-x</Number>",
        "<Number>13b</Number><PageCount>0</PageCount>",
        "<CoverDate>2024-02-29</CoverDate><StoreDate>2024-01-31</StoreDate>",
        '<IDS><ID source="Comic Vine" primary="true">123</ID>'
        '<ID source="Metron">456</ID><ID source="Grand Comics Database">789</ID></IDS>',
        '<URLs><URL primary="1">https://metron.cloud/issue/456/</URL>'
        '<URL primary="0">https://comicvine.gamespot.com/4000-123/</URL></URLs>',
        '<Publisher id="2"><Name>DC</Name><Imprint id="3">Vertigo</Imprint></Publisher>',
        '<Arcs><Arc id="4"><Name>Event</Name><Number>1</Number></Arc></Arcs>',
        '<Credits><Credit><Creator id="5">A Writer</Creator>'
        '<Roles><Role id="6">Writer</Role><Role>Cover</Role></Roles></Credit></Credits>',
        '<Prices><Price country="US">3.99</Price></Prices><AgeRating>Teen Plus</AgeRating>',
        '<Universes><Universe id="7"><Name>Earth</Name>'
        "<Designation>Prime</Designation></Universe></Universes>",
        "<GTIN><ISBN>9781234567897</ISBN></GTIN><LastModified>2026-09-28T00:00:00Z</LastModified>",
    ],
)
def test_supported_metroninfo_11_is_valid(content: str) -> None:
    assert validate_metroninfo_xml(document(content)) is None


@pytest.mark.parametrize(
    "content",
    [
        "<Title>Unsupported extension</Title>",
        "<PageCount>-1</PageCount>",
        "<PageCount>one</PageCount>",
        "<PageCount>2</PageCount><PageCount>3</PageCount>",
        "<CoverDate>2023-02-29</CoverDate>",
        "<StoreDate>2026-09</StoreDate>",
        "<AgeRating>Maybe</AgeRating>",
        '<IDS><ID source="Unknown">1</ID></IDS>',
        "<IDS><ID>1</ID></IDS>",
        '<IDS><ID source="Metron" primary="yes">1</ID></IDS>',
        '<Prices><Price country="USA">3.99</Price></Prices>',
        "<Publisher><Imprint>Vertigo</Imprint></Publisher>",
        "<Arcs><Arc><Name>Event</Name><Number>0</Number></Arc></Arcs>",
        "<Credits><Credit><Roles><Role>Writer</Role></Roles></Credit></Credits>",
        "<Credits><Credit><Creator>Name</Creator><Roles><Role>Invalid</Role></Roles>"
        "</Credit></Credits>",
        "<CommunityRating><AverageRating>6</AverageRating></CommunityRating>",
    ],
)
def test_schema_invalid_content_is_not_accepted(content: str) -> None:
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document(content))


@pytest.mark.parametrize(
    "series",
    [
        "",
        "<Name>Gen13</Name><IssueCount>0</IssueCount>",
        "<Name>Gen13</Name><Volume>-1</Volume>",
        "<Name>Gen13</Name><Volume>v2</Volume>",
        "<Name>Gen13</Name><Format>Unknown format</Format>",
        "<Name>Gen13</Name><StartYear>20</StartYear>",
    ],
)
def test_series_schema_constraints_are_enforced(series: str) -> None:
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document(series=series))


@pytest.mark.parametrize("primary", ["true", "1"])
@pytest.mark.parametrize("container", ["IDS", "URLs"])
def test_xsd_11_assertions_reject_multiple_primary_items(primary: str, container: str) -> None:
    tag = 'ID source="Metron"' if container == "IDS" else "URL"
    close = "ID" if container == "IDS" else "URL"
    content = f'<{container}><{tag} primary="{primary}">1</{close}>'
    content += f'<{tag} primary="{primary}">2</{close}></{container}>'
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document(content))


@pytest.mark.parametrize("false", ["false", "0", "\t false \n"])
@pytest.mark.parametrize("true", ["true", "1"])
@pytest.mark.parametrize("container", ["IDS", "URLs"])
def test_explicit_false_primary_is_equivalent_to_absence(
    false: str,
    true: str,
    container: str,
) -> None:
    tag = 'ID source="Metron"' if container == "IDS" else "URL"
    close = "ID" if container == "IDS" else "URL"
    content = f'<{container}><{tag} primary="{true}">1</{close}>'
    content += f'<{tag} primary="{false}">2</{close}>' * 2 + f"</{container}>"
    assert validate_metroninfo_xml(document(content)) is None
    conflicting = content.replace(f'primary="{false}"', f'primary="{true}"', 1)
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document(conflicting))


@pytest.mark.parametrize("false", ["FALSE", "no", "\u00a0false\u00a0"])
def test_false_normalization_does_not_accept_invalid_boolean_lexemes(false: str) -> None:
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document(f'<URLs><URL primary="{false}">x</URL></URLs>'))


def test_false_normalization_does_not_relax_other_schema_rules() -> None:
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document('<IDS><ID primary="false">1</ID></IDS>'))
    with pytest.raises(MetadataXmlError, match=r"^invalid_schema$"):
        validate_metroninfo_xml(document('<Series primary="false"><Name>X</Name></Series>'))


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ("<MetronInfo>", "invalid_xml"),
        ("<ComicInfo/>", "invalid_root"),
        ("<MetronInfo/>", "invalid_schema"),
        ('<MetronInfo xmlns="urn:fake"/>', "invalid_root"),
        ('<!DOCTYPE MetronInfo [<!ENTITY x "secret">]><MetronInfo>&x;</MetronInfo>', "unsafe_xml"),
        (
            document(
                '<xi:include xmlns:xi="http://www.w3.org/2001/XInclude" href="file:///etc/passwd"/>'
            ),
            "unsafe_xml",
        ),
        ("<MetronInfo>" + "x" * MAX_METADATA_XML_BYTES + "</MetronInfo>", "too_large"),
        (document("<Tags>" + "<Tag>x</Tag>" * 4096 + "</Tags>"), "complexity_limit"),
        (document("<a>" * 33 + "</a>" * 33), "complexity_limit"),
        ("file:///etc/passwd", "invalid_xml"),
        ("https://example.invalid/metadata.xml", "invalid_xml"),
    ],
    ids=[
        "malformed",
        "wrong-root",
        "missing-series",
        "namespace",
        "entity",
        "xinclude",
        "byte-limit",
        "node-limit",
        "depth-limit",
        "file-uri",
        "remote-uri",
    ],
)
def test_untrusted_input_is_bounded_before_schema_validation(payload: str, code: str) -> None:
    with pytest.raises(MetadataXmlError, match=f"^{code}$"):
        validate_metroninfo_xml(payload)


def test_errors_do_not_expose_document_values() -> None:
    with pytest.raises(MetadataXmlError) as caught:
        validate_metroninfo_xml(document("<PageCount>private-password</PageCount>"))
    assert str(caught.value) == "invalid_schema"
    assert caught.value.__cause__ is None


def test_schema_location_hints_never_trigger_network_or_local_file_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import xmlschema.resources.xml_resource as resources

    from pullbox.core.metroninfo_schema import _schema

    _schema.cache_clear()
    access = Mock(side_effect=AssertionError("XML must not access resource URLs"))
    monkeypatch.setattr(resources, "urlopen", access)
    monkeypatch.setattr(socket, "create_connection", access)
    for location in ("https://example.invalid/schema.xsd", "file:///etc/passwd"):
        payload = document().replace(
            "<MetronInfo>",
            '<MetronInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            f'xsi:noNamespaceSchemaLocation="{location}">',
        )
        validate_metroninfo_xml(payload)
    access.assert_not_called()


def test_parallel_validation_does_not_mix_documents() -> None:
    def validate(index: int) -> bool:
        try:
            validate_metroninfo_xml(document(f"<PageCount>{index % 2 - 1}</PageCount>"))
        except MetadataXmlError:
            return False
        return True

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(validate, range(64))) == [bool(index % 2) for index in range(64)]


def test_upstream_schema_and_license_are_bundled() -> None:
    resources = files("pullbox").joinpath("core/metadata_schemas")
    schema = resources.joinpath("MetronInfo-1.1.xsd").read_text(encoding="utf-8")
    assert hashlib.sha256(schema.encode("utf-8")).hexdigest() == (
        "c0af59fc39e17e32c1a01714edc42d920bddc9d14e880ffc55ab960b8243a092"
    )
    assert '<xs:assert test="count(ID[@primary = true()]) &lt;= 1"' in schema
    assert "MIT License" in resources.joinpath("METRONINFO-LICENSE").read_text(encoding="utf-8")
