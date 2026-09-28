"""Legacy arc references normalize only the documented ComicVine resource type."""

import pytest

from pullbox.core.comicvine_arc_identity import normalize_comicvine_arc_id


@pytest.mark.parametrize("value", [12, "12", "00012", " 12 ", "4045-12", " 4045-00012 "])
def test_exact_legacy_arc_forms_share_one_key(value):
    assert normalize_comicvine_arc_id(value) == "12"


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -12,
        12.0,
        "",
        " ",
        "0",
        "4045-0",
        "4045--12",
        "4050-12",
        "4000-12",
        "4045-12/",
        "https://comicvine.gamespot.com/arc/4045-12/",
        "12x",
        "12.0",
        "+12",
        "1e2",
        "\uff11\uff12",
        "4045-\uff11\uff12",
        "\t12",
        "12\n",
        "4045- 12",
        "4 045-12",
        "0" * 255 + "12",
        2**63,
        str(2**63),
        f"4045-{2**63}",
        {},
        [],
    ],
)
def test_other_syntax_is_not_interpreted_as_an_arc_identity(value):
    with pytest.raises(ValueError, match="Story Arc ID"):
        normalize_comicvine_arc_id(value)


def test_maximum_compatibility_id_is_exact():
    assert normalize_comicvine_arc_id(f"4045-{2**63 - 1}") == str(2**63 - 1)
