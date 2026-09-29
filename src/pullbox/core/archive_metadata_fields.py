"""The credit-role overlap between ComicInfo and richer canonical metadata."""

from pullbox.schemas.metadata_credits import MetadataCredit, parse_credits

COMICINFO_ROLES = {
    "Writer": "writer",
    "Penciller": "penciller",
    "Inker": "inker",
    "Colorist": "colorist",
    "Letterer": "letterer",
    "CoverArtist": "cover",
    "Editor": "editor",
    "Translator": "translator",
}


def comicinfo_credits(credits: tuple[MetadataCredit, ...]) -> tuple[MetadataCredit, ...]:
    """Project supported roles, without losing rich roles in the canonical record."""
    supported = set(COMICINFO_ROLES.values())
    return parse_credits(
        tuple(
            {"name": credit.name, "role": ", ".join(roles)}
            for credit in credits
            if (roles := [role for role in credit.role.split(", ") if role in supported])
        )
    )
