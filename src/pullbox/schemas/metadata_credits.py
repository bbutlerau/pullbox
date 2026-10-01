"""Bounded descriptive credits shared by providers, snapshots and library relations."""

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, TypeAdapter, field_validator

MAX_CREDITS = 128


class MetadataCredit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=255)
    role: str = Field(max_length=100)

    @field_validator("name", mode="after")
    @classmethod
    def normalize(cls, value: str) -> str:
        result = value.strip()
        if not result or any(ord(char) < 32 for char in result):
            raise ValueError("Credits require nonempty printable text")
        return result

    @field_validator("role", mode="after")
    @classmethod
    def normalize_roles(cls, value: str) -> str:
        if any(ord(char) < 32 for char in value):
            raise ValueError("Creator roles require printable text")
        if not value.strip():
            return ""
        roles = [role.strip().casefold() for role in value.split(",")]
        if any(not role for role in roles):
            raise ValueError("Credits require nonempty roles")
        # ComicVine's spelling and the XML schema describe the same pencils role.
        roles = ["penciller" if role == "penciler" else role for role in roles]
        return ", ".join(sorted(set(roles)))


def _normalize_credits(values: tuple[MetadataCredit, ...]) -> tuple[MetadataCredit, ...]:
    roles: dict[str, set[str]] = {}
    for credit in values:
        roles.setdefault(credit.name, set()).update(credit.role.split(", ") if credit.role else ())
    # Descriptive grouping only: names never prove a foreign creator identity.
    return tuple(
        MetadataCredit(name=name, role=", ".join(sorted(roles[name]))) for name in sorted(roles)
    )


type MetadataCredits = Annotated[
    tuple[MetadataCredit, ...], Field(max_length=MAX_CREDITS), AfterValidator(_normalize_credits)
]

_CREDITS: TypeAdapter[tuple[MetadataCredit, ...]] = TypeAdapter(MetadataCredits)


def parse_credits(value: object) -> tuple[MetadataCredit, ...]:
    return _CREDITS.validate_python(value)
