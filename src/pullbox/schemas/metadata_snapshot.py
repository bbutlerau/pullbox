"""Canonical descriptive values and their provenance, independent of output format."""

from datetime import date
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from pullbox.core.metadata_identity import ExternalIdentityRef, MetadataEntityKind, MetadataSource
from pullbox.schemas.metadata_sources import MetadataDomain


def field_domain(kind: MetadataEntityKind, field: str) -> MetadataDomain:
    if field == "image_url":
        return MetadataDomain.ARTWORK
    if kind is MetadataEntityKind.STORY_ARC:
        return MetadataDomain.STORY_ARCS
    if field == "issue_count":
        return MetadataDomain.ISSUES
    return MetadataDomain.CORE


class MetadataValues(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str | None = Field(default=None, max_length=500)
    sort_title: str | None = Field(default=None, max_length=500)
    publisher: str | None = Field(default=None, max_length=255)
    description: str | None = Field(default=None, max_length=200000)
    year_start: int | None = Field(default=None, ge=1, le=9999, strict=True)
    year_end: int | None = Field(default=None, ge=1, le=9999, strict=True)
    volume: str | None = Field(default=None, max_length=100)
    series_type: str | None = Field(default=None, max_length=50)
    status: str | None = Field(default=None, max_length=50)
    language: str | None = Field(default=None, max_length=100)
    issue_count: int | None = Field(default=None, ge=0, le=1000000, strict=True)
    issue_number_text: str | None = Field(default=None, max_length=320)
    cover_date: date | None = None
    store_date: date | None = None
    page_count: int | None = Field(default=None, ge=0, le=1000000, strict=True)
    image_url: str | None = Field(default=None, max_length=500)


class FieldOrigin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    field: str
    domain: MetadataDomain
    source: MetadataSource | None = None
    source_updated_at: AwareDatetime | None = None
    observed_at: AwareDatetime
    user_override: bool = False


class MetadataSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    entity_kind: MetadataEntityKind
    identities: tuple[ExternalIdentityRef, ...]
    values: MetadataValues
    origins: tuple[FieldOrigin, ...] = ()
    observed_identities: tuple[ExternalIdentityRef, ...] = ()
    diagnostics: tuple[str, ...] = ()

    @model_validator(mode="after")
    def consistent_provenance(self) -> Self:
        identities: dict[str, ExternalIdentityRef] = {}
        for identity in (*self.identities, *self.observed_identities):
            if identity.entity_kind is not self.entity_kind or (
                identity.namespace in identities and identities[identity.namespace] != identity
            ):
                raise ValueError("Snapshot identities disagree")
            identities[identity.namespace] = identity
        fields = set()
        for origin in self.origins:
            if (
                origin.field not in MetadataValues.model_fields
                or origin.field in fields
                or origin.domain is not field_domain(self.entity_kind, origin.field)
                or (origin.user_override and origin.source is not None)
                or (
                    origin.source is not None
                    and not any(
                        identity.namespace is origin.source.identity_namespace
                        for identity in self.identities
                    )
                )
            ):
                raise ValueError("Snapshot field provenance is inconsistent")
            fields.add(origin.field)
        return self
