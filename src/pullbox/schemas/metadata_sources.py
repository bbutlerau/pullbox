"""Versioned source policy and provider-aware discovery responses."""

import enum
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from pullbox.core.metadata_identity import IdentityNamespace, MetadataSource


class MetadataDomain(enum.StrEnum):
    CORE = "core"
    ISSUES = "issues"
    ARTWORK = "artwork"
    STORY_ARCS = "story_arcs"


class SourceCapability(enum.StrEnum):
    SERIES_SEARCH = "series_search"
    SERIES_DETAILS = "series_details"
    ISSUE_LIST = "issue_list"
    ISSUE_DETAILS = "issue_details"
    CROSS_IDENTITIES = "cross_identities"
    CONDITIONAL_REFRESH = "conditional_refresh"
    OFFLINE = "offline"
    COVER_REFERENCE = "cover_reference"
    STORY_ARC_SEARCH = "story_arc_search"
    STORY_ARC_DETAILS = "story_arc_details"


class SourceStatus(enum.StrEnum):
    OK = "ok"
    EMPTY = "empty"
    DISABLED = "disabled"
    FEATURE_DISABLED = "feature_disabled"
    NOT_IMPLEMENTED = "not_implemented"
    UNCONFIGURED = "unconfigured"
    INVALID_CONFIG = "invalid_configuration"
    AUTHENTICATION_FAILED = "authentication_failed"
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"
    INCOMPATIBLE_RESPONSE = "incompatible_response"
    UNSUPPORTED = "unsupported"


class SourceSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    database_path: str | None = Field(default=None, min_length=1, max_length=4096)


class SourcePolicyWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    revision: int = Field(ge=0, lt=2**63, strict=True)
    enabled: bool
    priority: int = Field(ge=0, le=1000, strict=True)
    domain_priorities: dict[MetadataDomain, int] = Field(default_factory=dict, max_length=4)
    settings: SourceSettings = Field(default_factory=SourceSettings)
    credential: SecretStr | None = Field(default=None, exclude=True)
    clear_credential: bool = False


class SourcePolicyRead(BaseModel):
    source: MetadataSource
    identity_namespace: IdentityNamespace
    enabled: bool
    priority: int
    domain_priorities: dict[MetadataDomain, int]
    settings: SourceSettings
    credential_configured: bool
    revision: int
    last_tested_at: datetime | None = None
    last_success_at: datetime | None = None
    last_status: SourceStatus | None = None
    configuration_status: SourceStatus | None = None


class SeriesDiscoveryQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=300)
    year: int | None = Field(default=None, ge=1, le=9999)
    sources: list[MetadataSource] | None = Field(default=None, min_length=1, max_length=5)
    mode: Literal["interactive", "automatic"] = "interactive"
    limit_per_source: int = Field(default=20, ge=1, le=100)
    offsets: dict[MetadataSource, int] = Field(default_factory=dict, max_length=5)


class ProviderSeriesRead(BaseModel):
    source: MetadataSource
    identity_namespace: IdentityNamespace
    external_id: str
    title: str
    year_start: int | None = None
    publisher: str | None = None
    issue_count: int | None = None
    description: str | None = None
    resource_url: str | None = None
    image_url: str | None = None
    also_from: list[MetadataSource] = Field(default_factory=list)


class SourceOutcome(BaseModel):
    source: MetadataSource
    status: SourceStatus
    total: int | None = None
    next_offset: int | None = None
    retry_after_seconds: int | None = None


class SeriesDiscoveryRead(BaseModel):
    results: list[ProviderSeriesRead]
    sources: list[SourceOutcome]
