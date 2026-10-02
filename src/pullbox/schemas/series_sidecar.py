"""Server-owned preview and explicit approval for one series' sidecars."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from pullbox.schemas.metadata_snapshot import MetadataSnapshot


class SeriesSidecarApproval(BaseModel):
    model_config = ConfigDict(extra="forbid")

    review_key: str = Field(pattern=r"^[0-9a-f]{64}$")


class SeriesSidecarTargetRead(BaseModel):
    directory: str
    action: Literal["create", "update", "unchanged", "blocked"]
    reason: str | None = None
    changes: list[str] = Field(default_factory=list)


class SeriesSidecarPreview(BaseModel):
    series_id: int
    snapshot: MetadataSnapshot
    aliases: list[str]
    targets: list[SeriesSidecarTargetRead]
    ready: bool
    review_key: str


class SeriesSidecarWriteRead(BaseModel):
    written: int
    unchanged: int
    targets: list[SeriesSidecarTargetRead]
