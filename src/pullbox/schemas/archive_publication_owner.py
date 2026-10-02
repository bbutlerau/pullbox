"""Immutable invoking-owner evidence retained with archive publication intent."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ImportArchiveOwner(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["import_enrichment"] = "import_enrichment"
    job_id: int = Field(gt=0, strict=True)
    imported_file_id: int = Field(gt=0, strict=True)
    action_id: int = Field(gt=0, strict=True)
    action_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    pending_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
