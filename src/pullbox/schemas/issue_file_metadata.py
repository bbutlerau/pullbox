"""Approval for writing the current canonical pair to one managed comic."""

from pydantic import BaseModel, ConfigDict, Field


class FileMetadataChange(BaseModel):
    document: str
    field: str
    before: str | None
    after: str | None


class FileMetadataPreview(BaseModel):
    file_id: int
    file_name: str
    documents: tuple[str, str] = ("ComicInfo.xml", "MetronInfo.xml")
    changes: list[FileMetadataChange]
    review_key: str
    unchanged: bool


class FileMetadataApproval(BaseModel):
    model_config = ConfigDict(extra="forbid")
    review_key: str = Field(pattern=r"^[a-f0-9]{64}$")
