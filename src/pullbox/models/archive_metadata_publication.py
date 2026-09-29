"""Retained publication intent, independent of the lifetime of a library row."""

from enum import StrEnum

from sqlalchemy import CheckConstraint, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin


class PublicationState(StrEnum):
    INTENDED = "intended"
    PUBLISHED = "published"
    ABANDONED = "abandoned"
    REVIEW = "review"


class ArchiveMetadataPublication(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "archive_metadata_publications"
    __table_args__ = (
        CheckConstraint("revision > 0", name="ck_archive_publication_revision"),
        CheckConstraint(
            "length(plan_json) BETWEEN 2 AND 4194304", name="ck_archive_publication_size"
        ),
        CheckConstraint(
            "(state = 'abandoned' AND active_file_id IS NULL AND active_path_key IS NULL) OR "
            "(state <> 'abandoned' AND active_path_key IS NOT NULL)",
            name="ck_archive_publication_reservation",
        ),
    )

    operation_id: Mapped[str] = mapped_column(String(36), unique=True)
    library_file_id: Mapped[int | None] = mapped_column(
        ForeignKey("library_files.id", ondelete="SET NULL")
    )
    active_file_id: Mapped[int | None] = mapped_column(
        ForeignKey("library_files.id", ondelete="SET NULL"), unique=True
    )
    active_path_key: Mapped[str | None] = mapped_column(String(64), unique=True)
    revision: Mapped[int] = mapped_column(Integer, default=1)
    state: Mapped[PublicationState] = mapped_column(
        Enum(
            PublicationState,
            native_enum=False,
            create_constraint=True,
            values_callable=lambda enum: [item.value for item in enum],
            name="archive_publication_state",
        ),
        default=PublicationState.INTENDED,
    )
    plan_json: Mapped[str] = mapped_column(Text)
