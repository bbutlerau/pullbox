"""Retained conversion evidence independent of the registered file's lifetime."""

from sqlalchemy import Boolean, CheckConstraint, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin


class LibraryConversion(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "library_conversions"
    __table_args__ = (
        CheckConstraint(
            "state IN ('intended','registered','complete','abandoned','review')",
            name="ck_library_conversion_state",
        ),
        CheckConstraint(
            "length(plan_json) BETWEEN 2 AND 65536", name="ck_library_conversion_plan_size"
        ),
    )

    operation_id: Mapped[str] = mapped_column(String(36), unique=True)
    library_file_id: Mapped[int | None] = mapped_column(
        ForeignKey("library_files.id", ondelete="SET NULL")
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    state: Mapped[str] = mapped_column(String(10), default="intended")
    plan_json: Mapped[str] = mapped_column(Text)
