"""Retained removal authorization and private staging evidence."""

from sqlalchemy import Boolean, CheckConstraint, Index, String, Text, false
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin


class LibraryRemoval(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "library_removals"
    __table_args__ = (
        Index("ix_library_removal_workspace_pending", "workspace_cleaned", "active", "id"),
        CheckConstraint(
            "state IN ('intended','detached','complete','abandoned','review')",
            name="ck_library_removal_state",
        ),
        CheckConstraint(
            "length(plan_json) BETWEEN 2 AND 65536", name="ck_library_removal_plan_size"
        ),
        CheckConstraint(
            "cleanup_json IS NULL OR length(cleanup_json) BETWEEN 2 AND 4096",
            name="ck_library_removal_cleanup_size",
        ),
    )

    operation_id: Mapped[str] = mapped_column(String(36), unique=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    state: Mapped[str] = mapped_column(String(10), default="intended")
    plan_json: Mapped[str] = mapped_column(Text)
    cleanup_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    trash_path_key: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    workspace_cleaned: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())
