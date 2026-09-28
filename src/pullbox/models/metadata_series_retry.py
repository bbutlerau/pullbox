"""Deferred scheduled metadata work, separate from successful catalog progress."""

from datetime import datetime

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin, UTCDateTime


class MetadataSeriesRetry(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "metadata_series_retries"
    __table_args__ = (
        UniqueConstraint("task_id", "series_id", "source", name="uq_metadata_series_retry"),
        CheckConstraint(
            "task_id IN ('sync_new_issues','refresh_metadata')", name="ck_metadata_retry_task"
        ),
        CheckConstraint(
            "source IN ('*','comicvine_local','comicvine_api','metron_api',"
            "'gcd_local','gcd_api_v2')",
            name="ck_metadata_retry_source",
        ),
        CheckConstraint("revision > 0", name="ck_metadata_retry_revision"),
        CheckConstraint(
            "status IN ('rate_limited','timeout','unavailable','authentication_failed')",
            name="ck_metadata_retry_status",
        ),
        CheckConstraint(
            "(status = 'authentication_failed' AND retry_at IS NULL) OR "
            "(status != 'authentication_failed' AND retry_at IS NOT NULL)",
            name="ck_metadata_retry_deadline",
        ),
        Index("ix_metadata_retry_due", "task_id", "retry_at", "series_id"),
    )

    task_id: Mapped[str] = mapped_column(String(30), nullable=False)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id", ondelete="CASCADE"))
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    config_key: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
