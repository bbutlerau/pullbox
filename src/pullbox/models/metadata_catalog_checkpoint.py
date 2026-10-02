"""Source-specific catalog progress; not metadata identity or provenance."""

from datetime import datetime

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin, UTCDateTime


class SeriesCatalogCheckpoint(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "series_catalog_checkpoints"
    __table_args__ = (
        UniqueConstraint("series_id", "source", name="uq_catalog_checkpoint_series_source"),
        CheckConstraint(
            "revision > 0 AND source_revision > 0 AND identity_revision > 0",
            name="ck_catalog_checkpoint_revisions",
        ),
        CheckConstraint("length(external_id) BETWEEN 1 AND 255", name="ck_catalog_checkpoint_id"),
        CheckConstraint("full_synced_at <= checked_at", name="ck_catalog_checkpoint_time"),
        CheckConstraint(
            "source NOT IN ('comicvine_local', 'gcd_local') OR source_updated_at IS NOT NULL",
            name="ck_catalog_checkpoint_generation",
        ),
        Index("ix_catalog_checkpoint_source", "source"),
        Index("ix_catalog_checkpoint_identity", "identity_id"),
    )

    series_id: Mapped[int] = mapped_column(
        ForeignKey("series.id", ondelete="CASCADE"), nullable=False
    )
    source: Mapped[str] = mapped_column(
        String(30), ForeignKey("metadata_source_configs.source", ondelete="CASCADE"), nullable=False
    )
    source_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    identity_id: Mapped[int] = mapped_column(
        ForeignKey("series_external_identities.id", ondelete="CASCADE"), nullable=False
    )
    identity_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    full_synced_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    source_updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
