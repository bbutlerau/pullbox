"""Bounded account admission state, independent of catalog or entity progress."""

from datetime import datetime

from sqlalchemy import CheckConstraint, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin, UTCDateTime


class MetadataSourceAccount(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "metadata_source_accounts"
    __table_args__ = (
        CheckConstraint(
            "source IN ('comicvine_api','metron_api','gcd_api_v2')",
            name="ck_metadata_account_source",
        ),
        CheckConstraint("revision > 0", name="ck_metadata_account_revision"),
        CheckConstraint(
            "(status IS NULL AND retry_at IS NULL AND lease_until IS NULL) OR "
            "(status IS NOT NULL AND status = 'authentication_failed' "
            "AND retry_at IS NULL AND lease_until IS NULL) OR "
            "(status IS NOT NULL AND status IN ('rate_limited','timeout','unavailable') "
            "AND retry_at IS NOT NULL)",
            name="ck_metadata_account_state",
        ),
    )

    source: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    account_key: Mapped[str] = mapped_column(String(64), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str | None] = mapped_column(String(30))
    retry_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    lease_until: Mapped[datetime | None] = mapped_column(UTCDateTime)
