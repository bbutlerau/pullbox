"""Source configuration is independent of durable provider identities."""

from datetime import datetime

from sqlalchemy import JSON, Boolean, CheckConstraint, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin, UTCDateTime


class MetadataSourceConfig(Base, IdentityMixin, TimestampMixin):
    __tablename__ = "metadata_source_configs"
    __table_args__ = (
        CheckConstraint(
            "source IN ('comicvine_local','comicvine_api','metron_api','gcd_local','gcd_api_v2')",
            name="ck_metadata_source_slug",
        ),
        CheckConstraint("priority BETWEEN 0 AND 1000", name="ck_metadata_source_priority"),
        CheckConstraint("revision > 0", name="ck_metadata_source_revision"),
        CheckConstraint(
            "credential_secret IS NULL OR source IN ('metron_api','gcd_api_v2')",
            name="ck_metadata_source_credential_owner",
        ),
    )

    source: Mapped[str] = mapped_column(String(30), unique=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    domain_priorities: Mapped[dict[str, int]] = mapped_column(JSON, default=dict, nullable=False)
    settings: Mapped[dict[str, object]] = mapped_column(JSON, default=dict, nullable=False)
    credential_secret: Mapped[str | None] = mapped_column(Text)
    revision: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    last_tested_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_success_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_status: Mapped[str | None] = mapped_column(String(40))
