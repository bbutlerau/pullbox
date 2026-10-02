"""
Pullbox ORM base — declarative base, timestamp mixin, and identity mixin.

All models inherit from Base and use these mixins for consistent
primary keys and audit timestamps.
"""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import DateTime, Integer, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator


class UTCDateTime(TypeDecorator[datetime]):
    """A DateTime type that ensures values are always timezone-aware UTC.

    SQLite stores timestamps without timezone info. This decorator ensures
    that every datetime read from the database is tagged with UTC tzinfo,
    so Pydantic serializes it with +00:00/Z and downstream code never
    has to guess whether a timestamp is UTC or local.

    SQLite stores naive UTC. PostgreSQL receives aware UTC so its timestamptz
    driver cannot interpret a naive value in the process's local timezone.
    Naive input and database values are interpreted as UTC, never local time.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Any) -> datetime | None:
        """Normalize to UTC before storing."""
        if value is None:
            return None
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return normalized if dialect.name == "postgresql" else normalized.replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Any) -> datetime | None:
        """Tag naive datetimes from the DB as UTC."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for all Pullbox models."""


class TimestampMixin:
    """Adds created_at and updated_at columns with automatic timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime,
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class IdentityMixin:
    """Provides auto-incrementing integer primary key."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
