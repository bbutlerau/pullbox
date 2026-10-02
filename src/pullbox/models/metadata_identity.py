"""Provider ownership and retained evidence, independent of provider configuration."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - SQLAlchemy resolves mapped types at runtime
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.core.metadata_identity import IdentityEvidenceKind, IdentityNamespace
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models.base import Base, IdentityMixin, TimestampMixin, UTCDateTime

if TYPE_CHECKING:
    import enum


def _stored_enum(kind: type[enum.Enum]) -> SQLAlchemyEnum:
    return SQLAlchemyEnum(
        kind,
        native_enum=False,
        create_constraint=True,
        values_callable=lambda members: [member.value for member in members],
    )


def _decimal_id_check() -> str:
    remaining = "external_id"
    for digit in "0123456789":
        remaining = f"replace({remaining}, '{digit}', '')"
    return (
        "length(external_id) BETWEEN 1 AND 255 AND "
        "substr(external_id, 1, 1) IN ('1','2','3','4','5','6','7','8','9') AND "
        f"{remaining} = ''"
    )


class _ClaimFields:
    identity_namespace: Mapped[IdentityNamespace] = mapped_column(
        _stored_enum(IdentityNamespace), nullable=False
    )
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    verification_state: Mapped[IdentityVerificationState] = mapped_column(
        _stored_enum(IdentityVerificationState), nullable=False
    )
    evidence_kind: Mapped[IdentityEvidenceKind] = mapped_column(
        _stored_enum(IdentityEvidenceKind), nullable=False
    )


class _OwnershipFields(_ClaimFields):
    resource_url: Mapped[str | None] = mapped_column(String(1000))
    evidence_locator: Mapped[str | None] = mapped_column(Text)
    verified_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_seen_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    revision: Mapped[int] = mapped_column(Integer, default=1, server_default="1", nullable=False)


def _ownership_constraints(kind: str) -> tuple[UniqueConstraint | CheckConstraint, ...]:
    prefix = f"{kind}_external_identities"
    return (
        UniqueConstraint("identity_namespace", "external_id", name=f"uq_{prefix}_owner"),
        UniqueConstraint(f"{kind}_id", "identity_namespace", name=f"uq_{prefix}_namespace"),
        CheckConstraint(
            "verification_state IN ('verified', 'stale', 'conflicted')",
            name=f"ck_{prefix}_active_state",
        ),
        CheckConstraint(_decimal_id_check(), name=f"ck_{prefix}_decimal_id"),
        CheckConstraint("revision > 0", name=f"ck_{prefix}_revision"),
    )


class SeriesExternalIdentity(Base, IdentityMixin, TimestampMixin, _OwnershipFields):
    __tablename__ = "series_external_identities"
    __table_args__ = _ownership_constraints("series")

    series_id: Mapped[int] = mapped_column(
        ForeignKey("series.id", ondelete="CASCADE"), nullable=False
    )


class IssueExternalIdentity(Base, IdentityMixin, TimestampMixin, _OwnershipFields):
    __tablename__ = "issue_external_identities"
    __table_args__ = _ownership_constraints("issue")

    issue_id: Mapped[int] = mapped_column(
        ForeignKey("issues.id", ondelete="CASCADE"), nullable=False
    )


class _EventFields(_ClaimFields):
    # Evidence events have no mutable timestamp; writers append new decisions.
    event_key: Mapped[str] = mapped_column(String(64), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    request_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime, server_default=func.now(), nullable=False
    )


def _event_constraints(kind: str) -> tuple[UniqueConstraint | CheckConstraint | Index, ...]:
    prefix = f"{kind}_identity_events"
    return (
        UniqueConstraint(f"{kind}_id", "event_key", name=f"uq_{prefix}_retry"),
        CheckConstraint(_decimal_id_check(), name=f"ck_{prefix}_decimal_id"),
        CheckConstraint("length(event_key) = 64", name=f"ck_{prefix}_event_key"),
        CheckConstraint("length(request_fingerprint) = 64", name=f"ck_{prefix}_fingerprint"),
        CheckConstraint("length(request_json) BETWEEN 2 AND 8192", name=f"ck_{prefix}_request"),
        Index(f"ix_{prefix}_claim", f"{kind}_id", "identity_namespace", "external_id", "id"),
    )


class SeriesIdentityEvent(Base, IdentityMixin, _EventFields):
    __tablename__ = "series_identity_events"
    __table_args__ = _event_constraints("series")

    series_id: Mapped[int] = mapped_column(
        ForeignKey("series.id", ondelete="CASCADE"), nullable=False
    )


class IssueIdentityEvent(Base, IdentityMixin, _EventFields):
    __tablename__ = "issue_identity_events"
    __table_args__ = _event_constraints("issue")

    issue_id: Mapped[int] = mapped_column(
        ForeignKey("issues.id", ondelete="CASCADE"), nullable=False
    )


class StoryArcIdentityEvent(Base, IdentityMixin, _EventFields):
    __tablename__ = "story_arc_identity_events"
    __table_args__ = _event_constraints("story_arc")

    story_arc_id: Mapped[int] = mapped_column(
        ForeignKey("story_arcs.id", ondelete="CASCADE"), nullable=False
    )
