"""Last committed canonical values; not an ownership or provider-response cache."""

from sqlalchemy import CheckConstraint, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from pullbox.models.base import Base, IdentityMixin, TimestampMixin


class _BaselineFields:
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)


def _constraints(kind: str) -> tuple[CheckConstraint | UniqueConstraint, ...]:
    return (
        UniqueConstraint(f"{kind}_id", name=f"uq_{kind}_metadata_baseline_target"),
        CheckConstraint("revision > 0", name=f"ck_{kind}_metadata_baseline_revision"),
        CheckConstraint(
            "length(snapshot_json) BETWEEN 2 AND 1048576",
            name=f"ck_{kind}_metadata_baseline_size",
        ),
    )


class SeriesMetadataBaseline(Base, IdentityMixin, TimestampMixin, _BaselineFields):
    __tablename__ = "series_metadata_baselines"
    __table_args__ = _constraints("series")

    series_id: Mapped[int] = mapped_column(ForeignKey("series.id", ondelete="CASCADE"))


class IssueMetadataBaseline(Base, IdentityMixin, TimestampMixin, _BaselineFields):
    __tablename__ = "issue_metadata_baselines"
    __table_args__ = _constraints("issue")

    issue_id: Mapped[int] = mapped_column(ForeignKey("issues.id", ondelete="CASCADE"))


class StoryArcMetadataBaseline(Base, IdentityMixin, TimestampMixin, _BaselineFields):
    __tablename__ = "story_arc_metadata_baselines"
    __table_args__ = _constraints("story_arc")

    story_arc_id: Mapped[int] = mapped_column(ForeignKey("story_arcs.id", ondelete="CASCADE"))
