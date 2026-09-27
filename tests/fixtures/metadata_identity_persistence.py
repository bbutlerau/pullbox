"""MP0 persistence rehearsal only; not an application model or migration."""

import enum
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy import Enum as SQLAlchemyEnum

from pullbox.core.metadata_identity import (
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
)
from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models.base import UTCDateTime
from pullbox.models.story_arc import StoryArcExternalIdentity

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

type IdentityProbeDatabase = tuple[
    AsyncEngine, async_sessionmaker[AsyncSession], IdentitySchemaProbe
]


@dataclass(frozen=True)
class IdentitySchemaProbe:
    series: Table
    issue: Table
    arc_index: Index | None
    events: dict[MetadataEntityKind, Table]


def _stored_enum(enum_class: type[enum.StrEnum], name: str) -> SQLAlchemyEnum:
    return SQLAlchemyEnum(
        enum_class,
        name=name,
        native_enum=False,
        create_constraint=True,
        values_callable=lambda members: [member.value for member in members],
    )


def build_identity_schema_probe() -> IdentitySchemaProbe:
    metadata = MetaData()
    for name in ("series", "issues", "story_arcs"):
        Table(name, metadata, Column("id", Integer, primary_key=True))
    identities = []
    for kind, parent in (("series", "series"), ("issue", "issues")):
        identities.append(
            Table(
                f"mp0_{kind}_identities",
                metadata,
                Column("id", Integer, primary_key=True),
                Column(
                    f"{kind}_id", ForeignKey(f"{parent}.id", ondelete="CASCADE"), nullable=False
                ),
                Column("identity_namespace", String(50), nullable=False),
                Column("external_id", String(255), nullable=False),
                Column(
                    "verification_state",
                    _stored_enum(IdentityVerificationState, f"mp0_{kind}_owner_state"),
                    nullable=False,
                ),
                CheckConstraint(
                    "verification_state IN ('verified', 'stale', 'conflicted')",
                    name=f"mp0_{kind}_attached_states",
                ),
                UniqueConstraint("identity_namespace", "external_id"),
                UniqueConstraint(f"{kind}_id", "identity_namespace"),
            )
        )
    arc_table = StoryArcExternalIdentity.__table__.to_metadata(metadata)
    canonical = (arc_table.c.namespace == "story_arc") & arc_table.c.source.in_(
        tuple(IdentityNamespace)
    )
    arc_index = Index(
        "mp0_one_provider_identity_per_arc",
        arc_table.c.story_arc_id,
        arc_table.c.source,
        unique=True,
        sqlite_where=canonical,
        postgresql_where=canonical,
    )
    events = {}
    for kind, parent in (
        (MetadataEntityKind.SERIES, "series"),
        (MetadataEntityKind.ISSUE, "issues"),
        (MetadataEntityKind.STORY_ARC, "story_arcs"),
    ):
        events[kind] = Table(
            f"mp0_{kind.value}_identity_events",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                f"{kind.value}_id", ForeignKey(f"{parent}.id", ondelete="CASCADE"), nullable=False
            ),
            Column("identity_namespace", String(50), nullable=False),
            Column("external_id", String(255), nullable=False),
            Column("event_key", String(64), nullable=False),
            Column(
                "verification_state",
                _stored_enum(IdentityVerificationState, f"mp0_{kind.value}_event_state"),
                nullable=False,
            ),
            Column(
                "evidence_kind",
                _stored_enum(IdentityEvidenceKind, f"mp0_{kind.value}_event_evidence"),
                nullable=False,
            ),
            Column("created_at", UTCDateTime, nullable=False, default=lambda: datetime.now(UTC)),
            UniqueConstraint(f"{kind.value}_id", "event_key"),
        )
    return IdentitySchemaProbe(identities[0], identities[1], arc_index, events)
