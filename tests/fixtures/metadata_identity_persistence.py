"""MP0 persistence rehearsal only; not an application model or migration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, Integer, MetaData, String, Table, UniqueConstraint

from pullbox.core.metadata_identity import IdentityNamespace
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


def build_identity_schema_probe() -> IdentitySchemaProbe:
    metadata = MetaData()
    for name in ("series", "issues"):
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
    return IdentitySchemaProbe(identities[0], identities[1], arc_index)
