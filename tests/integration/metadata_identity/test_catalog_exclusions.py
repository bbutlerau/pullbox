"""Reviewed omissions stay separate from issue ownership on both databases."""

import importlib.util
from dataclasses import replace
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select

from pullbox.core.metadata_identity import IdentityNamespace, MetadataSource
from pullbox.models import Issue, Series
from pullbox.services.metadata_catalog_review import approve_catalog_review, catalog_review
from pullbox.services.metadata_series_adoption import adopt_source_series_bundle
from tests.integration.metadata_identity.test_series_adoption import (  # noqa: F401
    bundle,
    configured_sources,
)


def reviewed_bundle():
    data = bundle(numbers=("1", "1 [2nd Printing]", "2"))
    data = replace(
        data,
        series=data.series.model_copy(
            update={"source": MetadataSource.GCD_LOCAL, "identity_namespace": IdentityNamespace.GCD}
        ),
        issues=tuple(
            item.model_copy(
                update={
                    "source": MetadataSource.GCD_LOCAL,
                    "identity_namespace": IdentityNamespace.GCD,
                }
            )
            for item in data.issues
        ),
    )
    return approve_catalog_review(data, catalog_review(data).token)


async def test_reviewed_catalog_persists_only_supported_issue_owners(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        result = await adopt_source_series_bundle(session, reviewed_bundle(), monitored=True)
        series_id = result.series.id
    async with factory() as session:
        series = await session.get(Series, series_id)
        assert series.issue_count == 2
        assert series.catalog_exclusions[0]["external_id"] == "101"
        assert [
            item.issue_number_text
            for item in await session.scalars(select(Issue).order_by(Issue.id))
        ] == ["1", "2"]


def revision(connection):
    path = (
        Path(__file__).resolve().parents[3]
        / "alembic/versions/g4a5b6c7d890_add_series_catalog_exclusions.py"
    )
    spec = importlib.util.spec_from_file_location("catalog_exclusions_revision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    return module


async def test_migration_preserves_existing_series_and_guards_review_evidence(identity_probe_db):
    engine, factory, _ = identity_probe_db
    async with factory.begin() as session:
        series = Series(title="Existing", sort_title="Existing")
        session.add(series)
        await session.flush()
        series_id = series.id
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: revision(conn).downgrade())
        await connection.run_sync(lambda conn: revision(conn).upgrade())
    async with factory.begin() as session:
        series = await session.get(Series, series_id)
        assert series.title == "Existing" and series.catalog_exclusions == []
        series.catalog_exclusions = [
            {
                "source": "gcd_local",
                "series_external_id": "3172",
                "external_id": "647784",
                "issue_number_text": "1 [2nd Printing]",
            }
        ]
    with pytest.raises(RuntimeError, match="catalog exclusions"):
        async with engine.begin() as connection:
            await connection.run_sync(lambda conn: revision(conn).downgrade())
    async with factory() as session:
        assert (await session.get(Series, series_id)).catalog_exclusions[0][
            "external_id"
        ] == "647784"
