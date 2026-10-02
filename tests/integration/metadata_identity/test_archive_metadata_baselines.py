"""Archive provenance persists through the real baseline and refresh boundaries."""

from datetime import UTC, datetime
from zipfile import ZipFile

import pytest
from sqlalchemy import func, select, update

from pullbox.core.archive_metadata import read_archive_metadata
from pullbox.core.metadata_identity import ExternalIdentityRef
from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Issue, IssueExternalIdentity
from pullbox.schemas.metadata_snapshot import MetadataValues
from pullbox.schemas.metadata_sources import ProviderIssueRead
from pullbox.services.archive_metadata_reconciliation import reconcile_archive_metadata
from pullbox.services.metadata_assembly import assemble_metadata
from pullbox.services.metadata_baselines import (
    MetadataBaselineConflictError,
    MetadataBaselineWrite,
    load_metadata_baseline,
    save_metadata_baselines,
)
from tests.integration.metadata_identity.test_metadata_baselines import seed
from tests.unit.test_metadata_discovery import runtime

NOW = datetime(2026, 9, 28, 12, tzinfo=UTC)
ISSUE = ExternalIdentityRef(Namespace.METRON, Kind.ISSUE, "42")
PARENT = ExternalIdentityRef(Namespace.METRON, Kind.SERIES, "12")
OBSERVED = ExternalIdentityRef(Namespace.COMICVINE, Kind.ISSUE, "7")


def real_archive(tmp_path):
    path = tmp_path / "local.cbz"
    with ZipFile(path, "w") as archive:
        archive.writestr("page.jpg", b"fixture page bytes")
        archive.writestr(
            "ComicInfo.xml",
            "<ComicInfo><Number>50-x</Number><Title>Local title</Title>"
            "<Writer/><Notes>[cv_issue_id:7]</Notes><Custom>Preserve me</Custom></ComicInfo>",
        )
        archive.writestr(
            "MetronInfo.xml", "<MetronInfo><Summary>Local summary</Summary></MetronInfo>"
        )
    path.chmod(0o444)
    before = path.read_bytes(), path.stat()
    evidence = reconcile_archive_metadata(
        read_archive_metadata(path, "cbz", max_solid_scan_bytes=1024)
    )
    value = assemble_metadata(
        Kind.ISSUE, (ISSUE,), [], [], now=NOW, archive=evidence, parent_identities=(PARENT,)
    )
    return path, before, value


def refreshed(saved, current):
    candidate = ProviderIssueRead(
        source=Source.METRON_API,
        identity_namespace=Namespace.METRON,
        external_id="42",
        series_external_id="12",
        issue_number_text="50-x",
        title="Provider replacement",
        description="Provider summary",
        credits=({"name": "Provider creator", "role": "writer"},),
    )
    return assemble_metadata(
        Kind.ISSUE,
        (ISSUE,),
        [candidate],
        [runtime(Source.METRON_API).policy],
        now=NOW,
        current=current,
        previous=saved.snapshot,
        parent_identities=(PARENT,),
        replace_managed=True,
    )


async def test_readonly_archive_provenance_survives_database_and_provider_roundtrip(
    identity_probe_db, tmp_path
):
    engine, factory, _ = identity_probe_db
    local_id = await seed(factory, Kind.ISSUE)
    path, (original, stat), value = real_archive(tmp_path)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, value)])
    await engine.dispose()
    async with factory.begin() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        assert saved.snapshot == value and saved.revision == 1
        after = refreshed(saved, saved.snapshot.values)
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, after, 1)])
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        assert saved.snapshot.values.title == "Local title"
        assert saved.snapshot.values.description == "Local summary"
        assert saved.snapshot.values.credits == ()
        assert saved.snapshot.identities == (ISSUE,)
        assert saved.snapshot.observed_identities == (OBSERVED,)
        assert "archive:ComicInfo.xml:unmapped_content" in saved.snapshot.diagnostics
        assert await session.scalar(select(func.count()).select_from(IssueExternalIdentity)) == 1
    assert path.read_bytes() == original
    assert (path.stat().st_ino, path.stat().st_mtime_ns, path.stat().st_mode) == (
        stat.st_ino,
        stat.st_mtime_ns,
        stat.st_mode,
    )


@pytest.mark.parametrize("title", ["My corrected title", "", None])
async def test_persisted_archive_value_can_be_user_edited_or_cleared(
    identity_probe_db, tmp_path, title
):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory, Kind.ISSUE)
    _, _, value = real_archive(tmp_path)
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, value)])
        await session.execute(update(Issue).where(Issue.id == local_id).values(title=title))
    async with factory.begin() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        row = await session.get(Issue, local_id)
        current = saved.snapshot.values.model_copy(update={"title": row.title})
        after = refreshed(saved, current)
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, after, 1)])
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        assert saved.snapshot.values.title == title
        origin = next(item for item in saved.snapshot.origins if item.field == "title")
        assert origin.user_override and origin.embedded_documents == ()


async def test_archive_baseline_cannot_promote_observation_or_hide_stale_revision(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    local_id = await seed(factory, Kind.ISSUE)
    _, _, value = real_archive(tmp_path)
    async with factory() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, value)])
        await session.rollback()
    async with factory() as session:
        assert await load_metadata_baseline(session, Kind.ISSUE, local_id) is None
    async with factory.begin() as session:
        await save_metadata_baselines(session, [MetadataBaselineWrite(local_id, value)])
    for snapshot, revision in (
        (value, 0),
        (value.model_copy(update={"identities": (ISSUE, OBSERVED), "observed_identities": ()}), 1),
    ):
        async with factory.begin() as session:
            with pytest.raises(MetadataBaselineConflictError):
                await save_metadata_baselines(
                    session, [MetadataBaselineWrite(local_id, snapshot, revision)]
                )
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        assert saved.revision == 1 and saved.snapshot == value


async def test_existing_snapshot_without_archive_field_loads_unchanged(identity_probe_db):
    import json

    from pullbox.models import IssueMetadataBaseline

    _, factory, _ = identity_probe_db
    local_id = await seed(factory, Kind.ISSUE)
    value = assemble_metadata(
        Kind.ISSUE, (ISSUE,), [], [], now=NOW, current=MetadataValues(title="Legacy")
    )
    legacy = value.model_dump(mode="json")
    for origin in legacy["origins"]:
        origin.pop("embedded_documents", None)
    async with factory.begin() as session:
        session.add(
            IssueMetadataBaseline(issue_id=local_id, revision=1, snapshot_json=json.dumps(legacy))
        )
    async with factory() as session:
        saved = await load_metadata_baseline(session, Kind.ISSUE, local_id)
        assert saved.snapshot.values.title == "Legacy"
        assert saved.snapshot.origins[0].embedded_documents == ()
