"""Single-issue enrichment is atomic and leaves real files and reader state untouched."""

import asyncio
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import func, select

from pullbox.core.metadata_identity import IdentityNamespace as Namespace
from pullbox.core.metadata_identity import MetadataEntityKind as Kind
from pullbox.core.metadata_identity import MetadataSource as Source
from pullbox.models import Issue, LibraryFile, User
from pullbox.models.issue import IssueStatus
from pullbox.models.library import FileFormat, LibraryFileStorageMode, LibraryRoot
from pullbox.models.metadata_identity import IssueExternalIdentity, SeriesExternalIdentity
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.reader import IssueReaderState
from pullbox.schemas.metadata_credits import MetadataCredit
from pullbox.schemas.metadata_sources import MetadataFetch, SourceCapability, SourceStatus
from pullbox.services.issue_metadata_links import read_issue_links
from pullbox.services.metadata_baselines import load_metadata_baseline
from pullbox.services.metadata_credits import read_issue_credits
from pullbox.services.metadata_discovery import MetadataSourceRegistry
from pullbox.services.metadata_issue_refresh import refresh_issue_from_sources
from tests.integration.metadata_identity.test_series_adoption import (
    configured_sources,  # noqa: F401
)
from tests.integration.metadata_identity.test_series_refresh import seed
from tests.unit.test_metadata_discovery import registration, runtime
from tests.unit.test_metadata_source_reads import ReadAdapter


class IssueAdapter(ReadAdapter):
    def __init__(self, data, *, session=None, wait=None, status=SourceStatus.OK):
        super().__init__(wait=wait)
        self.source = data.source
        self.data = data
        self.session = session
        self.status = status

    async def issue(self, external_id, *, validator=None):
        assert self.session is None or not self.session.in_transaction()
        self.calls.append(("issue", external_id))
        self.started.set()
        if self.wait:
            await self.wait.wait()
        if self.status is SourceStatus.TIMEOUT:
            raise TimeoutError
        return MetadataFetch(
            status=self.status, data=self.data if self.status is SourceStatus.OK else None
        )


def reader(*adapters):
    return MetadataSourceRegistry(
        [runtime(item.data.source, revision=1) for item in adapters],
        factories={
            item.data.source: registration(item, capabilities=list(SourceCapability))
            for item in adapters
        },
    )


async def test_issue_refresh_updates_managed_fields_credits_and_keeps_artifacts(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    series_id, issue_id, bundle = await seed(factory, count=2)
    artifact = tmp_path / "kept.cbz"
    artifact.write_bytes(b"unchanged issue archive")
    async with factory.begin() as session:
        root = LibraryRoot(path=str(tmp_path), name="Existing files")
        user = User(username="reader", password_hash="not-a-live-account")
        session.add_all([root, user])
        await session.flush()
        session.add(
            LibraryFile(
                issue_id=issue_id,
                library_root_id=root.id,
                file_path=str(artifact),
                file_name=artifact.name,
                file_format=FileFormat.CBZ,
                file_size=artifact.stat().st_size,
                file_modified_at=datetime.now(UTC),
                storage_mode=LibraryFileStorageMode.REFERENCED,
            )
        )
        session.add(
            IssueReaderState(user_id=user.id, issue_id=issue_id, last_page_index=17, page_count=24)
        )
        (await session.get(Issue, issue_id)).description = "Manual description"
    profile = bundle.issues[0].model_copy(
        update={
            "title": "Refreshed issue title",
            "description": "Provider description",
            "page_count": 48,
            "cover_date": date(2024, 1, 1),
            "credits": (MetadataCredit(name="Jane Writer", role="writer"),),
        }
    )
    async with factory() as session:
        adapter = IssueAdapter(profile, session=session)
        result = await refresh_issue_from_sources(
            session, issue_id, gcd_api_enabled=False, registry=reader(adapter)
        )
        assert result.outcomes == []
        await session.commit()
    async with factory() as session:
        issue = await session.get(Issue, issue_id)
        assert issue.title == "Refreshed issue title" and issue.description == "Manual description"
        assert issue.status is IssueStatus.OWNED and issue.manual_skip
        assert issue.series_id == series_id and issue.issue_number_text == "1"
        assert issue.page_count == 48
        assert await session.scalar(select(func.count()).select_from(Issue)) == 2
        other = await session.scalar(select(Issue).where(Issue.id != issue_id))
        assert other.title != "Refreshed issue title"
        file = await session.scalar(select(LibraryFile))
        assert file.issue_id == issue_id and file.file_path == str(artifact)
        assert file.storage_mode is LibraryFileStorageMode.REFERENCED
        assert (await session.scalar(select(IssueReaderState))).last_page_index == 17
        assert (await read_issue_credits(session, [issue_id]))[issue_id] == profile.credits
        saved = await load_metadata_baseline(session, Kind.ISSUE, issue_id)
        assert saved.revision == 2
        assert next(
            item for item in saved.snapshot.origins if item.field == "description"
        ).user_override
    assert artifact.read_bytes() == b"unchanged issue archive"
    assert adapter.calls == [("issue", "100")]


@pytest.mark.parametrize("change", ["edit", "policy", "parent", "baseline"])
async def test_changes_during_provider_io_abort_the_entire_issue_refresh(identity_probe_db, change):
    _, factory, _ = identity_probe_db
    series_id, issue_id, bundle = await seed(factory)
    wait = asyncio.Event()
    async with factory() as session:
        adapter = IssueAdapter(
            bundle.issues[0].model_copy(update={"title": "Stale title", "page_count": 48}),
            session=session,
            wait=wait,
        )
        task = asyncio.create_task(
            refresh_issue_from_sources(
                session, issue_id, gcd_api_enabled=False, registry=reader(adapter)
            )
        )
        try:
            await asyncio.wait_for(adapter.started.wait(), 5)
            async with factory.begin() as writer:
                if change == "edit":
                    (await writer.get(Issue, issue_id)).title = "New manual title"
                elif change == "policy":
                    config = await writer.scalar(
                        select(MetadataSourceConfig).where(
                            MetadataSourceConfig.source == "metron_api"
                        )
                    )
                    config.revision += 1
                elif change == "baseline":
                    from pullbox.models.metadata_baseline import IssueMetadataBaseline

                    (await writer.scalar(select(IssueMetadataBaseline))).revision += 1
                else:
                    claim = await writer.scalar(
                        select(SeriesExternalIdentity).where(
                            SeriesExternalIdentity.series_id == series_id
                        )
                    )
                    claim.revision += 1
            wait.set()
            with pytest.raises(ValueError, match="changed"):
                await task
            # Catching the domain error must not allow partial metadata writes to commit.
            await session.commit()
        finally:
            wait.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        issue = await session.get(Issue, issue_id)
        assert issue.title == ("New manual title" if change == "edit" else "Original issue title")
        assert issue.page_count is None
        saved = await load_metadata_baseline(session, Kind.ISSUE, issue_id)
        assert saved.revision == (2 if change == "baseline" else 1)


@pytest.mark.parametrize("fault", ["timeout", "renumber", "parent", "disabled"])
async def test_issue_refresh_never_applies_failed_or_contradictory_metadata(
    identity_probe_db, fault
):
    _, factory, _ = identity_probe_db
    _, issue_id, bundle = await seed(factory)
    profile = bundle.issues[0].model_copy(update={"title": "Must not save", "page_count": 48})
    if fault == "renumber":
        profile.issue_number_text = "2"
    elif fault == "parent":
        profile.series_external_id = "999"
    async with factory() as session:
        adapter = IssueAdapter(
            profile, status=SourceStatus.TIMEOUT if fault == "timeout" else SourceStatus.OK
        )
        registry = reader(adapter)
        if fault == "disabled":
            registry.runtime[profile.source] = runtime(profile.source, enabled=False, revision=1)
        with pytest.raises(ValueError):
            await refresh_issue_from_sources(
                session, issue_id, gcd_api_enabled=False, registry=registry
            )
        await session.commit()
    async with factory() as session:
        issue = await session.get(Issue, issue_id)
        assert issue.title == "Original issue title" and issue.page_count is None
        assert (await load_metadata_baseline(session, Kind.ISSUE, issue_id)).revision == 1


@pytest.mark.parametrize(
    "second_status",
    [SourceStatus.OK, SourceStatus.DISABLED, SourceStatus.FEATURE_DISABLED, SourceStatus.TIMEOUT],
)
async def test_issue_refresh_uses_verified_secondary_provider_for_missing_fields(
    identity_probe_db, second_status
):
    _, factory, _ = identity_probe_db
    series_id, issue_id, bundle = await seed(factory)
    async with factory.begin() as session:
        session.add(
            SeriesExternalIdentity(
                series_id=series_id,
                identity_namespace=Namespace.GCD,
                external_id="2999",
                verification_state="verified",
                evidence_kind="user_selection",
            )
        )
        session.add(
            IssueExternalIdentity(
                issue_id=issue_id,
                identity_namespace=Namespace.GCD,
                external_id="9000",
                verification_state="verified",
                evidence_kind="user_selection",
            )
        )
    metron = IssueAdapter(bundle.issues[0].model_copy(update={"title": "Metron title"}))
    second_source = (
        Source.GCD_API_V2 if second_status is SourceStatus.FEATURE_DISABLED else Source.GCD_LOCAL
    )
    gcd = IssueAdapter(
        bundle.issues[0].model_copy(
            update={
                "source": second_source,
                "identity_namespace": Namespace.GCD,
                "external_id": "9000",
                "series_external_id": "2999",
                "page_count": 64,
                "title": "Lower priority title",
            }
        ),
        status=SourceStatus.TIMEOUT if second_status is SourceStatus.TIMEOUT else SourceStatus.OK,
    )
    registry = reader(metron, gcd)
    if second_status is SourceStatus.DISABLED:
        registry.runtime[second_source] = runtime(second_source, enabled=False, revision=1)
    async with factory() as session:
        result = await refresh_issue_from_sources(
            session, issue_id, gcd_api_enabled=False, registry=registry
        )
        assert [(item.source, item.status) for item in result.outcomes] == (
            [(second_source, SourceStatus.TIMEOUT)] if second_status is SourceStatus.TIMEOUT else []
        )
        await session.commit()
        issue = await session.get(Issue, issue_id)
        assert issue.title == "Metron title"
        assert issue.page_count == (64 if second_status is SourceStatus.OK else None)
        panel = await read_issue_links(session, issue_id, gcd_api_enabled=False)
        origins = {item.field: item for item in panel.origins}
        assert origins["title"].source is Source.METRON_API
        if second_status is SourceStatus.OK:
            assert origins["page_count"].source is Source.GCD_LOCAL
    assert metron.calls == [("issue", "100")]
    assert gcd.calls == (
        []
        if second_status in {SourceStatus.DISABLED, SourceStatus.FEATURE_DISABLED}
        else [("issue", "9000")]
    )
