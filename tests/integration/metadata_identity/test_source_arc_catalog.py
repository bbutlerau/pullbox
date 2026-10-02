"""The existing arc saver must not interpret Metron identities as ComicVine IDs."""

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from pullbox.core.metadata_identity import (
    ExactIdentityEvidence,
    ExternalIdentityRef,
    IdentityEvidenceKind,
    IdentityNamespace,
    MetadataEntityKind,
    MetadataSource,
)
from pullbox.core.metadata_identity_events import IdentityEventEvidence, IdentityEventRequest
from pullbox.core.metadata_identity_state import IdentityVerificationAction
from pullbox.models import Issue, Series, StoryArc, StoryArcExternalIdentity
from pullbox.models.metadata_identity import (
    IssueExternalIdentity,
    IssueIdentityEvent,
    SeriesExternalIdentity,
    SeriesIdentityEvent,
    StoryArcIdentityEvent,
)
from pullbox.models.metadata_source import MetadataSourceConfig
from pullbox.models.story_arc import IssueStoryArc
from pullbox.services.metadata_identity_attachment import attach_verified_identities
from pullbox.services.story_arc_catalog import StoryArcCatalogError, StoryArcCatalogService
from pullbox.services.story_arc_catalog_types import snapshot_fingerprint
from tests.unit.test_story_arc_catalog import _issue, _provider, _root

SOURCE = MetadataSource.METRON_API


def _service(provider=None):
    return StoryArcCatalogService(provider or _provider(), source=SOURCE, source_revision=1)


async def _setup(factory, tmp_path):
    library = tmp_path / "comics"
    library.mkdir()
    async with factory.begin() as session:
        root = await _root(session, library)
        session.add(MetadataSourceConfig(source=SOURCE.value, enabled=True, priority=3, revision=1))
        return root.id


async def _add(service, session, preview, root):
    return await service.add(
        session,
        preview,
        ordered_issue_provider_ids=preview.metadata.issue_provider_ids,
        library_root_id=root,
    )


async def _verify(session, kind, target, external):
    identity = ExternalIdentityRef(IdentityNamespace.METRON, kind, external)
    parent = ExternalIdentityRef(IdentityNamespace.METRON, MetadataEntityKind.SERIES, "21")
    await attach_verified_identities(
        session,
        [
            IdentityEventRequest(
                uuid4(),
                target,
                IdentityVerificationAction.VERIFY,
                IdentityEventEvidence(
                    ExactIdentityEvidence(identity, IdentityEvidenceKind.PROVIDER_RESULT, SOURCE),
                    "a" * 64,
                    source_identity=identity,
                    parent_identity=parent if kind is MetadataEntityKind.ISSUE else None,
                ),
            )
        ],
    )


async def test_metron_arc_seeds_only_members_without_cv_ids(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    provider = _provider([_issue(number="13AU"), _issue("12", "22", "2")])
    service = _service(provider)
    preview = await service.preview("31")
    async with factory.begin() as session:
        arc = await _add(service, session, preview, root)
        assert arc.comicvine_id is None
        assert arc.diagnostics["provider_catalog"]["snapshot"]["provider"] == "metron"
        assert await service.find_existing(session, ["31"]) == {"31": arc.id}
        for model in (Series, Issue):
            rows = list(await session.scalars(select(model)))
            assert len(rows) == 2 and all(row.comicvine_id is None for row in rows)
        assert set(await session.scalars(select(Issue.issue_number_text))) == {"13AU", "2"}
        assert set(await session.scalars(select(Series.metadata_source))) == {"metron_api_partial"}
        for model in (SeriesExternalIdentity, IssueExternalIdentity):
            rows = list(await session.scalars(select(model)))
            assert len(rows) == 2
            assert all(row.identity_namespace == "metron" for row in rows)
        assert (await session.scalar(select(StoryArcExternalIdentity))).source == "metron"
        members = list(await session.scalars(select(IssueStoryArc)))
        assert all(row.evidence["provider"] == "metron" for row in members)
        assert all(row.resolution_method == "exact_metron_id" for row in members)
    assert not list((tmp_path / "comics").iterdir())
    assert provider.get_series.await_count == 2


async def test_same_numeric_cv_ids_do_not_capture_metron_arc(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    async with factory.begin() as session:
        parent = Series(title="Other", sort_title="Other", comicvine_id=21)
        session.add(parent)
        await session.flush()
        session.add_all(
            [
                Issue(series_id=parent.id, comicvine_id=11, issue_number=1),
                StoryArc(name="Other arc", comicvine_id=31),
            ]
        )
    service = _service()
    preview = await service.preview("31")
    async with factory.begin() as session:
        assert await service.find_existing(session, ["31"]) == {}
        arc = await _add(service, session, preview, root)
        assert arc.comicvine_id is None
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 2
        assert await session.scalar(select(func.count()).select_from(Issue)) == 3


async def test_metron_arc_reuses_verified_owned_members_without_overwrite(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    async with factory.begin() as session:
        parent = Series(title="My title", sort_title="My title", monitored=True, path="/untouched")
        session.add(parent)
        await session.flush()
        issue = Issue(series_id=parent.id, issue_number=1, title="My issue", status="owned")
        session.add(issue)
        await session.flush()
        await _verify(session, MetadataEntityKind.SERIES, parent.id, "21")
        await _verify(session, MetadataEntityKind.ISSUE, issue.id, "11")
        parent_id, issue_id = parent.id, issue.id
    service = _service()
    preview = await service.preview("31")
    async with factory.begin() as session:
        arc = await _add(service, session, preview, root)
        member_ids = set(
            await session.scalars(
                select(IssueStoryArc.issue_id).where(IssueStoryArc.story_arc_id == arc.id)
            )
        )
        assert issue_id in member_ids
        parent, issue = await session.get(Series, parent_id), await session.get(Issue, issue_id)
        assert (parent.title, parent.path, parent.monitored) == ("My title", "/untouched", True)
        assert (issue.title, issue.status) == ("My issue", "owned")


@pytest.mark.parametrize("change", ["revision", "disabled", "missing", "snapshot_source"])
async def test_metron_source_proof_is_required_before_any_write(
    identity_probe_db, tmp_path, change
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    service = _service()
    preview = await service.preview("31")
    if change == "missing":
        preview = replace(preview, source_revision=None)
        preview = replace(preview, fingerprint=snapshot_fingerprint(preview))
    elif change == "snapshot_source":
        preview = replace(preview, source=MetadataSource.COMICVINE_API)
    else:
        async with factory.begin() as session:
            await session.execute(
                update(MetadataSourceConfig).values(
                    **({"revision": 2} if change == "revision" else {"enabled": False})
                )
            )
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError):
            await _add(service, session, preview, root)
        for model in (StoryArc, Series, Issue, StoryArcIdentityEvent):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_metron_refresh_adds_new_members_preserves_order_and_removed_members(
    identity_probe_db, tmp_path
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    provider = _provider()
    service = _service(provider)
    preview = await service.preview("31")
    async with factory.begin() as session:
        arc = await _add(service, session, preview, root)
        arc_id, revision = arc.id, arc.revision
    provider.get_story_arc.return_value = replace(
        provider.get_story_arc.return_value, issue_provider_ids=("12", "13")
    )
    provider.get_story_arc_issues.return_value = [
        _issue("12", "22", "1000000"),
        _issue("13", "21", "2"),
    ]
    refreshed = await service.preview("31")
    async with factory.begin() as session:
        delta = await service.preview_refresh(session, arc_id, refreshed)
        assert delta.added_issue_provider_ids == ("13",)
        assert delta.removed_issue_provider_ids == ("11",)
        result = await service.refresh(session, arc_id, refreshed, expected_revision=revision)
        assert len(result.added_membership_ids) == 1
        rows = list(
            await session.scalars(
                select(IssueStoryArc)
                .where(IssueStoryArc.story_arc_id == arc_id)
                .order_by(IssueStoryArc.sequence_number)
            )
        )
        assert [row.source_issue_id for row in rows] == ["11", "12", "13"]
        assert rows[-1].sync_eligible is False
        assert rows[-1].evidence["catalog_review_required"] is True


@pytest.mark.parametrize("failure", ["caller_rollback", "conflicted", "wrong_parent"])
async def test_metron_adoption_is_one_atomic_graph(identity_probe_db, tmp_path, failure):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    service = _service()
    preview = await service.preview("31")
    if failure != "caller_rollback":
        async with factory.begin() as session:
            parent = Series(title="Existing", sort_title="Existing")
            session.add(parent)
            await session.flush()
            session.add(
                SeriesExternalIdentity(
                    series_id=parent.id,
                    identity_namespace="metron",
                    external_id="22",
                    verification_state="conflicted" if failure == "conflicted" else "verified",
                    evidence_kind="provider_result",
                )
            )
            if failure == "wrong_parent":
                issue = Issue(series_id=parent.id, issue_number=1)
                session.add(issue)
                await session.flush()
                session.add(
                    IssueExternalIdentity(
                        issue_id=issue.id,
                        identity_namespace="metron",
                        external_id="11",
                        verification_state="verified",
                        evidence_kind="provider_result",
                    )
                )
    async with factory() as session:
        if failure == "caller_rollback":
            await _add(service, session, preview, root)
        else:
            with pytest.raises(StoryArcCatalogError):
                await _add(service, session, preview, root)
        await session.rollback()
    async with factory() as session:
        for model in (
            StoryArc,
            StoryArcExternalIdentity,
            StoryArcIdentityEvent,
            SeriesIdentityEvent,
            IssueIdentityEvent,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert await session.scalar(select(func.count()).select_from(Series)) == (
            failure != "caller_rollback"
        )
    assert not list((tmp_path / "comics").iterdir())


@pytest.mark.parametrize("target", ["arc", "series", "issue"])
async def test_metron_refresh_does_not_override_reviewed_identity_state(
    identity_probe_db, tmp_path, target
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    service = _service()
    preview = await service.preview("31")
    async with factory.begin() as session:
        arc = await _add(service, session, preview, root)
        arc_id, revision = arc.id, arc.revision
    model = {
        "arc": StoryArcExternalIdentity,
        "series": SeriesExternalIdentity,
        "issue": IssueExternalIdentity,
    }[target]
    async with factory.begin() as session:
        await session.execute(update(model).values(verification_state="stale"))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError, match=r"identity.*review"):
            await service.refresh(session, arc_id, preview, expected_revision=revision)
        assert (await session.get(StoryArc, arc_id)).revision == revision


async def test_metron_native_ids_are_not_limited_to_signed_cv_integers(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    large = str(2**63 + 17)
    provider = _provider([_issue(large, large, "1")])
    provider.get_story_arc.return_value = replace(
        provider.get_story_arc.return_value, provider_id=large
    )
    service = _service(provider)
    preview = await service.preview(large)
    async with factory.begin() as session:
        arc = await _add(service, session, preview, root)
        assert arc.comicvine_id is None
        assert await service.find_existing(session, [large]) == {large: arc.id}


@pytest.mark.parametrize("mutation", ["duplicate_parents", "bad_parent", "blank_title", "count"])
async def test_catalog_snapshot_validation_covers_parent_and_membership_evidence(
    identity_probe_db, tmp_path, mutation
):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    service = _service()
    preview = await service.preview("31")
    if mutation == "duplicate_parents":
        preview = replace(preview, series=(*preview.series, preview.series[0]))
    elif mutation == "bad_parent":
        preview = replace(preview, series=(replace(preview.series[0], provider_id="021"),))
    elif mutation == "blank_title":
        preview = replace(preview, metadata=replace(preview.metadata, title=" "))
    else:
        preview = replace(preview, metadata=replace(preview.metadata, declared_issue_count=99))
    preview = replace(preview, fingerprint=snapshot_fingerprint(preview))
    async with factory.begin() as session:
        with pytest.raises(StoryArcCatalogError):
            await _add(service, session, preview, root)
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0


async def test_metron_path_collision_uses_native_namespace(identity_probe_db, tmp_path):
    _, factory, _ = identity_probe_db
    root = await _setup(factory, tmp_path)
    service = _service()
    preview = await service.preview("31")
    # Both parents propose the same path; the fallback must not imply a CV identity.
    preview = replace(
        preview, series=tuple(replace(row, title="Same title") for row in preview.series)
    )
    preview = replace(preview, fingerprint=snapshot_fingerprint(preview))
    async with factory.begin() as session:
        await _add(service, session, preview, root)
        paths = list(await session.scalars(select(Series.path).order_by(Series.id)))
        assert len(paths) == 2 and paths[0] != paths[1]
        assert paths[1].endswith("[metron-22]")
        assert all("[cv-" not in path for path in paths)


async def test_snapshot_fingerprint_binds_native_source_and_policy_revision():
    service = _service()
    preview = await service.preview("31")
    assert preview.fingerprint != snapshot_fingerprint(replace(preview, source_revision=2))
    assert preview.fingerprint != snapshot_fingerprint(
        replace(preview, source=MetadataSource.COMICVINE_API)
    )
    legacy = await StoryArcCatalogService(_provider()).preview("31")
    from pullbox.services.story_arc_catalog_types import catalog_snapshot

    snapshot = catalog_snapshot(legacy)
    assert snapshot["provider"] == "comicvine"
    assert "source" not in snapshot and "source_revision" not in snapshot
