"""Import identity history and rollback protect subsequent metadata decisions."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, func, select

from pullbox.core.metadata_identity_state import IdentityVerificationState
from pullbox.models import StoryArc, StoryArcExternalIdentity
from pullbox.models.import_job import ImportJobAction
from pullbox.models.metadata_identity import StoryArcIdentityEvent
from pullbox.models.story_arc import ImportedStoryArcStatus, StoryArcSourceKind
from pullbox.services.import_job_actions import record_action, rollback_action
from pullbox.services.import_story_arc_materialization import materialize_confirmed_story_arcs
from pullbox.services.metadata_identity_attachment import attach_verified_identities
from tests.integration.metadata_identity.test_arc_identity_attachment import _arc_request
from tests.unit.test_import_story_arc_materialization import (
    _add_job,
    _add_staged_arc,
    _add_staged_entry,
    _confirmed_policy,
)


async def _staging(session, *, source=StoryArcSourceKind.MYLAR3, target=None):
    job = await _add_job(session)
    staged = await _add_staged_arc(
        session,
        job=job,
        name="Imported arc",
        source_key="import:arc",
        source_arc_id="local-arc",
        source_kind=source,
        proposed_story_arc_id=target,
        policy=_confirmed_policy(source=source.value),
    )
    entry = await _add_staged_entry(
        session,
        staged_arc=staged,
        source_ordinal=1,
        reading_order=1,
        issue_number_text="50-x",
        cv_arc_id="4045-12",
    )
    return job, staged, entry


async def _run(session, job):
    return await materialize_confirmed_story_arcs(
        session, import_job_id=job.id, record_action=record_action, entry_checkpoint_size=1
    )


async def _undo(session, action):
    await rollback_action(
        session,
        action_id=action.id,
        action_type=action.action_type,
        payload=dict(action.payload),
        delete_series=AsyncMock(side_effect=AssertionError("Unexpected series deletion")),
    )
    await session.flush()


@pytest.mark.parametrize("source", [StoryArcSourceKind.MYLAR3, StoryArcSourceKind.FOLDER])
async def test_import_verifies_canonical_identity_with_stable_local_history(
    identity_probe_db, source
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, staged, entry = await _staging(session, source=source)
        result = await _run(session, job)
        assert result.arcs_created == 1
        arc = await session.get(StoryArc, staged.materialized_story_arc_id)
        assert arc.comicvine_id == 12
        owners = list(await session.scalars(select(StoryArcExternalIdentity)))
        canonical = next(row for row in owners if row.source == "comicvine")
        scoped = next(row for row in owners if row.source == source.value)
        assert canonical.verification_state == "verified" and canonical.revision == 1
        assert scoped.verification_state is None
        history = await session.scalar(select(StoryArcIdentityEvent))
        request = json.loads(history.request_json)
        assert request["origin"] == {"record_kind": "imported_story_arc", "record_id": staged.id}
        assert request["evidence_kind"] == (
            "mylar_database" if source == StoryArcSourceKind.MYLAR3 else "migration"
        )
        assert request["actor"] == "automation" and request["action"] == "verify"
        assert entry.evidence["cv_arc_id"] == "4045-12"
        first_actions = list(await session.scalars(select(ImportJobAction.id)))
        staged.status = ImportedStoryArcStatus.CONFIRMED
        result = await _run(session, job)
        assert result.arcs_reused == 1 and result.arcs_failed == 0
        assert list(await session.scalars(select(ImportJobAction.id))) == first_actions
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 1
        assert canonical.revision == 1
        # A genuinely changed source snapshot creates an observation, not a retry.
        entry.evidence = {**entry.evidence, "source_revision": "changed"}
        staged.status = ImportedStoryArcStatus.CONFIRMED
        await _run(session, job)
        await session.refresh(canonical)
        assert canonical.revision == 2
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 2


@pytest.mark.parametrize("obstruction", ["legacy_owner", "conflicted"])
async def test_identity_failure_rolls_back_only_its_arc_and_continues(
    identity_probe_db, obstruction
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        existing = StoryArc(name="Existing", comicvine_id=12)
        session.add(existing)
        await session.flush()
        if obstruction == "conflicted":
            await attach_verified_identities(session, [_arc_request(existing.id, "12")])
            owner = await session.scalar(select(StoryArcExternalIdentity))
            owner.verification_state = IdentityVerificationState.CONFLICTED
        job, staged, _ = await _staging(
            session, target=existing.id if obstruction == "conflicted" else None
        )
        other = await _add_staged_arc(
            session, job=job, name="Unrelated", source_key="other", source_arc_id="other"
        )
        result = await _run(session, job)
        assert result.arcs_failed == 1 and result.arcs_created == 1
        assert result.arcs_merged == 0 and result.external_identities_created == 1
        assert staged.status == ImportedStoryArcStatus.FAILED
        assert staged.materialized_story_arc_id is None
        assert other.status == ImportedStoryArcStatus.IMPORTED
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 2
        assert (
            await session.scalar(
                select(StoryArcExternalIdentity.id).where(
                    StoryArcExternalIdentity.external_id == "local-arc"
                )
            )
            is None
        )
        actions = list(await session.scalars(select(ImportJobAction)))
        assert all(row.payload.get("imported_story_arc_id") != staged.id for row in actions)
        await session.refresh(existing)
        assert existing.revision == 1 and not existing.monitored


@pytest.mark.parametrize("existing_identity", [False, True])
async def test_import_rollback_restores_prior_ownership_and_retains_history(
    identity_probe_db, existing_identity
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        arc = StoryArc(name="Existing")
        session.add(arc)
        await session.flush()
        if existing_identity:
            session.add(
                StoryArcExternalIdentity(
                    story_arc_id=arc.id,
                    source="comicvine",
                    namespace="story_arc",
                    external_id="12",
                    evidence={"keep": True},
                    source_url="https://example.invalid/arc",
                )
            )
        job, _staged, _ = await _staging(session, target=arc.id)
        await _run(session, job)
        history = await session.scalar(select(StoryArcIdentityEvent))
        assert history is not None
        original_request = history.request_json
        actions = list(
            await session.scalars(
                select(ImportJobAction).order_by(ImportJobAction.sequence_no.desc())
            )
        )
        for action in actions:
            await _undo(session, action)
        await session.refresh(arc)
        assert arc.comicvine_id is None and not arc.monitored
        assert arc.revision == 1
        canonical = await session.scalar(
            select(StoryArcExternalIdentity).where(StoryArcExternalIdentity.source == "comicvine")
        )
        if existing_identity:
            assert (
                canonical.evidence == {"keep": True}
                and canonical.source_url == "https://example.invalid/arc"
            )
            assert canonical.verification_state is None and canonical.revision == 3
        else:
            assert canonical is None
        events = list(
            await session.scalars(select(StoryArcIdentityEvent).order_by(StoryArcIdentityEvent.id))
        )
        assert len(events) == 2 and events[0].request_json == original_request
        assert events[-1].verification_state == "observed"
        assert json.loads(events[-1].request_json)["origin"]["record_kind"] == "import_job_action"
        identity_action = next(
            row for row in actions if row.action_type == "story_arc_identity_verified"
        )
        # A repeated rollback never undoes a subsequent fresh provider observation.
        await attach_verified_identities(session, [_arc_request(arc.id, "12")])
        await _undo(session, identity_action)
        await session.refresh(arc)
        assert arc.comicvine_id == 12
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 3


@pytest.mark.parametrize(
    "change", ["observation", "revision", "state", "column", "other_claim", "history_drift"]
)
async def test_rollback_refuses_later_identity_changes(identity_probe_db, change):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, staged, _ = await _staging(session)
        await _run(session, job)
        action = await session.scalar(
            select(ImportJobAction).where(
                ImportJobAction.action_type == "story_arc_identity_verified"
            )
        )
        assert action is not None
        arc = await session.get(StoryArc, staged.materialized_story_arc_id)
        owner = await session.scalar(
            select(StoryArcExternalIdentity).where(StoryArcExternalIdentity.source == "comicvine")
        )
        if change == "observation":
            await attach_verified_identities(session, [_arc_request(arc.id, "12")])
        elif change == "revision":
            owner.revision += 1
        elif change == "state":
            owner.verification_state = IdentityVerificationState.STALE
        elif change == "column":
            arc.comicvine_id = 13
        elif change == "history_drift":
            history = await session.scalar(select(StoryArcIdentityEvent))
            history.verification_state = IdentityVerificationState.REJECTED
        else:
            from pullbox.core.metadata_identity_events import prepare_identity_event

            request = _arc_request(arc.id, "13")
            prepared = prepare_identity_event(request)
            session.add(
                StoryArcIdentityEvent(
                    story_arc_id=arc.id,
                    identity_namespace="comicvine",
                    external_id="13",
                    verification_state="conflicted",
                    evidence_kind="provider_result",
                    event_key=prepared.event_key,
                    request_fingerprint=prepared.request_fingerprint,
                    request_json=prepared.request_json,
                )
            )
        await session.flush()
        with pytest.raises(ValueError, match="changed after import"):
            await _undo(session, action)
        assert await session.get(StoryArcExternalIdentity, owner.id) is not None


async def test_created_arc_can_be_rolled_back_without_hidden_commits(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, _staged, _ = await _staging(session)
        job_id = job.id
    async with factory() as session:
        from pullbox.models.import_job import ImportJob

        job = await session.get(ImportJob, job_id)
        await _run(session, job)
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 1
        await session.rollback()
    async with factory.begin() as session:
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 0
        job = await session.get(ImportJob, job_id)
        await _run(session, job)
        actions = list(
            await session.scalars(
                select(ImportJobAction).order_by(ImportJobAction.sequence_no.desc())
            )
        )
        for action in actions:
            await _undo(session, action)
        assert await session.scalar(select(func.count()).select_from(StoryArc)) == 0
        assert await session.scalar(select(func.count()).select_from(StoryArcExternalIdentity)) == 0
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 0


async def test_several_import_observations_undo_in_reverse_without_revision_rewind(
    identity_probe_db,
):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        arc = StoryArc(name="Existing")
        session.add(arc)
        await session.flush()
        await attach_verified_identities(session, [_arc_request(arc.id, "12")])
        job, staged, entry = await _staging(session, target=arc.id)
        for revision in range(3):
            entry.evidence = {**entry.evidence, "scan_revision": revision}
            staged.status = ImportedStoryArcStatus.CONFIRMED
            await _run(session, job)
        actions = list(
            await session.scalars(
                select(ImportJobAction)
                .where(ImportJobAction.action_type == "story_arc_identity_verified")
                .order_by(ImportJobAction.sequence_no.desc())
            )
        )
        assert len(actions) == 3
        for index, action in enumerate(actions, 5):
            from pullbox.services.import_story_arc_identity import identity_state

            owner = await session.scalar(
                select(StoryArcExternalIdentity)
                .where(StoryArcExternalIdentity.source == "comicvine")
                .execution_options(populate_existing=True)
            )
            assert identity_state(owner) == {
                **action.payload["expected_after"],
                "revision": index - 1,
            }
            await _undo(session, action)
            owner = await session.scalar(
                select(StoryArcExternalIdentity).where(
                    StoryArcExternalIdentity.source == "comicvine"
                )
            )
            assert owner.revision == index and owner.verification_state == "verified"
        events = list(
            await session.scalars(select(StoryArcIdentityEvent).order_by(StoryArcIdentityEvent.id))
        )
        assert len(events) == 7
        assert owner.evidence_locator == events[0].request_json


@pytest.mark.parametrize("changed", [False, True])
async def test_legacy_identity_journal_cannot_erase_new_verification(identity_probe_db, changed):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        arc = StoryArc(name="Existing")
        session.add(arc)
        await session.flush()
        job, staged, _ = await _staging(session, target=arc.id)
        staged.materialized_story_arc_id = arc.id
        owner = StoryArcExternalIdentity(
            story_arc_id=arc.id,
            source="comicvine",
            namespace="story_arc",
            external_id="12",
            evidence={"legacy": True},
        )
        session.add(owner)
        await session.flush()
        legacy_state = {
            "story_arc_id": arc.id,
            "source": "comicvine",
            "namespace": "story_arc",
            "external_id": "12",
            "source_url": None,
            "evidence": {"legacy": True},
        }
        await record_action(
            session,
            job,
            phase="story_arcs",
            action_type="story_arc_external_identity_created",
            payload={
                "external_identity_id": owner.id,
                "story_arc_id": arc.id,
                "imported_story_arc_id": staged.id,
                "expected_after": legacy_state,
            },
        )
        action = await session.scalar(select(ImportJobAction))
        if changed:
            await attach_verified_identities(session, [_arc_request(arc.id, "12")])
            # The old journal has no lifecycle fields, and the legacy JSON did not change.
            assert owner.evidence == legacy_state["evidence"]
            with pytest.raises(ValueError, match="changed after import"):
                await _undo(session, action)
        else:
            await _undo(session, action)
            assert await session.scalar(select(StoryArcExternalIdentity.id)) is None


async def test_import_rollback_rejects_modified_selected_action(identity_probe_db):
    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, _staged, _ = await _staging(session)
        await _run(session, job)
        action = await session.scalar(
            select(ImportJobAction).where(
                ImportJobAction.action_type == "story_arc_identity_verified"
            )
        )
        with pytest.raises(ValueError, match="action changed"):
            await rollback_action(
                session,
                action_id=action.id,
                action_type=action.action_type,
                payload={**action.payload, "comicvine_id_before": 999},
                delete_series=AsyncMock(),
            )
        assert await session.scalar(select(StoryArc.comicvine_id)) == 12


async def test_failure_after_identity_write_discards_history_and_cached_owner(
    identity_probe_db, monkeypatch
):
    from pullbox.services import import_story_arc_materialization as materializer
    from pullbox.services.metadata_identity_attachment import IdentityAttachmentConflictError

    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, staged, _ = await _staging(session)
        other = await _add_staged_arc(
            session, job=job, name="Surviving arc", source_key="other", source_arc_id="other"
        )
        await _add_staged_entry(
            session,
            staged_arc=other,
            source_ordinal=1,
            reading_order=1,
            issue_number_text="1",
            cv_arc_id="4045-12",
        )
        original = materializer.attach_import_arc_identity
        failed_id = staged.id

        async def fail_after_attach(*args, **kwargs):
            result = await original(*args, **kwargs)
            if kwargs["staged"].id == failed_id:
                raise IdentityAttachmentConflictError("Interleaved conflict")
            return result

        monkeypatch.setattr(materializer, "attach_import_arc_identity", fail_after_attach)
        result = await _run(session, job)
        assert result.arcs_created == result.arcs_failed == 1
        assert result.external_identities_created == 2
        assert staged.status == ImportedStoryArcStatus.FAILED
        assert other.status == ImportedStoryArcStatus.IMPORTED
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 1
        assert await session.scalar(select(StoryArc.name)) == "Surviving arc"
        actions = list(await session.scalars(select(ImportJobAction)))
        assert all(
            action.payload.get("imported_story_arc_id", other.id) == other.id for action in actions
        )


async def test_created_arc_rollback_preserves_later_unowned_provider_evidence(identity_probe_db):
    from pullbox.core.metadata_identity import IdentityNamespace
    from pullbox.core.metadata_identity_events import prepare_identity_event

    _, factory, _ = identity_probe_db
    async with factory.begin() as session:
        job, staged, _ = await _staging(session)
        await _run(session, job)
        arc_id = staged.materialized_story_arc_id
        request = _arc_request(arc_id, "44", IdentityNamespace.METRON)
        prepared = prepare_identity_event(request)
        # An observation without active ownership must not be lost when the
        # importer undoes only its own ComicVine attachment.
        session.add(
            StoryArcIdentityEvent(
                story_arc_id=arc_id,
                identity_namespace="metron",
                external_id="44",
                verification_state="observed",
                evidence_kind="provider_result",
                event_key=prepared.event_key,
                request_fingerprint=prepared.request_fingerprint,
                request_json=prepared.request_json,
            )
        )
        actions = list(
            await session.scalars(
                select(ImportJobAction).order_by(ImportJobAction.sequence_no.desc())
            )
        )
        for action in actions:
            if action.action_type == "story_arc_created":
                with pytest.raises(ValueError, match="identity history"):
                    await _undo(session, action)
            else:
                await _undo(session, action)
        assert await session.get(StoryArc, arc_id) is not None
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 3


async def test_rollback_waits_for_concurrent_provider_writer_and_rechecks(identity_probe_db):
    engine, factory, _ = identity_probe_db
    if engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL row-lock behavior; SQLite has a serialized writer")
    async with factory.begin() as session:
        job, staged, _ = await _staging(session)
        await _run(session, job)
        arc_id = staged.materialized_story_arc_id
        action_id = await session.scalar(
            select(ImportJobAction.id).where(
                ImportJobAction.action_type == "story_arc_identity_verified"
            )
        )
    attempted_lock = asyncio.Event()

    def capture_lock(_conn, _cursor, statement, _params, _context, _many):
        if "story_arcs" in statement and "FOR UPDATE" in statement:
            attempted_lock.set()

    async def undo():
        async with factory.begin() as session:
            action = await session.get(ImportJobAction, action_id)
            with pytest.raises(ValueError, match="changed after import"):
                await _undo(session, action)

    pending = None
    try:
        async with factory.begin() as writer:
            await attach_verified_identities(writer, [_arc_request(arc_id, "12")])
            event.listen(engine.sync_engine, "before_cursor_execute", capture_lock)
            pending = asyncio.create_task(undo())
            await asyncio.wait_for(attempted_lock.wait(), timeout=5)
            await asyncio.sleep(0.05)
            assert not pending.done()
        await asyncio.wait_for(pending, timeout=10)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_lock)
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    async with factory() as session:
        assert (
            await session.scalar(select(StoryArc.comicvine_id).where(StoryArc.id == arc_id)) == 12
        )
        assert await session.scalar(select(func.count()).select_from(StoryArcIdentityEvent)) == 2
