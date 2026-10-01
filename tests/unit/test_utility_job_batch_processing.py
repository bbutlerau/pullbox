"""Tests for utility batch processing orchestration."""

from __future__ import annotations

from typing import Any

import pytest

import pullbox.utilities.job_queue_batch_processing as batch_processing
from pullbox.utilities.base_executor import (
    ApplyResult,
    ItemResult,
    JobRunSummary,
    ProcessedItem,
    RuntimeLogEntry,
)
from pullbox.utilities.job_queue_batch_processing import process_dispatch_batch
from pullbox.utilities.job_queue_batch_state import BatchCheckpoint
from pullbox.utilities.models import JobState, JobType, UtilityJob, UtilityJobItem


class FakeSession:
    def __init__(self, *, job: UtilityJob, items: dict[str, UtilityJobItem]) -> None:
        self.job = job
        self.items = items
        self.commit_count = 0

    async def get(self, model: type[Any], item_id: str) -> Any:
        if model is UtilityJob:
            assert item_id == self.job.id
            return self.job
        if model is UtilityJobItem:
            return self.items.get(item_id)
        raise AssertionError(f"Unexpected model lookup: {model}")

    async def commit(self) -> None:
        self.commit_count += 1


class FakeSessionContext:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    async def __aenter__(self) -> FakeSession:
        return self.session

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeSessionFactory:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def __call__(self) -> FakeSessionContext:
        return FakeSessionContext(self.session)


class RecordingLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **kwargs: Any) -> None:
        self.warnings.append((event, kwargs))

    def error(self, event: str, **kwargs: Any) -> None:
        self.errors.append((event, kwargs))


class RecordingExecutor:
    def get_execution_mode(self, config, context):
        return "process"

    def __init__(self, apply_result: ApplyResult | None = None) -> None:
        self.apply_result = apply_result or ApplyResult()
        self.after_commit_calls: list[tuple[Any, ...]] = []

    async def apply_item_result(
        self,
        session: FakeSession,
        item: UtilityJobItem,
        payload_data: dict[str, Any],
        processed: ProcessedItem,
        job_config: dict[str, Any],
        job_context: dict[str, Any] | None,
        summary: JobRunSummary,
    ) -> ApplyResult:
        return self.apply_result

    async def after_item_commit(
        self,
        payload_data: dict[str, Any],
        processed: ProcessedItem,
        config: dict[str, Any],
        job_context: dict[str, Any] | None,
        summary: JobRunSummary,
        apply_result: ApplyResult,
    ) -> list[RuntimeLogEntry]:
        self.after_commit_calls.append(
            (payload_data, processed, config, job_context, summary, apply_result)
        )
        return [
            RuntimeLogEntry(
                level="INFO",
                message="post commit detail",
                extra={"step": "after"},
            )
        ]


class SuccessWorkerPool:
    async def iter_batch_results(
        self,
        payloads: list[dict[str, Any]],
        executor: Any,
        config: dict[str, Any],
        job_context: dict[str, Any] | None,
    ) -> Any:
        yield ProcessedItem(
            item_id=payloads[0]["id"],
            result=ItemResult.COMPLETED,
            duration_ms=123,
            worker_id=4,
        )


class FailingWorkerPool:
    async def iter_batch_results(
        self,
        payloads: list[dict[str, Any]],
        executor: Any,
        config: dict[str, Any],
        job_context: dict[str, Any] | None,
    ) -> Any:
        raise RuntimeError("worker pool exploded")
        yield


def _job() -> UtilityJob:
    return UtilityJob(
        id="job-1",
        job_type=JobType.FILE_CONVERT,
        display_name="Convert",
        state=JobState.RUNNING,
        config="{}",
        total_items=2,
        completed_items=0,
        failed_items=0,
        skipped_items=0,
        warning_count=0,
    )


def _item(item_id: str) -> UtilityJobItem:
    return UtilityJobItem(
        id=item_id,
        job_id="job-1",
        item_index=0,
        state="IN_PROGRESS",
        file_path=f"/imports/{item_id}.cbr",
        operation="convert",
    )


@pytest.mark.asyncio
async def test_process_dispatch_batch_persists_results_and_post_commit_logs() -> None:
    log_calls: list[dict[str, Any]] = []
    item = _item("item-1")
    job = _job()
    session = FakeSession(job=job, items={item.id: item})
    summary = JobRunSummary()
    executor = RecordingExecutor()
    logger = RecordingLogger()

    await process_dispatch_batch(
        session_factory=FakeSessionFactory(session),
        job_id="job-1",
        job_type=JobType.FILE_CONVERT,
        executor=executor,
        config={"target": "cbz"},
        job_context={"ready": True},
        summary=summary,
        utility_log_level="INFO",
        batch_items=[item],
        worker_pool=SuccessWorkerPool(),
        persist_log=lambda _session, **kwargs: log_calls.append(kwargs),
        logger=logger,
        timestamp_factory=lambda: "2026-06-07T12:00:00+00:00",
    )

    assert summary.completed == 1
    assert summary.failed == 0
    assert item.state == "COMPLETED"
    assert item.completed_at == "2026-06-07T12:00:00+00:00"
    assert job.completed_items == 1
    assert session.commit_count == 2
    assert logger.warnings == []
    assert logger.errors == []
    assert executor.after_commit_calls
    assert log_calls == [
        {
            "configured_level": "INFO",
            "job_id": "job-1",
            "item_id": "item-1",
            "level": "INFO",
            "message": "post commit detail",
            "file_path": "/imports/item-1.cbr",
            "extra": {"step": "after"},
            "worker_id": 4,
            "duration_ms": 123,
        }
    ]


@pytest.mark.asyncio
async def test_process_dispatch_batch_marks_unseen_items_failed_when_stream_fails() -> None:
    log_calls: list[dict[str, Any]] = []
    item = _item("item-1")
    job = _job()
    session = FakeSession(job=job, items={item.id: item})
    summary = JobRunSummary()
    logger = RecordingLogger()

    await process_dispatch_batch(
        session_factory=FakeSessionFactory(session),
        job_id="job-1",
        job_type=JobType.FILE_CONVERT,
        executor=RecordingExecutor(),
        config={},
        job_context=None,
        summary=summary,
        utility_log_level="INFO",
        batch_items=[item],
        worker_pool=FailingWorkerPool(),
        persist_log=lambda _session, **kwargs: log_calls.append(kwargs),
        logger=logger,
        timestamp_factory=lambda: "2026-06-07T12:30:00+00:00",
    )

    assert summary.completed == 0
    assert summary.failed == 1
    assert item.state == "FAILED"
    assert item.error_message == "Batch dispatch failed: worker pool exploded"
    assert job.failed_items == 1
    assert session.commit_count == 1
    assert logger.errors == [
        (
            "job_batch_dispatch_failed",
            {"job_id": "job-1", "error": "worker pool exploded"},
        )
    ]
    assert log_calls == [
        {
            "configured_level": "INFO",
            "job_id": "job-1",
            "item_id": "item-1",
            "level": "ERROR",
            "message": "Batch dispatch failed: worker pool exploded",
            "file_path": "/imports/item-1.cbr",
            "extra": {},
            "worker_id": None,
            "duration_ms": 0,
        }
    ]


@pytest.mark.asyncio
async def test_process_dispatch_batches_checkpoints_leases_and_processes_each_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint_calls: list[str] = []
    lease_calls: list[dict[str, Any]] = []
    process_calls: list[dict[str, Any]] = []
    timestamp_values = iter(
        [
            "2026-06-07T12:00:00+00:00",
            "2026-06-07T12:00:01+00:00",
        ]
    )
    items = [_item("item-1"), _item("item-2"), _item("item-3")]
    session = FakeSession(job=_job(), items={item.id: item for item in items})
    summary = JobRunSummary()

    async def fake_prepare_batch_checkpoint(
        active_session: FakeSession,
        *,
        job_id: str,
        summary: JobRunSummary,
        get_utility_log_level: Any,
    ) -> BatchCheckpoint:
        assert active_session is session
        checkpoint_calls.append(job_id)
        return BatchCheckpoint(
            should_continue=True,
            utility_log_level="DEBUG" if len(checkpoint_calls) == 1 else "WARNING",
        )

    async def fake_lease_dispatch_batch(
        active_session: FakeSession,
        *,
        pending_items: list[UtilityJobItem],
        batch_start: int,
        batch_size: int,
        started_at: str,
    ) -> list[UtilityJobItem]:
        assert active_session is session
        lease_calls.append(
            {
                "batch_start": batch_start,
                "batch_size": batch_size,
                "started_at": started_at,
            }
        )
        return pending_items[batch_start : batch_start + batch_size]

    async def fake_process_dispatch_batch(**kwargs: Any) -> None:
        process_calls.append(
            {
                "utility_log_level": kwargs["utility_log_level"],
                "batch_item_ids": [item.id for item in kwargs["batch_items"]],
                "job_id": kwargs["job_id"],
                "job_type": kwargs["job_type"],
                "summary": kwargs["summary"],
            }
        )

    monkeypatch.setattr(
        batch_processing,
        "prepare_batch_checkpoint",
        fake_prepare_batch_checkpoint,
    )
    monkeypatch.setattr(batch_processing, "lease_dispatch_batch", fake_lease_dispatch_batch)
    monkeypatch.setattr(batch_processing, "process_dispatch_batch", fake_process_dispatch_batch)

    await batch_processing.process_dispatch_batches(
        session_factory=FakeSessionFactory(session),
        job_id="job-1",
        job_type=JobType.FILE_CONVERT,
        executor=RecordingExecutor(),
        config={"target": "cbz"},
        job_context={"ready": True},
        summary=summary,
        pending_items=items,
        batch_size=2,
        worker_pool=SuccessWorkerPool(),
        get_utility_log_level=lambda _session: None,
        persist_log=lambda *_args, **_kwargs: None,
        logger=RecordingLogger(),
        timestamp_factory=lambda: next(timestamp_values),
    )

    assert checkpoint_calls == ["job-1", "job-1"]
    assert lease_calls == [
        {
            "batch_start": 0,
            "batch_size": 2,
            "started_at": "2026-06-07T12:00:00+00:00",
        },
        {
            "batch_start": 2,
            "batch_size": 2,
            "started_at": "2026-06-07T12:00:01+00:00",
        },
    ]
    assert process_calls == [
        {
            "utility_log_level": "DEBUG",
            "batch_item_ids": ["item-1", "item-2"],
            "job_id": "job-1",
            "job_type": JobType.FILE_CONVERT,
            "summary": summary,
        },
        {
            "utility_log_level": "WARNING",
            "batch_item_ids": ["item-3"],
            "job_id": "job-1",
            "job_type": JobType.FILE_CONVERT,
            "summary": summary,
        },
    ]


@pytest.mark.asyncio
async def test_process_dispatch_batches_stops_when_checkpoint_should_not_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease_called = False
    process_called = False
    item = _item("item-1")
    session = FakeSession(job=_job(), items={item.id: item})

    async def fake_prepare_batch_checkpoint(*_args: Any, **_kwargs: Any) -> BatchCheckpoint:
        return BatchCheckpoint(should_continue=False, utility_log_level="INFO")

    async def fake_lease_dispatch_batch(*_args: Any, **_kwargs: Any) -> list[UtilityJobItem]:
        nonlocal lease_called
        lease_called = True
        return [item]

    async def fake_process_dispatch_batch(**_kwargs: Any) -> None:
        nonlocal process_called
        process_called = True

    monkeypatch.setattr(
        batch_processing,
        "prepare_batch_checkpoint",
        fake_prepare_batch_checkpoint,
    )
    monkeypatch.setattr(batch_processing, "lease_dispatch_batch", fake_lease_dispatch_batch)
    monkeypatch.setattr(batch_processing, "process_dispatch_batch", fake_process_dispatch_batch)

    await batch_processing.process_dispatch_batches(
        session_factory=FakeSessionFactory(session),
        job_id="job-1",
        job_type=JobType.FILE_CONVERT,
        executor=RecordingExecutor(),
        config={},
        job_context=None,
        summary=JobRunSummary(),
        pending_items=[item],
        batch_size=1,
        worker_pool=SuccessWorkerPool(),
        get_utility_log_level=lambda _session: None,
        persist_log=lambda *_args, **_kwargs: None,
        logger=RecordingLogger(),
        timestamp_factory=lambda: "2026-06-07T12:00:00+00:00",
    )

    assert lease_called is False
    assert process_called is False
