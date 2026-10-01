"""Worker runtime setup helpers for utility queue dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any

from pullbox.utilities.base_executor import ExecutionMode
from pullbox.utilities.models import JobType

if TYPE_CHECKING:
    from collections.abc import Callable

    from pullbox.utilities.base_executor import JobExecutor


@dataclass(frozen=True, slots=True)
class WorkerRuntime:
    """Execution mode, batch sizing, and worker pool for a dispatch run."""

    execution_mode: ExecutionMode
    batch_size: int
    worker_pool: Any


def build_worker_runtime(
    *,
    executor: JobExecutor,
    config: dict[str, Any],
    job_context: dict[str, Any] | None,
    worker_count: int,
    worker_pool_factory: Callable[..., Any],
) -> WorkerRuntime:
    """Build the worker pool and batch size for one utility dispatch run."""
    execution_mode = executor.get_execution_mode(config, job_context)
    batch_size = (
        1 if execution_mode in {ExecutionMode.SERIAL, ExecutionMode.ASYNC} else worker_count
    )
    factory_signature = signature(worker_pool_factory)
    accepts_execution_mode = "execution_mode" in factory_signature.parameters or any(
        param.kind == Parameter.VAR_KEYWORD for param in factory_signature.parameters.values()
    )
    if accepts_execution_mode:
        worker_pool = worker_pool_factory(
            execution_mode=execution_mode,
            max_workers=worker_count,
        )
    else:
        worker_pool = worker_pool_factory(max_workers=worker_count)
    return WorkerRuntime(
        execution_mode=execution_mode,
        batch_size=batch_size,
        worker_pool=worker_pool,
    )


async def _iter_rollback_processed_items(
    *,
    payloads: list[dict[str, Any]],
    executor: JobExecutor,
    config: dict[str, Any],
) -> Any:
    """Run rollback work inline to preserve rollback executor DB boundaries."""
    for payload in payloads:
        processed = executor.process_item(payload, config)
        processed.worker_id = 1
        yield processed


async def _iter_async_processed_items(
    *,
    payloads: list[dict[str, Any]],
    executor: JobExecutor,
    config: dict[str, Any],
    job_context: dict[str, Any] | None,
) -> Any:
    for payload in payloads:
        processed = await executor.process_item_async(payload, config, job_context)
        processed.worker_id = 1
        yield processed


def build_processed_item_stream(
    *,
    job_type: str,
    worker_pool: Any,
    payloads: list[dict[str, Any]],
    executor: JobExecutor,
    config: dict[str, Any],
    job_context: dict[str, Any] | None,
) -> Any:
    """Return the async processed-item stream for one batch."""
    if executor.get_execution_mode(config, job_context) is ExecutionMode.ASYNC:
        return _iter_async_processed_items(
            payloads=payloads, executor=executor, config=config, job_context=job_context
        )
    if job_type == JobType.ROLLBACK:
        return _iter_rollback_processed_items(
            payloads=payloads,
            executor=executor,
            config=config,
        )

    try:
        return worker_pool.iter_batch_results(
            payloads,
            executor,
            config,
            job_context,
        )
    except TypeError:
        return worker_pool.iter_batch_results(
            payloads,
            executor,
            config,
        )
