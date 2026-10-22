"""Scheduling policy (§4)."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.models import Heartbeat, WorkerRegistration, WorkerState
from app.registry import WorkerRegistry
from app.scheduler import ModelNotAvailable, NoWorkerAvailable, Scheduler
from app.store import MemoryStore

MODEL = "qwen2.5-0.5b-instruct-q4_k_m"


@pytest.fixture
def registry(store: MemoryStore) -> WorkerRegistry:
    return WorkerRegistry(store, failure_threshold=2, recovery_threshold=2)


@pytest.fixture
def scheduler(registry: WorkerRegistry, settings: Settings) -> Scheduler:
    return Scheduler(registry, settings)


async def _add(registry: WorkerRegistry, worker_id: str, slots: int = 4, model: str = MODEL) -> None:
    await registry.register(
        WorkerRegistration(
            worker_id=worker_id,
            alias=worker_id,
            model=model,
            listen=f"http://{worker_id}:8080",
            slots=slots,
        )
    )


@pytest.mark.asyncio
async def test_single_worker_is_selected(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    selection = await scheduler.select(model=MODEL)
    assert selection.record.worker_id == "w1"
    assert selection.affinity_hit is False


@pytest.mark.asyncio
async def test_least_loaded_worker_wins(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")
    await registry.acquire("w1")
    await registry.acquire("w1")
    await registry.acquire("w2")

    for _ in range(10):
        selection = await scheduler.select(model=MODEL)
        assert selection.record.worker_id == "w2"


@pytest.mark.asyncio
async def test_unhealthy_workers_are_excluded(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")
    for _ in range(2):
        await registry.apply_probe_result("w1", ok=False)

    assert (await registry.get("w1")).state == WorkerState.UNHEALTHY  # type: ignore[union-attr]
    for _ in range(10):
        selection = await scheduler.select(model=MODEL)
        assert selection.record.worker_id == "w2"


@pytest.mark.asyncio
async def test_drained_workers_are_excluded(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")
    await registry.set_state("w1", WorkerState.DRAINED)
    for _ in range(10):
        selection = await scheduler.select(model=MODEL)
        assert selection.record.worker_id == "w2"


@pytest.mark.asyncio
async def test_model_filter(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2", model="qwen2.5-1.5b-instruct-q4_k_m")
    selection = await scheduler.select(model="qwen2.5-1.5b-instruct-q4_k_m")
    assert selection.record.worker_id == "w2"


@pytest.mark.asyncio
async def test_unknown_model_fails_fast(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    with pytest.raises(ModelNotAvailable):
        await scheduler.select(model="does-not-exist")


@pytest.mark.asyncio
async def test_session_affinity_pins_the_conversation(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")

    first = await scheduler.select(model=MODEL, conversation_id="conv-42")
    for _ in range(10):
        again = await scheduler.select(model=MODEL, conversation_id="conv-42")
        assert again.record.worker_id == first.record.worker_id
        assert again.affinity_hit is True


@pytest.mark.asyncio
async def test_affinity_is_dropped_when_the_pinned_worker_dies(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")

    first = await scheduler.select(model=MODEL, conversation_id="conv-7")
    pinned = first.record.worker_id
    other = "w2" if pinned == "w1" else "w1"

    for _ in range(2):
        await registry.apply_probe_result(pinned, ok=False)

    selection = await scheduler.select(model=MODEL, conversation_id="conv-7")
    assert selection.record.worker_id == other
    assert selection.affinity_hit is False


@pytest.mark.asyncio
async def test_admission_control_times_out_when_saturated(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1", slots=1)
    await registry.acquire("w1")  # now saturated

    with pytest.raises(NoWorkerAvailable):
        await scheduler.select(model=MODEL)


@pytest.mark.asyncio
async def test_thermal_penalty_demotes_hot_workers(
    registry: WorkerRegistry, scheduler: Scheduler, settings: Settings
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")
    await registry.heartbeat(
        "w1", Heartbeat(temp_c={"cpu": 60.0})
    )
    await registry.heartbeat(
        "w2", Heartbeat(temp_c={"cpu": 30.0})
    )

    for _ in range(10):
        selection = await scheduler.select(model=MODEL)
        assert selection.record.worker_id == "w2"


@pytest.mark.asyncio
async def test_excluded_workers_are_skipped(
    registry: WorkerRegistry, scheduler: Scheduler
) -> None:
    await _add(registry, "w1")
    await _add(registry, "w2")
    await _add(registry, "w3")
    for _ in range(10):
        selection = await scheduler.select(model=MODEL, exclude={"w1", "w2"})
        assert selection.record.worker_id == "w3"
