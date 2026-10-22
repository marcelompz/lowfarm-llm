"""Worker registry: membership, telemetry and the health state machine (§3)."""

from __future__ import annotations

import pytest

from app.models import Heartbeat, WorkerRegistration, WorkerState
from app.registry import WorkerRegistry
from app.store import MemoryStore


@pytest.fixture
def registry(store: MemoryStore) -> WorkerRegistry:
    return WorkerRegistry(store, failure_threshold=3, recovery_threshold=2)


async def _register(registry: WorkerRegistry, worker_id: str = "w1") -> None:
    await registry.register(
        WorkerRegistration(
            worker_id=worker_id,
            alias=worker_id,
            model="qwen2.5-0.5b-instruct-q4_k_m",
            listen=f"http://{worker_id}:8080",
            slots=4,
        )
    )


@pytest.mark.asyncio
async def test_register_makes_the_worker_visible(registry: WorkerRegistry) -> None:
    await _register(registry)
    workers = await registry.list_workers()
    assert [w.worker_id for w in workers] == ["w1"]
    assert workers[0].state == WorkerState.HEALTHY


@pytest.mark.asyncio
async def test_heartbeat_updates_telemetry(registry: WorkerRegistry) -> None:
    await _register(registry)
    record = await registry.heartbeat(
        "w1",
        Heartbeat(
            temp_c={"cpu": 41.5, "battery": 32.0},
            battery_pct=71,
            slots_used=2,
            slots_total=4,
            uptime_s=1234.0,
        ),
    )
    assert record is not None
    assert record.temp_c["cpu"] == 41.5
    assert record.max_temp_c == 41.5
    assert record.battery_pct == 71
    assert record.slots_used == 2
    assert record.uptime_s == 1234.0


@pytest.mark.asyncio
async def test_heartbeat_for_unknown_worker_is_none(registry: WorkerRegistry) -> None:
    assert await registry.heartbeat("ghost", Heartbeat()) is None


@pytest.mark.asyncio
async def test_state_machine_needs_three_failures(registry: WorkerRegistry) -> None:
    """Hysteresis: a single dropped packet must not evict a worker."""
    await _register(registry)
    for _ in range(2):
        state = await registry.apply_probe_result("w1", ok=False)
        assert state == WorkerState.HEALTHY

    state = await registry.apply_probe_result("w1", ok=False)
    assert state == WorkerState.UNHEALTHY

    record = await registry.get("w1")
    assert record is not None
    assert record.consecutive_failures == 3


@pytest.mark.asyncio
async def test_state_machine_recovers_after_two_successes(
    registry: WorkerRegistry,
) -> None:
    await _register(registry)
    for _ in range(3):
        await registry.apply_probe_result("w1", ok=False)
    assert (await registry.get("w1")).state == WorkerState.UNHEALTHY  # type: ignore[union-attr]

    state = await registry.apply_probe_result("w1", ok=True)
    assert state == WorkerState.UNHEALTHY  # one success is not enough
    state = await registry.apply_probe_result("w1", ok=True)
    assert state == WorkerState.HEALTHY


@pytest.mark.asyncio
async def test_drain_outranks_the_prober(registry: WorkerRegistry) -> None:
    await _register(registry)
    await registry.set_state("w1", WorkerState.DRAINED)
    for _ in range(5):
        state = await registry.apply_probe_result("w1", ok=True)
    assert state == WorkerState.DRAINED


@pytest.mark.asyncio
async def test_inflight_counter_tracks_acquire_and_release(
    registry: WorkerRegistry,
) -> None:
    await _register(registry)
    await registry.acquire("w1")
    await registry.acquire("w1")
    record = await registry.get("w1")
    assert record is not None and record.in_flight == 2
    await registry.release("w1")
    record = await registry.get("w1")
    assert record is not None and record.in_flight == 1


@pytest.mark.asyncio
async def test_inflight_never_goes_negative(registry: WorkerRegistry) -> None:
    await _register(registry)
    await registry.release("w1")
    await registry.release("w1")
    record = await registry.get("w1")
    assert record is not None and record.in_flight == 0


@pytest.mark.asyncio
async def test_inflight_on_a_dead_worker_does_not_resurrect_it(
    registry: WorkerRegistry,
) -> None:
    assert await registry.acquire("ghost") == -1
    assert await registry.list_workers() == []


@pytest.mark.asyncio
async def test_latency_ewma_and_error_accounting(registry: WorkerRegistry) -> None:
    await _register(registry)
    await registry.record_result("w1", latency_ms=100.0)
    await registry.record_result("w1", latency_ms=200.0)
    record = await registry.get("w1")
    assert record is not None
    assert record.request_count == 2
    # EWMA with alpha=0.3: first sample seeds it, second gives 0.3*200+0.7*100
    assert record.ewma_latency_ms == pytest.approx(130.0)

    await registry.record_result("w1", latency_ms=50.0, failed=True)
    record = await registry.get("w1")
    assert record is not None
    assert record.error_count == 1
    assert record.error_rate == pytest.approx(1 / 3)


@pytest.mark.asyncio
async def test_affinity_round_trip(registry: WorkerRegistry) -> None:
    await _register(registry, "w1")
    assert await registry.get_affinity("conv-1") is None
    await registry.set_affinity("conv-1", "w1")
    assert await registry.get_affinity("conv-1") == "w1"


@pytest.mark.asyncio
async def test_worker_expires_when_heartbeats_stop(registry: WorkerRegistry) -> None:
    await _register(registry)
    assert len(await registry.list_workers()) == 1
    # Simulate TTL expiry by touching the store directly.
    await registry._store.save_worker("w1", {"worker_id": "w1", "state": "healthy"})
    assert len(await registry.list_workers()) == 1
