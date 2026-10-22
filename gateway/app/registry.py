"""Worker registry: membership, telemetry and the health state machine.

Membership lives in the store (Redis in normal operation) and expires by TTL
when a worker stops heartbeating.  The health state machine (§3.3) applies
hysteresis so a single dropped packet never removes a node from rotation.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from .models import Heartbeat, WorkerRegistration, WorkerState, WorkerView
from .store import Store

# EWMA smoothing factor for the latency estimate.
_LATENCY_ALPHA = 0.3


@dataclass
class WorkerRecord:
    worker_id: str
    alias: str
    model: str
    listen: str
    slots: int = 4
    threads: int = 4
    ctx_size: int = 8192
    api_key: str | None = None
    state: WorkerState = WorkerState.HEALTHY
    in_flight: int = 0
    slots_used: int = 0
    ewma_latency_ms: float = 0.0
    request_count: int = 0
    error_count: int = 0
    temp_c: dict[str, float] = field(default_factory=dict)
    battery_pct: int | None = None
    battery_faked: bool = False
    uptime_s: float = 0.0
    registered_at: float = 0.0
    last_seen: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_probe: float = 0.0

    @property
    def max_temp_c(self) -> float:
        return max(self.temp_c.values()) if self.temp_c else 0.0

    @property
    def error_rate(self) -> float:
        if self.request_count <= 0:
            return 0.0
        return self.error_count / self.request_count

    @property
    def has_capacity(self) -> bool:
        return self.in_flight < self.slots

    def to_view(self) -> WorkerView:
        return WorkerView(
            worker_id=self.worker_id,
            alias=self.alias,
            model=self.model,
            listen=self.listen,
            slots=self.slots,
            state=self.state,
            in_flight=self.in_flight,
            slots_used=self.slots_used,
            ewma_latency_ms=round(self.ewma_latency_ms, 2),
            request_count=self.request_count,
            error_count=self.error_count,
            temp_c=self.temp_c,
            battery_pct=self.battery_pct,
            battery_faked=self.battery_faked,
            uptime_s=self.uptime_s,
            registered_at=self.registered_at,
            last_seen=self.last_seen,
            consecutive_failures=self.consecutive_failures,
            consecutive_successes=self.consecutive_successes,
        )

    @classmethod
    def from_store(cls, data: dict[str, Any]) -> "WorkerRecord":
        temp_c = data.get("temp_c") or {}
        if isinstance(temp_c, str):
            try:
                temp_c = json.loads(temp_c)
            except json.JSONDecodeError:
                temp_c = {}
        state = data.get("state", WorkerState.HEALTHY.value)
        try:
            state = WorkerState(state)
        except ValueError:
            state = WorkerState.HEALTHY
        return cls(
            worker_id=str(data.get("worker_id", "")),
            alias=str(data.get("alias", "")),
            model=str(data.get("model", "unknown")),
            listen=str(data.get("listen", "")),
            slots=_as_int(data.get("slots"), 4),
            threads=_as_int(data.get("threads"), 4),
            ctx_size=_as_int(data.get("ctx_size"), 8192),
            api_key=data.get("api_key") or None,
            state=state,
            in_flight=_as_int(data.get("in_flight"), 0),
            slots_used=_as_int(data.get("slots_used"), 0),
            ewma_latency_ms=_as_float(data.get("ewma_latency_ms"), 0.0),
            request_count=_as_int(data.get("request_count"), 0),
            error_count=_as_int(data.get("error_count"), 0),
            temp_c={k: float(v) for k, v in temp_c.items()},
            battery_pct=_as_optional_int(data.get("battery_pct")),
            battery_faked=str(data.get("battery_faked", "0")) == "1",
            uptime_s=_as_float(data.get("uptime_s"), 0.0),
            registered_at=_as_float(data.get("registered_at"), 0.0),
            last_seen=_as_float(data.get("last_seen"), 0.0),
            consecutive_failures=_as_int(data.get("consecutive_failures"), 0),
            consecutive_successes=_as_int(data.get("consecutive_successes"), 0),
            last_probe=_as_float(data.get("last_probe"), 0.0),
        )

    def to_store(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "alias": self.alias,
            "model": self.model,
            "listen": self.listen,
            "slots": self.slots,
            "threads": self.threads,
            "ctx_size": self.ctx_size,
            "api_key": self.api_key or "",
            "state": self.state.value,
            "in_flight": self.in_flight,
            "slots_used": self.slots_used,
            "ewma_latency_ms": self.ewma_latency_ms,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "temp_c": json.dumps(self.temp_c),
            "battery_pct": self.battery_pct if self.battery_pct is not None else "",
            "battery_faked": self.battery_faked,
            "uptime_s": self.uptime_s,
            "registered_at": self.registered_at,
            "last_seen": self.last_seen,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_probe": self.last_probe,
        }


class WorkerRegistry:
    def __init__(
        self,
        store: Store,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
    ) -> None:
        self._store = store
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold

    # -- membership ------------------------------------------------------

    async def register(self, payload: WorkerRegistration) -> WorkerRecord:
        now = time.time()
        existing = await self._store.load_worker(payload.worker_id)
        record = WorkerRecord(
            worker_id=payload.worker_id,
            alias=payload.alias or payload.worker_id,
            model=payload.model,
            listen=payload.listen,
            slots=payload.slots,
            threads=payload.threads,
            ctx_size=payload.ctx_size,
            api_key=payload.api_key,
            state=WorkerState.HEALTHY,
            registered_at=existing.get("registered_at", 0.0) if existing else now,
            last_seen=now,
        )
        await self._store.save_worker(record.worker_id, record.to_store())
        return record

    async def heartbeat(self, worker_id: str, beat: Heartbeat) -> WorkerRecord | None:
        record = await self.get(worker_id)
        if record is None:
            return None
        record.last_seen = time.time()
        record.temp_c = dict(beat.temp_c or {})
        record.battery_pct = beat.battery_pct
        record.battery_faked = beat.battery_faked
        record.uptime_s = beat.uptime_s
        record.slots_used = min(beat.slots_used, record.slots)
        if beat.slots_total:
            record.slots = beat.slots_total
        await self._store.save_worker(worker_id, record.to_store())
        return record

    async def get(self, worker_id: str) -> WorkerRecord | None:
        data = await self._store.load_worker(worker_id)
        return WorkerRecord.from_store(data) if data else None

    async def list_workers(self) -> list[WorkerRecord]:
        rows = await self._store.load_all_workers()
        return [WorkerRecord.from_store(row) for row in rows]

    async def delete(self, worker_id: str) -> None:
        await self._store.delete_worker(worker_id)

    # -- health state machine -------------------------------------------

    async def apply_probe_result(
        self, worker_id: str, ok: bool, latency_ms: float = 0.0
    ) -> WorkerState:
        """Apply one health-probe outcome, honouring hysteresis (§3.3)."""
        record = await self.get(worker_id)
        if record is None:
            return WorkerState.UNHEALTHY

        if ok:
            record.consecutive_successes += 1
            record.consecutive_failures = 0
        else:
            record.consecutive_failures += 1
            record.consecutive_successes = 0

        previous = record.state
        record.last_probe = time.time()
        if record.state == WorkerState.DRAINED:
            # An admin drain outranks the prober.
            pass
        elif ok:
            if record.consecutive_successes >= self._recovery_threshold:
                record.state = WorkerState.HEALTHY
        else:
            if record.consecutive_failures >= self._failure_threshold:
                record.state = WorkerState.UNHEALTHY

        payload: dict[str, Any] = {
            "consecutive_failures": record.consecutive_failures,
            "consecutive_successes": record.consecutive_successes,
            "last_probe": record.last_probe,
        }
        if record.state != previous:
            payload["state"] = record.state.value
        await self._store.update_worker(worker_id, payload)
        return record.state

    async def update_slots_used(self, worker_id: str, slots_used: int) -> None:
        await self._store.update_worker(worker_id, {"slots_used": slots_used})

    async def set_state(self, worker_id: str, state: WorkerState) -> None:
        await self._store.update_worker(worker_id, {"state": state.value})

    # -- request accounting ---------------------------------------------

    async def acquire(self, worker_id: str) -> int:
        return await self._store.incr_inflight(worker_id, 1)

    async def release(self, worker_id: str) -> int:
        return await self._store.incr_inflight(worker_id, -1)

    async def record_result(
        self, worker_id: str, latency_ms: float, failed: bool = False
    ) -> None:
        record = await self.get(worker_id)
        if record is None:
            return
        if record.ewma_latency_ms <= 0:
            record.ewma_latency_ms = latency_ms
        else:
            record.ewma_latency_ms = (
                _LATENCY_ALPHA * latency_ms + (1 - _LATENCY_ALPHA) * record.ewma_latency_ms
            )
        record.request_count += 1
        if failed:
            record.error_count += 1
        await self._store.update_worker(
            worker_id,
            {
                "ewma_latency_ms": record.ewma_latency_ms,
                "request_count": record.request_count,
                "error_count": record.error_count,
            },
        )

    # -- affinity --------------------------------------------------------

    async def get_affinity(self, conversation_id: str) -> str | None:
        return await self._store.get_affinity(conversation_id)

    async def set_affinity(self, conversation_id: str, worker_id: str) -> None:
        await self._store.set_affinity(conversation_id, worker_id)


# Threshold hooks are configured through the registry constructor.


def _as_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
