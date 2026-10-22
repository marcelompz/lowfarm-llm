"""Request scheduling.

Policy (§4.2): **least-outstanding-requests** with power-of-two random
tie-breaking, a thermal penalty and an error penalty.  Round-robin is
deliberately *not* used: `llama-server` does continuous batching, so
scattering concurrent requests across workers leaves every worker with a
half-empty batch.  Concentrating load is worth roughly 2x aggregate
throughput.

The scheduler also implements:

  * session affinity (§4.4) — a conversation is pinned to the worker that
    holds its prefix cache;
  * admission control (§4.3) — when every worker is at capacity the request
    waits instead of being pushed to an already-saturated node.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass

from .config import Settings
from .models import WorkerState
from .registry import WorkerRecord, WorkerRegistry


class NoWorkerAvailable(Exception):
    """Raised when the fleet cannot accept the request."""


class ModelNotAvailable(NoWorkerAvailable):
    """Raised when no registered worker serves the requested model."""


class NoEligibleWorker(NoWorkerAvailable):
    """Raised when every worker serving the model has already failed."""


@dataclass
class Selection:
    record: WorkerRecord
    affinity_hit: bool = False


class Scheduler:
    def __init__(self, registry: WorkerRegistry, settings: Settings) -> None:
        self._registry = registry
        self._settings = settings
        self._capacity_event = asyncio.Event()
        self._queue_depth = 0

    # -- scoring ---------------------------------------------------------

    def score(self, record: WorkerRecord) -> float:
        """Lower is better (§4.2)."""
        weight = max(0.25, record.slots / self._settings.default_slots_per_worker)
        load = (record.in_flight + 1) / weight

        penalty = 1.0
        penalty += 0.5 * min(1.0, record.error_rate)
        excess = record.max_temp_c - self._settings.thermal_soft_limit_c
        if excess > 0:
            penalty += 0.2 * (excess / 10.0)

        return load * penalty

    # -- candidate selection ---------------------------------------------

    def _candidates(
        self,
        workers: list[WorkerRecord],
        model: str | None,
        exclude: set[str] | None = None,
    ) -> list[WorkerRecord]:
        out = []
        for w in workers:
            if w.state != WorkerState.HEALTHY:
                continue
            if exclude and w.worker_id in exclude:
                continue
            if model and w.model and w.model != model:
                continue
            out.append(w)
        return out

    def _power_of_two(self, candidates: list[WorkerRecord]) -> WorkerRecord:
        if len(candidates) == 1:
            return candidates[0]
        a, b = random.sample(candidates, 2)
        return a if self.score(a) <= self.score(b) else b

    async def select(
        self,
        model: str | None = None,
        conversation_id: str | None = None,
        exclude: set[str] | None = None,
    ) -> Selection:
        """Pick a worker, waiting for capacity if the fleet is saturated."""
        deadline = time.monotonic() + self._settings.stream_queue_timeout
        affinity_hit = False

        while True:
            workers = await self._registry.list_workers()
            healthy = [w for w in workers if w.state == WorkerState.HEALTHY]

            # Workers that could serve this model, ignoring the exclusion set.
            serving = self._candidates(workers, model, None)
            if model and healthy and not serving:
                # A model that no worker serves must fail fast, not hang.
                raise ModelNotAvailable(f"no worker serves model={model!r}")

            candidates = self._candidates(workers, model, exclude)
            if not candidates and serving:
                # The model is served, but every serving worker was already
                # tried and failed: this is an upstream failure, not a
                # saturation problem.
                raise NoEligibleWorker(
                    f"all workers serving model={model!r} have failed"
                )

            # 1. Session affinity wins whenever the pinned worker is usable.
            if conversation_id:
                pinned_id = await self._registry.get_affinity(conversation_id)
                if pinned_id:
                    pinned = next((c for c in candidates if c.worker_id == pinned_id), None)
                    if pinned is not None:
                        affinity_hit = True
                        if pinned.has_capacity:
                            return Selection(pinned, affinity_hit=True)
                        # Pinned worker is busy: fall through to the general
                        # pool rather than queueing behind it.

            # 2. General pool, restricted to workers with free capacity.
            with_capacity = [c for c in candidates if c.has_capacity]
            if with_capacity:
                chosen = self._power_of_two(with_capacity)
                if not affinity_hit and conversation_id:
                    await self._registry.set_affinity(conversation_id, chosen.worker_id)
                return Selection(chosen, affinity_hit=affinity_hit)

            # 3. Admission control: everything is saturated.
            if time.monotonic() >= deadline:
                raise NoWorkerAvailable(
                    f"no worker with free capacity for model={model!r} "
                    f"({len(candidates)} candidates)"
                )
            self._queue_depth += 1
            try:
                await asyncio.wait_for(
                    self._capacity_event.wait(),
                    timeout=min(1.0, self._queue_timeout_remaining(deadline)),
                )
            except asyncio.TimeoutError:
                pass
            finally:
                self._queue_depth -= 1
            self._capacity_event.clear()

        raise NoWorkerAvailable(f"no worker serves model={model!r}")
    def _queue_timeout_remaining(self, deadline: float) -> float:
        return max(0.05, deadline - time.monotonic())

    # -- capacity signalling ---------------------------------------------

    def notify_capacity_freed(self) -> None:
        """Called when a request completes and frees a slot."""
        self._capacity_event.set()

    @property
    def queue_depth(self) -> int:
        return self._queue_depth
