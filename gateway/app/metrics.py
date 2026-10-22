"""Prometheus instrumentation.

Each metric maps 1:1 to the catalogue in the master plan (§5.1).  The
registry is per-instance so tests can spin up several apps without duplicate
collector registration errors.
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

_BUCKETS_LATENCY = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)
_BUCKETS_TTFT = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.requests_total = Counter(
            "lowfarm_requests_total",
            "Total chat completion requests handled.",
            ["worker", "status"],
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "lowfarm_request_duration_seconds",
            "End-to-end request duration.",
            labelnames=("worker",),
            buckets=_BUCKETS_LATENCY,
            registry=self.registry,
        )
        self.ttft = Histogram(
            "lowfarm_ttft_seconds",
            "Time to first token.",
            labelnames=("worker",),
            buckets=_BUCKETS_TTFT,
            registry=self.registry,
        )
        self.tokens_total = Counter(
            "lowfarm_tokens_total",
            "Tokens processed.",
            ["worker", "direction"],
            registry=self.registry,
        )
        self.worker_tokens_per_second = Gauge(
            "lowfarm_worker_tokens_per_second",
            "Observed token throughput per worker.",
            ["worker"],
            registry=self.registry,
        )
        self.worker_inflight = Gauge(
            "lowfarm_worker_inflight",
            "In-flight requests per worker.",
            ["worker"],
            registry=self.registry,
        )
        self.worker_slots_used = Gauge(
            "lowfarm_worker_slots_used",
            "Occupied llama.cpp slots per worker.",
            ["worker"],
            registry=self.registry,
        )
        self.workers_registered = Gauge(
            "lowfarm_workers_registered",
            "Number of workers known to the registry.",
            registry=self.registry,
        )
        self.workers_healthy = Gauge(
            "lowfarm_workers_healthy",
            "Number of workers available for routing.",
            registry=self.registry,
        )
        self.worker_temperature = Gauge(
            "lowfarm_worker_temperature_celsius",
            "Worker thermal sensor readings.",
            ["worker", "sensor"],
            registry=self.registry,
        )
        self.worker_battery = Gauge(
            "lowfarm_worker_battery_pct",
            "Worker battery percentage.",
            ["worker"],
            registry=self.registry,
        )
        self.worker_uptime = Gauge(
            "lowfarm_worker_uptime_seconds",
            "Worker uptime reported by the agent.",
            ["worker"],
            registry=self.registry,
        )
        self.circuit_state = Gauge(
            "lowfarm_circuit_state",
            "Circuit breaker state: 0=closed, 1=half-open, 2=open.",
            ["worker"],
            registry=self.registry,
        )
        self.cache_hit_ratio = Gauge(
            "lowfarm_cache_hit_ratio",
            "Prefix-cache hit ratio per worker.",
            ["worker"],
            registry=self.registry,
        )
        self.queue_depth = Gauge(
            "lowfarm_queue_depth",
            "Requests waiting for worker capacity.",
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)

    @staticmethod
    def content_type() -> str:
        return CONTENT_TYPE_LATEST
