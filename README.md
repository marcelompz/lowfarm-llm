# LowFarm LLM

> An open-source, edge-computing AI inference farm that turns obsolete smartphone hardware into a distributed cluster of stateless LLM workers, orchestrated by a central API Gateway.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Status: Design](https://img.shields.io/badge/status-design%20phase-yellow.svg)](docs/MasterProjectPlan.md)

Turn a drawer of old Android phones into a working LLM inference cluster. LowFarm exposes an
**OpenAI-compatible API** on a single gateway and spreads the actual inference across a fleet of
phones running `llama-server` under Termux — no datacenter, no GPU, no per-token cloud bill.

```
                                [ Client / Applications ]
                                            │
                                            ▼ (HTTP / SSE Streaming)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        CENTRAL GATEWAY NODE (Bare-Metal + Docker)                      │
│                                                                                        │
│   ┌───────────────────────────┐                     ┌──────────────────────────────┐   │
│   │   FastAPI Orchestrator    │ ◄─────────────────► │      Redis Session Store     │   │
│   │   - API Proxy (OpenAI fmt)│                     │   - Chat History (TTL)      │   │
│   │   - Scheduler (Least-Out) │                     │   - Worker Registry (TTL)    │   │
│   │   - Health / Circuit Brkr │                     │   - Session Affinity Map    │   │
│   │   - Auth (Bearer Token)   │                     └──────────────────────────────┘   │
│   └──────┬───────┬────────────┘                                                        │
│          │       └─────────────────────────────────────┐                              │
│          │ /v1/chat/completions (SSE)                  │ /metrics (Prometheus)         │
└──────────┼───────────────────────────────────────────┼──────────────────────────────┘
           │                                           │
   ┌───────┴────────┐                        ┌─────────┴────────┐
   │  CONTROL PLANE │                        │  DATA PLANE     │
   │  POST /register │                       │  inference reqs │
   │  POST /heartbeat│                       │  (SSE)          │
   └───────┬────────┘                        └─────────┬────────┘
           │                                           │
         ┌─┼─────────────────┬───────────────────────────┼───────────────────────┐
         ▼                   ▼                           ▼                       ▼
┌───────────────┐  ┌───────────────┐          ┌───────────────┐       ┌───────────────┐
│ Worker 1: A30 │  │ Worker 2: Moto│          │ Worker N: ... │       │ Mock Worker   │
│ (Termux ARM64)│  │ (Termux ARM64)│          │ (Termux ARM64)│       │ (Fase 1 test) │
│ - llama-server│  │ - llama-server│          │ - llama-server│       │ - fake tokens │
│ - worker_agent│  │ - worker_agent│          │ - worker_agent│       └───────────────┘
│ - Qwen2.5 0.5B│  │ - Qwen2.5 0.5B│         │ - Qwen2.5 1.5B│
│ - 4 slots     │  │ - 4 slots     │          │ - 4 slots     │
└───────────────┘  └───────────────┘          └───────────────┘
```

---

## Why

A phone is a surprisingly good inference node: a big.LITTLE ARM SoC with a working scheduler, a
battery-backed power supply, a Wi-Fi radio, and a thermal design that survives 24/7 if you keep it
cool. A fleet of them costs less than one server GPU and scales horizontally.

The hard part is not the inference — `llama.cpp` already does that on a Raspberry Pi. The hard part
is the **orchestration**: routing requests so the batcher stays full, keeping the KV cache warm
across turns, knowing which phone is throttling, and not letting a single dead handset take the
cluster down.

LowFarm is that orchestration layer.

## Design decisions

| Decision | Rationale |
|---|---|
| **Least-outstanding-requests, not round-robin** | `llama-server` does continuous batching. Round-robin scatters concurrent requests across workers, leaving every one at a half-empty batch. Concentrating load is worth ~2× aggregate throughput. |
| **Dynamic worker registration** | Workers POST to the gateway on boot. Adding a phone requires **zero** gateway changes — no IP list, no redeploy. |
| **Two planes: control vs. data** | Registration and heartbeats are best-effort telemetry; inference is the real traffic. A saturated worker is never mistaken for a dead one. |
| **Stateful gateway, stateless workers** | Chat history, worker registry, and session affinity all live in Redis with TTLs. The gateway can be restarted without losing worker identity. |
| **Session affinity** | The gateway pins a conversation to one worker so the `llama.cpp` prefix cache is hit and prefill collapses to only the new tokens. The full history is still sent every time, so a reassignment is always correct — just slower. |
| **Per-node thermal telemetry** | Silent throttling is the real failure mode of a phone farm. Every worker reports its thermal zones and battery state, and the scheduler *sheds load from hot devices before they throttle*. |
| **Never disconnect power from a live node** | A time-based charger cut-off scheme drains a 6 W node faster than a 15 W adapter can refill it — the node dies in one cycle. Charging is controlled by a current limiter, not by a timer. |
| **Docker on the gateway, bare metal on the phones** | Docker on Android needs kernel patches and wastes RAM. Termux + `llama-server` is lighter and more reliable. |

## Core capabilities

- **OpenAI-compatible API** — `POST /v1/chat/completions` with SSE streaming, bearer-token auth.
- **Continuous batching across the fleet** — slot accounting, admission control, and backpressure.
- **Self-healing** — active health probes with hysteresis, a per-worker circuit breaker, and
  automatic failover with a safe retry boundary (retries only happen *before* the first token).
- **Per-node observability** — Prometheus metrics, Grafana dashboards, and alerts on TTFT, error
  rate, thermal state, battery band, and fleet health.
- **Zero-touch node provisioning** — ADB script for Doze exemption, Wi-Fi keepalive hardening,
  OEM charge cap, and bloatware removal.
- **Measured, not guessed** — a Phase 0 benchmark gates the model, thread, and slot configuration
  before any hardware purchase.

## Hardware

| Component | Spec | Notes |
|---|---|---|
| Worker | Samsung A30 (or any ARM64 Android) | Exynos 7904 · 4 GB RAM · ~6 W steady draw |
| Gateway | 4–8 cores, 8–16 GB RAM | Ubuntu Server 24.04 / Debian 12 + Docker Compose |
| Cooling | USB fan, 1 per 2–3 nodes | **Thermal control dominates battery life** — not optional |
| Power | 100–130 W switched USB PSU + UPS ≥ 600 VA | A mains cut must not kill the cluster mid-stream |
| Instrumentation | USB inline power meter | **Mandatory** — the only ground truth for power decisions |

Full bill of materials with indicative prices: [`docs/MasterProjectPlan.md §9.7`](docs/MasterProjectPlan.md).

## Project status

| Phase | Scope | Status |
|---|---|---|
| **0** | Device benchmark, model & thread/slot decision | ⬜ Planned |
| **1** | Gateway (FastAPI + Redis), scheduler, worker lifecycle, SSE, mock workers | 🟡 In progress |
| **2** | Phone provisioning, Termux, ARM64 `llama.cpp` build, worker agent | ⬜ Planned |
| **3** | Fleet integration, dynamic discovery, thermal tuning, degradation tests | ⬜ Planned |
| **4** | Bare-metal deployment, monitoring, runbook | ⬜ Planned |

### Phase 1 — what exists today

The gateway is implemented and running. 45 tests green, verified end-to-end against a live Docker stack.

- ✅ OpenAI-compatible `POST /v1/chat/completions` with SSE streaming and non-streaming JSON
- ✅ Bearer auth on the public plane, a separate admin key on the control plane
- ✅ Least-outstanding-requests scheduler with power-of-two tie-breaking, thermal penalty, session
  affinity and admission control
- ✅ Dynamic worker registration + heartbeat, Redis-backed registry with TTL expiry, health state
  machine with hysteresis, per-worker circuit breaker
- ✅ Failover with a safe retry boundary (retry only before the first token)
- ✅ Prometheus metrics for latency, TTFT, in-flight, per-node thermal and battery state
- ✅ Mock `llama-server` that self-registers, so the fleet appears with zero gateway configuration
- ⬜ Redis-backed chat history
- ⬜ Per-client quotas
- ⬜ True `/v1/completions` text-completion path

See [`gateway/README.md`](gateway/README.md) for the API surface, the worker protocol and the
development workflow.

### Try it

```sh
cp .env.example .env && chmod 600 .env   # set the two API keys
docker compose up -d --build

curl -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4_k_m",
       "messages":[{"role":"user","content":"hello farm"}]}'
```

The full implementation plan — architecture, scheduling policy, failure matrices, metrics catalog,
risk assessment, and configuration templates — lives in
**[`docs/MasterProjectPlan.md`](docs/MasterProjectPlan.md)**.

## Roadmap

- [ ] Phase 0 — benchmark `Qwen2.5-0.5B` vs `1.5B` (Q4_K_M) on a real A30; freeze the config
- [x] Phase 1 — gateway with a mock fleet, fault injection and dynamic registration
- [ ] Phase 1b — Redis-backed chat history, per-client quotas, real text completions
- [ ] Phase 2 — provision and register a first real phone
- [ ] Phase 3 — scale to a multi-phone fleet with live telemetry
- [ ] Phase 4 — production deployment with unattended 72 h soak

## Documentation

| Document | Contents |
|---|---|
| [`docs/MasterProjectPlan.md`](docs/MasterProjectPlan.md) | The complete technical plan: architecture, scheduling, failure handling, metrics, security, energy strategy, templates, risks |
| [`gateway/README.md`](gateway/README.md) | Gateway API surface, worker protocol, configuration, development workflow |

## Contributing

Contributions are welcome — especially from anyone with spare Android hardware and an appetite for
thermal testing. See the roadmap above: Phase 0 and Phase 1 are the highest-value entry points.

Please read [`docs/MasterProjectPlan.md`](docs/MasterProjectPlan.md) first; it contains the design
constraints and the reasoning behind each decision, so you can align before opening a PR.

## License

[Apache License 2.0](LICENSE) — see [`LICENSE`](LICENSE) for the full text.

Model weights referenced by this project (Qwen2.5) are distributed by Alibaba Cloud under their own
Apache 2.0 license and are **not** included in this repository.
