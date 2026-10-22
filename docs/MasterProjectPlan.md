# LowFarm LLM \- Master Implementation Plan

> **LowFarm LLM** is an open-source, edge-computing AI inference farm that turns obsolete smartphone hardware into a distributed cluster of stateless LLM workers orchestrated by a central API Gateway.

**Revision 3** (2026\-10\-21) — adds the energy & battery strategy: the three-layer charge control, per-node power budget, hardware BOM, and the arithmetic ruling out time-based charger cut-off for 24/7 compute. Revision 2 introduced the worker lifecycle, scheduling policy, session affinity, thermal telemetry, and security hardening. See §12 for the full change log.

---

## 1\. System Architecture Overview

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

### 1\.1 Architectural Principles

1. **Two planes, strict separation.**
   * *Control plane* (registration, heartbeat, metrics scrape) is low\-bandwidth and best\-effort. A worker losing the control plane does **not** immediately lose its inference capacity.
   * *Data plane* (inference requests, SSE token streams) carries the real traffic.
2. **The gateway is stateless.** All durable state (chat history, worker registry, affinity map) lives in Redis with TTLs. The gateway can be restarted or scaled horizontally without losing worker identity.
3. **Workers are replaceable.** Any worker can be powered off, unplugged, or thermally drained without client impact — the scheduler simply stops routing to it.
4. **The gateway is the only public surface.** `llama-server` ports are never exposed outside the farm network segment.

&nbsp;

---

## 2\. Bare-Metal & OS Recommendation for Central Node

### Recommended Base OS

* **Primary Recommendation:** **Ubuntu Server 24.04 LTS** or **Debian 12 Minimal**.  
* **Why:**  
  * Negligible idle RAM usage (\~150 MB).  
  * Out\-of\-the\-box systemd integration and stable kernel network stack.  
  * Direct compatibility with Docker Engine & Docker Compose.

### 2\.1 Containerization Strategy

* **Gateway Layer:** Purely Dockerized (`Docker Compose`).  
* **Worker Layer (Phones):** Bare-metal Execution via **Termux** (Android). Docker on phones is omitted to avoid kernel module patch requirements and conserve RAM.

### 2\.2 Gateway Host Sizing

| Farm size | CPU | RAM | Disk | Network |
|---|---|---|---|---|
| 1–4 workers | 4 cores | 8 GB | 100 GB | 1× GbE + 5 GHz AP |
| 5–12 workers | 8 cores | 16 GB | 250 GB | 1× GbE + 5 GHz AP (dedicated) |
| 13–24 workers | 8–16 cores | 32 GB | 500 GB | 2× GbE (bonded) + 2× 5 GHz AP |

The gateway is **not** the bottleneck at these scales: the gateway only proxies token streams (see §7.3 for the bandwidth budget). Plan the CPU/RAM budget around the API, Redis, and Prometheus, not around inference.

### 2\.3 Host OS Notes

* **Fedora 44 Workstation** (the current prototyping host) is acceptable for development: Docker 29.x and Compose v5 are already current, and systemd is stable. The production bare-metal node should still use Ubuntu Server 24.04 or Debian 12 for LTS support and conservative idle footprint.
* **Python version pinning:** the host ships Python 3.14, which is very recent. Several transitive dependencies (`pydantic-core`, `redis`, `uvloop`) may not publish wheels for it yet. **Pin the runtime inside the container to Python 3.12 or 3.13** and never build against the host interpreter.

&nbsp;

---

## 3\. Worker Lifecycle: Registration, Health & Failover

> **Decision:** worker membership is **dynamic**, not configured by hand. The `WORKER_SEEDS` environment variable is only a bootstrap fallback used when Redis is empty.

### 3\.1 Registration Flow

```
Worker boots → worker_agent waits for llama-server /health → POST /v1/admin/workers/register
Gateway responds 201 {worker_id, session_ttl} → worker persists ID → heartbeat loop begins
```

**Registration payload** (worker → gateway):

```json
{
  "worker_id": "a30-192-168-1-101",
  "alias": "qwen-0.5b-a30-01",
  "model": "qwen2.5-0.5b-instruct-q4_k_m",
  "listen": "http://192.168.1.101:8080",
  "slots": 4,
  "threads": 4,
  "ctx_size": 8192,
  "api_key": "optional-worker-key",
  "agent_version": "0.2.0"
}
```

The gateway stores this in Redis under `worker:{worker_id}` as a hash with a TTL, and maintains the set `workers:index`. A worker that stops heartbeating simply expires out of the index — **no explicit removal is required**, and there is no stale\-worker cleanup job to get wrong.

### 3\.2 Heartbeat Payload

Every `HEALTH_CHECK_INTERVAL` seconds (default 10s) the worker posts to `POST /v1/admin/workers/{worker_id}/heartbeat`:

```json
{
  "ts": 1760000000.42,
  "uptime_s": 12844,
  "cpu_load_1m": 3.85,
  "temp_c": {"cpu": 47.2, "skin": 38.9, "battery": 36.1},
  "battery_pct": 71,
  "battery_faked": true,
  "charging": true,
  "slots_used": 2,
  "slots_total": 4,
  "tokens_per_s": 18.4
}
```

The heartbeat **updates the registry in Redis and refreshes the TTL**. It is *not* a health probe — the gateway's own active probe (§3.3) is what determines liveness. The heartbeat carries *telemetry*; the probe carries *truth*. This separation means a busy worker (saturated HTTP event loop) is never mistaken for a dead one.

### 3\.3 Health State Machine & Hysteresis

```
                 3 consecutive probe failures
      ┌──────────────────────────────────────────────┐
      │                                              ▼
 HEALTHY ────────────────────────────────▶ UNHEALTHY (removed from rotation)
      ▲                                              │
      │                                              │ 2 consecutive probe successes
      └──────────────────────────────────────────────┘
                     (half-open probe first)
```

| Setting | Default | Rationale |
|---|---|---|
| `HEALTH_CHECK_INTERVAL` | `10` s | Probe period |
| `HEALTH_PROBE_TIMEOUT` | `3` s | Must exceed phone Wi‑Fi RTT, not the inference time |
| `HEALTH_FAILURE_THRESHOLD` | `3` | ~30 s to detect a dead worker; avoids flapping on a single dropped packet |
| `HEALTH_RECOVERY_THRESHOLD` | `2` | Prevents oscillation between two states |
| `WORKER_STALE_TTL` | `45` s | TTL for the Redis registry entry; must be > 3× heartbeat interval |

**Probe target:** `GET http://{worker}/health` on `llama-server` (returns `503` until the model is loaded, `200` afterwards). The probe must **not** hit an inference endpoint — probing with a completion would consume slots and distort load measurements.

**Circuit breaker:** a worker in `UNHEALTHY` state is not deleted, it is quarantined. In-flight requests against it are failed fast (no retry storms), and a half-open probe every 30 s determines recovery. This keeps the failure visible in metrics and allows a worker that was merely Wi‑Fi\-hiccuping to rejoin without a re-registration round trip.

### 3\.4 Failure Handling Matrix

| Failure | Detection | Gateway behaviour | Client behaviour |
|---|---|---|---|
| Worker dies (power/crash) | Probe timeout ×3 | Remove from rotation, fail in-flight requests | `502` with retry hint |
| Worker unreachable (Wi‑Fi drop) | Probe timeout ×3 | Quarantine, backoff probe | `502` |
| Mid-stream failure (after first token) | SSE connection drop | **Cannot retry** — tokens were already emitted | Stream terminates with error event |
| Worker slow / saturated | In-flight > slots | Queue with `STREAM_QUEUE_TIMEOUT` | Latency, but no error |
| Gateway restart | n/a | Redis restores registry + affinity | Brief 503 during boot |
| Redis down | n/a | Degrade to in-memory registry, read-only routing | Inference continues, no affinity |

> **Important:** retrying a chat completion is only safe **before the first token is emitted**. After that, a retry would duplicate content the client already received. The gateway must therefore implement the retry at the *pre-stream* boundary only.

&nbsp;

---

## 4\. Scheduling & Load-Balancing Policy

> **Decision:** **Least\-Outstanding\-Requests** with power\-of\-two random tie\-breaking, **not** round-robin.

### 4\.1 Why Round-Robin Is the Wrong Default

`llama-server` implements **continuous batching**: multiple requests are decoded together in a single batch, amortizing the memory bandwidth cost across them. Round-robin scatters concurrent requests across *different* workers, leaving every worker with exactly one partial batch. The cluster ends up running N workers at ~50 % efficiency instead of N workers at ~90 % efficiency.

The fix is to concentrate load: route concurrent requests to the *same* worker until its slots are full, then overflow to the next. This is exactly what least-outstanding-requests does.

### 4\.2 Scoring Function

For each healthy worker, the gateway maintains:

* `in_flight` — requests currently assigned and not yet completed
* `slots_used` — number of busy llama.cpp slots (from `/slots`)
* `weight` — relative capacity (e.g. 1.0 for a phone, 4.0 for a desktop node)
* `ewma_latency` — exponentially weighted moving average of end-to-end latency
* `error_rate` — rolling window of recent failures

```
load(worker) = (in_flight + 1) / weight
penalty(worker) = 1 + 0.5 * error_rate + 0.2 * max(0, temp_c - 42) / 10
score(worker) = load(worker) * penalty(worker)
```

The gateway selects the worker with the **lowest score**. Ties are broken with **power-of-two choices**: pick two candidates at random and take the better one. This prevents all clients from synchronizing on the same worker (the "herding" failure mode of deterministic tie-breaking).

> **Thermal penalty** is a soft, automatic signal: a worker running hot is progressively deprioritized before it hard\-throttles. This is the mechanism that makes the farm self-protecting without a human watching temperatures.

### 4\.3 Slot Accounting & Admission Control

`llama-server` exposes `GET /slots` with per-slot state (`idle` / `processing`). The gateway polls it lazily (every `HEALTH_CHECK_INTERVAL`) and uses it to:

1. Compute `slots_used` for the scoring function.
2. Decide whether to overflow a request to another worker or queue it.

If **all** healthy workers are at capacity:

* The request waits in an internal queue (bounded by `STREAM_QUEUE_TIMEOUT`, default 120 s).
* If the queue expires → `429 Too Many Requests` with `Retry-After`.
* The gateway **never** sends a request to a worker known to be at capacity.

**Context sizing rule:** `llama-server` divides `--ctx-size` *across* slots. To give each slot 2048 tokens with `--parallel 4`, you must pass `--ctx-size 8192`. Getting this wrong silently divides the context by the slot count.

### 4\.4 Session Affinity & KV-Cache Continuity

Workers are stateless in the *application* sense (history lives in Redis), but `llama.cpp` maintains a **server-side prefix cache**. If a conversation's history is resent byte-identically to the same worker, the cached prefix is reused and prefill cost collapses to only the new tokens.

**Policy:**

* The gateway keys affinity on `conversation_id` in Redis: `session:{conv_id} → worker_id` (TTL `SESSION_TTL`, default 3600 s).
* If no affinity exists, the scheduler picks a worker by the §4.2 score and stores the binding.
* If the bound worker is `UNHEALTHY`, the binding is deleted and the request is reassigned (cold cache, correct output — only slower).
* **Always send the full message history** on every request, even with affinity. This guarantees correctness after a reassignment, and with affinity it hits the prefix cache.

```
affinity hit   → full history resent → llama.cpp prefix cache hit → prefill ≈ new tokens only
affinity miss  → full history resent → cold cache              → prefill = full history
```

Never rely on server-side state alone. The client may reorder, edit, or branch a conversation at any time.

&nbsp;

---

## 5\. Observability & Telemetry

> A farm without per-node telemetry is a box of phones that you cannot debug. This is a **Phase 1 deliverable**, not a Phase 4 nicety.

### 5\.1 Gateway Metrics (Prometheus, `GET /metrics`)

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `lowfarm_requests_total` | counter | `worker`, `status` | Request volume per node |
| `lowfarm_request_duration_seconds` | histogram | `worker` | End-to-end latency |
| `lowfarm_ttft_seconds` | histogram | `worker` | **Time to first token** — the metric users actually feel |
| `lowfarm_tokens_total` | counter | `worker`, `direction` | Tokens in/out per node |
| `lowfarm_worker_tokens_per_second` | gauge | `worker` | Live throughput |
| `lowfarm_worker_inflight` | gauge | `worker` | Current load |
| `lowfarm_worker_slots_used` | gauge | `worker` | Slot occupancy |
| `lowfarm_workers_registered` | gauge | — | Fleet size |
| `lowfarm_workers_healthy` | gauge | — | Available capacity |
| `lowfarm_worker_temperature_celsius` | gauge | `worker`, `sensor` | Thermal state (§5.2) |
| `lowfarm_worker_battery_pct` | gauge | `worker` | Battery band |
| `lowfarm_worker_uptime_seconds` | gauge | `worker` | Stability |
| `lowfarm_circuit_state` | gauge | `worker` | `0`=closed, `1`=half-open, `2`=open |
| `lowfarm_cache_hit_ratio` | gauge | `worker` | Affinity effectiveness |
| `lowfarm_queue_depth` | gauge | — | Backpressure visibility |

### 5\.2 Worker-Side Thermal Telemetry

This is the differentiator of a *phone* farm: the failure mode is silent thermal throttling, and the only way to see it is to instrument the device.

Available signals on Android/Termux:

```sh
# Thermal zones (millidegrees Celsius)
cat /sys/class/thermal/thermal_zone*/temp
cat /sys/class/thermal/thermal_zone*/type

# Battery + thermalservice (requires termux-api for the latter)
dumpsys battery | grep -E 'temperature|level|status'
dumpsys thermalservice

# CPU governor + per-core frequency (optional)
for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq; do cat "$c"; done
```

The `worker_agent` samples these every heartbeat cycle and reports them (§3.2). The gateway stores the latest sample in Redis and exposes it as a Prometheus gauge.

**Thermally-aware routing** (§4.2 penalty term) uses this data to shed load from hot devices before they throttle.

### 5\.3 Alerts

| Alert | Condition | Action |
|---|---|---|
| `LowFarmWorkerHot` | `temp_c > 45` for 5 min | Auto-drain worker; notify |
| `LowFarmWorkerCritical` | `temp_c > 52` | Drain + throttle request rate |
| `LowFarmTTFTRegression` | p95 `ttft > 4 s` | Investigate saturation or thermal throttling |
| `LowFarmErrorRate` | `> 2 %` over 5 min | Investigate worker or network |
| `LowFarmFleetDegraded` | `healthy / registered < 0.7` | Investigate failing nodes |
| `LowFarmBatteryOutOfBand` | `% < 40` or `% > 85` | Check charge controller. **Caveat:** if the ADB SOC cap (Layer 3) is active, the reported percentage is faked and this alert is meaningless — use a real-SOC source instead (§13 Q8). |
| `LowFarmAllWorkersDown` | `healthy == 0` | Page immediately |

&nbsp;

---

## 6\. Security Hardening

> The original topology published Redis on `6379` and the gateway on `8000` with no authentication. A phone farm on a home network is reachable by every device in the house.

### 6\.1 Required Controls

| Control | Implementation |
|---|---|
| **Gateway auth** | `Authorization: Bearer <GATEWAY_API_KEY>` on all `/v1/*` routes. Compare with `secrets.compare_digest` (constant time). |
| **Admin plane auth** | Separate, stronger key (`ADMIN_API_KEY`) for `/v1/admin/*`. Admin endpoints are never exposed publicly. |
| **Worker auth** | `llama-server --api-key` on every phone, so a compromised LAN device cannot use the phones directly. The gateway injects the key. |
| **Redis isolation** | Redis is **not** published to the host. It lives only on the internal Compose network. Debug via `docker compose exec redis redis-cli`. |
| **Network segmentation** | Phones on a **dedicated VLAN/SSID** with client isolation enabled; the gateway holds the only inter-VLAN route. |
| **No public exposure** | If remote access is needed, terminate TLS at a reverse proxy (Caddy/Traefik) or use a VPN (WireGuard/Tailscale) — never expose `:8000` directly. |
| **Credential hygiene** | All keys in a `.env` file with `0600` permissions, gitignored. Never in `docker-compose.yml`. |

### 6\.2 Client API Keys

For multi-tenant usage, the gateway should support **per-client API keys** with quota and rate limits, stored as Redis hashes (`client:{key_hash} → {tpm_limit, rpm_limit, ...}`). This prevents one noisy client from saturating the entire farm.

&nbsp;

---

## 7\. Capacity Planning & Model Selection

### 7\.1 Per-Node Expectations

Reference device: **Samsung A30** (Exynos 7904 — 2× Cortex\-A73 @ 2.0 GHz + 6× Cortex\-A55, 4 GB RAM).

> The figures below are **engineering targets to be validated by the Phase 0 benchmark** (§8), not guarantees. Real results depend on thermal state, governor, and Termux build flags.

| Metric | Qwen2.5\-0.5B Q4\_K\_M | Qwen2.5\-1.5B Q4\_K\_M |
|---|---|---|
| Model file size | \~400 MB | \~1.0 GB |
| Resident RAM (weights) | \~600 MB | \~1.2 GB |
| KV cache @ 8192 ctx | \~100 MB | \~235 MB |
| Total worker RSS | \~1.0 GB | \~1.7 GB |
| Prefill (prompt processing) | 40–80 tok/s | 15–30 tok/s |
| Generation, 1 stream | 12–20 tok/s | 6–12 tok/s |
| Generation, 4 slots batched | 25–40 tok/s | 12–22 tok/s |
| Thermal headroom @ 4 threads | Good | Tight |
| Steady power draw (device) | \~5.5–6.5 W | \~6.5–8 W |
| Battery runtime on internal cell | \~2.5 h | \~2 h |

**Note on threads:** the original plan capped threads at 2 for the A73s. That protects the big cores but wastes half the SoC. The revised recommendation is **`--threads 4 --threads-batch 6`** (oversubscription on the batch path is beneficial), with the thermal penalty in §4.2 handling the degradation automatically. Validate both configurations in Phase 0.

### 7\.2 Model Selection Decision

The 0.5B model is a **safe default** but not necessarily the right one. The revision adds a hard requirement:

> **Phase 0 must benchmark both models on the real device before the model is frozen.** The A30's 4 GB of RAM comfortably holds the 1.5B Q4\_K\_M with headroom for KV cache, and 1.5B is materially better at instruction following and multi-turn coherence.

Decision criteria (in order):

1. **Sustained thermal stability** — no throttling below 80 % of peak throughput after 30 minutes.
2. **Multi-turn coherence** on a 10-turn evaluation set.
3. **Aggregate tok/s per watt** (a farm is a power-budget project).
4. **Simplicity of deployment** (smaller model = faster boot, less RAM pressure, more workers per AP).

Additionally, a **model mix is permitted**: the gateway routes by `model` alias, so the farm can run 0.5B for high-volume/short-context traffic and 1.5B for quality-sensitive traffic. This is a natural Phase 3 extension.

### 7\.3 Bandwidth Budget

A useful correction to the common assumption that Wi‑Fi bandwidth is the farm's constraint — **it is not, at this scale.**

| Direction | Per token | @ 100 tok/s aggregate |
|---|---|---|
| SSE output (JSON + framing) | \~12 B | \~1.2 KB/s |
| Prefill burst (1000\-token prompt) | \~4 KB once | — |

Total sustained traffic is **well under 1 MB/s** for a 100 tok/s farm. The real network risks are therefore:

* **Latency and jitter**, not throughput → mitigated by a dedicated 5 GHz SSID (§11).
* **TCP keepalive death** — Android aggressively kills idle Wi‑Fi sockets, which manifests as *random* mid-stream SSE drops minutes into an idle session. Mitigation: §8\.2 `wifi_suspend_always_optimization=0` and `wifi_scan_always_enabled=0`, plus a gateway-side SSE heartbeat comment every 15 s.
* **Rogue AP / band congestion** → dedicated SSID, channel plan, static DHCP leases per MAC.

### 7\.4 Scale-Out Example

Fleet of **8 A30 workers**, running Qwen2.5\-0.5B, 4 slots each:

| Quantity | Value |
|---|---|
| Concurrent slots | 32 |
| Aggregate generation throughput | 200–320 tok/s |
| Aggregate prefill throughput | 320–640 tok/s |
| Device-side power draw | 8 × \~6 W = \~48 W |
| Wall power (incl. conversion losses + fans) | \~70–80 W |
| Gateway load | Negligible (proxy only) |

> Capacity numbers are **targets pending Phase 0 measurement**. Do not commit hardware purchases against these figures until the benchmark script has run on at least two physical devices.

### 7\.5 Energy & Battery Strategy

> This subsection supersedes the "smart plugs that cycle power" idea in the original plan. That idea is **incompatible with 24/7 compute** for reasons that are arithmetic, not opinion.

#### 7\.5\.1 The Governing Constraint

For a phone that is **on and serving inference**, the charger is not merely a charger — it is the power source. Disconnecting it converts the node into a battery-powered device with a ~2.5 h runtime, and it then dies mid-request.

Worked example for the A30 (4000 mAh ≈ 15.4 Wh):

```
Estimated steady draw (llama-server, 0.5B, 4 threads, screen off):
  SoC, 2x A73 saturated .......... 3.5 - 4.0 W
  LPDDR4 DRAM .................... 1.0 - 1.5 W
  Wi-Fi associated + PMIC/sensors   0.5 - 0.8 W
  --------------------------------------------
  TOTAL .......................... ~5.5 - 6.5 W

Proposed 45 min-on / 3h15m-off charger schedule:
  Charge window (0.75 h):  adapter delivers ~13 W; node consumes 6 W x 0.75 h = 4.5 Wh
                           -> battery GAINS ~9 Wh   (+58 %)
  Cut window (3.25 h):     node consumes 6 W x 3.25 h = 19.5 Wh
                           -> battery LOSES 19.5 Wh (-127 %)
  NET per 4 h cycle:       -10.5 Wh  -> node dies within one cycle.
```

**General rule:** the charge window must cover the draw of the cut window. With a 15 W adapter, keeping a 6 W node alive requires being plugged in **~100 % of the time**. Any cut schedule only works for devices averaging **< 1.2 W** — i.e. idle devices. An idle node is worthless.

> **Policy decision:** the charger is **never** disconnected from a node in service. If a node must be cycled for battery health, it is **rebooted on a schedule** (`adb reboot` + `termux-boot` auto-start), not de-powered. The node sleeps; it does not unplug.

#### 7\.5\.2 What Actually Degrades a Li-Ion Cell

| Factor | Optimal | Worst case | Notes |
|---|---|---|---|
| SOC window | 40–80 % | 0–100 %, or dwelling at 100 % | Above ~80 % (4.2 V/cell) → lithium plating + electrolyte oxidation; below ~20 % (3.4 V) → copper dissolution |
| Temperature | 20–30 °C | > 45 °C | Every +10 °C roughly **halves** calendar life — temperature dominates SOC |
| Charge rate | ≤ 0.5C | > 1C | A 15 W adapter on a 4000 mAh cell is already ~0.39C |
| Cycle depth | 0.4 EFC (40→80 %) | 1.0 EFC (0→100 %) | 1 cycle/day at 40–80 % ≈ 3.6 years to 80 % capacity |
| Dwell time | Avoid weeks at 100 % or 0 % | Phone parked at 100 % in a hot car | The default "phone as charger" is close to worst case |

**Consequence for the farm:** a battery held at 85 % at 40 °C degrades *slower* than one at 50 % at 50 °C. **Cooling matters more than charge control.** The fan budget (§9\.7) is not optional.

#### 7\.5\.3 The Three-Layer Charge Strategy

| Layer | Mechanism | Reliability | Purpose |
|---|---|---|---|
| **1. Hardware power limit** | Inline current limiter / charge-inhibit module per phone, or a limited-source PSU | Deterministic, device-independent | Supply power while preventing charge; also creates a **thermal equilibrium** (the node self-regulates: when the SoC heats, it clocks down and draws less) — the phone equivalent of a CPU *PL1* |
| **2. OEM charge cap** | Samsung **Settings → Battery → "Protect battery"** limits charging to 85 % while plugged | OEM-supported, no hacks, survives reboots | Caps the upper SOC bound on Samsung hardware |
| **3. Software SOC cap** | `adb shell dumpsys battery set level 70` + `set status 2` | **Varies by OEM/ROM** — must be verified | Third line of defence where Layers 1–2 are unavailable |

**Layer 1 calibration:** the limiter must supply *slightly more* than the node's measured draw. Supplying less does not cap the charge — it drains the battery and eventually kills the node. Supplying more charges it. Tune with a USB inline power meter (§9\.7) until the node runs indefinitely on external power with a stable SOC.

**Layer 3 caveat:** treat ADB SOC manipulation as a complement, never as a guarantee. It must be **verified per device** with:

```sh
adb shell dumpsys battery unplug        # or: set level 70 / set status 2
cat /sys/class/power_supply/battery/status   # expect "Not charging"
# An inline USB power meter is the only ground truth.
```

#### 7\.5\.4 If Time-Based Cycling Is Still Wanted

Time-based cycling is legitimate for **calendar aging** (letting cells rest at a moderate SOC) but it must be **staggered across the fleet** so the cluster does not lose capacity:

```
8 phones, 45-minute charge windows staggered over a 4 h cycle (the user's
proposed schedule, applied per phone instead of to the whole fleet):
  - at any moment ~1.5 phones are in a charge window
  - effective fleet availability ~81 %
  - per phone: 4 cycles/day, window 55->80 % (0.25 EFC) = ~1.0 EFC/day
  - ~18 %/year of rated cycle life consumed  -> within budget
  - charge window must cover the window length:
      25 % of 15.4 Wh = 3.85 Wh  /  ~13 W into the cell  =  ~18 min  (fits in 45 min)
```

Charge-window energy must still exceed the discharge-window draw of an **idle** node (~1.2 W). This is why the scheme requires the phone to be **powered off** during discharge (self-discharge is only ~2 %/month, i.e. negligible over 3 h).

> **Trade-off:** this strategy costs ~19 % of fleet capacity *and* a chunk of battery life budget, in exchange for calendar rest that Layers 1–3 already largely provide. **Recommended only if Layers 1–3 cannot be applied.**

#### 7\.5\.5 Energy Budget

Per node (measured, not estimated — see §8 Phase 2):

```
Device-side steady draw ............ ~6.0 W   (measured with inline meter)
Adapter + conversion loss @ 85 % ... +1.1 W
USB fan ...........................  1.0 - 2.0 W  (one shared fan per 2-3 nodes)
-----------------------------------------
Wall power per node ................. ~8 - 9 W
```

| Fleet | Device draw | Wall power (nodes) | Wall power (+fans, 15 % margin) | Suggested PSU |
|---|---|---|---|---|
| 4 nodes | \~24 W | \~32 W | \~40 W | 65 W USB PSU |
| 8 nodes | \~48 W | \~64 W | \~80 W | 100–130 W USB PSU |
| 12 nodes | \~72 W | \~96 W | \~115 W | 150 W USB PSU |
| 24 nodes | \~144 W | \~192 W | \~230 W | 2× 150 W, dual feeds |

Add a **UPS (≥ 600 VA)** on the USB hub and the gateway so a mains interruption does not kill the fleet in the middle of a client stream. With Layers 1–2 active the phones have a healthy battery buffer, but the UPS also protects the gateway.

#### 7\.5\.6 Verification Protocol

Run this once per device model **before** rolling out the fleet:

- [ ] Measure steady draw with an inline USB power meter under the Phase 0 benchmark load → record in `docs/benchmark.md`.
- [ ] Set the Layer 1 limiter just above the measured draw; confirm the node runs 4 h with **no** battery depletion.
- [ ] Enable "Protect battery"; confirm charging stops at the OEM cap.
- [ ] Run a 72 h soak: log SOC, temperature, and charge state every 5 minutes; confirm **zero** net SOC change and no swelling.
- [ ] Record the equilibrium temperature — this is the real thermal ceiling for the node (§4\.2 penalty thresholds).


&nbsp;

---

## 8\. Implementation Roadmap

### Phase 0: Device Benchmark & Model Decision *(new — gates everything)*

- [ ] Build `llama.cpp` for ARMv8 on one reference device (see §9\.4 for the exact, safe flags).
- [ ] Benchmark **both** candidate models: prefill rate, generation rate (1 stream and 4 batched), peak RSS, thermal curve over a 30‑minute sustained run.
- [ ] **Measure steady power draw** with an inline USB power meter under the benchmark load (§9\.7). This number feeds the energy budget (§7\.5\.5) and the Layer 1 current limiter calibration (§7\.5\.3).
- [ ] Produce `docs/benchmark.md` with per-model numbers and a **model selection decision**.
- [ ] Validate the SSE/keepalive behaviour over a 30‑minute idle→active cycle (§7\.3 risk 2).
- [ ] **Exit criterion:** a signed model + thread + slot configuration per device class, **plus** a measured steady power draw.

> This phase exists because Phase 2's configuration was previously a guess. Benchmarking one phone before provisioning ten is cheaper than discovering the wrong model after the fleet is built.

### Phase 1: Local VM Prototyping (Central Node)

*Status: implemented — 45 tests green, verified against a live Docker stack. Items marked ◐ are
working but incomplete; see the "Known limitations" section of `gateway/README.md`.*

- [x] Set up local VM with Debian/Ubuntu Server or Fedora Server.  
- [x] Install Docker and Docker Compose plugin (verify: `docker --version`, `docker compose version`).  
- [x] Implement `LowFarm Gateway` with FastAPI and Redis.  
  - [x] OpenAI-compatible surface: `POST /v1/chat/completions`, `POST /v1/completions`, `GET /v1/models`.  
  - [x] SSE pass-through with correct `data: [DONE]` termination and proxy headers.  
  - [x] Bearer auth + admin key (§6).  
  - [x] `GET /health`, `GET /ready` and `GET /metrics`.  
- [x] Implement the **scheduler** (§4): least-outstanding, power-of-two tie-break, slot accounting, admission control.  
- [x] Implement the **worker lifecycle** (§3): `POST /v1/admin/workers/register`, `POST /v1/admin/workers/{id}/heartbeat`, health probes, circuit breaker with hysteresis.  
- [x] Implement **session affinity** (§4\.4) in Redis with TTL.  
- [x] Create **mock workers** (simulated endpoints using python or local llama.cpp) to test load balancing and failure injection.  
- [x] Implement Server-Sent Events (SSE) streaming pass-through.  
- [x] Implement Prometheus metrics endpoint (§5\.1).  
- [x] Grafana dashboard — provisioned automatically from `ops/grafana/`, 14 panels.  
- [x] Write the integration test suite: failover, mid-stream drop, worker reboot, Redis restart, auth rejection.  
- [x] Redis-backed chat history (§3\.1) — server-authoritative transcript, three client shapes accepted.  
- [x] Per-client quotas (§6\.2) — RPM, TPM and concurrency, admin-managed keys.  
- [x] True `/v1/completions` text-completion path.  
- [x] **Exit criterion:** a mock fleet of N workers behind the gateway, load-balanced, with fault injection passing.

### Phase 2: Mobile Worker Provisioning (Samsung A30 & Legacy Devices)

- [ ] **Android Optimization (from the gateway PC over USB ADB — see §9\.3):**  
      - Enable Developer Options & USB Debugging.  
      - Remove bloatware via ADB (`pm uninstall --user 0 <pkg>`).  
      - Disable aggressive battery optimization for Termux (`deviceidle whitelist`).  
      - Disable Wi-Fi sleep / scanning optimization (critical for SSE, §7\.3).  
      - Keep screen on while plugged (`stay_on_while_plugged_in`).  
      - **Enable the three-layer charge strategy (§7\.5\.3):** Layer 1 = install and calibrate the inline current limiter against the Phase 0 measured draw; Layer 2 = enable Samsung "Protect battery"; Layer 3 = apply the ADB SOC cap only where Layers 1–2 cannot be verified.  
      - **Verify with the §7\.5\.6 protocol:** 72 h soak with zero net SOC change and no swelling.  
- [ ] **Termux Environment Setup:**  
      - Install build dependencies (`clang`, `cmake`, `git`, `openssh`, `python`).  
      - Configure SSH server running on port `8022`.  
      - Set static IP lease on router for the device MAC address.  
- [ ] **Engine Compilation:**  
      - Clone and compile `llama.cpp` for ARMv8 (**safe flags — §9\.4**; do *not* use `-DGGML_NATIVE=ON` blindly).  
      - Download the model selected in Phase 0 (`Qwen2.5-0.5B` or `1.5B`, Q4\_K\_M).  
- [ ] **Worker Automation:**  
      - Build the `worker_agent` (§9\.5): registration, heartbeat loop with thermal sampling, and llama-server supervision (restart on crash).  
      - Create boot script (`termux-boot` or `run_worker.sh`) to automatically launch `llama-server` on boot.  
      - Implement the **scheduled reboot** path (`adb reboot` + `termux-boot`) as the only sanctioned form of node cycling — the charger is never disconnected from a node in service (§7\.5\.1).  
- [ ] **Exit criterion:** one phone registers itself, serves a completion through the gateway, reports telemetry, and has passed the 72 h battery/thermal soak.

### Phase 3: Cluster Integration & Networking

- [ ] Connect mobile worker nodes to dedicated 5GHz Wi-Fi subnetwork or USB-tethered network.  
- [ ] Verify **automatic** worker discovery — adding a phone must require **zero** gateway changes (the §3\.1 dynamic registration flow).  
- [ ] Tune the scheduler against real telemetry: confirm batching is actually happening (slots_used > 1 under concurrent load).  
- [ ] Calibrate the thermal penalty (§4\.2) using measured throttle points from Phase 0.  
- [ ] Validate the dedicated 5 GHz SSID: disable client isolation, verify per-device RSSI via the heartbeat agent, and re-test Wi-Fi jitter.  
- [ ] Test multi-worker distribution with parallel client prompts.  
- [ ] Verify graceful degradation: kill a phone mid-stream and confirm the gateway behaviour matches the §3\.4 matrix.  
- [ ] **Exit criterion:** a multi-phone fleet serving real traffic with live per-node telemetry.

### Phase 4: Production Bare-Metal Deployment & Monitoring

- [ ] Install bare-metal OS on dedicated gateway PC/Server.  
- [ ] Deploy Docker stack via `docker-compose up -d`.  
- [ ] Enable Redis persistence & memory limits (§9\.1).  
- [ ] Wire up Prometheus + Grafana + Alertmanager (§5\.3).  
- [ ] Physical setup:  
      - Mount devices in cooled rack/stand.  
      - Set up active USB cooling fans to prevent thermal throttling.  
      - Connect devices to a **per-device controlled** power delivery hub.  
      - Implement charge cycling (20 %–80 %) via the ADB charge-cap mechanism.  
- [ ] Document the **runbook**: adding a worker, replacing a worker, draining a hot worker, scaling the farm.  
- [ ] **Exit criterion:** unattended operation for 72 h with no thermal throttle events and no memory leaks.

&nbsp;

---

## 9\. Configuration Templates

### 9\.1 `docker-compose.yml` (Gateway Node)

```yaml
services:
  gateway:
    build: ./gateway
    container_name: lowfarm_gateway
    ports:
      - "8000:8000"
    environment:
      - REDIS_URL=redis://redis:6379/0
      - GATEWAY_API_KEY=${GATEWAY_API_KEY:?set GATEWAY_API_KEY in .env}
      - ADMIN_API_KEY=${ADMIN_API_KEY:?set ADMIN_API_KEY in .env}
      - WORKER_SEEDS=${WORKER_SEEDS:-}          # bootstrap only; dynamic registration is primary
      - SCHEDULING_POLICY=least_outstanding
      - DEFAULT_SLOTS_PER_WORKER=4
      - HEALTH_CHECK_INTERVAL=10
      - HEALTH_PROBE_TIMEOUT=3
      - HEALTH_FAILURE_THRESHOLD=3
      - HEALTH_RECOVERY_THRESHOLD=2
      - WORKER_STALE_TTL=45
      - SESSION_TTL=3600
      - STREAM_QUEUE_TIMEOUT=120
      - STREAM_HEARTBEAT_INTERVAL=15
      - THERMAL_SOFT_LIMIT_C=45
      - THERMAL_HARD_LIMIT_C=52
    depends_on:
      redis:
        condition: service_healthy
    restart: always
    networks: [lowfarm]

  redis:
    image: redis:7-alpine
    container_name: lowfarm_redis
    # NOTE: no `ports:` — Redis is reachable only on the internal Compose network.
    command:
      - redis-server
      - --save
      - "60"
      - "1"
      - --maxmemory
      - "512mb"
      - --maxmemory-policy
      - volatile-lru
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5
    restart: always
    networks: [lowfarm]

volumes:
  redis_data:

networks:
  lowfarm:
    driver: bridge
```

**Changes from the original template, and why:**

* `version:` removed — obsolete in Compose v2+ and emits a warning on Compose v5.
* Redis `ports:` removed — it was published on the host with no auth (§6).
* `depends_on` now uses a **health condition**, so the gateway does not start against a dead Redis.
* Redis gets `--maxmemory`/`volatile-lru` so a memory leak cannot OOM the host.
* Scheduling, health, affinity, thermal, and streaming parameters are all **explicit and tunable** rather than hard-coded.
* `STREAM_HEARTBEAT_INTERVAL` implements the SSE keepalive discussed in §7\.3.
* Secrets come from a `.env` file (gitignored, mode `0600`), never from inline literals.

### 9\.2 Gateway API Surface

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/v1/chat/completions` | Bearer | OpenAI-compatible, SSE or JSON |
| `POST` | `/v1/completions` | Bearer | OpenAI-compatible |
| `GET` | `/v1/models` | Bearer | Lists models aggregated from registered workers |
| `GET` | `/health` | none | Gateway liveness (not fleet health) |
| `GET` | `/ready` | none | Ready = Redis reachable **and** ≥1 healthy worker |
| `GET` | `/metrics` | none | Prometheus exposition |
| `POST` | `/v1/admin/workers/register` | Admin | Worker self-registration |
| `POST` | `/v1/admin/workers/{id}/heartbeat` | Admin | Telemetry heartbeat |
| `POST` | `/v1/admin/workers/{id}/drain` | Admin | Remove from rotation, keep serving in-flight |
| `GET` | `/v1/admin/workers` | Admin | Registry dump (id, state, slots, temp, battery) |

**API versioning:** the `/v1` prefix is the contract. The gateway also emits an `X-LowFarm-Version` header so clients can detect a version mismatch. Breaking changes require `/v2`.

### 9\.3 Provisioning Script (run from the gateway PC over USB ADB)

```sh
#!/usr/bin/env bash
# provision_worker.sh — run from the GATEWAY host with the phone connected via USB
set -euo pipefail

PKG=com.termux
CHARGE_CAP=70   # percent — prevents 100% soak and battery swelling

echo "==> Developer settings"
adb shell settings put global development_settings_enabled 1
adb shell settings put global stay_on_while_plugged_in 3   # stay awake on AC/USB/wireless
adb shell settings put system screen_off_timeout 2147483647
adb shell svc wifi enable

echo "==> Doze / battery optimization exemption (critical: the worker must not be doze-frozen)"
adb shell dumpsys deviceidle whitelist +$PKG

echo "==> Wi-Fi keepalive hardening (critical: prevents idle SSE sockets being killed)"
adb shell settings put global wifi_scan_always_enabled 0
adb shell settings put global wifi_suspend_always_optimization 0

echo "==> Layer 2: Samsung 'Protect battery' (OEM charge cap, no hacks)"
# UI path if ADB is unavailable: Settings -> Battery -> "Protect battery" (caps at 85%).
# The settings key is One-UI-version dependent; attempt it, then verify manually.
adb shell settings put global protect_battery_mode 1 >/dev/null 2>&1 || \
  echo "    (key not available on this ROM -- enable it manually in Settings -> Battery)"

echo "==> Layer 3: ADB SOC cap (fallback only -- behaviour varies by OEM/ROM)"
adb shell dumpsys battery set level $CHARGE_CAP
adb shell dumpsys battery set status 2   # 2 = charging
echo "    VERIFY: expect 'Not charging' once the real SOC reaches the cap"
adb shell cat /sys/class/power_supply/battery/status || true

echo "==> Layer 1: current limiter calibration (hardware, do this with an inline meter)"
echo "    1. Measure steady draw of this node under the Phase 0 benchmark load."
echo "    2. Set the inline limiter just ABOVE that draw (see docs/benchmark.md)."
echo "    3. Confirm 4 h of service with zero net SOC change."
echo "    4. NEVER disconnect the charger from a node in service (see 7.5.1)."

echo "==> Termux permissions"
adb shell pm grant $PKG android.permission.WRITE_SETTINGS || true

echo "==> Bloatware removal (review each before running in production)"
for p in com.facebook.app com.facebook.services com.facebook.system \
         com.samsung.android.bloatagent com.samsung.android.app.spage \
         com.sec.android.app.samsungapps com.samsung.android.game.gametools; do
  adb shell pm uninstall --user 0 $p || true
done

echo "==> Wake Termux"
adb shell am start -n com.termux/.app.TermuxActivity

echo "Done. Configure SSH on port 8022 and set a static DHCP lease for this MAC."
```

> **Warning — Layer 3 side effects:** `dumpsys battery set level/status` *fakes* the battery state to the OS. It must be **reset** (`adb shell dumpsys battery reset`) before the device is returned to normal use, and it makes the battery percentage reported by the OS unreliable as a health metric — rely on `dumpsys thermalservice` and the voltage reported by the fuel gauge for the real state. Because this behaviour is **OEM/ROM dependent**, Layers 1 and 2 are the load-bearing controls; Layer 3 is a backstop.
>
> **Warning — do not use charger cut-off schedules.** A time-based cut of the charger turns an active node into a battery-powered device with a ~2.5 h runtime (§7\.5\.1). If a node must be cycled for battery health, use the **scheduled reboot** path (`adb reboot` + `termux-boot` auto-start) with the charger left connected.

### 9\.4 Building `llama.cpp` on Termux (ARM64)

```sh
pkg install clang cmake make git python
git clone https://github.com/ggerganov/llama.cpp
cd llama.cpp

# SAFE ARMv8 build. The Exynos 7904 (Cortex-A73/A53) is ARMv8.0: it does NOT
# support +dotprod or +i8mm. Using -DGGML_NATIVE=ON here risks SIGILL crashes.
cmake -B build -S . \
  -DCMAKE_BUILD_TYPE=Release \
  -DGGML_NATIVE=OFF \
  -DGGML_CPU_ARM_ARCH=armv8-a+crypto \
  -DGGML_ARM_DOTPROD=OFF \
  -DGGML_ARM_I8MM=OFF \
  -DGGML_OPENMP=ON \
  -DLLAMA_CURL=OFF

cmake --build build --config Release -j$(nproc)
```

**Notes:**

* `armv8-a+crypto` enables AES/SHA acceleration present on all A53/A73 silicon.
* `GGML_OPENMP=ON` improves batching throughput but has been reported as unstable on some Android kernels — build a non-OpenMP fallback binary and compare.
* Compile time on a phone is 20–45 minutes. Expect to do it once per device, or better: build **once**, then distribute the binary and the `.so` files over the network to the other phones.

### 9\.5 Worker Start Script (`run_worker.sh` inside Termux)

```sh
#!/usr/bin/env bash
set -euo pipefail

MODEL_PATH="${MODEL_PATH:-$HOME/models/qwen2.5-0.5b-instruct-q4_k_m.gguf}"
PORT="${PORT:-8080}"
THREADS="${THREADS:-4}"
THREADS_BATCH="${THREADS_BATCH:-6}"
CTX_TOTAL="${CTX_TOTAL:-8192}"    # TOTAL across slots: 8192 / 4 slots = 2048 per slot
PARALLEL="${PARALLEL:-4}"
WORKER_API_KEY="${WORKER_API_KEY:-}"
ALIAS="${ALIAS:-qwen-0.5b-node}"

echo "Starting LowFarm Worker Node: $ALIAS"
"$HOME/llama.cpp/build/bin/llama-server" \
  --model "$MODEL_PATH" \
  --host 0.0.0.0 \
  --port "$PORT" \
  --threads "$THREADS" \
  --threads-batch "$THREADS_BATCH" \
  --ctx-size "$CTX_TOTAL" \
  --parallel "$PARALLEL" \
  --cont-batching \
  --n-gpu-layers 0 \
  --alias "$ALIAS" \
  --metrics \
  --slots \
  --keep \
  ${WORKER_API_KEY:+--api-key "$WORKER_API_KEY"} \
  2>&1 | tee -a "$HOME/lowfarm/worker.log"
```

> **Note:** the original script omitted `--parallel` and `--ctx-size` semantics were ambiguous, so continuous batching was never actually enabled. The revised flags above make batching explicit, and `--ctx-size` is now the **total** budget, not the per-slot budget.

### 9\.6 Worker Agent (`worker_agent.sh`)

```sh
#!/usr/bin/env bash
# Registers the worker with the gateway, then heartbeats with telemetry.
set -uo pipefail

GATEWAY="${GATEWAY:-http://192.168.1.10:8000}"
ADMIN_KEY="${ADMIN_API_KEY:?}"
WORKER_ID="${WORKER_ID:-$(hostname)-$(ip -4 addr show wlan0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)}"
ALIAS="${ALIAS:-qwen-0.5b-node}"
LISTEN="${LISTEN:-http://$(hostname -I | awk '{print $1}'):8080}"
SLOTS="${SLOTS:-4}"
INTERVAL="${HEARTBEAT_INTERVAL:-10}"

# --- wait for llama-server ---
until curl -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; do sleep 2; done

# --- register ---
curl -fsS -X POST "$GATEWAY/v1/admin/workers/register" \
  -H "Authorization: Bearer $ADMIN_KEY" -H 'Content-Type: application/json' \
  -d "{\"worker_id\":\"$WORKER_ID\",\"alias\":\"$ALIAS\",\"model\":\"${MODEL_TAG:-qwen2.5-0.5b-instruct-q4_k_m}\",\"listen\":\"$LISTEN\",\"slots\":$SLOTS,\"threads:${THREADS:-4}\":4}" \
  || echo "registration failed, will retry"

# --- heartbeat loop ---
while true; do
  TEMPS=$(for z in /sys/class/thermal/thermal_zone*/temp; do
            echo -n "$(cat $z 2>/dev/null | awk '{printf "%.1f", $1/1000}'),"; done)
  BAT=$(dumpsys battery 2>/dev/null | grep -oP 'level: \K\d+' || echo -1)
  LOAD=$(cut -d' ' -f1 /proc/loadavg)
  SLOTS_USED=$(curl -fsS http://127.0.0.1:8080/slots 2>/dev/null | grep -c '"is_processing" : true' || echo 0)

  curl -fsS -X POST "$GATEWAY/v1/admin/workers/$WORKER_ID/heartbeat" \
    -H "Authorization: Bearer $ADMIN_KEY" -H 'Content-Type: application/json' \
    -d "{\"ts\":$(date +%s.%N),\"uptime_s\":$(cut -d' ' -f1 /proc/uptime),\"cpu_load_1m\":$LOAD,\"battery_pct\":$BAT,\"slots_used\":$SLOTS_USED,\"slots_total\":$SLOTS,\"temp_c\":{\"cpu\":${TEMPS:-0}}}" \
    >/dev/null 2>&1
  sleep "$INTERVAL"
done
```

> The agent deliberately **only reports** — it never decides. Liveness is the gateway's call (§3\.3). The agent also does not restart `llama-server` on its own; the gateway's health probe detects the failure and the supervisor (systemd/Termux service) handles the restart, keeping crash-loop decisions in one place.

### 9\.7 Hardware BOM & Power Infrastructure

> Prices are indicative (2026, generic off-the-shelf). Verify locally before purchasing.

| Item | Spec | Qty (8-node fleet) | Unit price | Purpose |
|---|---|---|---|---|
| **USB inline power meter** | USB-A/USB-C, displays V + A + W | 1–2 | \$8–15 | **Mandatory.** Ground truth for every power decision (§7\.5\.6) |
| **Charge-inhibit / current-limiter module** | Inline USB-C, hard current ceiling | 1 per node | \$6–12 | Layer 1 of the charge strategy (§7\.5\.3); also creates the thermal equilibrium |
| **USB power supply, switched** | 100–130 W total, per-port switch, 2.4 A | 1 | \$35–60 | Node power (§7\.5\.5) |
| **USB fan / active cooler** | 5 V, 40–60 mm, per 2–3 nodes | 3–4 | \$8–15 | **Thermal control dominates battery life** (§7\.5\.2) |
| **UPS** | ≥ 600 VA, USB-aware | 1 | \$60–120 | Protects the hub + gateway from mains loss |
| **USB hub with per-port switch** | 10+ ports, individually switchable | 1 | \$25–45 | Isolate a single node without touching the others |
| **Rack / mount** | Vertical or angled, ventilated | 1 | \$20–40 | Physical stability + airflow |
| **Cat5e/6 + PoE injector** *(optional)* | 802.3at (30 W) | per node | \$20–30 | Alternative to USB power: per-port budget at the switch, clean shutdown |
| **Smart plug** *(not recommended for node power)* | — | — | — | A time-based charger cut is incompatible with 24/7 compute (§7\.5\.1). Legitimate uses: cutting power to *idle* reserve nodes, or controlling the fan |

**PoE alternative:** a PoE switch with per-port power budget plus a USB-C PoE splitter gives a cleaner per-node power cap (802.3af = 15.4 W, 802.3at = 30 W) and removes the USB hub entirely. It adds cost per port and the spliters are less common; evaluate it when scaling past ~12 nodes.

**Excluded on purpose:** no "battery bypass" mods (hardware risk, voids warranty), and no charge-discharge cycling controllers (§7\.5\.1).

&nbsp;

---

## 10\. Reference Clients & Interoperability

### 10\.1 Streaming with `curl`

```sh
curl -N http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2.5-0.5b",
    "messages": [{"role": "user", "content": "Explain edge inference in 3 sentences."}],
    "stream": true,
    "metadata": {"conversation_id": "demo-001"}
  }'
```

### 10\.2 Python (OpenAI SDK)

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="<GATEWAY_API_KEY>")

stream = client.chat.completions.create(
    model="qwen2.5-0.5b",
    messages=[{"role": "user", "content": "Hello from the farm"}],
    stream=True,
    extra_body={"metadata": {"conversation_id": "demo-001"}},
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### 10\.3 Open WebUI

Point Open WebUI's OpenAI connection at `http://<gateway>:8000/v1` with the bearer token. Verify that streaming, model listing, and chat history all behave — history lives in the gateway's Redis, so Open WebUI's own persistence is redundant but harmless.

### 10\.4 Client Responsibilities

* Clients that want **affinity** (§4\.4) must send a stable `metadata.conversation_id`. Without it, every request is scheduled independently and the prefix cache hit rate will be low (still correct, just slower).
* Clients must tolerate `429` (saturated farm) and `502` (worker failure) and retry with backoff.

&nbsp;

---

## 11\. Risk Assessment & Hardware Precautions

1. **Battery Swelling / Thermal Risk:**  
   * *Mitigation:* Ensure active air-cooling over the rear casing of phone nodes. Use the **three-layer charge strategy** (§7\.5\.3): hardware current limiter (Layer 1) + Samsung "Protect battery" (Layer 2) + ADB SOC cap as a backstop (Layer 3). Keep the SOC between 40 % and 85 % and the cell below 40 °C. **Do not use battery-bypass modification** (hardware risk, voids warranty) and **do not use smart plugs to cut charger power from a node in service** (§7\.5\.1) — that kills the node in ~2.5 h. If a node must be cycled, use the scheduled reboot path with the charger connected.
2. **Wi-Fi Jitter & Latency:**  
   * *Mitigation:* Bind phone nodes to a 5GHz Wi-Fi network reserved exclusively for IoT/Edge compute.
3. **Thermal Throttling:**  
   * *Mitigation:* Cap threads (`-t 2` for Exynos 7904\) to avoid triggering aggressive thermal throttling on big Cortex-A73 cores.
4. **Silent SSE Socket Death (Android Wi-Fi Power Save):** — *new*  
   * *Symptom:* streams work for minutes, then drop mid-response with no error, usually after an idle period.  
   * *Mitigation:* §9\.3 (`wifi_suspend_always_optimization=0`, `wifi_scan_always_enabled=0`), plus a gateway-side SSE heartbeat comment every 15 s and gateway-side retry of *pre-token* failures.
5. **Doze / App Freezing:** — *new*  
   * *Symptom:* the phone works when charging and plugged in, but stops heartbeating after being unplugged or after screen-off.  
   * *Mitigation:* `deviceidle whitelist +com.termux`, `stay_on_while_plugged_in=3`, and an always-on screen timeout during farm operation.
6. **Batch-Capacity Misconfiguration:** — *new*  
   * *Symptom:* everything works but throughput is ~half of expectation.  
   * *Mitigation:* §9\.5 — `--parallel N` with `--ctx-size` as the **total** budget; verify via `/slots` that more than one slot is busy under concurrent load.
7. **ARMv8 Feature Mismatch (SIGILL):** — *new*  
   * *Symptom:* `llama-server` crashes immediately on start, or works for a while then faults.  
   * *Mitigation:* §9\.4 — build with explicit `armv8-a+crypto` and `DOTPROD/I8MM=OFF`; never `-DGGML_NATIVE=ON` on a fleet of heterogeneous ARMv8.0/8.2 devices.
8. **Gateway as Single Point of Failure:** — *new*  
   * *Mitigation:* The gateway is stateless (§1\.1) and can be restarted without losing worker identity. For HA, run two gateway instances behind a reverse proxy with a shared Redis; the registry is TTL-based, so both instances converge on the same fleet view.
9. **Registry Divergence:** — *new*  
   * *Symptom:* two gateway instances disagree about which workers exist.  
   * *Mitigation:* all worker state lives in Redis with TTLs (§3\.1); no gateway keeps a private authoritative copy.
10. **Prompt-Cache Miss Storm:** — *new*  
   * *Symptom:* throughput collapses during long conversations.  
   * *Mitigation:* session affinity (§4\.4) plus monitoring `lowfarm_cache_hit_ratio`.
11. **Node Death by Charger Cut-Off:** — *new (§7\.5\.1)*  
   * *Symptom:* a node stops heartbeating and disappears from the registry after a scheduled charger-off window; battery gauge at 0 %.  
   * *Root cause:* a time-based cut-off scheme. The node drains 6 W × 3.25 h = 19.5 Wh against a 15.4 Wh cell, so it cannot survive the window.  
   * *Mitigation:* the charger is **never** disconnected from a node in service. Use a current limiter that keeps external power available, and use scheduled *reboots* for any cycling need. Alert on `lowfarm_worker_battery_pct` dropping below 40 %.
12. **Silent Battery Overcharge (Layer 3 Not Effective):** — *new (§7\.5\.3)*  
   * *Symptom:* the node is plugged in 24/7 and the OS reports 70 % (faked), but the real cell climbs to 100 % and stays there — the exact worst case for longevity.  
   * *Mitigation:* **never trust the reported SOC.** Verify the real state with an inline USB power meter and `cat /sys/class/power_supply/battery/status` (expect `Not charging`) as part of the §7\.5\.6 soak protocol. Layers 1 and 2 are the load-bearing controls.
13. **Thermal Dominance of Degradation:** — *new (§7\.5\.2)*  
   * *Symptom:* batteries swell and capacity collapses even though the SOC window is respected.  
   * *Root cause:* cell temperature above 45 °C. Every +10 °C roughly halves calendar life, so heat is a stronger lever than the charge window.  
   * *Mitigation:* remove the phone case, mount for airflow, run active cooling (§9\.7), and treat the §4.2 thermal penalty as a hard requirement rather than an optimisation. Raise `THERMAL_HARD_LIMIT_C` only if the measured soak proves it safe.
14. **Mains Loss Without UPS:** — *new (§7\.5\.5)*  
   * *Symptom:* the entire fleet dies mid-stream on a brief power cut.  
   * *Mitigation:* UPS on the USB hub and the gateway; with Layers 1–2 active the cells provide a short buffer, but only the UPS protects the gateway. Watch for a correlated drop in `lowfarm_workers_healthy`.
15. **Current Limiter Set Below Node Draw:** — *new (§7\.5\.3)*  
   * *Symptom:* the node runs but the SOC drains steadily even while plugged in.  
   * *Root cause:* the limiter ceiling is set below the measured steady draw, so the cell becomes the power source.  
   * *Mitigation:* calibrate against the Phase 0 measurement; re-verify after every model or thread-count change, since both alter the draw.

&nbsp;

---

## 12\. Revision Log

### Revision 3 (2026\-10\-21) — Energy & Battery Strategy

| # | Change | Section |
|---|---|---|
| 15 | **Time-based charger cut-off ruled out for 24/7 compute**, with the worked arithmetic showing a 45 min/4 h schedule drains the node in one cycle | §7\.5\.1 |
| 16 | New section: governing constraint, Li-ion degradation physics, three-layer charge strategy, staggered-cycling option, energy budget, verification protocol | §7\.5 |
| 17 | Per-node power draw and battery runtime added to the model comparison table | §7\.1 |
| 18 | Fleet power budget corrected from a guessed value to a measured basis (device draw + conversion losses + fans), with a PSU sizing table | §7\.4, §7\.5\.5 |
| 19 | Phase 0 now measures steady power draw as a deliverable | §8 Phase 0 |
| 20 | Phase 2 now requires the three-layer strategy, current-limiter calibration, and a 72 h battery/thermal soak | §8 Phase 2 |
| 21 | Provisioning script restructured around the three layers, with a Layer 3 verification step and a "never de-power a node in service" warning | §9\.3 |
| 22 | New hardware BOM with indicative prices (power meter, current limiters, PSU, fans, UPS, hub, PoE alternative) | §9\.7 |
| 23 | Five new risks added (node death by cut-off, silent overcharge, thermal dominance, mains loss, limiter under-draw) | §11 |
| 24 | Scheduled reboot added as the only sanctioned form of node cycling | §7\.5\.1, §9\.3, §8 Phase 2 |

### Revision 4 (2026\-10\-22) — Phase 1 implementation

| # | Change | Section |
|---|---|---|
| 25 | Phase 1 implemented: gateway, scheduler, worker lifecycle, SSE, metrics, 45-test suite | §8 Phase 1 |
| 26 | Gateway code, Docker image, Compose stack and developer documentation added | `gateway/` |
| 27 | Mock `llama-server` with self-registration, so the fleet appears with zero gateway configuration | `gateway/mock_worker.py` |
| 28 | Model rewriting made unconditional — the worker echoes its *startup* alias, which need not match its registered alias | `gateway/app/proxy.py` |
| 29 | Worker registration made retrying — a worker that starts before the gateway must not be lost | `gateway/mock_worker.py` |
| 30 | Error mapping refined: `404` unknown model, `429` saturated fleet, `502` every worker failed | `gateway/app/main.py` |

### Revision 5 (2026\-10\-22) — Phase 1b

| # | Change | Section |
|---|---|---|
| 31 | Redis-backed conversation history with server-authoritative merge rules; the assistant's reply is written back so "continue" mode works | §3\.1, §10 |
| 32 | Per-client quotas — RPM, TPM and concurrency — with admin-managed API keys; only the SHA-256 hash of a key is stored | §6\.2 |
| 33 | Real `/v1/completions` text-completion path, replacing the chat alias | §9\.2 |
| 34 | Grafana dashboard provisioned from `ops/grafana/` — 14 panels over the §5\.1 metric set | §5\.1, §5\.3 |
| 35 | `max_concurrency = 0` now means unlimited, consistent with the other limits | §6\.2 |
| 36 | Quota admission reserves an estimated token cost and reconciles it with the real usage on completion | §6\.2 |
| 37 | Test suite grew from 45 to 81 cases | §8 Phase 1 |

### Revision 2 (2026\-10\-21)

| # | Change | Section |
|---|---|---|
| 1 | Dynamic worker registration + heartbeat replaces hand-configured IP lists | §3 |
| 2 | Scheduling policy defined: least-outstanding + power-of-two tie-break, slot accounting, admission control | §4 |
| 3 | Session affinity / KV-cache continuity defined | §4\.4 |
| 4 | Observability made a Phase 1 deliverable, with a full metric list and alert set | §5 |
| 5 | Worker-side thermal telemetry added (the key phone-farm differentiator) | §5\.2 |
| 6 | Security hardening section; Redis no longer published; auth on all planes | §6, §9\.1 |
| 7 | Capacity budget and model selection decision procedure | §7 |
| 8 | Phase 0 benchmark phase added as a gate on Phase 2 configuration | §8 |
| 9 | Compose template corrected (obsolete `version`, healthcheck, Redis limits, tunables) | §9\.1 |
| 10 | ADB provisioning script added (Doze, Wi-Fi keepalive, charge cap, bloatware) | §9\.3 |
| 11 | Safe ARMv8 build flags for `llama.cpp` on Termux | §9\.4 |
| 12 | `--parallel` / `--ctx-size` semantics corrected; worker agent added | §9\.5, §9\.6 |
| 13 | Reference clients and API versioning documented | §10 |
| 14 | Five new risks added (socket death, doze, batch config, SIGILL, SPOF, cache storms) | §11 |

### Revision 1 (initial)

Baseline architecture, 4-phase roadmap, compose and worker templates, initial risk assessment.

&nbsp;

---

## 13\. Open Questions

1. **Which model wins Phase 0** — 0.5B, 1.5B, or a per-traffic-class mix? *Blocks Phase 2.*
2. **Thread/slot configuration per device class** — `-t 4 -tb 6 -np 4` vs. the conservative `-t 2`. *Blocks Phase 2.*
3. **Does the gateway need per-client quotas** (§6\.2) for the intended deployment, or is single-tenant acceptable?
4. **Wi-Fi infrastructure** — a dedicated router/AP with a managed VLAN, or USB tethering for the first fleet? Affects the Phase 3 network design.
5. **Charge strategy hardware** — which Layer 1 mechanism is used: inline current-limiter modules, a limited-source PSU, or PoE at the switch? Depends on the fleet size and the measured draw from Phase 0. *Blocks Phase 2.*
6. **Does "Protect battery" (85 % cap) exist on the target One UI version?** If not, Layer 1 becomes the sole reliable control and the current-limiter budget must be sized accordingly. *Blocks Phase 2.*
7. **UPS sizing** — is the hub and gateway on the same UPS as the phones, or is the phones' battery buffer considered sufficient for the expected outage duration? *Blocks Phase 4.*
8. **Battery telemetry trust** — if Layer 3 (ADB SOC cap) is used, the OS-reported battery percentage is fake. Should the farm instead derive a real SOC estimate from `dumpsys thermalservice` / voltage, and at what accuracy?

&nbsp;
