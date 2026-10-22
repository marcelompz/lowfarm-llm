# LowFarm Gateway

The control plane of the LowFarm edge inference farm. Exposes an
OpenAI-compatible API on a single endpoint and spreads inference across a
fleet of `llama-server` workers (obsolete Android phones, or mocks during
development).

The full design — architecture, scheduling policy, failure matrices, energy
strategy — is in [`../docs/MasterProjectPlan.md`](../docs/MasterProjectPlan.md).
This README covers how to run and extend the code.

---

## Quick start

```sh
cp .env.example .env && chmod 600 .env   # fill in the two API keys
cd ..
docker compose up -d --build
```

The stack comes up with three mock workers that **self-register** with the
gateway — exactly the flow real phones will use in Phase 2.

```sh
# fleet state
curl -s localhost:8000/v1/admin/workers -H "Authorization: Bearer $ADMIN_API_KEY"

# a chat completion
curl -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4_k_m",
       "messages":[{"role":"user","content":"hello farm"}]}'

# streaming
curl -N localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4_k_m",
       "messages":[{"role":"user","content":"hello farm"}],"stream":true}'
```

---

## Layout

| File | Responsibility |
|---|---|
| `app/main.py` | FastAPI app, routes, request dispatch, background health loop |
| `app/config.py` | Every tunable, backed by environment variables |
| `app/security.py` | Bearer auth for the public and admin planes |
| `app/registry.py` | Worker membership, telemetry, health state machine |
| `app/scheduler.py` | Least-outstanding-requests, affinity, admission control |
| `app/proxy.py` | Upstream forwarding, SSE pass-through, model rewriting |
| `app/history.py` | Server-side conversation history |
| `app/quotas.py` | Per-client limits (RPM, TPM, concurrency) |
| `app/store.py` | Redis backend + in-memory degraded-mode backend |
| `app/metrics.py` | Prometheus collectors |
| `app/models.py` | OpenAI-compatible and control-plane schemas |
| `mock_worker.py` | Stand-in for `llama-server` (dev + tests) |

---

## API surface

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/v1/chat/completions` | Bearer | OpenAI-compatible, SSE or JSON |
| `POST` | `/v1/completions` | Bearer | OpenAI-compatible text completions |
| `GET` | `/v1/models` | Bearer | Models aggregated from the fleet |
| `DELETE` | `/v1/chat/conversations/{id}` | Bearer | Clear a stored conversation |
| `GET` | `/health` | — | Gateway liveness |
| `GET` | `/ready` | — | Ready = Redis up **and** ≥1 healthy worker |
| `GET` | `/metrics` | — | Prometheus exposition |
| `POST` | `/v1/admin/workers/register` | Admin | Worker self-registration |
| `POST` | `/v1/admin/workers/{id}/heartbeat` | Admin | Telemetry heartbeat |
| `POST` | `/v1/admin/workers/{id}/drain` | Admin | Remove from rotation |
| `GET` | `/v1/admin/workers` | Admin | Fleet dump |
| `POST` | `/v1/admin/clients` | Admin | Create an API key with its own quota |
| `GET` | `/v1/admin/clients` | Admin | Clients and their live counters |
| `DELETE` | `/v1/admin/clients/{hash}` | Admin | Revoke a key |
| `GET` | `/v1/admin/conversations/{id}` | Admin | Inspect a stored conversation |

Error mapping:

| Situation | Status |
|---|---|
| Missing/invalid bearer token | `401` |
| No worker serves the requested model | `404` |
| Fleet saturated, request queued past the timeout | `429` |
| Client over its RPM/TPM/concurrency limit | `429` + `Retry-After` |
| Every worker serving the model failed | `502` |

---

## Conversations

With a `metadata.conversation_id` the gateway keeps the authoritative
transcript in Redis. Three client shapes are accepted:

| Client sends | Interpretation |
|---|---|
| full history + new turn | the client extends the conversation |
| a truncated history | the server keeps everything it has |
| a single new user message | "continue the conversation" |
| anything else that diverges | the client edited or branched — it wins |

The assistant's reply is written back into the history, so a client that
only sends the new turn still gets the full context on the next request.
Without a `conversation_id` the gateway is completely stateless.

```sh
curl -s localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4_k_m",
       "messages":[{"role":"user","content":"hola"}],
       "metadata":{"conversation_id":"demo-1"}}'
```

---

## Client quotas

A farm has a finite number of slots, so one noisy client must not be able to
starve the others. Each API key carries its own limits:

```sh
curl -s -X POST localhost:8000/v1/admin/clients \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"team-a","rpm_limit":30,"tpm_limit":50000,"max_concurrency":4}'
```

The response contains the generated key — **it is shown once and never
stored** (only its SHA-256 hash is kept). A limit of `0` means unlimited.

| Dimension | Enforcement |
|---|---|
| RPM | Fixed window. Checked before the request reaches a worker. |
| TPM | The real token count is only known at the end, so admission reserves an estimate and reconciles it with the actual usage on completion. |
| Concurrency | Simultaneous in-flight requests per key. |

The fixed window can allow a burst of up to 2× the limit across a window
boundary — an accepted trade-off, since a sliding window costs a sorted set
per client.

---

## How a request flows

```
client ──▶ /v1/chat/completions
            │
            ├─ scheduler.select(model, conversation_id)
            │    ├─ affinity hit?  → pinned worker (prefix-cache continuity)
            │    ├─ else power-of-two choice over workers with free capacity
            │    └─ else wait for capacity (admission control) → 429
            │
            ├─ registry.acquire(worker)          # in_flight += 1
            ├─ proxy.open_stream / proxy.complete
            │    └─ on failure BEFORE first byte → retry another worker
            │
            └─ registry.release(worker)          # in_flight -= 1, EWMA + counters
```

**The retry boundary matters.** A chat completion may only be retried before
the first token reaches the client; afterwards the client already saw part of
the answer and a retry would duplicate content. The proxy therefore raises
only from `open_stream`, which happens before any byte is emitted.

---

## Worker protocol

A worker announces itself and then heartbeats. It never needs to be listed
anywhere in the gateway's configuration.

```sh
# registration (idempotent)
curl -X POST localhost:8000/v1/admin/workers/register \
  -H "Authorization: Bearer $ADMIN_API_KEY" -H "Content-Type: application/json" \
  -d '{"worker_id":"a30-01","alias":"a30-01","model":"qwen2.5-0.5b-instruct-q4_k_m",
       "listen":"http://192.168.1.101:8080","slots":4,"threads":4,"ctx_size":8192}'

# heartbeat, every 10s
curl -X POST localhost:8000/v1/admin/workers/a30-01/heartbeat \
  -H "Authorization: Bearer $ADMIN_API_KEY" -H "Content-Type: application/json" \
  -d '{"ts":1760000000.4,"uptime_s":12844,"cpu_load_1m":3.85,
       "temp_c":{"cpu":47.2,"skin":38.9,"battery":36.1},"battery_pct":71,
       "slots_used":2,"slots_total":4,"tokens_per_s":18.4}'
```

A worker that stops heartbeating **expires from the registry by TTL**
(`WORKER_STALE_TTL`, default 45 s) — no cleanup job, no stale entries.

The heartbeat reports *telemetry*; the gateway's own probe decides *liveness*.
A saturated worker is never mistaken for a dead one.

---

## Scheduling policy

```
load(w)    = (in_flight + 1) / weight(w)
penalty(w) = 1 + 0.5·error_rate(w) + 0.2·max(0, temp(w) − soft_limit)/10
score(w)   = load(w) · penalty(w)
```

* **weight** — relative capacity, derived from the worker's slot count.
* **power-of-two choices** — among the two least-loaded candidates the better
  one wins, but the pair is picked at random, which prevents every client
  from stampeding the same worker.
* **thermal penalty** — a hot worker is progressively demoted *before* it
  hard-throttles. This is what makes the farm self-protecting.
* **affinity** — a `metadata.conversation_id` pins a conversation to the
  worker holding its prefix cache. The full history is still sent every time,
  so a reassignment is always correct, just slower.

---

## Configuration

All settings are environment variables; see `.env.example` for the full list
with defaults. The ones that matter most:

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_API_KEY` | *required* | Bearer token for `/v1/*` |
| `ADMIN_API_KEY` | *required* | Bearer token for `/v1/admin/*` |
| `REDIS_URL` | `redis://redis:6379/0` | State backend |
| `HEALTH_CHECK_INTERVAL` | `10` | Probe period (s) |
| `HEALTH_FAILURE_THRESHOLD` | `3` | Failures before a worker is quarantined |
| `HEALTH_RECOVERY_THRESHOLD` | `2` | Successes before it rejoins |
| `HALF_OPEN_PROBE_INTERVAL` | `30` | Probe period for a quarantined worker |
| `WORKER_STALE_TTL` | `45` | Registry TTL (s) |
| `STREAM_QUEUE_TIMEOUT` | `120` | How long a request waits for capacity |
| `STREAM_HEARTBEAT_INTERVAL` | `15` | SSE keepalive comment period (s) |
| `MAX_RETRIES` | `2` | Upstream retries before the first token |
| `THERMAL_SOFT_LIMIT_C` | `45` | Demotion threshold |
| `THERMAL_HARD_LIMIT_C` | `52` | Drain threshold |
| `CHAT_HISTORY_ENABLED` | `true` | Server-side conversation history |
| `CHAT_HISTORY_MAX_MESSAGES` | `40` | History window (oldest turns dropped) |
| `CHAT_HISTORY_TTL` | `86400` | History lifetime in Redis (s) |
| `QUOTAS_ENABLED` | `true` | Per-client limits |
| `DEFAULT_RPM_LIMIT` | `60` | Requests/min for the bootstrap key |
| `DEFAULT_TPM_LIMIT` | `100000` | Tokens/min for the bootstrap key |
| `DEFAULT_MAX_CONCURRENCY` | `8` | In-flight requests for the bootstrap key |
| `QUOTA_WINDOW_SECONDS` | `60` | Quota window length |

---

## Development

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
cd gateway && ../.venv/bin/python -m pytest
```

The suite runs entirely in-process: the mock workers are mounted as ASGI
apps through a routing transport, so there is no network, no Docker and no
flakiness.

| File | Covers |
|---|---|
| `tests/test_security.py` | Auth on both planes, key separation |
| `tests/test_registry.py` | Membership, telemetry, hysteresis, counters |
| `tests/test_scheduler.py` | Scoring, affinity, admission control, penalties |
| `tests/test_history.py` | Merge rules, trimming, isolation, deletion |
| `tests/test_quotas.py` | RPM/TPM/concurrency limits, reconciliation, lifecycle |
| `tests/test_api.py` | End-to-end: proxying, SSE, failover, drain, metrics |
| `tests/test_completions.py` | Text completions, client management, conversations |

### Observability stack (optional)

```sh
docker compose --profile observability up -d
```

Prometheus on `:9090`, Grafana on `:3000` (default `admin`/`admin` — change
`GRAFANA_PASSWORD` in `.env`). The dashboard **LowFarm LLM — overview** is
provisioned automatically: 14 panels covering fleet health, TTFT and latency
percentiles, per-worker in-flight and slot occupancy, thermal state, battery
and circuit-breaker state.

---

## Known limitations

* **A single gateway is the SPOF.** The design tolerates horizontal
  gateways sharing one Redis (§11 risk 8), but the in-memory hot counters
  (`ewma_latency_ms`, error counts) are per-process today.
* **The chat history merge is not fully serialised.** Each request does a
  read-merge-write under an optimistic transaction; two concurrent requests
  on the *same* conversation can still interleave.
* **Streaming responses rarely carry usage.** llama-server only reports
  token usage in non-streaming mode, so the TPM counter is reconciled from
  an estimate of the streamed bytes in that case.
* **Quota windows are fixed, not sliding.** A burst of up to 2× the limit can
  cross a window boundary.
* **No rate limiting on the admin plane** beyond the single shared key.
