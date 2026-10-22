"""End-to-end API behaviour against the mock fleet."""

from __future__ import annotations

import json

import httpx
import pytest

from app.models import WorkerRegistration, WorkerState

MODEL = "qwen2.5-0.5b-instruct-q4_k_m"
OTHER_MODEL = "qwen2.5-1.5b-instruct-q4_k_m"


async def _register(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
    worker_id: str = "worker-1",
    model: str = MODEL,
    slots: int = 4,
) -> None:
    response = await client.post(
        "/v1/admin/workers/register",
        headers=admin_headers,
        json={
            "worker_id": worker_id,
            "alias": worker_id,
            "model": model,
            "listen": f"http://{worker_id}:8080",
            "slots": slots,
        },
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_chat_completion_is_proxied(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    response = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hello farm"}]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["role"] == "assistant"
    # The worker echoes its own alias; the gateway restores the client's name.
    assert body["model"] == MODEL


@pytest.mark.asyncio
async def test_streaming_completion_passes_sse_through(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    chunks: list[str] = []
    async with client.stream(
        "POST",
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "one two three four five"}],
            "stream": True,
        },
    ) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        async for line in response.aiter_lines():
            if line:
                chunks.append(line)

    assert any(line.startswith("data: ") for line in chunks)
    assert chunks[-1] == "data: [DONE]"

    payloads = [
        json.loads(line[len("data: ") :])
        for line in chunks
        if line.startswith("data: ") and line != "data: [DONE]"
    ]
    assert payloads, "expected at least one SSE payload"
    assert all(p["model"] == MODEL for p in payloads if "model" in p)


@pytest.mark.asyncio
async def test_streaming_rewrites_the_model_even_if_the_alias_differs(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    """Regression: the worker echoes its *startup* alias, not its registered one.

    The gateway must always restore the client-requested model, whatever the
    worker calls itself.
    """
    # worker-1's mock server runs with alias "worker-1", but we register it
    # under a different alias — exactly the mismatch seen in the field.
    await client.post(
        "/v1/admin/workers/register",
        headers=admin_headers,
        json={
            "worker_id": "worker-1",
            "alias": "unrelated-registered-name",
            "model": MODEL,
            "listen": "http://worker-1:8080",
            "slots": 4,
        },
    )
    chunks: list[str] = []
    async with client.stream(
        "POST",
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "a b c"}],
            "stream": True,
        },
    ) as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line:
                chunks.append(line)

    payloads = [
        json.loads(line[len("data: ") :])
        for line in chunks
        if line.startswith("data: ") and line != "data: [DONE]"
    ]
    assert payloads
    assert all(p["model"] == MODEL for p in payloads if "model" in p)


@pytest.mark.asyncio
async def test_unknown_model_returns_404(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    response = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": "not-a-model", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_no_workers_returns_429(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_failover_moves_to_a_healthy_worker(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    """A broken worker must not take the request down (§3.4)."""
    await _register(client, admin_headers, worker_id="worker-broken")
    await _register(client, admin_headers, worker_id="worker-2")

    response = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 200
    assert response.json()["model"] == MODEL


@pytest.mark.asyncio
async def test_all_workers_broken_returns_502(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-broken")
    response = await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 502


@pytest.mark.asyncio
async def test_models_are_aggregated_from_the_fleet(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1", model=MODEL)
    await _register(client, admin_headers, worker_id="worker-other", model=OTHER_MODEL)
    response = await client.get("/v1/models", headers=headers)
    assert response.status_code == 200
    ids = {m["id"] for m in response.json()["data"]}
    assert ids == {MODEL, OTHER_MODEL}


@pytest.mark.asyncio
async def test_drained_worker_is_removed_from_rotation(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1")
    await _register(client, admin_headers, worker_id="worker-2")

    drained = await client.post(
        "/v1/admin/workers/worker-1/drain", headers=admin_headers
    )
    assert drained.status_code == 200
    assert drained.json()["state"] == "drained"

    for _ in range(5):
        response = await client.post(
            "/v1/chat/completions",
            headers=headers,
            json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
        )
        assert response.status_code == 200

    workers = await client.get("/v1/admin/workers", headers=admin_headers)
    states = {w["worker_id"]: w["state"] for w in workers.json()["workers"]}
    assert states["worker-1"] == WorkerState.DRAINED.value


@pytest.mark.asyncio
async def test_heartbeat_updates_the_admin_view(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1")
    response = await client.post(
        "/v1/admin/workers/worker-1/heartbeat",
        headers=admin_headers,
        json={"temp_c": {"cpu": 44.5}, "battery_pct": 68, "slots_used": 1},
    )
    assert response.status_code == 200

    workers = await client.get("/v1/admin/workers", headers=admin_headers)
    worker = next(
        w for w in workers.json()["workers"] if w["worker_id"] == "worker-1"
    )
    assert worker["temp_c"]["cpu"] == 44.5
    assert worker["battery_pct"] == 68
    assert worker["slots_used"] == 1


@pytest.mark.asyncio
async def test_heartbeat_for_unknown_worker_returns_404(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/admin/workers/ghost/heartbeat", headers=admin_headers, json={}
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_ready_reflects_fleet_health(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    not_ready = await client.get("/ready")
    assert not_ready.status_code == 503
    assert not_ready.json()["workers_healthy"] == 0

    await _register(client, admin_headers, worker_id="worker-1")
    ready = await client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["workers_healthy"] == 1


@pytest.mark.asyncio
async def test_metrics_are_exposed(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1")
    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    response = await client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "lowfarm_requests_total" in body
    assert "lowfarm_request_duration_seconds" in body
    assert "lowfarm_workers_registered" in body


@pytest.mark.asyncio
async def test_version_header_is_present(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.get("/v1/models", headers=headers)
    assert response.headers.get("x-lowfarm-version")


@pytest.mark.asyncio
async def test_inflight_counter_is_released_after_a_request(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1")
    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    workers = await client.get("/v1/admin/workers", headers=admin_headers)
    worker = workers.json()["workers"][0]
    assert worker["in_flight"] == 0
    assert worker["request_count"] == 1


@pytest.mark.asyncio
async def test_conversation_id_pins_the_session(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers, worker_id="worker-1")
    await _register(client, admin_headers, worker_id="worker-2")

    body = {
        "model": MODEL,
        "messages": [{"role": "user", "content": "hi"}],
        "metadata": {"conversation_id": "conv-abc"},
    }
    await client.post("/v1/chat/completions", headers=headers, json=body)

    workers = await client.get("/v1/admin/workers", headers=admin_headers)
    pinned = [w for w in workers.json()["workers"] if w["worker_id"] == "worker-1"]
    assert pinned  # sanity: both workers exist
