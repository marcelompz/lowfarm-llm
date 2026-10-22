"""Text completions, client management and conversation endpoints."""

from __future__ import annotations

import httpx
import pytest

MODEL = "qwen2.5-0.5b-instruct-q4_k_m"


async def _register(
    client: httpx.AsyncClient,
    admin_headers: dict[str, str],
    worker_id: str = "worker-1",
    model: str = MODEL,
) -> None:
    response = await client.post(
        "/v1/admin/workers/register",
        headers=admin_headers,
        json={
            "worker_id": worker_id,
            "alias": worker_id,
            "model": model,
            "listen": f"http://{worker_id}:8080",
            "slots": 4,
        },
    )
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_text_completion_is_proxied(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    response = await client.post(
        "/v1/completions",
        headers=headers,
        json={"model": MODEL, "prompt": "the quick brown fox"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "text_completion"
    assert body["choices"][0]["text"]
    assert body["model"] == MODEL


@pytest.mark.asyncio
async def test_text_completion_streams(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    chunks: list[str] = []
    async with client.stream(
        "POST",
        "/v1/completions",
        headers=headers,
        json={"model": MODEL, "prompt": "one two three", "stream": True},
    ) as response:
        assert response.status_code == 200
        async for line in response.aiter_lines():
            if line:
                chunks.append(line)

    assert chunks[-1] == "data: [DONE]"
    assert any('"object":"text_completion"' in line for line in chunks)


@pytest.mark.asyncio
async def test_text_completion_unknown_model_returns_404(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    response = await client.post(
        "/v1/completions",
        headers=headers,
        json={"model": "nope", "prompt": "x"},
    )
    assert response.status_code == 404


# --------------------------------------------------------------------------
# Clients
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_can_create_a_client(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/admin/clients",
        headers=admin_headers,
        json={"name": "team-a", "rpm_limit": 5, "tpm_limit": 1000, "max_concurrency": 2},
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["name"] == "team-a"
    assert payload["api_key"].startswith("lf_")
    assert payload["key_hash"]


@pytest.mark.asyncio
async def test_a_new_client_key_works_on_the_public_api(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    created = await client.post(
        "/v1/admin/clients",
        headers=admin_headers,
        json={"name": "team-a", "rpm_limit": 10, "tpm_limit": 100000},
    )
    api_key = created.json()["api_key"]

    response = await client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"model": MODEL, "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_client_rpm_limit_produces_429(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    created = await client.post(
        "/v1/admin/clients",
        headers=admin_headers,
        json={"name": "limited", "rpm_limit": 1, "tpm_limit": 100000},
    )
    api_key = created.json()["api_key"]
    auth = {"Authorization": f"Bearer {api_key}"}
    payload = {"model": MODEL, "messages": [{"role": "user", "content": "hi"}]}

    first = await client.post("/v1/chat/completions", headers=auth, json=payload)
    assert first.status_code == 200

    second = await client.post("/v1/chat/completions", headers=auth, json=payload)
    assert second.status_code == 429
    assert "Retry-After" in second.headers
    assert "rpm" in second.json()["detail"]


@pytest.mark.asyncio
async def test_deleted_client_key_stops_working(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    created = await client.post(
        "/v1/admin/clients", headers=admin_headers, json={"name": "temp"}
    )
    api_key = created.json()["api_key"]
    key_hash = created.json()["key_hash"]

    ok = await client.get(
        "/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert ok.status_code == 200

    deleted = await client.delete(f"/v1/admin/clients/{key_hash}", headers=admin_headers)
    assert deleted.status_code == 200

    denied = await client.get(
        "/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    assert denied.status_code == 401


@pytest.mark.asyncio
async def test_clients_are_listed(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    await client.post("/v1/admin/clients", headers=admin_headers, json={"name": "x"})
    response = await client.get("/v1/admin/clients", headers=admin_headers)
    assert response.status_code == 200
    assert any(c["name"] == "x" for c in response.json()["clients"])


# --------------------------------------------------------------------------
# Conversations
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_conversation_history_is_persisted_and_reused(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)

    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "hola"}],
            "metadata": {"conversation_id": "conv-persist"},
        },
    )

    view = await client.get(
        "/v1/admin/conversations/conv-persist", headers=admin_headers
    )
    assert view.status_code == 200
    # The user turn and the assistant's reply are both stored.
    assert view.json()["message_count"] == 2
    roles = [m["role"] for m in view.json()["messages"]]
    assert roles == ["user", "assistant"]
    assert view.json()["messages"][0]["content"] == "hola"
    assert view.json()["messages"][1]["content"]


@pytest.mark.asyncio
async def test_conversation_can_be_deleted(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    await _register(client, admin_headers)
    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "hola"}],
            "metadata": {"conversation_id": "conv-del"},
        },
    )
    deleted = await client.delete("/v1/chat/conversations/conv-del", headers=headers)
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    view = await client.get("/v1/admin/conversations/conv-del", headers=admin_headers)
    assert view.json()["message_count"] == 0


@pytest.mark.asyncio
async def test_continue_mode_appends_to_the_stored_history(
    client: httpx.AsyncClient, headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    """A client that sends only the new turn still gets the full context."""
    await _register(client, admin_headers)

    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "primera"}],
            "metadata": {"conversation_id": "conv-cont"},
        },
    )
    await client.post(
        "/v1/chat/completions",
        headers=headers,
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": "segunda"}],
            "metadata": {"conversation_id": "conv-cont"},
        },
    )

    view = await client.get("/v1/admin/conversations/conv-cont", headers=admin_headers)
    contents = [m["content"] for m in view.json()["messages"]]
    assert "primera" in contents
    assert "segunda" in contents
