"""Authentication and authorisation (§6.1)."""

from __future__ import annotations

import httpx
import pytest


@pytest.mark.asyncio
async def test_chat_completions_requires_a_token(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/chat/completions",
        json={"model": "x", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 401
    assert response.headers.get("www-authenticate") == "Bearer"


@pytest.mark.asyncio
async def test_chat_completions_rejects_a_wrong_token(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Bearer nope-nope-nope"},
        json={"model": "x", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_wrong_scheme_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Basic dXNlcjpwYXNz"},
        json={"model": "x", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_gateway_key_does_not_grant_admin_access(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.get("/v1/admin/workers", headers=headers)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_admin_key_does_not_work_on_the_public_api(
    client: httpx.AsyncClient, admin_headers: dict[str, str]
) -> None:
    response = await client.post(
        "/v1/chat/completions",
        headers=admin_headers,
        json={"model": "x", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_health_and_metrics_are_unauthenticated(
    client: httpx.AsyncClient,
) -> None:
    assert (await client.get("/health")).status_code == 200
    assert (await client.get("/metrics")).status_code == 200
