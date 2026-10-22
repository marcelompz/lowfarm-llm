"""Shared test fixtures.

The gateway is exercised against in-process mock workers through a routing
ASGI transport, so the whole suite runs without Docker or a network.
"""

from __future__ import annotations

import os
from typing import Any, AsyncIterator, Iterator

import httpx
import pytest
import pytest_asyncio

os.environ.setdefault("GATEWAY_API_KEY", "test-gateway-key")
os.environ.setdefault("ADMIN_API_KEY", "test-admin-key")

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.store import MemoryStore, RedisStore  # noqa: E402
from mock_worker import create_mock_worker  # noqa: E402

GATEWAY_KEY = "test-gateway-key"
ADMIN_KEY = "test-admin-key"
MODEL = "qwen2.5-0.5b-instruct-q4_k_m"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        redis_url="redis://localhost:6379/15",
        gateway_api_key=GATEWAY_KEY,
        admin_api_key=ADMIN_KEY,
        health_check_interval=0.2,
        health_probe_timeout=1.0,
        health_failure_threshold=2,
        health_recovery_threshold=2,
        half_open_probe_interval=0.2,
        worker_stale_ttl=5,
        session_ttl=60,
        stream_queue_timeout=1.0,
        stream_heartbeat_interval=1.0,
        max_retries=2,
        default_slots_per_worker=4,
    )


@pytest.fixture
def store(settings: Settings) -> Iterator[MemoryStore]:
    yield MemoryStore(worker_ttl=settings.worker_stale_ttl, session_ttl=settings.session_ttl)


class RoutingTransport(httpx.AsyncBaseTransport):
    """Dispatch requests to the mock worker registered for that host:port."""

    def __init__(self) -> None:
        self._transports: dict[str, httpx.ASGITransport] = {}

    def add(self, netloc: str, app: Any) -> None:
        self._transports[netloc] = httpx.ASGITransport(app=app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # httpx >= 0.28 returns ``URL.netloc`` as bytes.
        netloc = request.url.netloc
        if isinstance(netloc, bytes):
            netloc = netloc.decode("ascii")
        transport = self._transports.get(netloc)
        if transport is None:
            raise httpx.ConnectError(f"no mock worker for {netloc}")
        return await transport.handle_async_request(request)


@pytest_asyncio.fixture
async def upstream(settings: Settings) -> AsyncIterator[RoutingTransport]:
    transport = RoutingTransport()
    transport.add(
        "worker-1:8080",
        create_mock_worker(alias="worker-1", model=MODEL, tokens_per_sec=200),
    )
    transport.add(
        "worker-2:8080",
        create_mock_worker(alias="worker-2", model=MODEL, tokens_per_sec=200),
    )
    transport.add(
        "worker-broken:8080",
        create_mock_worker(alias="broken", model=MODEL, fail_mode="error"),
    )
    transport.add(
        "worker-other:8080",
        create_mock_worker(
            alias="other", model="qwen2.5-1.5b-instruct-q4_k_m", tokens_per_sec=200
        ),
    )
    yield transport


@pytest_asyncio.fixture
async def client(
    settings: Settings, store: MemoryStore, upstream: RoutingTransport
) -> AsyncIterator[httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=upstream)
    app = create_app(settings=settings, store=store, client=http_client)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://gateway"
    ) as api_client:
        # Trigger the lifespan (registry, health loop).
        async with app.router.lifespan_context(app):
            api_client.app = app  # type: ignore[attr-defined]
            yield api_client
    await http_client.aclose()


@pytest.fixture
def headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {GATEWAY_KEY}"}


@pytest.fixture
def admin_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {ADMIN_KEY}"}


@pytest.fixture
def redis_store(settings: Settings) -> Iterator[RedisStore]:
    """A RedisStore backed by fakeredis, for the Redis-specific code paths."""
    import fakeredis.aioredis

    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield RedisStore(
        "redis://localhost:6379/15",
        worker_ttl=settings.worker_stale_ttl,
        session_ttl=settings.session_ttl,
        redis_client=client,
    )
