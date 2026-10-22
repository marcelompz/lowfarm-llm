"""LowFarm Gateway — FastAPI application.

Exposes:

  * ``/v1/chat/completions``  OpenAI-compatible, streaming or not
  * ``/v1/completions``       OpenAI-compatible text completions
  * ``/v1/models``            models aggregated from the registered fleet
  * ``/health`` ``/ready``    liveness and readiness
  * ``/metrics``              Prometheus exposition
  * ``/v1/admin/workers/*``   worker control plane
  * ``/v1/admin/clients/*``   client and quota management
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any, AsyncIterator

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from . import __version__
from .config import Settings, get_settings
from .history import ChatHistory
from .metrics import Metrics
from .models import (
    ChatCompletionRequest,
    ClientRegistration,
    CompletionRequest,
    Heartbeat,
    ModelCard,
    ModelList,
    WorkerRegistration,
    WorkerState,
)
from .proxy import UpstreamError, WorkerProxy
from .quotas import ClientIdentity, QuotaExceeded, QuotaManager
from .registry import WorkerRecord, WorkerRegistry
from .scheduler import (
    ModelNotAvailable,
    NoEligibleWorker,
    NoWorkerAvailable,
    Scheduler,
)
from .security import audit_keys, require_admin_key, require_client
from .store import RedisStore, Store

API_VERSION = "v1"

logger = logging.getLogger("lowfarm.gateway")


def create_app(
    settings: Settings | None = None,
    store: Store | None = None,
    client: httpx.AsyncClient | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    metrics = Metrics()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved_store = store or RedisStore(
            settings.redis_url,
            settings.worker_stale_ttl,
            settings.session_ttl,
            chat_ttl=settings.chat_history_ttl,
        )
        registry = WorkerRegistry(
            resolved_store,
            failure_threshold=settings.health_failure_threshold,
            recovery_threshold=settings.health_recovery_threshold,
        )
        scheduler = Scheduler(registry, settings)
        owned_client = client is None
        http_client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.health_probe_timeout,
                read=settings.upstream_request_timeout,
                write=settings.upstream_request_timeout,
                pool=settings.health_probe_timeout,
            ),
            limits=httpx.Limits(
                max_connections=200, max_keepalive_connections=50, keepalive_expiry=30
            ),
        )
        proxy = WorkerProxy(http_client, settings)
        quota_manager = QuotaManager(resolved_store, settings)
        chat_history = ChatHistory(resolved_store, settings)

        app.state.settings = settings
        app.state.store = resolved_store
        app.state.registry = registry
        app.state.scheduler = scheduler
        app.state.metrics = metrics
        app.state.proxy = proxy
        app.state.client = http_client
        app.state.quota_manager = quota_manager
        app.state.chat_history = chat_history

        _audit_secrets(settings)

        await quota_manager.ensure_default_client()
        await _seed_workers(registry, settings)

        health_task = asyncio.create_task(_health_loop(app))
        app.state.health_task = health_task
        try:
            yield
        finally:
            health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await health_task
            if owned_client:
                await http_client.aclose()
            await resolved_store.close()

    app = FastAPI(
        title="LowFarm Gateway",
        version=__version__,
        description="OpenAI-compatible gateway for the LowFarm edge inference farm.",
        lifespan=lifespan,
    )
    app.dependency_overrides[get_settings] = lambda: settings

    @app.middleware("http")
    async def add_version_header(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-LowFarm-Version"] = __version__
        return response

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @app.post("/v1/chat/completions")
    async def chat_completions(
        request: ChatCompletionRequest,
        identity: ClientIdentity = Depends(require_client),
    ) -> Response:
        return await _dispatch_chat(app, request, identity)

    @app.post("/v1/completions")
    async def completions(
        request: CompletionRequest,
        identity: ClientIdentity = Depends(require_client),
    ) -> Response:
        return await _dispatch_text(app, request, identity)

    @app.get("/v1/models")
    async def models(identity: ClientIdentity = Depends(require_client)) -> ModelList:
        workers = await app.state.registry.list_workers()
        seen: dict[str, ModelCard] = {}
        for w in workers:
            if w.state == WorkerState.HEALTHY and w.model and w.model not in seen:
                seen[w.model] = ModelCard(id=w.model)
        return ModelList(data=list(seen.values()))

    @app.delete("/v1/chat/conversations/{conversation_id}")
    async def delete_conversation(
        conversation_id: str,
        identity: ClientIdentity = Depends(require_client),
    ) -> dict[str, Any]:
        history: ChatHistory = app.state.chat_history
        existed = await history.delete(conversation_id)
        return {"conversation_id": conversation_id, "deleted": existed}

    # ------------------------------------------------------------------
    # Health / observability
    # ------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        store_ok = await app.state.store.healthy()
        workers = await app.state.registry.list_workers()
        healthy = [w for w in workers if w.state == WorkerState.HEALTHY]
        payload = {
            "redis": "ok" if store_ok else "degraded",
            "workers_registered": len(workers),
            "workers_healthy": len(healthy),
        }
        status_code = 200 if store_ok and healthy else 503
        return JSONResponse(payload, status_code=status_code)

    @app.get("/metrics")
    async def prometheus_metrics() -> Response:
        return Response(content=metrics.render(), media_type=metrics.content_type())

    # ------------------------------------------------------------------
    # Admin / control plane
    # ------------------------------------------------------------------

    @app.post(
        "/v1/admin/workers/register",
        dependencies=[Depends(require_admin_key)],
        status_code=201,
    )
    async def register_worker(payload: WorkerRegistration) -> dict[str, Any]:
        record = await app.state.registry.register(payload)
        return {
            "worker_id": record.worker_id,
            "state": record.state.value,
            "session_ttl": settings.session_ttl,
        }

    @app.post(
        "/v1/admin/workers/{worker_id}/heartbeat",
        dependencies=[Depends(require_admin_key)],
    )
    async def heartbeat(worker_id: str, beat: Heartbeat) -> dict[str, Any]:
        record = await app.state.registry.heartbeat(worker_id, beat)
        if record is None:
            raise HTTPException(status_code=404, detail="worker not registered")
        return {"worker_id": worker_id, "state": record.state.value}

    @app.post(
        "/v1/admin/workers/{worker_id}/drain",
        dependencies=[Depends(require_admin_key)],
    )
    async def drain_worker(worker_id: str) -> dict[str, Any]:
        workers = await app.state.registry.list_workers()
        if not any(w.worker_id == worker_id for w in workers):
            raise HTTPException(status_code=404, detail="worker not registered")
        await app.state.registry.set_state(worker_id, WorkerState.DRAINED)
        return {"worker_id": worker_id, "state": WorkerState.DRAINED.value}

    @app.get("/v1/admin/workers", dependencies=[Depends(require_admin_key)])
    async def list_workers() -> dict[str, Any]:
        workers = await app.state.registry.list_workers()
        return {"workers": [w.to_view().model_dump() for w in workers]}

    # -- clients & quotas -------------------------------------------------

    @app.post(
        "/v1/admin/clients",
        dependencies=[Depends(require_admin_key)],
        status_code=201,
    )
    async def create_client(payload: ClientRegistration) -> dict[str, Any]:
        manager: QuotaManager = app.state.quota_manager
        api_key, key_hash = await manager.create_client(payload, payload.api_key)
        return {
            "api_key": api_key,
            "key_hash": key_hash,
            "name": payload.name,
            "rpm_limit": payload.rpm_limit,
            "tpm_limit": payload.tpm_limit,
            "max_concurrency": payload.max_concurrency,
        }

    @app.get("/v1/admin/clients", dependencies=[Depends(require_admin_key)])
    async def list_clients() -> dict[str, Any]:
        manager: QuotaManager = app.state.quota_manager
        clients = await manager.list_clients()
        return {"clients": [c.model_dump() for c in clients]}

    @app.delete(
        "/v1/admin/clients/{key_hash}", dependencies=[Depends(require_admin_key)]
    )
    async def delete_client(key_hash: str) -> dict[str, Any]:
        manager: QuotaManager = app.state.quota_manager
        if not await manager.delete_client(key_hash):
            raise HTTPException(status_code=404, detail="client not found")
        return {"key_hash": key_hash, "deleted": True}

    @app.get(
        "/v1/admin/conversations/{conversation_id}",
        dependencies=[Depends(require_admin_key)],
    )
    async def view_conversation(conversation_id: str) -> dict[str, Any]:
        history: ChatHistory = app.state.chat_history
        messages = await history.get(conversation_id)
        return {
            "conversation_id": conversation_id,
            "message_count": len(messages),
            "ttl_seconds": settings.chat_history_ttl,
            "messages": messages,
        }

    @app.get("/")
    async def root() -> dict[str, str]:
        return {
            "name": "lowfarm-gateway",
            "version": __version__,
            "api": f"/{API_VERSION}",
        }

    return app


# ----------------------------------------------------------------------
# Chat completions
# ----------------------------------------------------------------------


async def _dispatch_chat(
    app: FastAPI,
    request: ChatCompletionRequest,
    identity: ClientIdentity,
) -> Response:
    registry: WorkerRegistry = app.state.registry
    scheduler: Scheduler = app.state.scheduler
    proxy: WorkerProxy = app.state.proxy
    metrics: Metrics = app.state.metrics
    settings: Settings = app.state.settings
    quota: QuotaManager = app.state.quota_manager
    history: ChatHistory = app.state.chat_history

    body = _chat_body(request)

    # Server-side history: the client may send a full transcript, a
    # truncated one, or just the new turn.
    effective_messages = body["messages"]
    if request.conversation_id:
        effective_messages = await history.resolve(
            request.conversation_id, effective_messages
        )
        body["messages"] = effective_messages

    ticket = await _admit(quota, identity, json.dumps(effective_messages))

    requested_model = request.model
    conversation_id = request.conversation_id
    attempts = settings.max_retries + 1
    last_error: UpstreamError | None = None
    failed_workers: set[str] = set()

    for attempt in range(attempts):
        try:
            selection = await scheduler.select(
                model=requested_model,
                conversation_id=conversation_id,
                exclude=failed_workers,
            )
        except ModelNotAvailable as exc:
            await quota.release(ticket)
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NoEligibleWorker as exc:
            await quota.release(ticket)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except NoWorkerAvailable as exc:
            metrics.queue_depth.set(scheduler.queue_depth)
            await quota.release(ticket)
            raise HTTPException(status_code=429, detail=str(exc)) from exc

        record = selection.record
        await registry.acquire(record.worker_id)
        metrics.worker_inflight.labels(worker=record.worker_id).set(record.in_flight + 1)
        metrics.cache_hit_ratio.labels(worker=record.worker_id).set(
            1 if selection.affinity_hit else 0
        )

        started = time.monotonic()
        try:
            if request.stream:
                upstream = await proxy.open_stream(record, body)
                return _stream_response(
                    app,
                    record,
                    upstream,
                    requested_model,
                    started,
                    metrics,
                    ticket,
                    conversation_id,
                )
            data = await proxy.complete(record, body, requested_model)
        except UpstreamError as exc:
            last_error = exc
            failed_workers.add(record.worker_id)
            await _finish(registry, metrics, record, started, failed=True)
            metrics.requests_total.labels(
                worker=record.worker_id, status="error"
            ).inc()
            if attempt == attempts - 1:
                break
            # Nothing was emitted to the client, so retrying is safe (§3.4).
            continue

        await _finish(registry, metrics, record, started, failed=False)
        metrics.requests_total.labels(worker=record.worker_id, status="ok").inc()
        await quota.release(ticket, _usage_tokens(data))
        await history.record_assistant(conversation_id, _assistant_text(data))
        return JSONResponse(data)

    metrics.requests_total.labels(worker="none", status="error").inc()
    await quota.release(ticket)
    raise HTTPException(
        status_code=last_error.status_code if last_error else 502,
        detail=str(last_error) if last_error else "no upstream available",
    )


# ----------------------------------------------------------------------
# Text completions
# ----------------------------------------------------------------------


async def _dispatch_text(
    app: FastAPI,
    request: CompletionRequest,
    identity: ClientIdentity,
) -> Response:
    registry: WorkerRegistry = app.state.registry
    scheduler: Scheduler = app.state.scheduler
    proxy: WorkerProxy = app.state.proxy
    metrics: Metrics = app.state.metrics
    settings: Settings = app.state.settings
    quota: QuotaManager = app.state.quota_manager

    body = _text_body(request)
    ticket = await _admit(quota, identity, str(request.prompt))

    requested_model = request.model
    attempts = settings.max_retries + 1
    last_error: UpstreamError | None = None
    failed_workers: set[str] = set()

    for attempt in range(attempts):
        try:
            selection = await scheduler.select(
                model=requested_model, exclude=failed_workers
            )
        except ModelNotAvailable as exc:
            await quota.release(ticket)
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except NoEligibleWorker as exc:
            await quota.release(ticket)
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except NoWorkerAvailable as exc:
            await quota.release(ticket)
            raise HTTPException(status_code=429, detail=str(exc)) from exc

        record = selection.record
        await registry.acquire(record.worker_id)
        metrics.worker_inflight.labels(worker=record.worker_id).set(record.in_flight + 1)

        started = time.monotonic()
        try:
            if request.stream:
                upstream = await proxy.open_text_stream(record, body)
                return _text_stream_response(
                    app, record, upstream, requested_model, started, metrics, ticket
                )
            data = await proxy.complete_text(record, body, requested_model)
        except UpstreamError as exc:
            last_error = exc
            failed_workers.add(record.worker_id)
            await _finish(registry, metrics, record, started, failed=True)
            metrics.requests_total.labels(
                worker=record.worker_id, status="error"
            ).inc()
            if attempt == attempts - 1:
                break
            continue

        await _finish(registry, metrics, record, started, failed=False)
        metrics.requests_total.labels(worker=record.worker_id, status="ok").inc()
        await quota.release(ticket, _usage_tokens(data))
        return JSONResponse(data)

    metrics.requests_total.labels(worker="none", status="error").inc()
    await quota.release(ticket)
    raise HTTPException(
        status_code=last_error.status_code if last_error else 502,
        detail=str(last_error) if last_error else "no upstream available",
    )


# ----------------------------------------------------------------------
# Startup checks
# ----------------------------------------------------------------------


def _audit_secrets(settings: Settings) -> None:
    """Refuse to start with placeholder keys, if configured to.

    A weak key is a public API key, so it is worth catching at boot rather
    than after a scanner does.  The values are never logged — only the
    name of the offending variable.
    """
    problems = audit_keys(
        {
            "GATEWAY_API_KEY": settings.gateway_api_keys,
            "ADMIN_API_KEY": settings.admin_api_keys,
        },
        settings.min_key_length,
    )
    if not problems:
        return
    message = "weak secret(s): " + "; ".join(problems)
    if settings.require_strong_keys:
        raise RuntimeError(
            f"{message}. Run `make secrets` and set the values in "
            f"secrets/lowfarm.env, or set REQUIRE_STRONG_KEYS=false for local work."
        )
    logger.warning("%s", message)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


async def _admit(
    quota: QuotaManager, identity: ClientIdentity, payload: str
) -> Any:
    """Enforce the client's quotas before the request reaches a worker."""
    estimated = quota.estimate_tokens(payload)
    try:
        return await quota.check_and_acquire(identity, estimated)
    except QuotaExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail=str(exc),
            headers={"Retry-After": str(exc.retry_after)},
        ) from exc


def _chat_body(request: ChatCompletionRequest) -> dict[str, Any]:
    body = request.model_dump(exclude_none=True, exclude={"metadata", "extra"})
    body["messages"] = [m.model_dump(exclude_none=True) for m in request.messages]
    body.update(request.extra)
    return body


def _text_body(request: CompletionRequest) -> dict[str, Any]:
    body = request.model_dump(exclude_none=True, exclude={"metadata", "extra"})
    body.update(request.extra)
    return body


def _usage_tokens(data: Any) -> int:
    if not isinstance(data, dict):
        return 0
    usage = data.get("usage")
    if isinstance(usage, dict):
        try:
            return int(usage.get("total_tokens") or 0)
        except (TypeError, ValueError):
            return 0
    return 0


def _assistant_text(data: Any) -> str:
    """Extract the assistant's reply from a chat-completion response."""
    if not isinstance(data, dict):
        return ""
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return content
    return ""


def _stream_response(
    app: FastAPI,
    record: WorkerRecord,
    upstream: httpx.Response,
    requested_model: str,
    started: float,
    metrics: Metrics,
    ticket: Any,
    conversation_id: str | None = None,
) -> StreamingResponse:
    proxy: WorkerProxy = app.state.proxy
    registry: WorkerRegistry = app.state.registry
    quota: QuotaManager = app.state.quota_manager
    history: ChatHistory = app.state.chat_history
    sink: list[str] = []

    async def generator() -> AsyncIterator[bytes]:
        first_token = True
        failed = False
        produced = 0
        try:
            async for chunk in proxy.iter_sse(upstream, requested_model, sink):
                if first_token:
                    first_token = False
                    metrics.ttft.labels(worker=record.worker_id).observe(
                        time.monotonic() - started
                    )
                produced += len(chunk)
                yield chunk
        except Exception:
            failed = True
            raise
        finally:
            # Must run on client disconnect too, otherwise the in-flight
            # counter leaks and the worker looks permanently busy.
            await _finish(registry, metrics, record, started, failed=failed)
            metrics.requests_total.labels(
                worker=record.worker_id, status="error" if failed else "ok"
            ).inc()
            # Streaming responses rarely carry usage; estimate from bytes.
            await quota.release(ticket, max(0, produced // 4))
            if not failed:
                await history.record_assistant(conversation_id, "".join(sink))

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _text_stream_response(
    app: FastAPI,
    record: WorkerRecord,
    upstream: httpx.Response,
    requested_model: str,
    started: float,
    metrics: Metrics,
    ticket: Any,
) -> StreamingResponse:
    proxy: WorkerProxy = app.state.proxy
    registry: WorkerRegistry = app.state.registry
    quota: QuotaManager = app.state.quota_manager

    async def generator() -> AsyncIterator[bytes]:
        failed = False
        produced = 0
        try:
            async for chunk in proxy.iter_sse(upstream, requested_model):
                produced += len(chunk)
                yield chunk
        except Exception:
            failed = True
            raise
        finally:
            await _finish(registry, metrics, record, started, failed=failed)
            metrics.requests_total.labels(
                worker=record.worker_id, status="error" if failed else "ok"
            ).inc()
            await quota.release(ticket, max(0, produced // 4))

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _finish(
    registry: WorkerRegistry,
    metrics: Metrics,
    record: WorkerRecord,
    started: float,
    failed: bool,
) -> None:
    elapsed = time.monotonic() - started
    await registry.release(record.worker_id)
    await registry.record_result(record.worker_id, elapsed * 1000, failed=failed)
    metrics.request_duration.labels(worker=record.worker_id).observe(elapsed)
    current = await registry.get(record.worker_id)
    if current is not None:
        metrics.worker_inflight.labels(worker=record.worker_id).set(current.in_flight)
        metrics.worker_slots_used.labels(worker=record.worker_id).set(current.slots_used)


# ----------------------------------------------------------------------
# Background health checking
# ----------------------------------------------------------------------


async def _health_loop(app: FastAPI) -> None:
    settings: Settings = app.state.settings
    registry: WorkerRegistry = app.state.registry
    metrics: Metrics = app.state.metrics
    client: httpx.AsyncClient = app.state.client

    while True:
        try:
            await asyncio.sleep(settings.health_check_interval)
            workers = await registry.list_workers()
            metrics.workers_registered.set(len(workers))

            healthy = 0
            now = time.monotonic()
            for worker in workers:
                # A worker in UNHEALTHY state is probed less aggressively
                # (half-open) so a recovering node rejoins without the
                # prober hammering a dead one (§3.3).
                if (
                    worker.state == WorkerState.UNHEALTHY
                    and now - worker.last_probe < settings.half_open_probe_interval
                ):
                    continue

                ok, latency_ms = await _probe(client, worker, settings)
                state = await registry.apply_probe_result(worker.worker_id, ok, latency_ms)
                if state == WorkerState.HEALTHY:
                    healthy += 1
                metrics.circuit_state.labels(worker=worker.worker_id).set(
                    {"healthy": 0, "unhealthy": 2, "drained": 1}.get(state.value, 0)
                )

                if ok:
                    await _refresh_slots(client, worker, registry)

                latest = await registry.get(worker.worker_id)
                if latest is not None:
                    for sensor, value in latest.temp_c.items():
                        metrics.worker_temperature.labels(
                            worker=worker.worker_id, sensor=sensor
                        ).set(value)
                    if latest.battery_pct is not None:
                        metrics.worker_battery.labels(worker=worker.worker_id).set(
                            latest.battery_pct
                        )
                    metrics.worker_uptime.labels(worker=worker.worker_id).set(latest.uptime_s)
                    metrics.worker_slots_used.labels(worker=worker.worker_id).set(
                        latest.slots_used
                    )
            metrics.workers_healthy.set(healthy)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A health loop must never die.
            await asyncio.sleep(settings.health_check_interval)


async def _probe(
    client: httpx.AsyncClient, worker: WorkerRecord, settings: Settings
) -> tuple[bool, float]:
    started = time.monotonic()
    try:
        response = await client.get(
            f"{worker.listen.rstrip('/')}/health",
            timeout=settings.health_probe_timeout,
        )
        elapsed = (time.monotonic() - started) * 1000
        return response.status_code < 400, elapsed
    except httpx.HTTPError:
        return False, (time.monotonic() - started) * 1000


async def _refresh_slots(
    client: httpx.AsyncClient, worker: WorkerRecord, registry: WorkerRegistry
) -> None:
    """Read llama-server's slot state so the scheduler knows real capacity."""
    try:
        response = await client.get(f"{worker.listen.rstrip('/')}/slots", timeout=3.0)
        if response.status_code >= 400:
            return
        slots = response.json()
    except (httpx.HTTPError, ValueError):
        return
    if not isinstance(slots, list):
        return
    busy = sum(1 for s in slots if isinstance(s, dict) and s.get("is_processing"))
    await registry.update_slots_used(worker.worker_id, busy)


async def _seed_workers(registry: WorkerRegistry, settings: Settings) -> None:
    """Bootstrap from WORKER_SEEDS.

    Only used when the registry is empty; dynamic registration is the primary
    path (§3.1).
    """
    existing = await registry.list_workers()
    if existing:
        return
    for entry in settings.seed_entries:
        await registry.register(
            WorkerRegistration(
                worker_id=entry["listen"],
                alias=entry["alias"],
                model=entry["model"],
                listen=entry["listen"],
                slots=settings.default_slots_per_worker,
            )
        )


__all__ = ["create_app", "API_VERSION"]
