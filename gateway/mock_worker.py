"""Mock `llama-server`.

Implements the subset of the llama.cpp HTTP API the gateway depends on:

  * ``GET  /health``   readiness probe
  * ``GET  /slots``    slot occupancy
  * ``GET  /props``    model metadata
  * ``POST /v1/chat/completions``  chat completion (JSON or SSE)

Used to exercise the gateway without a phone, and as the test double for
integration tests.

Run standalone::

    uvicorn mock_worker:app --host 0.0.0.0 --port 8080 --factory
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
import uuid
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

ALIAS = os.environ.get("MOCK_ALIAS", "qwen-0.5b-node")
MODEL = os.environ.get("MOCK_MODEL", "qwen2.5-0.5b-instruct-q4_k_m")
SLOTS = int(os.environ.get("MOCK_SLOTS", "4"))
TOKENS_PER_SEC = float(os.environ.get("MOCK_TOKENS_PER_SEC", "24"))
FAIL_MODE = os.environ.get("MOCK_FAIL_MODE", "")  # "", "error", "hang"
START_TIME = time.time()


def create_mock_worker(
    alias: str = ALIAS,
    model: str = MODEL,
    slots: int = SLOTS,
    tokens_per_sec: float = TOKENS_PER_SEC,
    fail_mode: str = FAIL_MODE,
) -> FastAPI:
    state: dict[str, Any] = {"busy": 0, "requests": 0}

    gateway_url = os.environ.get("MOCK_GATEWAY_URL", "")
    admin_key = os.environ.get("MOCK_ADMIN_KEY", "")
    worker_id = os.environ.get("MOCK_WORKER_ID", alias)
    listen = os.environ.get("MOCK_LISTEN", f"http://{worker_id}:8080")

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        """Self-register with the gateway, mirroring the real worker agent.

        This is the dynamic-registration path from §3.1: the fleet changes
        without any gateway reconfiguration.  The heartbeat runs as a
        background task so it never blocks startup.
        """
        if not gateway_url or not admin_key:
            yield
            return

        import httpx

        payload = {
            "worker_id": worker_id,
            "alias": alias,
            "model": model,
            "listen": listen,
            "slots": slots,
            "threads": 4,
            "ctx_size": 8192,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Registration must be retried: the gateway may not be up yet.
            registered = False
            for attempt in range(1, 31):
                try:
                    response = await client.post(
                        f"{gateway_url}/v1/admin/workers/register",
                        headers={"Authorization": f"Bearer {admin_key}"},
                        json=payload,
                    )
                    response.raise_for_status()
                    print(f"[mock {alias}] registered as {worker_id}")
                    registered = True
                    break
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[mock {alias}] registration attempt {attempt} failed: {exc}"
                    )
                    await asyncio.sleep(3)
            if not registered:
                yield
                return

            async def heartbeat_loop() -> None:
                while True:
                    await asyncio.sleep(10)
                    try:
                        await client.post(
                            f"{gateway_url}/v1/admin/workers/{worker_id}/heartbeat",
                            headers={"Authorization": f"Bearer {admin_key}"},
                            json={
                                "ts": time.time(),
                                "uptime_s": time.time() - START_TIME,
                                "cpu_load_1m": 0.4,
                                "temp_c": {"cpu": 38.0, "battery": 33.0},
                                "battery_pct": 72,
                                "slots_used": state["busy"],
                                "slots_total": slots,
                                "tokens_per_s": tokens_per_sec,
                            },
                        )
                    except Exception:  # noqa: BLE001
                        pass

            task = asyncio.create_task(heartbeat_loop())
            try:
                yield
            finally:
                task.cancel()

    app = FastAPI(title="Mock llama-server", lifespan=lifespan)

    @app.get("/health")
    async def health() -> JSONResponse:
        if fail_mode == "unhealthy":
            return JSONResponse({"status": "error"}, status_code=503)
        return JSONResponse({"status": "ok"})

    @app.get("/props")
    async def props() -> JSONResponse:
        return JSONResponse(
            {
                "model_path": f"/models/{model}.gguf",
                "default_generation_settings": {
                    "model": alias,
                    "n_ctx": 8192,
                    "n_parallel": slots,
                },
            }
        )

    @app.get("/slots")
    async def get_slots() -> JSONResponse:
        return JSONResponse(
            [
                {
                    "id": i,
                    "is_processing": i < state["busy"],
                    "n_ctx": 8192,
                }
                for i in range(slots)
            ]
        )

    @app.get("/metrics")
    async def metrics() -> Response:
        """llama-server exposes Prometheus metrics; mimic the shape."""
        return Response(
            content=(
                "# HELP llama_model_load_duration_seconds Model load time.\n"
                "# TYPE llama_model_load_duration_seconds gauge\n"
                f'llama_model_load_duration_seconds{{model="{model}"}} 0.0\n'
                "# HELP llama_requests_total Total requests.\n"
                "# TYPE llama_requests_total counter\n"
                f'llama_requests_total{{model="{model}"}} {state["requests"]}\n'
            ),
            media_type="text/plain; version=0.0.4",
        )

    @app.post("/v1/completions")
    async def completions(request: Request) -> Any:
        """Text completion — the real ``/v1/completions``, not a chat alias."""
        body = await request.json()
        if fail_mode == "error":
            return JSONResponse({"error": "mock failure"}, status_code=500)

        prompt = body.get("prompt", "")
        if isinstance(prompt, list):
            prompt = prompt[0] if prompt else ""
        state["requests"] += 1

        if body.get("stream"):
            return StreamingResponse(
                _sse_text_body(alias, str(prompt)), media_type="text/event-stream"
            )
        return JSONResponse(
            {
                "id": f"cmpl-{uuid.uuid4().hex}",
                "object": "text_completion",
                "created": int(time.time()),
                "model": alias,
                "choices": [
                    {
                        "index": 0,
                        "text": f" completion of: {prompt[:40]}",
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 8,
                    "completion_tokens": 9,
                    "total_tokens": 17,
                },
            }
        )

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Any:
        body = await request.json()
        if fail_mode == "error":
            return JSONResponse({"error": "mock failure"}, status_code=500)

        state["requests"] += 1
        state["busy"] = min(slots, state["busy"] + 1)
        try:
            if body.get("stream"):
                return StreamingResponse(
                    _sse_body(alias, body), media_type="text/event-stream"
                )
            return JSONResponse(_json_body(alias, body))
        finally:
            state["busy"] = max(0, state["busy"] - 1)

    return app


def _json_body(alias: str, body: dict[str, Any]) -> dict[str, Any]:
    content = "This is a mock response from the LowFarm mock worker."
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": alias,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 11,
            "total_tokens": 23,
        },
    }


async def _sse_body(alias: str, body: dict[str, Any]):
    prompt = body.get("messages", [{}])[-1].get("content", "") or ""
    words = [w for w in str(prompt).split() if w][:5] or ["mock", "response"]
    delay = 1.0 / max(1.0, TOKENS_PER_SEC)

    first = {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": alias,
        "choices": [
            {"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}
        ],
    }
    yield f"data: {json.dumps(first)}\n\n"

    for word in words:
        chunk = {
            "id": first["id"],
            "object": "chat.completion.chunk",
            "created": first["created"],
            "model": alias,
            "choices": [
                {"index": 0, "delta": {"content": f" {word}"}, "finish_reason": None}
            ],
        }
        yield f"data: {json.dumps(chunk)}\n\n"
        await asyncio.sleep(delay)

    final = {
        "id": first["id"],
        "object": "chat.completion.chunk",
        "created": first["created"],
        "model": alias,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    }
    yield f"data: {json.dumps(final)}\n\n"
    yield "data: [DONE]\n\n"


async def _sse_text_body(alias: str, prompt: str):
    """Text-completion SSE stream."""
    delay = 1.0 / max(1.0, TOKENS_PER_SEC)
    base = {
        "id": f"cmpl-{uuid.uuid4().hex}",
        "object": "text_completion",
        "created": int(time.time()),
        "model": alias,
    }
    words = [w for w in prompt.split() if w][:5] or ["mock", "completion"]
    for word in words:
        chunk = {
            **base,
            "choices": [
                {"index": 0, "text": f" {word}", "finish_reason": None, "logprobs": None}
            ],
        }
        yield f"data: {json.dumps(chunk)}\n\n"
        await asyncio.sleep(delay)
    final = {
        **base,
        "choices": [{"index": 0, "text": "", "finish_reason": "stop", "logprobs": None}],
    }
    yield f"data: {json.dumps(final)}\n\n"
    yield "data: [DONE]\n\n"


app = create_mock_worker()
