"""Upstream proxying to `llama-server`.

Two responsibilities beyond plain forwarding:

  * **SSE heartbeats.**  Android kills idle Wi-Fi sockets, so a silent stream
    can die mid-response (§7.3).  The gateway emits an SSE comment every
    ``stream_heartbeat_interval`` seconds even when the upstream is quiet.
  * **Model rewriting.**  The worker echoes its own alias; the client asked
    for a different name.  The gateway restores the requested name so the
    surface stays OpenAI-compatible.

The retry boundary is enforced by the caller: `open_stream` raises only
*before* any byte reaches the client, which is the only point where a retry
is safe (§3.4).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator

import httpx

from .config import Settings
from .registry import WorkerRecord

SSE_COMMENT = b": ping\n\n"


class UpstreamError(Exception):
    """The worker failed in a way the gateway cannot paper over."""

    def __init__(self, message: str, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = status_code


class WorkerProxy:
    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        self._client = client
        self._settings = settings

    # -- helpers ---------------------------------------------------------

    def _headers(self, record: WorkerRecord, stream: bool) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if record.api_key:
            headers["Authorization"] = f"Bearer {record.api_key}"
        if stream:
            headers["Accept"] = "text/event-stream"
        return headers

    def _payload(self, record: WorkerRecord, body: dict[str, Any]) -> dict[str, Any]:
        """Send the worker's own alias upstream so strict builds accept it."""
        payload = dict(body)
        payload["model"] = record.alias or record.model or body.get("model", "")
        return payload

    # -- non streaming ---------------------------------------------------

    async def complete(
        self, record: WorkerRecord, body: dict[str, Any], requested_model: str
    ) -> dict[str, Any]:
        url = f"{record.listen.rstrip('/')}/v1/chat/completions"
        try:
            response = await self._client.post(
                url,
                json=self._payload(record, body),
                headers=self._headers(record, stream=False),
            )
        except httpx.HTTPError as exc:
            raise UpstreamError(f"worker {record.worker_id} unreachable: {exc}") from exc

        if response.status_code >= 400:
            raise UpstreamError(
                f"worker {record.worker_id} returned {response.status_code}: "
                f"{response.text[:200]}",
                status_code=502,
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise UpstreamError(f"worker {record.worker_id} returned non-JSON") from exc

        if isinstance(data, dict):
            data["model"] = requested_model
        return data

    # -- text completions -------------------------------------------------

    async def complete_text(
        self, record: WorkerRecord, body: dict[str, Any], requested_model: str
    ) -> dict[str, Any]:
        """Forward a text completion to the worker's ``/v1/completions``."""
        url = f"{record.listen.rstrip('/')}/v1/completions"
        try:
            response = await self._client.post(
                url,
                json=self._payload(record, body),
                headers=self._headers(record, stream=False),
            )
        except httpx.HTTPError as exc:
            raise UpstreamError(f"worker {record.worker_id} unreachable: {exc}") from exc

        if response.status_code >= 400:
            raise UpstreamError(
                f"worker {record.worker_id} returned {response.status_code}: "
                f"{response.text[:200]}",
                status_code=502,
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise UpstreamError(f"worker {record.worker_id} returned non-JSON") from exc

        if isinstance(data, dict):
            data["model"] = requested_model
        return data

    async def open_text_stream(
        self, record: WorkerRecord, body: dict[str, Any]
    ) -> httpx.Response:
        """Open an upstream text-completion stream (safe to retry)."""
        url = f"{record.listen.rstrip('/')}/v1/completions"
        request = self._client.build_request(
            "POST",
            url,
            json=self._payload(record, body),
            headers=self._headers(record, stream=True),
        )
        try:
            return await self._client.send(request, stream=True)
        except httpx.HTTPError as exc:
            raise UpstreamError(f"worker {record.worker_id} unreachable: {exc}") from exc

    # -- streaming -------------------------------------------------------
    async def open_stream(
        self, record: WorkerRecord, body: dict[str, Any]
    ) -> httpx.Response:
        """Open the upstream stream.

        Raises before a single byte reaches the client, so the caller may
        safely retry against a different worker.
        """
        url = f"{record.listen.rstrip('/')}/v1/chat/completions"
        request = self._client.build_request(
            "POST",
            url,
            json=self._payload(record, body),
            headers=self._headers(record, stream=True),
        )
        try:
            return await self._client.send(request, stream=True)
        except httpx.HTTPError as exc:
            raise UpstreamError(f"worker {record.worker_id} unreachable: {exc}") from exc

    async def iter_sse(
        self,
        response: httpx.Response,
        requested_model: str,
        text_sink: list[str] | None = None,
    ) -> AsyncIterator[bytes]:
        """Transform the upstream SSE stream into the client-facing stream.

        ``text_sink`` receives the generated text as it streams, so the
        caller can persist the assistant's reply into the conversation
        history without re-parsing the stream.
        """
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def pump() -> None:
            try:
                async for chunk in response.aiter_bytes():
                    await queue.put(chunk)
            except httpx.HTTPError:
                pass
            finally:
                await queue.put(None)

        task = asyncio.create_task(pump())
        buffer = b""
        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        queue.get(), timeout=self._settings.stream_heartbeat_interval
                    )
                except asyncio.TimeoutError:
                    # Upstream is quiet: keep the socket warm (§7.3).
                    yield SSE_COMMENT
                    continue

                if item is None:
                    break

                buffer += item
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if text_sink is not None:
                        _collect_text(line, text_sink)
                    yield _rewrite_model_line(line + b"\n", requested_model)
                if buffer:
                    # A partial line: hold it until the terminator arrives.
                    pass
            if buffer:
                if text_sink is not None:
                    _collect_text(buffer, text_sink)
                yield _rewrite_model_line(buffer, requested_model)
        finally:
            if not task.done():
                task.cancel()
            await response.aclose()


def _collect_text(line: bytes, sink: list[str]) -> None:
    """Accumulate the generated text from an SSE data line."""
    stripped = line.strip()
    if not stripped.startswith(b"data:"):
        return
    raw = stripped[len(b"data:") :].strip()
    if not raw or raw == b"[DONE]":
        return
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return
    if not isinstance(payload, dict):
        return
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return
    first = choices[0]
    if not isinstance(first, dict):
        return
    text = first.get("text")
    if isinstance(text, str):
        sink.append(text)
        return
    delta = first.get("delta")
    if isinstance(delta, dict):
        content = delta.get("content")
        if isinstance(content, str):
            sink.append(content)


def _rewrite_model_line(line: bytes, requested_model: str) -> bytes:
    """Restore the client-requested model name inside an SSE data line.

    The worker echoes whatever name it was started with (`llama-server
    --alias`), which is not necessarily the name it registered under, so the
    rewrite is unconditional: the client's model always wins.  The scheduler
    already guarantees the worker serves that model.
    """
    if not requested_model:
        return line
    stripped = line.strip()
    if not stripped.startswith(b"data:"):
        return line
    raw = stripped[len(b"data:") :].strip()
    if not raw or raw == b"[DONE]":
        return line
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return line
    if not isinstance(payload, dict) or "model" not in payload:
        return line
    if payload["model"] == requested_model:
        return line
    payload["model"] = requested_model
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return b"data: " + encoded + b"\n"


def now() -> float:
    return time.monotonic()
