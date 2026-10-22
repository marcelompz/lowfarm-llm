"""Wire schemas: OpenAI-compatible payloads and the worker control plane."""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------
# OpenAI-compatible chat
# --------------------------------------------------------------------------


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool", "function"]
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None


class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage] = Field(min_length=1)
    stream: bool = False
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    max_completion_tokens: int | None = None
    stop: str | list[str] | None = None
    seed: int | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None
    n: int | None = None
    user: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict, exclude=True)

    # Non-OpenAI extension: pins the conversation to a worker for prefix-cache
    # continuity (§4.4).  Clients that omit it still work, they just lose the
    # cache hit rate.
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def conversation_id(self) -> str | None:
        value = self.metadata.get("conversation_id")
        return str(value) if value else None


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class Choice(BaseModel):
    index: int = 0
    message: ChatMessage | None = None
    delta: dict[str, Any] | None = None
    finish_reason: str | None = None


class ChatCompletionResponse(BaseModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}")
    object: Literal["chat.completion"] = "chat.completion"
    created: int = Field(default_factory=lambda: int(time.time()))
    model: str
    choices: list[Choice]
    usage: Usage = Field(default_factory=Usage)


class ModelCard(BaseModel):
    id: str
    object: Literal["model"] = "model"
    created: int = Field(default_factory=lambda: int(time.time()))
    owned_by: str = "lowfarm"


class ModelList(BaseModel):
    object: Literal["list"] = "list"
    data: list[ModelCard]


# --------------------------------------------------------------------------
# Worker control plane
# --------------------------------------------------------------------------


class WorkerState(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DRAINED = "drained"


class WorkerRegistration(BaseModel):
    """Payload a worker POSTs to `/v1/admin/workers/register` on boot."""

    worker_id: str = Field(min_length=1, max_length=128)
    alias: str = Field(default="", max_length=128)
    model: str = Field(default="unknown")
    listen: str = Field(min_length=1)
    slots: int = Field(default=4, ge=1, le=64)
    threads: int = Field(default=4, ge=1)
    ctx_size: int = Field(default=8192, ge=512)
    api_key: str | None = None
    agent_version: str | None = None


class Heartbeat(BaseModel):
    ts: float = 0.0
    uptime_s: float = 0.0
    cpu_load_1m: float = 0.0
    temp_c: dict[str, float] = Field(default_factory=dict)
    battery_pct: int | None = None
    battery_faked: bool = False
    charging: bool | None = None
    slots_used: int = 0
    slots_total: int = 0
    tokens_per_s: float = 0.0


class WorkerView(BaseModel):
    """What the admin API returns for a registered worker."""

    worker_id: str
    alias: str
    model: str
    listen: str
    slots: int
    state: WorkerState
    in_flight: int
    slots_used: int
    ewma_latency_ms: float
    request_count: int
    error_count: int
    temp_c: dict[str, float] = Field(default_factory=dict)
    battery_pct: int | None = None
    battery_faked: bool = False
    uptime_s: float = 0.0
    registered_at: float = 0.0
    last_seen: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class ErrorResponse(BaseModel):
    error: dict[str, str]
