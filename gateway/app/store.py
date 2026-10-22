"""State backends.

The registry is authoritative in Redis so that a gateway restart — or a second
gateway instance — sees the same fleet (§3.1).  When Redis is unreachable the
gateway degrades to an in-memory store: routing keeps working, but the fleet
view is local to this process.

TTL semantics matter here and are easy to get wrong:

  * ``save_worker`` (registration + heartbeat) **refreshes** the TTL.
  * ``update_worker`` (probe results, counters) does **not** refresh the TTL.

That asymmetry is what makes a dead worker expire even though the health
prober keeps writing its state.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Protocol

WORKER_KEY = "worker:{worker_id}"
WORKER_INDEX = "workers:index"
AFFINITY_KEY = "session:{conversation_id}"

# Atomically increment in_flight only if the worker still exists; avoids
# resurrecting an expired worker as a hash holding a single field.
_INCR_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('HINCRBY', KEYS[1], 'in_flight', ARGV[1])
end
return -1
"""


class Store(Protocol):
    async def save_worker(self, worker_id: str, data: dict[str, Any]) -> None: ...
    async def update_worker(self, worker_id: str, data: dict[str, Any]) -> None: ...
    async def load_worker(self, worker_id: str) -> dict[str, Any] | None: ...
    async def load_all_workers(self) -> list[dict[str, Any]]: ...
    async def delete_worker(self, worker_id: str) -> None: ...
    async def incr_inflight(self, worker_id: str, delta: int) -> int: ...
    async def set_affinity(self, conversation_id: str, worker_id: str) -> None: ...
    async def get_affinity(self, conversation_id: str) -> str | None: ...
    async def healthy(self) -> bool: ...
    async def close(self) -> None: ...


def _decode(raw: dict[Any, Any]) -> dict[str, Any]:
    """Redis returns bytes; normalise to str keys/values for storage."""
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(key, bytes):
            key = key.decode("utf-8", "replace")
        if isinstance(value, bytes):
            value = value.decode("utf-8", "replace")
        out[key] = value
    return out


class RedisStore:
    """Primary backend.  Redis is the source of truth for fleet membership."""

    def __init__(
        self,
        redis_url: str,
        worker_ttl: int,
        session_ttl: int,
        redis_client: Any | None = None,
    ) -> None:
        if redis_client is not None:
            self._redis = redis_client
        else:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=15,
            )
        self._worker_ttl = worker_ttl
        self._session_ttl = session_ttl
        # Lua scripting needs the `lupa` extra; fall back to a guarded HINCRBY
        # when it is unavailable (e.g. fakeredis in CI).
        try:
            self._incr = self._redis.register_script(_INCR_LUA)
        except Exception:
            self._incr = None

    # -- workers ---------------------------------------------------------

    async def save_worker(self, worker_id: str, data: dict[str, Any]) -> None:
        key = WORKER_KEY.format(worker_id=worker_id)
        pipe = self._redis.pipeline(transaction=False)
        pipe.hset(key, mapping={k: _stringify(v) for k, v in data.items()})
        pipe.expire(key, self._worker_ttl)
        pipe.sadd(WORKER_INDEX, worker_id)
        await pipe.execute()

    async def update_worker(self, worker_id: str, data: dict[str, Any]) -> None:
        key = WORKER_KEY.format(worker_id=worker_id)
        if not await self._redis.exists(key):
            return
        await self._redis.hset(key, mapping={k: _stringify(v) for k, v in data.items()})

    async def load_worker(self, worker_id: str) -> dict[str, Any] | None:
        raw = await self._redis.hgetall(WORKER_KEY.format(worker_id=worker_id))
        return _decode(raw) if raw else None

    async def load_all_workers(self) -> list[dict[str, Any]]:
        ids = await self._redis.smembers(WORKER_INDEX)
        if not ids:
            return []
        pipe = self._redis.pipeline(transaction=False)
        for wid in ids:
            pipe.hgetall(WORKER_KEY.format(worker_id=wid))
        rows = await pipe.execute()

        alive: list[dict[str, Any]] = []
        stale: list[str] = []
        for wid, raw in zip(ids, rows):
            if raw:
                alive.append(_decode(raw))
            else:
                # The hash expired but the index entry survived — prune it.
                stale.append(wid)
        if stale:
            await self._redis.srem(WORKER_INDEX, *stale)
        return alive

    async def delete_worker(self, worker_id: str) -> None:
        pipe = self._redis.pipeline(transaction=False)
        pipe.delete(WORKER_KEY.format(worker_id=worker_id))
        pipe.srem(WORKER_INDEX, worker_id)
        await pipe.execute()

    async def incr_inflight(self, worker_id: str, delta: int) -> int:
        key = WORKER_KEY.format(worker_id=worker_id)
        if self._incr is not None:
            result = await self._incr(keys=[key], args=[delta])
            return int(result)
        # Fallback: not atomic, but it never resurrects an expired worker.
        if not await self._redis.exists(key):
            return -1
        value = int(await self._redis.hincrby(key, "in_flight", delta))
        if value < 0:
            await self._redis.hset(key, "in_flight", 0)
            return 0
        return value

    # -- affinity --------------------------------------------------------

    async def set_affinity(self, conversation_id: str, worker_id: str) -> None:
        key = AFFINITY_KEY.format(conversation_id=conversation_id)
        pipe = self._redis.pipeline(transaction=False)
        pipe.set(key, worker_id)
        if self._session_ttl > 0:
            pipe.expire(key, self._session_ttl)
        await pipe.execute()

    async def get_affinity(self, conversation_id: str) -> str | None:
        value = await self._redis.get(AFFINITY_KEY.format(conversation_id=conversation_id))
        return value if value else None

    # -- lifecycle -------------------------------------------------------

    async def healthy(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def close(self) -> None:
        await self._redis.aclose()


class MemoryStore:
    """Degraded-mode backend used when Redis is unreachable.

    Same TTL semantics, but the fleet view is local to this process.
    """

    def __init__(self, worker_ttl: int, session_ttl: int) -> None:
        self._workers: dict[str, dict[str, Any]] = {}
        self._expiry: dict[str, float] = {}
        self._affinity: dict[str, tuple[str, float]] = {}
        self._worker_ttl = worker_ttl
        self._session_ttl = session_ttl
        self._lock = asyncio.Lock()

    def _expire_if_needed(self, worker_id: str) -> None:
        deadline = self._expiry.get(worker_id)
        if deadline is not None and deadline < time.monotonic():
            self._workers.pop(worker_id, None)
            self._expiry.pop(worker_id, None)

    async def save_worker(self, worker_id: str, data: dict[str, Any]) -> None:
        async with self._lock:
            self._expire_if_needed(worker_id)
            self._workers[worker_id] = {**data}
            self._expiry[worker_id] = time.monotonic() + self._worker_ttl

    async def update_worker(self, worker_id: str, data: dict[str, Any]) -> None:
        async with self._lock:
            self._expire_if_needed(worker_id)
            if worker_id in self._workers:
                self._workers[worker_id].update(data)

    async def load_worker(self, worker_id: str) -> dict[str, Any] | None:
        async with self._lock:
            self._expire_if_needed(worker_id)
            data = self._workers.get(worker_id)
            return dict(data) if data else None

    async def load_all_workers(self) -> list[dict[str, Any]]:
        async with self._lock:
            for worker_id in list(self._workers):
                self._expire_if_needed(worker_id)
            return [dict(d) for d in self._workers.values()]

    async def delete_worker(self, worker_id: str) -> None:
        async with self._lock:
            self._workers.pop(worker_id, None)
            self._expiry.pop(worker_id, None)

    async def incr_inflight(self, worker_id: str, delta: int) -> int:
        async with self._lock:
            self._expire_if_needed(worker_id)
            if worker_id not in self._workers:
                return -1
            current = int(self._workers[worker_id].get("in_flight", 0))
            current = max(0, current + delta)
            self._workers[worker_id]["in_flight"] = current
            return current

    async def set_affinity(self, conversation_id: str, worker_id: str) -> None:
        async with self._lock:
            self._affinity[conversation_id] = (
                worker_id,
                time.monotonic() + self._session_ttl,
            )

    async def get_affinity(self, conversation_id: str) -> str | None:
        async with self._lock:
            entry = self._affinity.get(conversation_id)
            if entry is None:
                return None
            worker_id, deadline = entry
            if deadline < time.monotonic():
                self._affinity.pop(conversation_id, None)
                return None
            return worker_id

    async def healthy(self) -> bool:
        return True

    async def close(self) -> None:
        return None


def _stringify(value: Any) -> str:
    """Redis hashes only hold strings; normalise before writing."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, str)):
        return str(value)
    import json

    return json.dumps(value)
