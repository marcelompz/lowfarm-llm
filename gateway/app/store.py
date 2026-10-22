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
import json
import time
from typing import Any, Protocol

try:  # redis is optional for the in-memory backend
    from redis.exceptions import WatchError
except Exception:  # pragma: no cover
    class WatchError(Exception):  # type: ignore[no-redef]
        pass

WORKER_KEY = "worker:{worker_id}"
WORKER_INDEX = "workers:index"
AFFINITY_KEY = "session:{conversation_id}"
CHAT_KEY = "chat:{conversation_id}"
CLIENT_KEY = "client:{key_hash}"
CLIENT_INDEX = "clients:index"
META_KEY = "meta:{name}"
QUOTA_RPM_KEY = "quota:rpm:{key_hash}:{window}"
QUOTA_TPM_KEY = "quota:tpm:{key_hash}:{window}"
QUOTA_INFLIGHT_KEY = "quota:inflight:{key_hash}"

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

    # -- chat history ----------------------------------------------------
    async def merge_chat(
        self, conversation_id: str, incoming: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]: ...
    async def append_chat(
        self, conversation_id: str, messages: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]: ...
    async def get_chat(self, conversation_id: str) -> list[dict[str, Any]]: ...
    async def delete_chat(self, conversation_id: str) -> bool: ...

    # -- clients & quotas ------------------------------------------------
    async def save_client(self, key_hash: str, data: dict[str, Any]) -> None: ...
    async def load_client(self, key_hash: str) -> dict[str, Any] | None: ...
    async def load_all_clients(self) -> list[dict[str, Any]]: ...
    async def delete_client(self, key_hash: str) -> None: ...
    async def get_meta(self, name: str) -> str | None: ...
    async def set_meta(self, name: str, value: str) -> None: ...
    async def incr_counter(
        self, key: str, delta: int, ttl: int, window: int | None = None
    ) -> int: ...
    async def read_counter(self, key: str) -> int: ...
    async def reset_counter(self, key: str) -> None: ...


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


def merge_history(
    stored: list[dict[str, Any]], incoming: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Merge a client's messages into the server-side history.

    Three cases, in order:

    1. The client sent the full history plus new messages — the client's
       list already extends the stored one, so it wins outright.
    2. The client's list is a prefix of the stored one — the client sent a
       truncated history, so the server keeps everything it has.
    3. Otherwise the histories diverged (the client edited or branched the
       conversation) — the client is authoritative and replaces the history.

    A single trailing user message with no counterpart in the stored history
    is treated as "continue the conversation" and appended.
    """
    if not stored:
        return list(incoming)
    if not incoming:
        return list(stored)

    n = len(stored)
    if len(incoming) >= n and incoming[:n] == stored:
        return list(incoming)
    if len(incoming) < n and stored[: len(incoming)] == incoming:
        return list(stored)

    # Divergence.  A lone user message means "continue", not "reset".
    if (
        len(incoming) == 1
        and incoming[0].get("role") == "user"
        and stored[-1].get("role") == "assistant"
    ):
        return list(stored) + list(incoming)

    return list(incoming)


def _trim_history(
    messages: list[dict[str, Any]], max_messages: int
) -> list[dict[str, Any]]:
    """Keep the system message and the most recent turns."""
    if max_messages <= 0 or len(messages) <= max_messages:
        return messages
    head = messages[:1] if messages and messages[0].get("role") == "system" else []
    tail = messages[len(head) :]
    keep = max(0, max_messages - len(head))
    return head + tail[-keep:] if keep else head


class RedisStore:
    """Primary backend.  Redis is the source of truth for fleet membership."""

    def __init__(
        self,
        redis_url: str,
        worker_ttl: int,
        session_ttl: int,
        redis_client: Any | None = None,
        chat_ttl: int = 86400,
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
        self._chat_ttl = chat_ttl
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

    # -- chat history ----------------------------------------------------

    async def merge_chat(
        self, conversation_id: str, incoming: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]:
        """Atomically merge client messages into the stored conversation.

        Uses an optimistic WATCH/MULTI transaction so concurrent requests on
        the same conversation cannot interleave a read-modify-write.
        """
        key = CHAT_KEY.format(conversation_id=conversation_id)
        for _ in range(5):
            async with self._redis.pipeline() as pipe:
                try:
                    await pipe.watch(key)
                    raw_items = await pipe.lrange(key, 0, -1)
                except Exception:
                    await pipe.reset()
                    raise

                stored: list[dict[str, Any]] = []
                for item in raw_items:
                    try:
                        stored.append(json.loads(item))
                    except json.JSONDecodeError:
                        continue

                merged = _trim_history(merge_history(stored, incoming), max_messages)
                pipe.multi()
                pipe.delete(key)
                if merged:
                    pipe.rpush(key, *[json.dumps(m) for m in merged])
                    pipe.expire(key, self._chat_ttl)
                try:
                    await pipe.execute()
                    return merged
                except WatchError:
                    continue  # someone else wrote first: retry
        # Contention: fall back to the last known state.
        return incoming

    async def get_chat(self, conversation_id: str) -> list[dict[str, Any]]:
        key = CHAT_KEY.format(conversation_id=conversation_id)
        items = await self._redis.lrange(key, 0, -1)
        out: list[dict[str, Any]] = []
        for item in items:
            try:
                out.append(json.loads(item))
            except json.JSONDecodeError:
                continue
        return out

    async def append_chat(
        self, conversation_id: str, messages: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]:
        """Append to the stored conversation (the assistant's reply)."""
        key = CHAT_KEY.format(conversation_id=conversation_id)
        for _ in range(5):
            async with self._redis.pipeline() as pipe:
                try:
                    await pipe.watch(key)
                    raw_items = await pipe.lrange(key, 0, -1)
                except Exception:
                    await pipe.reset()
                    raise

                stored: list[dict[str, Any]] = []
                for item in raw_items:
                    try:
                        stored.append(json.loads(item))
                    except json.JSONDecodeError:
                        continue

                merged = _trim_history(stored + list(messages), max_messages)
                pipe.multi()
                pipe.delete(key)
                if merged:
                    pipe.rpush(key, *[json.dumps(m) for m in merged])
                    pipe.expire(key, self._chat_ttl)
                try:
                    await pipe.execute()
                    return merged
                except WatchError:
                    continue
        return stored + list(messages)

    async def delete_chat(self, conversation_id: str) -> bool:
        key = CHAT_KEY.format(conversation_id=conversation_id)
        return bool(await self._redis.delete(key))

    # -- clients & quotas ------------------------------------------------

    async def save_client(self, key_hash: str, data: dict[str, Any]) -> None:
        pipe = self._redis.pipeline(transaction=False)
        pipe.hset(CLIENT_KEY.format(key_hash=key_hash), mapping=data)
        pipe.sadd(CLIENT_INDEX, key_hash)
        await pipe.execute()

    async def load_client(self, key_hash: str) -> dict[str, Any] | None:
        raw = await self._redis.hgetall(CLIENT_KEY.format(key_hash=key_hash))
        return _decode(raw) if raw else None

    async def load_all_clients(self) -> list[dict[str, Any]]:
        ids = await self._redis.smembers(CLIENT_INDEX)
        if not ids:
            return []
        pipe = self._redis.pipeline(transaction=False)
        for key_hash in ids:
            pipe.hgetall(CLIENT_KEY.format(key_hash=key_hash))
        rows = await pipe.execute()
        out: list[dict[str, Any]] = []
        for key_hash, raw in zip(ids, rows):
            if raw:
                decoded = _decode(raw)
                decoded["key_hash"] = key_hash
                out.append(decoded)
        return out

    async def delete_client(self, key_hash: str) -> None:
        pipe = self._redis.pipeline(transaction=False)
        pipe.delete(CLIENT_KEY.format(key_hash=key_hash))
        pipe.srem(CLIENT_INDEX, key_hash)
        await pipe.execute()

    async def get_meta(self, name: str) -> str | None:
        value = await self._redis.get(META_KEY.format(name=name))
        return value if value else None

    async def set_meta(self, name: str, value: str) -> None:
        await self._redis.set(META_KEY.format(name=name), value)

    async def incr_counter(
        self, key: str, delta: int, ttl: int, window: int | None = None
    ) -> int:
        pipe = self._redis.pipeline(transaction=False)
        pipe.incrby(key, delta)
        pipe.expire(key, ttl)
        results = await pipe.execute()
        return int(results[0])

    async def read_counter(self, key: str) -> int:
        value = await self._redis.get(key)
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    async def reset_counter(self, key: str) -> None:
        await self._redis.delete(key)


class MemoryStore:
    """Degraded-mode backend used when Redis is unreachable.

    Same TTL semantics, but the fleet view is local to this process.
    """

    def __init__(
        self,
        worker_ttl: int,
        session_ttl: int,
        chat_ttl: int = 86400,
    ) -> None:
        self._workers: dict[str, dict[str, Any]] = {}
        self._expiry: dict[str, float] = {}
        self._affinity: dict[str, tuple[str, float]] = {}
        self._chats: dict[str, list[dict[str, Any]]] = {}
        self._chat_expiry: dict[str, float] = {}
        self._clients: dict[str, dict[str, Any]] = {}
        self._meta: dict[str, str] = {}
        self._counters: dict[str, int] = {}
        self._counter_expiry: dict[str, float] = {}
        self._worker_ttl = worker_ttl
        self._session_ttl = session_ttl
        self._chat_ttl = chat_ttl
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

    # -- chat history ----------------------------------------------------

    def _expire_chat_if_needed(self, conversation_id: str) -> None:
        deadline = self._chat_expiry.get(conversation_id)
        if deadline is not None and deadline < time.monotonic():
            self._chats.pop(conversation_id, None)
            self._chat_expiry.pop(conversation_id, None)

    async def merge_chat(
        self, conversation_id: str, incoming: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]:
        async with self._lock:
            self._expire_chat_if_needed(conversation_id)
            stored = self._chats.get(conversation_id, [])
            merged = _trim_history(merge_history(stored, incoming), max_messages)
            self._chats[conversation_id] = merged
            self._chat_expiry[conversation_id] = time.monotonic() + self._chat_ttl
            return list(merged)

    async def get_chat(self, conversation_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            self._expire_chat_if_needed(conversation_id)
            return list(self._chats.get(conversation_id, []))

    async def append_chat(
        self, conversation_id: str, messages: list[dict[str, Any]], max_messages: int
    ) -> list[dict[str, Any]]:
        async with self._lock:
            self._expire_chat_if_needed(conversation_id)
            stored = self._chats.get(conversation_id, [])
            merged = _trim_history(stored + list(messages), max_messages)
            self._chats[conversation_id] = merged
            self._chat_expiry[conversation_id] = time.monotonic() + self._chat_ttl
            return list(merged)

    async def delete_chat(self, conversation_id: str) -> bool:
        async with self._lock:
            self._expire_chat_if_needed(conversation_id)
            existed = conversation_id in self._chats
            self._chats.pop(conversation_id, None)
            self._chat_expiry.pop(conversation_id, None)
            return existed

    # -- clients & quotas ------------------------------------------------

    async def save_client(self, key_hash: str, data: dict[str, Any]) -> None:
        async with self._lock:
            self._clients[key_hash] = dict(data)

    async def load_client(self, key_hash: str) -> dict[str, Any] | None:
        async with self._lock:
            data = self._clients.get(key_hash)
            return dict(data) if data else None

    async def load_all_clients(self) -> list[dict[str, Any]]:
        async with self._lock:
            out = []
            for key_hash, data in self._clients.items():
                item = dict(data)
                item["key_hash"] = key_hash
                out.append(item)
            return out

    async def delete_client(self, key_hash: str) -> None:
        async with self._lock:
            self._clients.pop(key_hash, None)

    async def get_meta(self, name: str) -> str | None:
        async with self._lock:
            return self._meta.get(name)

    async def set_meta(self, name: str, value: str) -> None:
        async with self._lock:
            self._meta[name] = value

    async def incr_counter(
        self, key: str, delta: int, ttl: int, window: int | None = None
    ) -> int:
        async with self._lock:
            deadline = self._counter_expiry.get(key)
            if deadline is not None and deadline < time.monotonic():
                self._counters.pop(key, None)
            value = self._counters.get(key, 0) + delta
            self._counters[key] = value
            self._counter_expiry[key] = time.monotonic() + ttl
            return value

    async def read_counter(self, key: str) -> int:
        async with self._lock:
            deadline = self._counter_expiry.get(key)
            if deadline is not None and deadline < time.monotonic():
                self._counters.pop(key, None)
                self._counter_expiry.pop(key, None)
                return 0
            return self._counters.get(key, 0)

    async def reset_counter(self, key: str) -> None:
        async with self._lock:
            self._counters.pop(key, None)
            self._counter_expiry.pop(key, None)


def _stringify(value: Any) -> str:
    """Redis hashes only hold strings; normalise before writing."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, str)):
        return str(value)
    import json

    return json.dumps(value)
