"""Per-client quotas.

A farm has a finite number of slots.  Without quotas, one noisy client can
saturate every worker and starve everyone else.  Three dimensions are
enforced per API key:

  * **RPM** — requests per minute, fixed window.
  * **TPM** — tokens per minute, fixed window.  Because the real token count
    is only known once the response completes, admission reserves an
    estimate and the reservation is reconciled with the actual usage
    afterwards.
  * **Concurrency** — simultaneous in-flight requests.

Limits live in the store so several gateway instances share them.

The fixed window can allow a burst of up to 2x the limit across a window
boundary.  That is an accepted trade-off: a sliding window costs a sorted
set per client, and the extra precision is not worth it here.
"""

from __future__ import annotations

import hashlib
import secrets
import time
from dataclasses import dataclass, field

from .config import Settings
from .models import ClientQuota, ClientView
from .store import (
    QUOTA_INFLIGHT_KEY,
    QUOTA_RPM_KEY,
    QUOTA_TPM_KEY,
    Store,
)


def hash_key(api_key: str) -> str:
    """Stable identifier for an API key.  The key itself is never stored."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


# Store pointer to the hash of the current bootstrap key.
_DEFAULT_CLIENT_META = "default_client"


@dataclass
class ClientIdentity:
    key_hash: str
    name: str
    rpm_limit: int
    tpm_limit: int
    max_concurrency: int
    is_default: bool = False

    @property
    def unlimited(self) -> bool:
        return self.rpm_limit == 0 and self.tpm_limit == 0


@dataclass
class QuotaTicket:
    """What a request reserved at admission time.

    Released with the real token count so the TPM window reflects reality
    rather than the pre-flight estimate.
    """

    identity: ClientIdentity
    concurrency_acquired: bool = False
    request_reserved: bool = False
    reserved_tokens: int = 0
    extras: dict[str, int] = field(default_factory=dict)


class QuotaExceeded(Exception):
    """Raised when a client is over one of its limits."""

    def __init__(self, dimension: str, limit: int, retry_after: int) -> None:
        super().__init__(
            f"quota exceeded: {dimension} limit is {limit} "
            f"(retry after {retry_after}s)"
        )
        self.dimension = dimension
        self.limit = limit
        self.retry_after = retry_after


class QuotaManager:
    def __init__(self, store: Store, settings: Settings) -> None:
        self._store = store
        self._settings = settings

    # -- identity --------------------------------------------------------

    async def resolve(self, api_key: str) -> ClientIdentity | None:
        """Resolve an API key to a client identity, or None if unknown."""
        key_hash = hash_key(api_key)
        record = await self._store.load_client(key_hash)
        if record is not None:
            if str(record.get("enabled", "1")) != "1":
                return None
            return ClientIdentity(
                key_hash=key_hash,
                name=str(record.get("name", "client")),
                rpm_limit=int(record.get("rpm_limit", 0)),
                tpm_limit=int(record.get("tpm_limit", 0)),
                max_concurrency=int(record.get("max_concurrency", 1)),
            )

        # The bootstrap key(s) always work, with the default limits.
        # Several may be valid at once so a rotation does not drop traffic.
        if any(
            secrets.compare_digest(
                api_key.encode("utf-8"), candidate.encode("utf-8")
            )
            for candidate in self._settings.gateway_api_keys
        ):
            return ClientIdentity(
                key_hash=key_hash,
                name="default",
                rpm_limit=self._settings.default_rpm_limit,
                tpm_limit=self._settings.default_tpm_limit,
                max_concurrency=self._settings.default_max_concurrency,
                is_default=True,
            )
        return None

    async def ensure_default_client(self) -> None:
        """Create the default client record so it is visible and tunable.

        The record of the *previous* bootstrap key is removed.  Without
        this, rotating ``GATEWAY_API_KEY`` would leave the old key working
        forever, because the default client record lives in Redis and
        survives the restart.
        """
        key_hash = hash_key(self._settings.gateway_api_key)
        previous = await self._store.get_meta(_DEFAULT_CLIENT_META)

        if previous and previous != key_hash:
            # The bootstrap key changed: revoke the old one.
            await self._store.delete_client(previous)
            await self._store.reset_counter(
                QUOTA_INFLIGHT_KEY.format(key_hash=previous)
            )

        existing = await self._store.load_client(key_hash)
        if existing is None:
            await self._store.save_client(
                key_hash,
                {
                    "name": "default",
                    "rpm_limit": self._settings.default_rpm_limit,
                    "tpm_limit": self._settings.default_tpm_limit,
                    "max_concurrency": self._settings.default_max_concurrency,
                    "enabled": 1,
                    "created_at": time.time(),
                },
            )
        await self._store.set_meta(_DEFAULT_CLIENT_META, key_hash)

        # Invariant: exactly one client is named "default", and it is the
        # current bootstrap key.  Anything else with that name is a leftover
        # from an earlier rotation and would keep a revoked key alive.
        for record in await self._store.load_all_clients():
            other_hash = str(record.get("key_hash", ""))
            if other_hash and other_hash != key_hash and record.get("name") == "default":
                await self._store.delete_client(other_hash)
                await self._store.reset_counter(
                    QUOTA_INFLIGHT_KEY.format(key_hash=other_hash)
                )

    # -- client management ------------------------------------------------

    async def create_client(
        self, quota: ClientQuota, api_key: str | None = None
    ) -> tuple[str, str]:
        """Create a client.  Returns (api_key, key_hash)."""
        key = api_key or f"lf_{secrets.token_urlsafe(32)}"
        key_hash = hash_key(key)
        await self._store.save_client(
            key_hash,
            {
                "name": quota.name,
                "rpm_limit": quota.rpm_limit,
                "tpm_limit": quota.tpm_limit,
                "max_concurrency": quota.max_concurrency,
                "enabled": 1 if quota.enabled else 0,
                "created_at": time.time(),
            },
        )
        return key, key_hash

    async def list_clients(self) -> list[ClientView]:
        records = await self._store.load_all_clients()
        window = self._window_index()
        views: list[ClientView] = []
        for record in records:
            key_hash = str(record.get("key_hash", ""))
            views.append(
                ClientView(
                    key_hash=key_hash,
                    name=str(record.get("name", "client")),
                    rpm_limit=int(record.get("rpm_limit", 0)),
                    tpm_limit=int(record.get("tpm_limit", 0)),
                    max_concurrency=int(record.get("max_concurrency", 1)),
                    enabled=str(record.get("enabled", "1")) == "1",
                    created_at=float(record.get("created_at", 0.0)),
                    requests_current_window=await self._store.read_counter(
                        QUOTA_RPM_KEY.format(key_hash=key_hash, window=window)
                    ),
                    tokens_current_window=await self._store.read_counter(
                        QUOTA_TPM_KEY.format(key_hash=key_hash, window=window)
                    ),
                    in_flight=await self._store.read_counter(
                        QUOTA_INFLIGHT_KEY.format(key_hash=key_hash)
                    ),
                )
            )
        return views

    async def delete_client(self, key_hash: str) -> bool:
        if await self._store.load_client(key_hash) is None:
            return False
        await self._store.delete_client(key_hash)
        return True

    # -- enforcement ------------------------------------------------------

    def _window_index(self) -> int:
        return int(time.time() // self._settings.quota_window_seconds)

    def _retry_after(self) -> int:
        window = self._settings.quota_window_seconds
        return max(1, window - int(time.time() % window))

    def estimate_tokens(self, text: str) -> int:
        ratio = max(1, self._settings.quota_token_estimate_ratio)
        return max(1, len(text) // ratio)

    async def check_and_acquire(
        self, identity: ClientIdentity, estimated_tokens: int = 0
    ) -> QuotaTicket:
        """Admit a request or raise QuotaExceeded.

        The returned ticket records what was reserved so the caller can
        reconcile the token estimate with the real usage on completion.
        """
        ticket = QuotaTicket(identity=identity)
        if not self._settings.quotas_enabled:
            return ticket

        window = self._window_index()
        ttl = self._settings.quota_window_seconds + 1

        if identity.max_concurrency > 0:
            inflight_key = QUOTA_INFLIGHT_KEY.format(key_hash=identity.key_hash)
            current = await self._store.incr_counter(inflight_key, 1, ttl=300)
            if current > identity.max_concurrency:
                await self._store.incr_counter(inflight_key, -1, ttl=300)
                raise QuotaExceeded(
                    "concurrency", identity.max_concurrency, self._retry_after()
                )
            ticket.concurrency_acquired = True

        try:
            if identity.rpm_limit > 0:
                rpm_key = QUOTA_RPM_KEY.format(
                    key_hash=identity.key_hash, window=window
                )
                current = await self._store.incr_counter(rpm_key, 1, ttl=ttl)
                if current > identity.rpm_limit:
                    await self._store.incr_counter(rpm_key, -1, ttl=ttl)
                    raise QuotaExceeded("rpm", identity.rpm_limit, self._retry_after())
                ticket.request_reserved = True

            if identity.tpm_limit > 0 and estimated_tokens > 0:
                tpm_key = QUOTA_TPM_KEY.format(
                    key_hash=identity.key_hash, window=window
                )
                current = await self._store.incr_counter(
                    tpm_key, estimated_tokens, ttl=ttl
                )
                if current > identity.tpm_limit:
                    await self._store.incr_counter(tpm_key, -estimated_tokens, ttl=ttl)
                    raise QuotaExceeded("tpm", identity.tpm_limit, self._retry_after())
                ticket.reserved_tokens = estimated_tokens
        except QuotaExceeded:
            if ticket.concurrency_acquired:
                await self._store.incr_counter(
                    QUOTA_INFLIGHT_KEY.format(key_hash=identity.key_hash), -1, ttl=300
                )
            raise

        return ticket

    async def release(self, ticket: QuotaTicket, actual_tokens: int = 0) -> None:
        """Release the concurrency slot and reconcile the token estimate."""
        if not self._settings.quotas_enabled:
            return

        if ticket.concurrency_acquired:
            await self._store.incr_counter(
                QUOTA_INFLIGHT_KEY.format(key_hash=ticket.identity.key_hash),
                -1,
                ttl=300,
            )

        if ticket.reserved_tokens and actual_tokens:
            window = self._window_index()
            tpm_key = QUOTA_TPM_KEY.format(
                key_hash=ticket.identity.key_hash, window=window
            )
            # Swap the reservation for the real figure.
            await self._store.incr_counter(
                tpm_key, actual_tokens - ticket.reserved_tokens, ttl=300
            )
