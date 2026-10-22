"""Per-client quotas."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.models import ClientQuota
from app.quotas import QuotaExceeded, QuotaManager, hash_key
from app.store import MemoryStore


@pytest.fixture
def manager(store: MemoryStore, settings: Settings) -> QuotaManager:
    return QuotaManager(store, settings)


async def _client(manager: QuotaManager, **kwargs) -> tuple[str, str]:
    quota = ClientQuota(name=kwargs.pop("name", "test"), **kwargs)
    return await manager.create_client(quota)


@pytest.mark.asyncio
async def test_bootstrap_key_resolves_to_the_default_client(
    manager: QuotaManager, settings: Settings
) -> None:
    identity = await manager.resolve(settings.gateway_api_key)
    assert identity is not None
    assert identity.is_default is True
    assert identity.rpm_limit == settings.default_rpm_limit


@pytest.mark.asyncio
async def test_unknown_key_is_rejected(manager: QuotaManager) -> None:
    assert await manager.resolve("totally-unknown-key") is None


@pytest.mark.asyncio
async def test_created_client_resolves(manager: QuotaManager) -> None:
    api_key, key_hash = await _client(manager, rpm_limit=10, tpm_limit=1000)
    identity = await manager.resolve(api_key)
    assert identity is not None
    assert identity.key_hash == key_hash
    assert identity.rpm_limit == 10


@pytest.mark.asyncio
async def test_disabled_client_is_rejected(manager: QuotaManager) -> None:
    api_key, _ = await _client(manager, enabled=False)
    assert await manager.resolve(api_key) is None


@pytest.mark.asyncio
async def test_rpm_limit_is_enforced(manager: QuotaManager) -> None:
    api_key, _ = await _client(manager, rpm_limit=3)
    identity = await manager.resolve(api_key)
    assert identity is not None

    for _ in range(3):
        await manager.check_and_acquire(identity)

    with pytest.raises(QuotaExceeded) as exc:
        await manager.check_and_acquire(identity)
    assert exc.value.dimension == "rpm"
    assert exc.value.limit == 3
    assert exc.value.retry_after > 0


@pytest.mark.asyncio
async def test_tpm_limit_is_enforced(manager: QuotaManager) -> None:
    api_key, _ = await _client(manager, tpm_limit=100)
    identity = await manager.resolve(api_key)
    assert identity is not None

    await manager.check_and_acquire(identity, 60)
    with pytest.raises(QuotaExceeded) as exc:
        await manager.check_and_acquire(identity, 60)
    assert exc.value.dimension == "tpm"


@pytest.mark.asyncio
async def test_concurrency_limit_is_enforced(manager: QuotaManager) -> None:
    api_key, _ = await _client(manager, max_concurrency=2)
    identity = await manager.resolve(api_key)
    assert identity is not None

    await manager.check_and_acquire(identity)
    await manager.check_and_acquire(identity)
    with pytest.raises(QuotaExceeded) as exc:
        await manager.check_and_acquire(identity)
    assert exc.value.dimension == "concurrency"


@pytest.mark.asyncio
async def test_concurrency_is_released(manager: QuotaManager) -> None:
    api_key, _ = await _client(manager, max_concurrency=1)
    identity = await manager.resolve(api_key)
    assert identity is not None

    ticket = await manager.check_and_acquire(identity)
    await manager.release(ticket)
    # Now it fits again.
    await manager.check_and_acquire(identity)


@pytest.mark.asyncio
async def test_zero_limit_means_unlimited(manager: QuotaManager) -> None:
    api_key, _ = await _client(
        manager, rpm_limit=0, tpm_limit=0, max_concurrency=0
    )
    identity = await manager.resolve(api_key)
    assert identity is not None
    for _ in range(50):
        await manager.check_and_acquire(identity, 1000)


@pytest.mark.asyncio
async def test_token_reconciliation_replaces_the_estimate(
    manager: QuotaManager,
) -> None:
    api_key, _ = await _client(manager, tpm_limit=1000)
    identity = await manager.resolve(api_key)
    assert identity is not None

    ticket = await manager.check_and_acquire(identity, 100)
    await manager.release(ticket, actual_tokens=250)

    from app.store import QUOTA_TPM_KEY

    window = manager._window_index()
    counter = await manager._store.read_counter(
        QUOTA_TPM_KEY.format(key_hash=identity.key_hash, window=window)
    )
    assert counter == 250


@pytest.mark.asyncio
async def test_list_clients_reports_live_counters(manager: QuotaManager) -> None:
    api_key, key_hash = await _client(manager, rpm_limit=10, max_concurrency=5)
    identity = await manager.resolve(api_key)
    assert identity is not None
    ticket = await manager.check_and_acquire(identity)

    clients = await manager.list_clients()
    entry = next(c for c in clients if c.key_hash == key_hash)
    assert entry.requests_current_window == 1
    assert entry.in_flight == 1
    await manager.release(ticket)


@pytest.mark.asyncio
async def test_delete_client(manager: QuotaManager) -> None:
    api_key, key_hash = await _client(manager)
    assert await manager.delete_client(key_hash) is True
    assert await manager.resolve(api_key) is None
    assert await manager.delete_client(key_hash) is False


@pytest.mark.asyncio
async def test_hash_key_is_stable_and_irreversible() -> None:
    h = hash_key("secret")
    assert h == hash_key("secret")
    assert h != hash_key("other")
    assert "secret" not in h
