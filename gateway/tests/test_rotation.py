"""Rotation must actually revoke the old key.

The default client record lives in Redis, so it survives a restart.  If the
bootstrap key changes and the old record is left behind, the old key keeps
working — a silent rotation failure.
"""

from __future__ import annotations

import httpx
import pytest

from app.config import Settings
from app.models import ClientQuota
from app.quotas import QuotaManager, hash_key
from app.store import MemoryStore

OLD_KEY = "old-bootstrap-key-0000000000000000000"
NEW_KEY = "new-bootstrap-key-1111111111111111111"


@pytest.mark.asyncio
async def test_rotating_the_bootstrap_key_revokes_the_old_one(
    store: MemoryStore, settings: Settings
) -> None:
    with_old = settings.model_copy(update={"gateway_api_key": OLD_KEY})
    manager_old = QuotaManager(store, with_old)
    await manager_old.ensure_default_client()
    assert await manager_old.resolve(OLD_KEY) is not None

    # The operator rotates the key.
    with_new = settings.model_copy(update={"gateway_api_key": NEW_KEY})
    manager_new = QuotaManager(store, with_new)
    await manager_new.ensure_default_client()

    assert await manager_new.resolve(NEW_KEY) is not None
    assert await manager_new.resolve(OLD_KEY) is None, (
        "the previous bootstrap key must stop working after a rotation"
    )


@pytest.mark.asyncio
async def test_rotation_keeps_the_other_clients_untouched(
    store: MemoryStore, settings: Settings
) -> None:
    manager = QuotaManager(store, settings)
    await manager.ensure_default_client()

    # A client created through the admin API.
    extra_key, extra_hash = await manager.create_client(ClientQuota(name="team-a"))

    rotated = QuotaManager(
        store, settings.model_copy(update={"gateway_api_key": NEW_KEY})
    )
    await rotated.ensure_default_client()

    assert await rotated.resolve(extra_key) is not None
    assert await store.load_client(extra_hash) is not None


@pytest.mark.asyncio
async def test_only_one_client_may_be_named_default(
    store: MemoryStore, settings: Settings
) -> None:
    """A leftover "default" record must not keep a revoked key alive."""
    manager = QuotaManager(store, settings)
    await manager.ensure_default_client()

    # Simulate a record left behind by an earlier rotation.
    stale_key = "stale-bootstrap-key-000000000000000"
    await store.save_client(
        hash_key(stale_key),
        {
            "name": "default",
            "rpm_limit": 60,
            "tpm_limit": 1000,
            "max_concurrency": 1,
            "enabled": 1,
            "created_at": 0.0,
        },
    )
    assert await manager.resolve(stale_key) is not None

    rotated = QuotaManager(
        store, settings.model_copy(update={"gateway_api_key": NEW_KEY})
    )
    await rotated.ensure_default_client()

    assert await rotated.resolve(stale_key) is None
    clients = await rotated.list_clients()
    defaults = [c for c in clients if c.name == "default"]
    assert len(defaults) == 1


@pytest.mark.asyncio
async def test_the_default_client_is_visible_in_the_admin_api(
    store: MemoryStore, settings: Settings
) -> None:
    manager = QuotaManager(store, settings)
    await manager.ensure_default_client()
    clients = await manager.list_clients()
    assert any(c.name == "default" for c in clients)


@pytest.mark.asyncio
async def test_rotating_revokes_the_old_key_over_http(
    store, upstream, settings: Settings
) -> None:
    """End to end: the old key is rejected after the gateway is restarted."""
    from app.main import create_app

    old_app = create_app(
        settings=settings.model_copy(update={"gateway_api_key": OLD_KEY}),
        store=store,
        client=upstream,
    )
    async with old_app.router.lifespan_context(old_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=old_app), base_url="http://gw"
        ) as api:
            response = await api.get(
                "/v1/models", headers={"Authorization": f"Bearer {OLD_KEY}"}
            )
            assert response.status_code == 200

    new_app = create_app(
        settings=settings.model_copy(update={"gateway_api_key": NEW_KEY}),
        store=store,
        client=upstream,
    )
    async with new_app.router.lifespan_context(new_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=new_app), base_url="http://gw"
        ) as api:
            stale = await api.get(
                "/v1/models", headers={"Authorization": f"Bearer {OLD_KEY}"}
            )
            assert stale.status_code == 401
            fresh = await api.get(
                "/v1/models", headers={"Authorization": f"Bearer {NEW_KEY}"}
            )
            assert fresh.status_code == 200
