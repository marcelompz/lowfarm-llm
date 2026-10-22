"""Secret hygiene: rotation windows, weak-key detection, audit."""

from __future__ import annotations

import httpx
import pytest

from app.config import Settings
from app.security import audit_keys


def _settings(**kwargs) -> Settings:
    base = {
        "gateway_api_key": "a" * 43,
        "admin_api_key": "b" * 43,
        "redis_url": "redis://localhost:6379/15",
    }
    base.update(kwargs)
    return Settings(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# Rotation windows
# --------------------------------------------------------------------------


def test_multiple_gateway_keys_are_parsed() -> None:
    settings = _settings(gateway_api_key="old-key-value-0000000000000,new-key-value-000000000000")
    assert settings.gateway_api_keys == [
        "old-key-value-0000000000000",
        "new-key-value-000000000000",
    ]


def test_multiple_admin_keys_are_parsed() -> None:
    settings = _settings(admin_api_key=",".join(["x" * 30, "y" * 30]))
    assert len(settings.admin_api_keys) == 2
    assert settings.admin_api_keys[0] == "x" * 30
    assert settings.admin_api_keys[1] == "y" * 30


@pytest.mark.asyncio
async def test_both_keys_work_during_a_rotation_window(
    store, upstream, settings: Settings
) -> None:
    """An old key stays valid until it is removed from the list."""
    from app.main import create_app

    rotating = settings.model_copy(
        update={"gateway_api_key": "old-key-0000000000000000000000000,new-key-1111111111111111111111111"}
    )
    app = create_app(settings=rotating, store=store, client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as api:
            for key in ("old-key-0000000000000000000000000", "new-key-1111111111111111111111111"):
                response = await api.get(
                    "/v1/models", headers={"Authorization": f"Bearer {key}"}
                )
                assert response.status_code == 200, key


@pytest.mark.asyncio
async def test_the_admin_plane_rotates_independently(
    store, upstream, settings: Settings
) -> None:
    from app.main import create_app

    rotating = settings.model_copy(
        update={"admin_api_key": "admin-old-0000000000000000000,admin-new-1111111111111111111"}
    )
    app = create_app(settings=rotating, store=store, client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gw"
        ) as api:
            for key in ("admin-old-0000000000000000000", "admin-new-1111111111111111111"):
                response = await api.get(
                    "/v1/admin/workers", headers={"Authorization": f"Bearer {key}"}
                )
                assert response.status_code == 200, key
            denied = await api.get(
                "/v1/admin/workers",
                headers={"Authorization": "Bearer admin-nope"},
            )
            assert denied.status_code == 401


@pytest.mark.asyncio
async def test_admin_key_rejects_a_non_rotated_key(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(
        "/v1/admin/workers", headers={"Authorization": "Bearer totally-wrong"}
    )
    assert response.status_code == 401


# --------------------------------------------------------------------------
# Weak key detection
# --------------------------------------------------------------------------


def test_audit_flags_short_keys() -> None:
    problems = audit_keys({"GATEWAY_API_KEY": ["short"]}, min_length=24)
    assert problems
    assert "GATEWAY_API_KEY" in problems[0]


def test_audit_flags_placeholder_markers() -> None:
    problems = audit_keys(
        {"GATEWAY_API_KEY": ["change-me-to-something-real-please"]}, min_length=24
    )
    assert problems
    assert "placeholder" in problems[0]


def test_audit_flags_admin_placeholder() -> None:
    problems = audit_keys(
        {"ADMIN_API_KEY": ["admin-password-not-a-real-secret"]}, min_length=24
    )
    assert problems


def test_audit_accepts_strong_keys() -> None:
    problems = audit_keys(
        {"GATEWAY_API_KEY": ["k" * 43], "ADMIN_API_KEY": ["z" * 43]}, min_length=24
    )
    assert problems == []


def test_audit_reports_each_key_in_a_rotation_list() -> None:
    problems = audit_keys(
        {"GATEWAY_API_KEY": ["a" * 30, "b" * 4]}, min_length=24
    )
    assert len(problems) == 1
    assert "GATEWAY_API_KEY[1]" in problems[0]


# --------------------------------------------------------------------------
# Startup behaviour
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gateway_refuses_to_start_with_a_weak_key(
    store, upstream, settings: Settings
) -> None:
    from app.main import create_app

    weak = settings.model_copy(
        update={"gateway_api_key": "change-me", "require_strong_keys": True}
    )
    app = create_app(settings=weak, store=store)
    with pytest.raises(RuntimeError, match="weak secret"):
        async with app.router.lifespan_context(app):
            pass


@pytest.mark.asyncio
async def test_gateway_starts_with_a_weak_key_when_not_enforced(
    store, upstream, settings: Settings
) -> None:
    from app.main import create_app

    weak = settings.model_copy(
        update={"gateway_api_key": "change-me", "require_strong_keys": False}
    )
    app = create_app(settings=weak, store=store)
    async with app.router.lifespan_context(app):
        assert app.state.quota_manager is not None
