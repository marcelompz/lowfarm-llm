"""Loading keys from a mounted secret file (docs/SECRETS.md §7)."""

from __future__ import annotations

import pytest

from app.config import Settings


def _base(**kwargs) -> dict:
    base = {
        "redis_url": "redis://localhost:6379/15",
        "gateway_api_key": "a" * 43,
        "admin_api_key": "b" * 43,
    }
    base.update(kwargs)
    return base


def test_keys_can_come_from_a_file(tmp_path) -> None:
    key_file = tmp_path / "gateway_key"
    key_file.write_text("file-provided-gateway-key-value-12345\n")

    settings = Settings(**_base(gateway_api_key_file=str(key_file)))  # type: ignore[arg-type]
    assert settings.gateway_api_key == "file-provided-gateway-key-value-12345"
    # The plain variable is ignored once a file is configured.
    assert settings.gateway_api_keys == ["file-provided-gateway-key-value-12345"]


def test_missing_key_file_is_a_clear_error(tmp_path) -> None:
    with pytest.raises(ValueError, match="cannot read gateway_api_key_file"):
        Settings(**_base(gateway_api_key_file=str(tmp_path / "nope")))  # type: ignore[arg-type]


def test_empty_key_file_is_rejected(tmp_path) -> None:
    empty = tmp_path / "empty"
    empty.write_text("   \n")
    with pytest.raises(ValueError, match="is empty"):
        Settings(**_base(gateway_api_key_file=str(empty)))  # type: ignore[arg-type]


def test_a_missing_plain_key_is_rejected() -> None:
    # The environment provides a key (the conftest sets one), so it has to
    # be overridden explicitly to exercise the "not configured" path.
    with pytest.raises(ValueError, match="GATEWAY_API_KEY is required"):
        Settings(
            redis_url="redis://x", gateway_api_key="", admin_api_key="b" * 43
        )  # type: ignore[arg-type]


def test_a_missing_admin_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="ADMIN_API_KEY is required"):
        Settings(
            redis_url="redis://x", gateway_api_key="a" * 43, admin_api_key=""
        )  # type: ignore[arg-type]


def test_file_values_also_support_a_rotation_list(tmp_path) -> None:
    key_file = tmp_path / "rotating"
    key_file.write_text("first-key-000000000000000000000000,second-key-11111111111111111111")
    settings = Settings(**_base(gateway_api_key_file=str(key_file)))  # type: ignore[arg-type]
    assert len(settings.gateway_api_keys) == 2
