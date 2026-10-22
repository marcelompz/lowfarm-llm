"""Runtime configuration.

Every knob is an environment variable so the container can be tuned without a
rebuild.  Names map 1:1 to the `docker-compose.yml` environment block.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- Core -----------------------------------------------------------
    redis_url: str = "redis://redis:6379/0"
    # Comma-separated: several keys stay valid at once, which is what makes
    # a rotation window possible without dropping traffic.
    gateway_api_key: str = ""
    admin_api_key: str = ""
    # Docker-style secret mounts: when set, the value is read from the file
    # and the plain variable is ignored.
    gateway_api_key_file: str | None = None
    admin_api_key_file: str | None = None

    # ---- Secret hygiene -------------------------------------------------
    require_strong_keys: bool = False
    min_key_length: int = Field(default=24, ge=8)

    # ---- Bootstrap ------------------------------------------------------
    # Comma-separated entries; fields within an entry separated by "|".
    # Accepted form per entry: "url", "url|model" or "url|model|alias".
    # Example: http://192.168.1.101:8080|qwen2.5-0.5b|a30-01,http://192.168.1.102:8080
    worker_seeds: str = ""

    # ---- Scheduling -----------------------------------------------------
    scheduling_policy: Literal["least_outstanding", "round_robin"] = "least_outstanding"
    default_slots_per_worker: int = Field(default=4, ge=1, le=64)
    session_ttl: int = Field(default=3600, ge=0)
    stream_queue_timeout: float = Field(default=120.0, ge=0)

    # ---- Health ---------------------------------------------------------
    health_check_interval: float = Field(default=10.0, ge=0.05)
    health_probe_timeout: float = Field(default=3.0, gt=0)
    health_failure_threshold: int = Field(default=3, ge=1)
    health_recovery_threshold: int = Field(default=2, ge=1)
    half_open_probe_interval: float = Field(default=30.0, ge=0.05)
    worker_stale_ttl: int = Field(default=45, ge=1)

    # ---- Streaming ------------------------------------------------------
    stream_heartbeat_interval: float = Field(default=15.0, ge=0.1)
    upstream_request_timeout: float = Field(default=300.0, gt=0)
    max_retries: int = Field(default=2, ge=0, le=10)

    # ---- Thermal --------------------------------------------------------
    thermal_soft_limit_c: float = Field(default=45.0, ge=0)
    thermal_hard_limit_c: float = Field(default=52.0, ge=0)

    # ---- Chat history (Redis-backed session store) ----------------------
    chat_history_enabled: bool = True
    chat_history_max_messages: int = Field(default=40, ge=2)
    chat_history_ttl: int = Field(default=86400, ge=60)

    # ---- Client quotas --------------------------------------------------
    quotas_enabled: bool = True
    default_rpm_limit: int = Field(default=60, ge=0)
    default_tpm_limit: int = Field(default=100_000, ge=0)
    default_max_concurrency: int = Field(default=8, ge=1)
    quota_window_seconds: int = Field(default=60, ge=1)
    # Rough characters-per-token ratio used to estimate prompt size before
    # the real usage is known.
    quota_token_estimate_ratio: int = Field(default=4, ge=1)

    # ---- Derived --------------------------------------------------------

    @model_validator(mode="after")
    def _load_key_files(self) -> "Settings":
        """Read keys from a mounted secret file when one is configured.

        Production deployments use Docker secrets (see docs/SECRETS.md §7);
        the value then never appears in `docker inspect` output.
        """
        for field_name, file_field in (
            ("gateway_api_key", "gateway_api_key_file"),
            ("admin_api_key", "admin_api_key_file"),
        ):
            path = getattr(self, file_field)
            if not path:
                continue
            try:
                content = Path(path).read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise ValueError(
                    f"cannot read {file_field}={path!r}: {exc}"
                ) from exc
            if not content:
                raise ValueError(f"{file_field}={path!r} is empty")
            object.__setattr__(self, field_name, content)

        if not self.gateway_api_key:
            raise ValueError(
                "GATEWAY_API_KEY is required (or point GATEWAY_API_KEY_FILE at a file)"
            )
        if not self.admin_api_key:
            raise ValueError(
                "ADMIN_API_KEY is required (or point ADMIN_API_KEY_FILE at a file)"
            )
        return self

    @property
    def gateway_api_keys(self) -> list[str]:
        """Every key accepted on the public plane."""
        return [k.strip() for k in self.gateway_api_key.split(",") if k.strip()]

    @property
    def admin_api_keys(self) -> list[str]:
        """Every key accepted on the admin plane."""
        return [k.strip() for k in self.admin_api_key.split(",") if k.strip()]

    @property
    def seed_entries(self) -> list[dict[str, str]]:
        """Parse ``worker_seeds`` into registration payloads.

        Accepted form per entry: ``url`` or ``url,model`` or ``url,model,alias``.
        """
        out: list[dict[str, str]] = []
        for raw in self.worker_seeds.split(","):
            chunk = raw.strip()
            if not chunk:
                continue
            parts = [p.strip() for p in chunk.split("|")]
            url = parts[0]
            model = parts[1] if len(parts) > 1 and parts[1] else "unknown"
            alias = parts[2] if len(parts) > 2 and parts[2] else url
            out.append(
                {
                    "listen": url if url.startswith("http") else f"http://{url}",
                    "model": model,
                    "alias": alias,
                }
            )
        return out


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
