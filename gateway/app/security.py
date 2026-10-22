"""Bearer-token authentication.

Two planes with two independent keys (§6.1):

  * the **public** plane (``/v1/*``) accepts the bootstrap key plus any
    per-client key registered through the admin API.  Each client carries
    its own quota, so the identity is resolved, not just validated;
  * the **admin** plane (``/v1/admin/*``) uses a single stronger key.

Comparisons are constant time and happen over bytes so non-ASCII keys work.
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import Settings, get_settings
from .quotas import ClientIdentity, QuotaManager

bearer_scheme = HTTPBearer(auto_error=False)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _extract(credentials: HTTPAuthorizationCredentials | None) -> str:
    if credentials is None:
        raise _unauthorized("Missing bearer token")
    if credentials.scheme.lower() != "bearer":
        raise _unauthorized("Authorization scheme must be 'Bearer'")
    if not credentials.credentials:
        raise _unauthorized("Empty bearer token")
    return credentials.credentials


def _matches(provided: str, expected: str) -> bool:
    return secrets.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def _matches_any(provided: str, expected_list: list[str]) -> bool:
    """Constant-time match against a list of valid keys (rotation window)."""
    ok = False
    for candidate in expected_list:
        # Do not short-circuit: every candidate must be compared.
        ok |= _matches(provided, candidate)
    return ok


# Substrings that indicate a placeholder was never replaced.
_PLACEHOLDER_MARKERS = (
    "change-me",
    "changeme",
    "example",
    "your-",
    "placeholder",
    "admin",
    "password",
    "secret123",
    "test-",
)


def audit_keys(keys: dict[str, list[str]], min_length: int) -> list[str]:
    """Report weak or placeholder keys.

    Used at startup so an operator finds out *before* the farm is exposed,
    not after a scanner does.
    """
    problems: list[str] = []
    for name, values in keys.items():
        for index, value in enumerate(values):
            label = name if len(values) == 1 else f"{name}[{index}]"
            if len(value) < min_length:
                problems.append(
                    f"{label} is only {len(value)} characters "
                    f"(minimum {min_length})"
                )
                continue
            lowered = value.lower()
            for marker in _PLACEHOLDER_MARKERS:
                if marker in lowered:
                    problems.append(f"{label} contains the placeholder marker {marker!r}")
                    break
    return problems


async def require_client(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> ClientIdentity:
    """Resolve the caller to a client identity with its own quota."""
    token = _extract(credentials)
    manager: QuotaManager = request.app.state.quota_manager
    identity = await manager.resolve(token)
    if identity is None:
        raise _unauthorized("Invalid API key")
    return identity


async def require_admin_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> str:
    token = _extract(credentials)
    if not _matches_any(token, settings.admin_api_keys):
        raise _unauthorized("Invalid admin key")
    return token
