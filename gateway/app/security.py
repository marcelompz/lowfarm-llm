"""Bearer-token authentication.

Two independent keys (§6.1):
  * ``GATEWAY_API_KEY`` protects the public OpenAI-compatible surface.
  * ``ADMIN_API_KEY``  protects the worker control plane.

Comparisons are constant time and happen over bytes so non-ASCII keys work.
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .config import Settings, get_settings

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


async def require_gateway_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> str:
    token = _extract(credentials)
    if not _matches(token, settings.gateway_api_key):
        raise _unauthorized("Invalid API key")
    return token


async def require_admin_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> str:
    token = _extract(credentials)
    if not _matches(token, settings.admin_api_key):
        raise _unauthorized("Invalid admin key")
    return token
