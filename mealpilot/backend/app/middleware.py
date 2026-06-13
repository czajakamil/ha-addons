"""Cloudflare Access middleware z weryfikacją podpisu JWT.

Aby działało w produkcji ustaw:
- MEALPILOT_REQUIRE_CF_ACCESS=1
- MEALPILOT_CF_ACCESS_TEAM_DOMAIN=<your-team>.cloudflareaccess.com
- MEALPILOT_CF_ACCESS_AUD=<application audience tag z dashboardu Cloudflare>

MEALPILOT_CF_ACCESS_BYPASS (comma-separated path prefixes) pomija check —
domyślnie /healthz i /docs.

Bez tych zmiennych middleware przepuszcza wszystko (local dev).
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Optional

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

log = logging.getLogger(__name__)


class _JWKSCache:
    """Cache PyJWKClient per team_domain (lazy init, thread-safe).

    Import jwt/cryptography jest leniwy — gdy CF Access wyłączony, nie ładujemy
    natywnego `cryptography`, który na niektórych CPU/arch krzyczy SIGILL.
    """

    def __init__(self) -> None:
        self._clients: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def get(self, team_domain: str):
        with self._lock:
            client = self._clients.get(team_domain)
            if client is None:
                from jwt import PyJWKClient
                url = f"https://{team_domain}/cdn-cgi/access/certs"
                client = PyJWKClient(url, cache_keys=True, lifespan=600)
                self._clients[team_domain] = client
            return client


_jwks_cache = _JWKSCache()


class CloudflareAccessMiddleware:
    """Pure-ASGI middleware — nie używa BaseHTTPMiddleware, żeby nie psuć SSE/streaming."""

    HEADER = "cf-access-jwt-assertion"

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.enabled = os.environ.get("MEALPILOT_REQUIRE_CF_ACCESS", "0") == "1"
        self.team_domain = os.environ.get("MEALPILOT_CF_ACCESS_TEAM_DOMAIN", "").strip()
        self.audience = os.environ.get("MEALPILOT_CF_ACCESS_AUD", "").strip()
        bypass = os.environ.get(
            "MEALPILOT_CF_ACCESS_BYPASS", "/healthz,/docs,/openapi.json,/redoc"
        )
        self.bypass_prefixes = tuple(p.strip() for p in bypass.split(",") if p.strip())

        if self.enabled and (not self.team_domain or not self.audience):
            raise RuntimeError(
                "MEALPILOT_REQUIRE_CF_ACCESS=1 wymaga "
                "MEALPILOT_CF_ACCESS_TEAM_DOMAIN oraz MEALPILOT_CF_ACCESS_AUD."
            )

    def _is_bypassed(self, path: str) -> bool:
        for prefix in self.bypass_prefixes:
            if path == prefix or path.startswith(prefix + "/"):
                return True
        return False

    def _verify(self, token: str) -> Optional[Dict[str, Any]]:
        import jwt
        from jwt import PyJWKClientError
        try:
            client = _jwks_cache.get(self.team_domain)
            signing_key = client.get_signing_key_from_jwt(token)
            return jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256", "ES256"],
                audience=self.audience,
                issuer=f"https://{self.team_domain}",
                options={"require": ["exp", "iat", "iss", "aud"]},
            )
        except (jwt.InvalidTokenError, PyJWKClientError) as e:
            log.warning("CF Access JWT verification failed: %s", e)
            return None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.enabled:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if self._is_bypassed(path):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        token = headers.get(self.HEADER.encode(), b"").decode().strip()
        if not token:
            response = JSONResponse(
                {"detail": "Missing Cloudflare Access assertion"},
                status_code=401,
            )
            await response(scope, receive, send)
            return

        claims = self._verify(token)
        if claims is None:
            response = JSONResponse(
                {"detail": "Invalid Cloudflare Access assertion"},
                status_code=401,
            )
            await response(scope, receive, send)
            return

        scope["state"] = scope.get("state", {})
        scope["state"]["cf_access"] = claims
        await self.app(scope, receive, send)
