"""HTTP security headers + Cloudflare Access middleware with JWT signature verification.

To enable in production set:
- MEALPILOT_REQUIRE_CF_ACCESS=1
- MEALPILOT_CF_ACCESS_TEAM_DOMAIN=<your-team>.cloudflareaccess.com
- MEALPILOT_CF_ACCESS_AUD=<application audience tag from the Cloudflare dashboard>

MEALPILOT_CF_ACCESS_BYPASS (comma-separated path prefixes) skips the check —
defaults to /healthz only. /docs and the OpenAPI schema are hidden behind MEALPILOT_DEBUG=1.

Without these variables the middleware passes all requests through (local dev).
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

log = logging.getLogger(__name__)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Appends HTTP security headers to every response.

    HSTS is only enabled when MEALPILOT_COOKIE_SECURE=1 (production HTTPS).
    In dev mode (no MEALPILOT_STATIC_DIR) script-src includes 'unsafe-inline'
    because Vite HMR requires it. Removed in production — the Vite build uses
    only external JS files (type=module).
    """

    _HSTS = "max-age=63072000; includeSubDomains"

    def __init__(self, app, https_only: bool = False):
        super().__init__(app)
        self._https_only = https_only
        # dev = Vite dev server; prod = pre-built static files served by FastAPI
        self._is_dev = not os.environ.get("MEALPILOT_STATIC_DIR")

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        script_src = "'self' 'unsafe-inline'" if self._is_dev else "'self'"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            f"script-src {script_src}; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data: blob:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'"
        )
        if self._https_only:
            response.headers["Strict-Transport-Security"] = self._HSTS
        return response



class _JWKSCache:
    """Cache PyJWKClient per team_domain (lazy init, thread-safe).

    The jwt/cryptography import is deferred — when CF Access is disabled we avoid
    loading the native `cryptography` extension, which raises SIGILL on some CPU/arch combos.
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


class CloudflareAccessMiddleware(BaseHTTPMiddleware):
    HEADER = "cf-access-jwt-assertion"

    def __init__(self, app):
        super().__init__(app)
        self.enabled = os.environ.get("MEALPILOT_REQUIRE_CF_ACCESS", "0") == "1"
        self.team_domain = os.environ.get("MEALPILOT_CF_ACCESS_TEAM_DOMAIN", "").strip()
        self.audience = os.environ.get("MEALPILOT_CF_ACCESS_AUD", "").strip()
        bypass = os.environ.get(
            "MEALPILOT_CF_ACCESS_BYPASS", "/healthz"
        )
        self.bypass_prefixes = tuple(p.strip() for p in bypass.split(",") if p.strip())

        if self.enabled and (not self.team_domain or not self.audience):
            # Fail-fast: crash at startup rather than silently skip verification.
            raise RuntimeError(
                "MEALPILOT_REQUIRE_CF_ACCESS=1 requires "
                "MEALPILOT_CF_ACCESS_TEAM_DOMAIN and MEALPILOT_CF_ACCESS_AUD."
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

    async def dispatch(self, request, call_next):
        if not self.enabled:
            return await call_next(request)
        path = request.url.path
        if self._is_bypassed(path):
            return await call_next(request)

        token = request.headers.get(self.HEADER, "").strip()
        if not token:
            return JSONResponse(
                {"detail": "Missing Cloudflare Access assertion"},
                status_code=401,
            )

        claims = self._verify(token)
        if claims is None:
            return JSONResponse(
                {"detail": "Invalid Cloudflare Access assertion"},
                status_code=401,
            )

        # Expose claims for downstream handlers (e.g. audit log).
        request.state.cf_access = claims
        return await call_next(request)
