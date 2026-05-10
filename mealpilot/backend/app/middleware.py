import os
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


class CloudflareAccessMiddleware(BaseHTTPMiddleware):
    """Validate the presence of Cloudflare Access JWT header.

    Set MEALPILOT_REQUIRE_CF_ACCESS=1 to enforce. Off by default for local dev.
    Paths in MEALPILOT_CF_ACCESS_BYPASS (comma-separated prefixes) skip the check —
    typically /healthz and /docs.
    """

    HEADER = "cf-access-jwt-assertion"

    def __init__(self, app):
        super().__init__(app)
        self.enabled = os.environ.get("MEALPILOT_REQUIRE_CF_ACCESS", "0") == "1"
        bypass = os.environ.get("MEALPILOT_CF_ACCESS_BYPASS", "/healthz,/docs,/openapi.json,/redoc")
        self.bypass_prefixes = tuple(p.strip() for p in bypass.split(",") if p.strip())

    async def dispatch(self, request, call_next):
        if not self.enabled:
            return await call_next(request)
        path = request.url.path
        if path.startswith(self.bypass_prefixes):
            return await call_next(request)
        if not request.headers.get(self.HEADER):
            return JSONResponse(
                {"detail": "Missing Cloudflare Access assertion"},
                status_code=401,
            )
        return await call_next(request)
