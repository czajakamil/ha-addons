"""MCP over Streamable HTTP — the transport that supersedes SSE in the spec.

SSE (``/mcp/sse`` + ``/mcp/messages``) was deprecated in MCP revision 2025-03-26
in favour of a single endpoint. This router adds that endpoint at ``/mcp``
*alongside* the SSE one, so existing Claude Desktop configs keep working and can
move over when convenient.

The manager runs **stateless**: a fresh transport per request, no session table.
That suits MealPilot's auth model exactly — every request carries the API key,
so there is no long-lived session whose ownership could be spoofed. The
"does this token own that session id?" problem disappears rather than being
policed, and no private attribute of the transport has to be read to do it.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from fastapi import APIRouter, Header, HTTPException, Request, status
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from ..mcpserver.server import current_principal, server
from ..ratelimit import mcp_message_limiter
from .mcp_sse import _AlreadySentResponse, _client_key, _require_token, _resolve_principal, _throttle

router = APIRouter()

# A manager instance is single-use: its run() context may be entered exactly once.
# Build a fresh one per app lifetime so the app can be started more than once in
# a process (which is exactly what the test suite does).
_session_manager: StreamableHTTPSessionManager | None = None


@contextlib.asynccontextmanager
async def session_manager_lifespan() -> AsyncIterator[None]:
    """Must wrap the app's lifetime: the manager runs requests in its own task group."""
    global _session_manager
    manager = StreamableHTTPSessionManager(app=server, stateless=True, json_response=False)
    _session_manager = manager
    try:
        async with manager.run():
            yield
    finally:
        _session_manager = None


@router.api_route("/mcp", methods=["GET", "POST", "DELETE"])
async def mcp_streamable_http(
    request: Request,
    x_mealpilot_token: str | None = Header(default=None, alias="X-MealPilot-Token"),
):
    client = _client_key(request)
    _throttle(mcp_message_limiter, client, "wiadomości MCP")
    principal = _resolve_principal(_require_token(x_mealpilot_token), client)

    manager = _session_manager
    if manager is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Transport MCP nie jest uruchomiony (aplikacja startuje lub jest zatrzymywana).",
        )

    token = current_principal.set(principal)
    try:
        await manager.handle_request(request.scope, request.receive, request._send)
    finally:
        current_principal.reset(token)
    return _AlreadySentResponse()
