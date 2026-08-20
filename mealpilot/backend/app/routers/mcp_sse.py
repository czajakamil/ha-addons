"""MCP SSE transport — exposes the MealPilot MCP server over HTTP/SSE.

Claude Desktop config example:
    {
      "mcpServers": {
        "mealpilot": {
          "url": "http://<HA_IP>:8000/mcp/sse",
          "headers": { "X-MealPilot-Token": "mp_xxx" }
        }
      }
    }
"""

import hashlib
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import Response
from mcp.server.sse import SseServerTransport
from sqlalchemy.orm import Session
from starlette.types import Receive, Scope, Send

import mcp_server as _mcp

from .. import models
from ..db import SessionLocal

_LAST_USED_THROTTLE = timedelta(seconds=60)


class _AlreadySentResponse(Response):
    """No-op response used when the SSE transport already sent the full HTTP response."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        pass


router = APIRouter()
sse_transport = SseServerTransport("/mcp/messages")


def _resolve_user(raw_key: str) -> models.User:
    digest = hashlib.sha256(raw_key.encode()).hexdigest()
    db: Session = SessionLocal()
    try:
        api_key = db.query(models.ApiKey).filter(models.ApiKey.key_hash == digest).one_or_none()
        if not api_key:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
        user = db.get(models.User, api_key.user_id)
        if not user or not user.is_active:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User inactive or not found")
        now = datetime.now(UTC)
        last_used = api_key.last_used_at
        if last_used is not None and last_used.tzinfo is None:
            # SQLite zwraca naive datetime — traktuj jako UTC, żeby móc odjąć od aware `now`.
            last_used = last_used.replace(tzinfo=UTC)
        if last_used is None or (now - last_used) > _LAST_USED_THROTTLE:
            api_key.last_used_at = now
            db.commit()
        return user
    finally:
        db.close()


@router.post("/mcp/messages")
async def mcp_post_message(
    request: Request,
    x_mealpilot_token: str | None = Header(default=None, alias="X-MealPilot-Token"),
):
    if not x_mealpilot_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "X-MealPilot-Token header required")
    _resolve_user(x_mealpilot_token.strip())
    await sse_transport.handle_post_message(request.scope, request.receive, request._send)


@router.get("/mcp/sse")
async def mcp_sse(
    request: Request,
    x_mealpilot_token: str | None = Header(default=None, alias="X-MealPilot-Token"),
):
    if not x_mealpilot_token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "X-MealPilot-Token header required")
    _resolve_user(x_mealpilot_token.strip())

    token = _mcp.request_api_key.set(x_mealpilot_token.strip())
    try:
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await _mcp.server.run(
                streams[0], streams[1], _mcp.server.create_initialization_options()
            )
    finally:
        _mcp.request_api_key.reset(token)
    return _AlreadySentResponse()
