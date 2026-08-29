"""MCP over HTTP/SSE.

Claude Desktop config example:
    {
      "mcpServers": {
        "mealpilot": {
          "url": "http://<HA_IP>:8000/mcp/sse",
          "headers": { "X-MealPilot-Token": "mp_xxx" }
        }
      }
    }

Two things this layer is responsible for beyond plumbing:

1. **Binding a POST to its SSE session.** ``/mcp/messages`` is routed by the
   ``session_id`` in the query string. Checking only that the bearer token is
   *some* valid key would let one user's key drive another user's live session,
   so every POST is checked against the key that opened that session.
2. **Rate limiting.** ``/mcp/*`` takes unauthenticated traffic and does a
   database lookup per request; both the handshake and the message channel are
   throttled.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import Response
from mcp.server.sse import SseServerTransport
from sqlalchemy.orm import Session
from starlette.types import Receive, Scope, Send

from .. import models
from ..db import SessionLocal
from ..mcpserver.server import Principal, current_principal, server
from ..ratelimit import mcp_auth_limiter, mcp_message_limiter, mcp_session_limiter

_LAST_USED_THROTTLE = timedelta(seconds=60)


class _AlreadySentResponse(Response):
    """No-op response used when the SSE transport already sent the full HTTP response."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        pass


router = APIRouter()
sse_transport = SseServerTransport("/mcp/messages")

# session_id (hex) -> user id that opened it. Session ids are server-generated
# UUID4s; this map is what makes them *bearer-bound* rather than merely unguessable.
_session_owner: dict[str, int] = {}


def _client_key(request: Request) -> str:
    cf = request.headers.get("cf-connecting-ip")
    if cf:
        return cf.strip()
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _throttle(limiter, key: str, what: str) -> None:
    allowed, retry_after = limiter.check(key)
    if not allowed:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"Za dużo żądań ({what}). Spróbuj ponownie za {int(retry_after)} s.",
            headers={"Retry-After": str(int(retry_after))},
        )


def _resolve_principal(raw_key: str, client: str) -> Principal:
    """Map an API key to its owner and scope, or raise 401."""
    digest = hashlib.sha256(raw_key.encode()).hexdigest()
    db: Session = SessionLocal()
    try:
        api_key = db.query(models.ApiKey).filter(models.ApiKey.key_hash == digest).one_or_none()
        if not api_key:
            _throttle(mcp_auth_limiter, client, "nieudane uwierzytelnienie")
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
        user = db.get(models.User, api_key.user_id)
        if not user or not user.is_active:
            _throttle(mcp_auth_limiter, client, "nieudane uwierzytelnienie")
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User inactive or not found")
        now = datetime.now(UTC)
        last_used = api_key.last_used_at
        if last_used is not None and last_used.tzinfo is None:
            # SQLite zwraca naive datetime — traktuj jako UTC, żeby móc odjąć od aware `now`.
            last_used = last_used.replace(tzinfo=UTC)
        if last_used is None or (now - last_used) > _LAST_USED_THROTTLE:
            api_key.last_used_at = now
            db.commit()
        return Principal(user_id=user.id, scope=(api_key.scope or "write"))
    finally:
        db.close()


def _resolve_user(raw_key: str) -> models.User:
    """Back-compat helper: resolve an API key to its (detached) User row."""
    principal = _resolve_principal(raw_key, "internal")
    db: Session = SessionLocal()
    try:
        return db.get(models.User, principal.user_id)
    finally:
        db.close()


def _require_token(token: str | None) -> str:
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "X-MealPilot-Token header required")
    return token.strip()


@router.post("/mcp/messages")
async def mcp_post_message(
    request: Request,
    x_mealpilot_token: str | None = Header(default=None, alias="X-MealPilot-Token"),
):
    client = _client_key(request)
    _throttle(mcp_message_limiter, client, "wiadomości MCP")
    principal = _resolve_principal(_require_token(x_mealpilot_token), client)

    session_id = request.query_params.get("session_id") or ""
    owner = _session_owner.get(session_id)
    if owner is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Nieznana lub zakończona sesja MCP")
    if owner != principal.user_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Token nie należy do właściciela tej sesji MCP")

    await sse_transport.handle_post_message(request.scope, request.receive, request._send)


def _claim_session_id(known_before: set[UUID]) -> UUID | None:
    """The session id `connect_sse` just minted (it does not return one)."""
    new = set(sse_transport._read_stream_writers) - known_before
    return next(iter(new), None) if len(new) == 1 else None


@router.get("/mcp/sse")
async def mcp_sse(
    request: Request,
    x_mealpilot_token: str | None = Header(default=None, alias="X-MealPilot-Token"),
):
    client = _client_key(request)
    _throttle(mcp_session_limiter, client, "połączeń MCP")
    principal = _resolve_principal(_require_token(x_mealpilot_token), client)

    known_before = set(sse_transport._read_stream_writers)
    ctx_token = current_principal.set(principal)
    session_id: UUID | None = None
    try:
        async with sse_transport.connect_sse(request.scope, request.receive, request._send) as streams:
            session_id = _claim_session_id(known_before)
            if session_id is None:
                # We could not bind this session to its owner, so we cannot police
                # /mcp/messages for it. Fail closed rather than serve it unbound.
                raise HTTPException(
                    status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "Nie udało się powiązać sesji MCP z użytkownikiem (niezgodna wersja biblioteki mcp).",
                )
            _session_owner[session_id.hex] = principal.user_id
            await server.run(streams[0], streams[1], server.create_initialization_options())
    finally:
        current_principal.reset(ctx_token)
        if session_id is not None:
            _session_owner.pop(session_id.hex, None)
    return _AlreadySentResponse()
