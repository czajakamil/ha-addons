"""MealPilot MCP server.

The tool list, schemas and behaviour all come from ``app/services/registry.py``,
so this module is a *transport adapter* and nothing more. It no longer proxies
over HTTP to the very FastAPI app it runs inside: tools execute against the
database directly, which removes a loopback round-trip, a second
re-authentication per call and — most importantly — a second permission model.

Failures are raised, not stringified: the low-level ``Server.call_tool``
decorator converts an exception into ``CallToolResult(isError=True)``, so a
client can tell a failed call from a successful one.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from mcp.server import Server
from mcp.types import TextContent, Tool, ToolAnnotations

from .. import models
from ..db import SessionLocal
from ..services import registry as tools
from ..services.errors import Forbidden, ServiceError

logger = logging.getLogger(__name__)

SERVER_NAME = "mealpilot"


@dataclass(frozen=True)
class Principal:
    """Who a tool call runs as, and what that credential is allowed to do."""

    user_id: int
    scope: str = "write"


current_principal: ContextVar[Principal | None] = ContextVar("current_principal", default=None)

server: Server = Server(SERVER_NAME)


def _annotations(spec: tools.ToolSpec) -> ToolAnnotations:
    return ToolAnnotations(
        title=spec.title,
        readOnlyHint=spec.read_only,
        destructiveHint=spec.destructive,
        idempotentHint=spec.idempotent,
        openWorldHint=False,
    )


def _build_tools() -> list[Tool]:
    return [
        Tool(
            name=spec.name,
            title=spec.title,
            description=spec.description,
            inputSchema=spec.input_schema,
            outputSchema=spec.output_schema,
            annotations=_annotations(spec),
        )
        for spec in tools.TOOL_SPECS
    ]


TOOLS: list[Tool] = _build_tools()


@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
    """Run one tool as the connection's principal.

    Returns a dict so the SDK fills both ``structuredContent`` and a JSON text
    block; raising propagates as ``isError=True``.
    """
    principal = current_principal.get()
    if principal is None:
        raise RuntimeError("Brak kontekstu użytkownika dla wywołania MCP (nieuwierzytelniona sesja).")

    spec = tools.get_spec(name)
    if principal.scope == "read" and not spec.read_only:
        raise Forbidden(
            f"Klucz API ma zakres tylko-do-odczytu — narzędzie {spec.name} zapisuje dane. "
            "Utwórz klucz o zakresie 'write' w Ustawieniach → Klucze API."
        )

    db = SessionLocal()
    try:
        user = db.get(models.User, principal.user_id)
        if user is None or not user.is_active:
            raise RuntimeError("Użytkownik nie istnieje lub jest nieaktywny.")
        result = await tools.invoke(db, user, name, arguments or {})
    except ServiceError as exc:
        db.rollback()
        # Surfaces as isError=True with a message the model can act on.
        raise RuntimeError(f"[{exc.code}] {exc}") from exc
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    if not isinstance(result, dict):
        return {"result": result}
    return result


def text_result(payload: str) -> list[TextContent]:
    return [TextContent(type="text", text=payload)]
