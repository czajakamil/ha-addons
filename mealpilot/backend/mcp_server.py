"""MealPilot MCP server — stdio entry point.

The server itself lives in ``app/mcpserver/server.py`` and executes tools
against the database in-process; this file only wires up stdio transport and
resolves the API key from the environment into a principal.

Run with::

    MEALPILOT_API_KEY=mp_xxx MEALPILOT_DB=/data/mealpilot.db python mcp_server.py

Because tools run in-process, this must be executed on the host that holds the
MealPilot database. Inside the add-on, prefer the HTTP/SSE transport
(``GET /mcp/sse``), which is served by the running FastAPI app.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys

from mcp.server.stdio import stdio_server

from app import models
from app.db import SessionLocal
from app.mcpserver.server import TOOLS, Principal, current_principal, server

__all__ = ["TOOLS", "current_principal", "server"]


def _principal_from_env() -> Principal:
    raw = os.environ.get("MEALPILOT_API_KEY", "").strip()
    if not raw:
        raise SystemExit("MEALPILOT_API_KEY is required (create one in MealPilot → Ustawienia → Klucze API).")
    digest = hashlib.sha256(raw.encode()).hexdigest()
    db = SessionLocal()
    try:
        api_key = db.query(models.ApiKey).filter(models.ApiKey.key_hash == digest).one_or_none()
        if not api_key:
            raise SystemExit("MEALPILOT_API_KEY is not a valid MealPilot API key.")
        user = db.get(models.User, api_key.user_id)
        if not user or not user.is_active:
            raise SystemExit("The user behind MEALPILOT_API_KEY is inactive or missing.")
        return Principal(user_id=user.id, scope=(api_key.scope or "write"))
    finally:
        db.close()


async def main() -> None:
    current_principal.set(_principal_from_env())
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    asyncio.run(main())
