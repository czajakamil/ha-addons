"""Adapter between the in-app agent loop and the shared tool registry.

The dispatch table lives in ``app/services/registry.py``; this module only
translates its results into the (text, is_error) pair the providers expect.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from ... import models
from ...services import registry as tools
from ...services.errors import ServiceError

TOOL_SPECS = tools.TOOL_SPECS
SPECS_BY_NAME = tools.SPECS_BY_NAME
TOOL_NAMES = tools.TOOL_NAMES

# Kept for backwards compatibility with older imports.
TOOL_HANDLERS: dict[str, Any] = {s.name: s.handler for s in tools.TOOL_SPECS}
TOOL_CHANGED: dict[str, list[str]] = {s.name: list(s.changed) for s in tools.TOOL_SPECS if s.changed}


async def call_tool(
    db: Session,
    user: models.User,
    name: str,
    input_args: dict[str, Any],
    changed_set: set,
) -> tuple[str, bool]:
    """Execute a single tool call. Returns (result_text, is_error)."""
    try:
        out = await tools.invoke(db, user, name, input_args)
    except ServiceError as exc:
        db.rollback()
        return json.dumps({"error": exc.code, "message": str(exc)}, ensure_ascii=False), True
    except Exception as exc:
        db.rollback()
        return json.dumps({"error": "internal_error", "message": str(exc)}, ensure_ascii=False), True

    spec = tools.get_spec(name)
    for domain in spec.changed:
        changed_set.add(domain)
    return json.dumps(out, ensure_ascii=False), False
