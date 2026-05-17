from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .. import models


@dataclass
class _ToolReg:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Callable
    changes: List[str] = field(default_factory=list)


_REGISTRY: List[_ToolReg] = []
_BY_NAME: Dict[str, _ToolReg] = {}


def tool(
    name: str,
    description: str,
    input_schema: Dict[str, Any],
    changes: Optional[List[str]] = None,
) -> Callable:
    def decorator(fn: Callable) -> Callable:
        reg = _ToolReg(
            name=name,
            description=description,
            input_schema=input_schema,
            handler=fn,
            changes=changes or [],
        )
        _REGISTRY.append(reg)
        _BY_NAME[name] = reg
        return fn
    return decorator


def _safe_json_parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def _json_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)


def _is_anthropic(endpoint: str) -> bool:
    return "anthropic.com" in endpoint or "/v1/messages" in endpoint


def _call_tool(
    db: Session,
    user: models.User,
    name: str,
    input_args: Dict[str, Any],
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> Tuple[str, bool]:
    reg = _BY_NAME.get(name)
    is_error = False
    if not reg:
        result_text = f"Unknown tool: {name}"
        is_error = True
    else:
        try:
            out = reg.handler(db, user, input_args)
            result_text = json.dumps(out, ensure_ascii=False)
            for domain in reg.changes:
                changed_set.add(domain)
        except Exception as exc:
            result_text = str(exc)
            is_error = True

    tool_events.append({
        "tool_use_id": str(uuid.uuid4()),
        "name": name,
        "input": input_args,
        "output": None if is_error else _safe_json_parse(result_text),
        "error": result_text if is_error else None,
    })
    return result_text, is_error
