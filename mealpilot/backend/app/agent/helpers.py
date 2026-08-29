"""JSON plumbing for the agent providers.

Domain helpers (slugify, week handling, serializers, ownership guards) moved to
``app/services/common.py`` — they are shared with REST and MCP and no longer
belong to the agent layer.
"""

from __future__ import annotations

import json
from typing import Any


def safe_json_parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def json_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)
