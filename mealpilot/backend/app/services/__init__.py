"""Domain services — one implementation per operation, shared by every surface.

REST routers, the in-app agent and the MCP server all call into this package.
Nothing here knows about HTTP, MCP or LLM message formats.
"""

from .errors import Conflict, Forbidden, Invalid, NotFound, ServiceError, Unavailable, Upstream
from .registry import (
    DESTRUCTIVE_TOOLS,
    GROUP_ORDER,
    SPECS_BY_NAME,
    TOOL_NAMES,
    TOOL_SPECS,
    WRITE_TOOLS,
    ToolSpec,
    describe,
    get_spec,
    invoke,
)

__all__ = [
    "DESTRUCTIVE_TOOLS",
    "GROUP_ORDER",
    "SPECS_BY_NAME",
    "TOOL_NAMES",
    "TOOL_SPECS",
    "WRITE_TOOLS",
    "Conflict",
    "Forbidden",
    "Invalid",
    "NotFound",
    "ServiceError",
    "ToolSpec",
    "Unavailable",
    "Upstream",
    "describe",
    "get_spec",
    "invoke",
]
