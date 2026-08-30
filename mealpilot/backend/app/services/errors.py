"""Domain-level errors shared by REST, the in-app agent and the MCP server.

Each transport maps these to its own failure shape:
  * FastAPI  -> HTTPException(status_code)
  * agent    -> tool result with is_error=True
  * MCP      -> CallToolResult(isError=True)
"""

from __future__ import annotations


class ServiceError(Exception):
    """Base class for expected, user-facing failures."""

    status_code = 400
    code = "invalid_request"


class Invalid(ServiceError):
    status_code = 400
    code = "invalid_request"


class NotFound(ServiceError):
    status_code = 404
    code = "not_found"


class Forbidden(ServiceError):
    status_code = 403
    code = "forbidden"


class Conflict(ServiceError):
    status_code = 409
    code = "conflict"


class Unavailable(ServiceError):
    """A dependency the operator must configure is missing (LLM, quota)."""

    status_code = 424
    code = "unavailable"


class Upstream(ServiceError):
    """An upstream service answered, but not in a usable way."""

    status_code = 502
    code = "upstream_error"
