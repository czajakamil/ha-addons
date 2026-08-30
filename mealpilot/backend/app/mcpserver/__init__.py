"""MealPilot's MCP server, built from the shared tool registry."""

from .server import TOOLS, current_principal, server

__all__ = ["TOOLS", "current_principal", "server"]
