"""Tool schemas for the LLM, generated from the shared registry.

Do not hand-edit: add a ``ToolSpec`` in ``app/services/registry.py`` instead.
"""

from __future__ import annotations

from ...services.registry import TOOL_SPECS

TOOL_DEFS = [
    {
        "name": spec.name,
        "description": spec.description,
        "input_schema": spec.input_schema,
    }
    for spec in TOOL_SPECS
]

TOOL_DEFS_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    }
    for t in TOOL_DEFS
]
