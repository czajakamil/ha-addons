"""Tool dispatch table — maps tool names to handlers and tracks side effects."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from sqlalchemy.orm import Session

from ... import models
from . import handlers as h


TOOL_HANDLERS = {
    "list_recipes": h.tool_list_recipes,
    "get_recipe": h.tool_get_recipe,
    "list_tags": h.tool_list_tags,
    "list_meal_types": h.tool_list_meal_types,
    "filter_recipes": h.tool_filter_recipes,
    "create_recipe": h.tool_create_recipe,
    "update_recipe": h.tool_update_recipe,
    "delete_recipe": h.tool_delete_recipe,
    "get_week_plan": h.tool_get_week_plan,
    "get_current_week_plan": h.tool_get_current_week_plan,
    "set_week_plan": h.tool_set_week_plan,
    "add_plan_entry": h.tool_add_plan_entry,
    "remove_plan_entry": h.tool_remove_plan_entry,
    "get_week_nutrition_summary": h.tool_get_week_nutrition_summary,
    "get_shopping_list": h.tool_get_shopping_list,
    "generate_shopping_list": h.tool_generate_shopping_list,
    "check_shopping_item": h.tool_check_shopping_item,
    "add_shopping_item": h.tool_add_shopping_item,
    "remove_shopping_item": h.tool_remove_shopping_item,
    "clear_shopping_list": h.tool_clear_shopping_list,
}

# Tools that mutate data and which "changed" categories they affect
TOOL_CHANGED: Dict[str, List[str]] = {
    "create_recipe": ["recipes"],
    "update_recipe": ["recipes"],
    "delete_recipe": ["recipes", "plan", "shopping"],
    "set_week_plan": ["plan"],
    "add_plan_entry": ["plan"],
    "remove_plan_entry": ["plan"],
    "generate_shopping_list": ["shopping"],
    "check_shopping_item": ["shopping"],
    "add_shopping_item": ["shopping"],
    "remove_shopping_item": ["shopping"],
    "clear_shopping_list": ["shopping"],
}


def call_tool(
    db: Session,
    user: models.User,
    name: str,
    input_args: Dict[str, Any],
    changed_set: set,
) -> Tuple[str, bool]:
    """Execute a single tool call. Returns (result_text, is_error)."""
    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return f"Unknown tool: {name}", True
    try:
        out = handler(db, user, input_args)
        for domain in TOOL_CHANGED.get(name, []):
            changed_set.add(domain)
        return json.dumps(out, ensure_ascii=False), False
    except Exception as exc:
        return str(exc), True
