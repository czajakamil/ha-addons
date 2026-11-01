"""Helpers for persisting lightweight user preferences on disk."""

from __future__ import annotations

import json
from typing import Any

from .config import PROJECT_ROOT

PREFERENCES_PATH = PROJECT_ROOT / "data" / "preferences.json"
TABLE_COLUMN_WIDTHS_KEY = "tableColumnWidths"


def _load_preferences() -> dict[str, Any]:
    if not PREFERENCES_PATH.exists():
        return {}
    try:
        return json.loads(PREFERENCES_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        # Corrupted file – start fresh to keep the UI responsive.
        return {}


def _save_preferences(preferences: dict[str, Any]) -> None:
    PREFERENCES_PATH.parent.mkdir(parents=True, exist_ok=True)
    PREFERENCES_PATH.write_text(json.dumps(preferences, indent=2, sort_keys=True), encoding="utf-8")


def get_table_column_widths(table_id: str, user_id: str) -> dict[str, int]:
    preferences = _load_preferences()
    table_preferences = preferences.get(TABLE_COLUMN_WIDTHS_KEY, {})
    widths = table_preferences.get(user_id, {}).get(table_id, {})
    if not widths and table_id in table_preferences:
        widths = table_preferences.get(table_id, {})
    sanitized: dict[str, int] = {}
    for column_id, width in widths.items():
        try:
            width_value = int(width)
        except (TypeError, ValueError):
            continue
        if width_value > 0:
            sanitized[column_id] = width_value
    return sanitized


def set_table_column_widths(table_id: str, widths: dict[str, int], user_id: str) -> dict[str, int]:
    sanitized: dict[str, int] = {}
    for column_id, width in widths.items():
        if not isinstance(width, (int, float)):
            continue
        width_value = round(width)
        if width_value <= 0:
            continue
        sanitized[column_id] = width_value

    preferences = _load_preferences()
    table_preferences = preferences.setdefault(TABLE_COLUMN_WIDTHS_KEY, {})
    legacy = table_preferences.pop(table_id, None)
    user_preferences = table_preferences.setdefault(user_id, {})
    if legacy and table_id not in user_preferences:
        user_preferences[table_id] = legacy
    user_preferences[table_id] = sanitized
    _save_preferences(preferences)
    return sanitized
