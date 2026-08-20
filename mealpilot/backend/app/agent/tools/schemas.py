"""Tool schema definitions for the LLM (Anthropic + OpenAI flavors)."""

from __future__ import annotations

TOOL_DEFS = [
    {
        "name": "list_recipes",
        "description": ("Zwraca skróty wszystkich przepisów (bez składników i kroków). Po szczegóły użyj get_recipe."),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_recipe",
        "description": "Szczegóły jednego przepisu (składniki, kroki, makro).",
        "input_schema": {
            "type": "object",
            "properties": {"recipe_id": {"type": "string"}},
            "required": ["recipe_id"],
        },
    },
    {
        "name": "list_tags",
        "description": "Wszystkie unikalne tagi używane w bibliotece przepisów.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_meal_types",
        "description": "Wszystkie unikalne typy posiłków zdefiniowane w przepisach.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "filter_recipes",
        "description": (
            "Zwraca skróty przepisów spełniających kryteria (bez składników i kroków). Po szczegóły użyj get_recipe."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "meal_types": {"type": "array", "items": {"type": "string"}},
                "max_kcal": {"type": "number"},
                "min_protein": {"type": "number"},
            },
        },
    },
    {
        "name": "create_recipe",
        "description": "Dodaje nowy przepis do biblioteki.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "meal_types": {"type": "array", "items": {"type": "string"}},
                "servings": {"type": "integer"},
                "prep_time": {"type": "integer"},
                "cook_time": {"type": "integer"},
                "kcal": {"type": "number"},
                "p": {"type": "number"},
                "f": {"type": "number"},
                "c": {"type": "number"},
                "hue": {"type": "integer", "minimum": 0, "maximum": 360},
                "ingredients": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit": {"type": "string"},
                        },
                        "required": ["name", "qty", "unit"],
                    },
                },
                "steps": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["title"],
        },
    },
    {
        "name": "update_recipe",
        "description": "Aktualizuje wybrane pola istniejącego przepisu.",
        "input_schema": {
            "type": "object",
            "properties": {
                "recipe_id": {"type": "string"},
                "title": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "meal_types": {"type": "array", "items": {"type": "string"}},
                "servings": {"type": "integer"},
                "prep_time": {"type": "integer"},
                "cook_time": {"type": "integer"},
                "kcal": {"type": "number"},
                "p": {"type": "number"},
                "f": {"type": "number"},
                "c": {"type": "number"},
                "hue": {"type": "integer", "minimum": 0, "maximum": 360},
                "ingredients": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit": {"type": "string"},
                        },
                        "required": ["name", "qty", "unit"],
                    },
                },
                "steps": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["recipe_id"],
        },
    },
    {
        "name": "delete_recipe",
        "description": "Usuwa przepis i powiązane wpisy w planie tygodnia.",
        "input_schema": {
            "type": "object",
            "properties": {"recipe_id": {"type": "string"}},
            "required": ["recipe_id"],
        },
    },
    {
        "name": "get_week_plan",
        "description": "Plan posiłków na dany tydzień. week_start = poniedziałek (YYYY-MM-DD).",
        "input_schema": {
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
        },
    },
    {
        "name": "get_current_week_plan",
        "description": "Plan na bieżący tydzień (week_start liczony automatycznie).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "set_week_plan",
        "description": "Zastępuje cały plan tygodnia.",
        "input_schema": {
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "entries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "day": {"type": "integer", "minimum": 0, "maximum": 6},
                            "meal": {"type": "string"},
                            "recipe_id": {"type": "string"},
                            "servings": {"type": "integer"},
                        },
                        "required": ["day", "meal", "recipe_id", "servings"],
                    },
                },
            },
            "required": ["week_start", "entries"],
        },
    },
    {
        "name": "add_plan_entry",
        "description": "Dodaje jeden slot do planu.",
        "input_schema": {
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "day": {"type": "integer", "minimum": 0, "maximum": 6},
                "meal": {"type": "string"},
                "recipe_id": {"type": "string"},
                "servings": {"type": "integer"},
            },
            "required": ["week_start", "day", "meal", "recipe_id", "servings"],
        },
    },
    {
        "name": "remove_plan_entry",
        "description": "Usuwa slot (dzień + posiłek) z planu.",
        "input_schema": {
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "day": {"type": "integer", "minimum": 0, "maximum": 6},
                "meal": {"type": "string"},
            },
            "required": ["week_start", "day", "meal"],
        },
    },
    {
        "name": "get_week_nutrition_summary",
        "description": "Suma kcal/białka/tłuszczu/węglowodanów dla każdego dnia tygodnia.",
        "input_schema": {
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
        },
    },
    {
        "name": "get_shopping_list",
        "description": "Aktualna lista zakupów na dany tydzień.",
        "input_schema": {
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
        },
    },
    {
        "name": "generate_shopping_list",
        "description": "Regeneruje listę zakupów z planu tygodnia.",
        "input_schema": {
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
        },
    },
    {
        "name": "check_shopping_item",
        "description": "Oznacza pozycję jako kupioną lub odznacza.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item_id": {"type": "integer"},
                "checked": {"type": "boolean"},
            },
            "required": ["item_id", "checked"],
        },
    },
    {
        "name": "add_shopping_item",
        "description": "Dodaje własną pozycję do listy zakupów.",
        "input_schema": {
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "name": {"type": "string"},
                "qty": {"type": "number"},
                "unit": {"type": "string"},
                "category": {"type": "string"},
            },
            "required": ["week_start", "name"],
        },
    },
    {
        "name": "remove_shopping_item",
        "description": "Usuwa pojedynczą pozycję z listy zakupów.",
        "input_schema": {
            "type": "object",
            "properties": {
                "item_id": {"type": "integer"},
            },
            "required": ["item_id"],
        },
    },
    {
        "name": "clear_shopping_list",
        "description": "Usuwa wszystkie pozycje listy zakupów danego tygodnia.",
        "input_schema": {
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
        },
    },
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
