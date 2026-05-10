"""MealPilot MCP server (stdio transport).

Exposes tools that proxy to the local FastAPI backend. Authentication uses an
API key issued from the MealPilot UI / API (``POST /api/auth/api-keys``) and
passed via ``MEALPILOT_API_KEY``. A session cookie via
``MEALPILOT_SESSION_COOKIE`` is also accepted as a fallback.

Run with::

    MEALPILOT_API_KEY=mp_xxx python mcp_server.py
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

BASE_URL = os.environ.get("MEALPILOT_BASE_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.environ.get("MEALPILOT_API_KEY", "")
SESSION_COOKIE = os.environ.get("MEALPILOT_SESSION_COOKIE", "")
COOKIE_NAME = os.environ.get("MEALPILOT_COOKIE_NAME", "mealpilot_session")

server: Server = Server("mealpilot")


def _headers() -> Dict[str, str]:
    headers: Dict[str, str] = {"Accept": "application/json"}
    if API_KEY:
        headers["X-MealPilot-Token"] = API_KEY
    elif SESSION_COOKIE:
        headers["Cookie"] = f"{COOKIE_NAME}={SESSION_COOKIE}"
    return headers


def _result(payload: Any) -> List[TextContent]:
    if isinstance(payload, str):
        return [TextContent(type="text", text=payload)]
    return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]


def _error(message: str) -> List[TextContent]:
    return [TextContent(type="text", text=f"ERROR: {message}")]


async def _request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Any] = None,
) -> Any:
    url = f"{BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(
            method,
            url,
            headers=_headers(),
            params=params,
            json=json_body,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
    if resp.status_code == 204 or not resp.content:
        return None
    ctype = resp.headers.get("content-type", "")
    if "application/json" in ctype:
        return resp.json()
    return resp.text


def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _current_week_start() -> str:
    return _monday_of(date.today()).isoformat()


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: List[Tool] = [
    Tool(
        name="list_recipes",
        description="Zwraca wszystkie przepisy zalogowanego użytkownika.",
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="get_recipe",
        description="Szczegóły jednego przepisu (składniki, kroki, makro).",
        inputSchema={
            "type": "object",
            "properties": {"recipe_id": {"type": "string"}},
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="list_tags",
        description=(
            "Wszystkie unikalne tagi używane w bibliotece przepisów. Wywołaj jako pierwsze, "
            "by wiedzieć jakimi wartościami operuje użytkownik."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="list_meal_types",
        description="Wszystkie unikalne typy posiłków zdefiniowane w przepisach.",
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="filter_recipes",
        description=(
            "Zwraca przepisy spełniające podane kryteria. Użyj gdy użytkownik chce np. "
            "'coś azjatyckiego na obiad' lub 'lekkie śniadanie poniżej 400 kcal'."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tags": {"type": "array", "items": {"type": "string"}},
                "meal_types": {"type": "array", "items": {"type": "string"}},
                "max_kcal": {"type": "number"},
                "min_protein": {"type": "number"},
            },
            "additionalProperties": False,
        },
    ),
    Tool(
        name="create_recipe",
        description="Dodaje nowy przepis do biblioteki użytkownika.",
        inputSchema={
            "type": "object",
            "properties": {
                "id": {"type": "string"},
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
                "hue": {"type": "integer"},
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
            "required": ["id", "title"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="update_recipe",
        description="Aktualizuje wybrane pola istniejącego przepisu.",
        inputSchema={
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
                "hue": {"type": "integer"},
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
            "additionalProperties": False,
        },
    ),
    Tool(
        name="delete_recipe",
        description="Usuwa przepis i powiązane wpisy w planie tygodnia.",
        inputSchema={
            "type": "object",
            "properties": {"recipe_id": {"type": "string"}},
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_week_plan",
        description=(
            "Zwraca plan posiłków na dany tydzień wraz z tytułami przepisów."
            " week_start musi być poniedziałkiem w formacie YYYY-MM-DD."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_current_week_plan",
        description="Plan na bieżący tydzień (week_start liczony automatycznie).",
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="set_week_plan",
        description=(
            "Zastępuje cały plan tygodnia. UWAGA: zawsze pokaż użytkownikowi podgląd "
            "i poczekaj na potwierdzenie przed wywołaniem."
        ),
        inputSchema={
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
            "additionalProperties": False,
        },
    ),
    Tool(
        name="add_plan_entry",
        description="Dodaje jeden slot do planu, nie kasując reszty.",
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "day": {"type": "integer", "minimum": 0, "maximum": 6},
                "meal": {"type": "string"},
                "recipe_id": {"type": "string"},
                "servings": {"type": "integer"},
            },
            "required": ["week_start", "day", "meal", "recipe_id", "servings"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="remove_plan_entry",
        description="Usuwa slot (dzień + posiłek) z planu tygodnia.",
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "day": {"type": "integer", "minimum": 0, "maximum": 6},
                "meal": {"type": "string"},
            },
            "required": ["week_start", "day", "meal"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_week_nutrition_summary",
        description=(
            "Suma kcal/białka/tłuszczu/węglowodanów dla każdego dnia tygodnia "
            "na podstawie planu i przepisów."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_shopping_list",
        description="Aktualna lista zakupów na dany tydzień.",
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="generate_shopping_list",
        description=(
            "Generuje listę zakupów z planu tygodnia (agreguje składniki). "
            "Nadpisuje poprzednią listę."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="check_shopping_item",
        description="Oznacza pozycję jako kupioną lub odznacza.",
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string"},
                "item_id": {"type": "integer"},
                "checked": {"type": "boolean"},
            },
            "required": ["week_start", "item_id", "checked"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="clear_shopping_list",
        description="Usuwa wszystkie pozycje listy zakupów danego tygodnia.",
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string"}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="estimate_recipe_macros",
        description=(
            "Szacuje makroskładniki (kcal, białko, tłuszcz, węglowodany) dla przepisu "
            "na podstawie listy składników, używając skonfigurowanego modelu AI. "
            "Zwraca sumy dla CAŁEGO przepisu (wszystkich porcji łącznie). "
            "Użyj przed create_recipe gdy użytkownik nie podał makro."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Nazwa przepisu"},
                "servings": {"type": "integer", "description": "Liczba porcji"},
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
            },
            "required": ["title", "servings", "ingredients"],
            "additionalProperties": False,
        },
    ),
]


@server.list_tools()
async def list_tools() -> List[Tool]:
    return TOOLS


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


async def _enrich_plan_with_titles(week_plan: Dict[str, Any]) -> Dict[str, Any]:
    recipes = await _request("GET", "/api/recipes")
    titles = {r["id"]: r.get("title", "") for r in (recipes or [])}
    for e in week_plan.get("entries", []) or []:
        e["recipe_title"] = titles.get(e.get("recipe_id"), "")
    return week_plan


async def _get_plan_entries(week_start: str) -> List[Dict[str, Any]]:
    plan = await _request("GET", f"/api/plan/{week_start}")
    return list(plan.get("entries") or [])


async def _put_plan(week_start: str, entries: List[Dict[str, Any]]) -> Any:
    payload = [
        {
            "day": int(e["day"]),
            "meal": str(e["meal"]),
            "recipe_id": str(e["recipe_id"]),
            "servings": int(e.get("servings", 1)),
        }
        for e in entries
    ]
    return await _request("PUT", f"/api/plan/{week_start}", json_body=payload)


async def _dispatch(name: str, args: Dict[str, Any]) -> Any:
    if name == "list_recipes":
        return await _request("GET", "/api/recipes")

    if name == "get_recipe":
        return await _request("GET", f"/api/recipes/{args['recipe_id']}")

    if name == "list_tags":
        return await _request("GET", "/api/recipes/meta/tags")

    if name == "list_meal_types":
        return await _request("GET", "/api/recipes/meta/meal_types")

    if name == "filter_recipes":
        params: Dict[str, Any] = {}
        if args.get("tags"):
            params["tags"] = ",".join(args["tags"])
        if args.get("meal_types"):
            params["meal_types"] = ",".join(args["meal_types"])
        if args.get("max_kcal") is not None:
            params["max_kcal"] = args["max_kcal"]
        if args.get("min_protein") is not None:
            params["min_protein"] = args["min_protein"]
        return await _request("GET", "/api/recipes", params=params)

    if name == "create_recipe":
        return await _request("POST", "/api/recipes", json_body=args)

    if name == "update_recipe":
        rid = args.pop("recipe_id")
        return await _request("PUT", f"/api/recipes/{rid}", json_body=args)

    if name == "delete_recipe":
        await _request("DELETE", f"/api/recipes/{args['recipe_id']}")
        return {"ok": True}

    if name == "get_week_plan":
        plan = await _request("GET", f"/api/plan/{args['week_start']}")
        return await _enrich_plan_with_titles(plan)

    if name == "get_current_week_plan":
        ws = _current_week_start()
        plan = await _request("GET", f"/api/plan/{ws}")
        return await _enrich_plan_with_titles(plan)

    if name == "set_week_plan":
        return await _put_plan(args["week_start"], args["entries"])

    if name == "add_plan_entry":
        ws = args["week_start"]
        entries = await _get_plan_entries(ws)
        entries = [
            e for e in entries
            if not (int(e["day"]) == int(args["day"]) and e["meal"] == args["meal"])
        ]
        entries.append({
            "day": int(args["day"]),
            "meal": args["meal"],
            "recipe_id": args["recipe_id"],
            "servings": int(args["servings"]),
        })
        return await _put_plan(ws, entries)

    if name == "remove_plan_entry":
        ws = args["week_start"]
        entries = await _get_plan_entries(ws)
        entries = [
            e for e in entries
            if not (int(e["day"]) == int(args["day"]) and e["meal"] == args["meal"])
        ]
        return await _put_plan(ws, entries)

    if name == "get_week_nutrition_summary":
        ws = args["week_start"]
        plan = await _request("GET", f"/api/plan/{ws}")
        recipes = await _request("GET", "/api/recipes") or []
        rmap = {r["id"]: r for r in recipes}
        out: Dict[int, Dict[str, float]] = {
            d: {"kcal": 0.0, "p": 0.0, "f": 0.0, "c": 0.0} for d in range(7)
        }
        for e in plan.get("entries") or []:
            r = rmap.get(e.get("recipe_id"))
            if not r or not r.get("servings"):
                continue
            scale = (e.get("servings") or 0) / float(r["servings"])
            d = int(e["day"])
            out[d]["kcal"] += float(r.get("kcal") or 0) * scale
            out[d]["p"] += float(r.get("p") or 0) * scale
            out[d]["f"] += float(r.get("f") or 0) * scale
            out[d]["c"] += float(r.get("c") or 0) * scale
        return {str(d): {k: round(v, 2) for k, v in vals.items()} for d, vals in out.items()}

    if name == "get_shopping_list":
        return await _request("GET", f"/api/shopping/{args['week_start']}")

    if name == "generate_shopping_list":
        return await _request("POST", f"/api/shopping/{args['week_start']}/generate")

    if name == "check_shopping_item":
        return await _request(
            "PATCH",
            f"/api/shopping/{args['week_start']}/items/{args['item_id']}",
            json_body={"checked": bool(args["checked"])},
        )

    if name == "clear_shopping_list":
        await _request("DELETE", f"/api/shopping/{args['week_start']}")
        return {"ok": True}

    if name == "estimate_recipe_macros":
        return await _request(
            "POST",
            "/api/recipes/estimate-macros",
            json_body={
                "title": args["title"],
                "servings": int(args["servings"]),
                "ingredients": args["ingredients"],
            },
        )

    raise RuntimeError(f"Unknown tool: {name}")


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any] | None) -> List[TextContent]:
    args = dict(arguments or {})
    try:
        result = await _dispatch(name, args)
    except RuntimeError as e:
        return _error(str(e))
    except httpx.HTTPError as e:
        return _error(f"HTTP error: {e}")
    except Exception as e:  # noqa: BLE001
        return _error(f"{type(e).__name__}: {e}")
    return _result(result)


async def main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
