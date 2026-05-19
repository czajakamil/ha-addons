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


_CANONICAL_MEALS = ['Śniadanie', 'II Śniadanie', 'Obiad', 'Przekąska', 'Kolacja']


def _normalize_meal(meal: str) -> str:
    stripped = meal.strip()
    lower = stripped.lower()
    for cm in _CANONICAL_MEALS:
        if cm.lower() == lower:
            return cm
    return stripped[:1].upper() + stripped[1:] if stripped else stripped


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS: List[Tool] = [
    Tool(
        name="list_recipes",
        description=(
            "Zwraca pełną listę przepisów widocznych dla użytkownika (własne + dzielone w household). "
            "Bez filtrów. Odpowiedź: tablica obiektów Recipe — każdy zawiera pola: "
            "id (string), title, tags (string[]), meal_types (string[]), servings (int), "
            "prep_time i cook_time (minuty, int), kcal/p/f/c (sumy dla CAŁEGO przepisu — wszystkich porcji), "
            "hue (0–360, kolor karty w UI), ingredients ([{name, qty, unit}]), steps (string[]), "
            "image_filename (string|null), created_by (user id), owner_user_id (int|null — gdy ustawiony, prywatny), "
            "owner_household_id (int|null — gdy ustawiony, dzielony w household). "
            "Może zwrócić pustą tablicę. Dla wyszukiwania użyj filter_recipes."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="get_recipe",
        description=(
            "Zwraca pełny obiekt Recipe (taka sama struktura jak w list_recipes) dla podanego ID. "
            "Błąd 404, jeśli przepis nie istnieje lub użytkownik nie ma do niego dostępu."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "recipe_id": {
                    "type": "string",
                    "description": "ID przepisu (pole `id` z list_recipes/filter_recipes).",
                }
            },
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="list_tags",
        description=(
            "Zwraca {\"tags\": string[]} — posortowaną listę unikalnych tagów używanych "
            "w widocznych dla użytkownika przepisach (np. 'azjatyckie', 'wegetariańskie'). "
            "Wywołaj jako pierwsze przed filter_recipes, aby wiedzieć jakim słownictwem "
            "operuje użytkownik (filtrowanie po tagach jest case-sensitive i wymaga DOKŁADNEGO dopasowania)."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="list_meal_types",
        description=(
            "Zwraca {\"meal_types\": string[]} — posortowaną listę unikalnych typów posiłków "
            "(np. 'śniadanie', 'obiad', 'kolacja') zdefiniowanych w widocznych przepisach. "
            "Filtrowanie wymaga DOKŁADNEGO dopasowania wartości — najpierw pobierz słownik."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="filter_recipes",
        description=(
            "Zwraca tablicę Recipe (ta sama struktura co list_recipes) spełniających kryteria. "
            "Semantyka: `tags` — AND (wszystkie wymagane), `meal_types` — OR (przynajmniej jeden), "
            "`max_kcal` — pełna suma przepisu (nie na porcję!) ≤ wartość, "
            "`min_protein` — pełne białko przepisu (g) ≥ wartość. "
            "Bez argumentów zachowuje się jak list_recipes. Może zwrócić pustą tablicę. "
            "Wartości tagów/meal_types są case-sensitive — najpierw użyj list_tags / list_meal_types."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Wszystkie tagi muszą wystąpić (AND). Dokładne dopasowanie.",
                },
                "meal_types": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Przynajmniej jeden typ musi pasować (OR). Dokładne dopasowanie.",
                },
                "max_kcal": {
                    "type": "number",
                    "description": "Górny limit kcal sumarycznie dla całego przepisu (nie na porcję).",
                },
                "min_protein": {
                    "type": "number",
                    "description": "Dolny limit białka w gramach sumarycznie dla całego przepisu.",
                },
            },
            "additionalProperties": False,
        },
    ),
    Tool(
        name="create_recipe",
        description=(
            "Tworzy nowy przepis. Zwraca pełny obiekt Recipe (z polami created_by, owner_*, "
            "image_filename=null). Błąd 409, jeśli `id` jest już zajęte. "
            "Pola kcal/p/f/c traktuj jako sumy dla CAŁEGO przepisu (wszystkich porcji łącznie) — "
            "jeśli użytkownik nie podał makro, wywołaj najpierw estimate_recipe_macros."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Unikalne ID (slug). Musi być nowe — błąd 409 gdy zajęte.",
                },
                "title": {"type": "string", "description": "Nazwa przepisu wyświetlana w UI."},
                "tags": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Tagi tematyczne (np. 'azjatyckie'). Trzymaj się słownictwa z list_tags.",
                },
                "meal_types": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Typy posiłków (np. 'obiad'). Wartości spójne z list_meal_types.",
                },
                "servings": {"type": "integer", "description": "Liczba porcji (domyślnie 1)."},
                "prep_time": {"type": "integer", "description": "Czas przygotowania w minutach."},
                "cook_time": {"type": "integer", "description": "Czas gotowania w minutach."},
                "kcal": {"type": "number", "description": "Kalorie sumarycznie dla całego przepisu (wszystkich porcji)."},
                "p": {"type": "number", "description": "Białko (g) sumarycznie dla całego przepisu."},
                "f": {"type": "number", "description": "Tłuszcz (g) sumarycznie dla całego przepisu."},
                "c": {"type": "number", "description": "Węglowodany (g) sumarycznie dla całego przepisu."},
                "hue": {"type": "integer", "description": "Odcień karty w UI (0–360, domyślnie 40)."},
                "ingredients": {
                    "type": "array",
                    "description": "Składniki dla podanej liczby porcji (servings).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number", "description": "Ilość w jednostce `unit`."},
                            "unit": {"type": "string", "description": "Jednostka, np. 'g', 'ml', 'szt', 'łyżka'."},
                        },
                        "required": ["name", "qty", "unit"],
                    },
                },
                "steps": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Kolejne kroki przygotowania, każdy jako oddzielny string.",
                },
            },
            "required": ["id", "title"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="update_recipe",
        description=(
            "Aktualizuje wybrane pola istniejącego przepisu (PATCH-semantyka — pomijaj pola, których nie zmieniasz). "
            "Zwraca pełny zaktualizowany obiekt Recipe. Tablice (ingredients, steps, tags, meal_types) "
            "są NADPISYWANE w całości — przekaż pełną nową wersję, nie różnicę. "
            "Błąd 403 gdy użytkownik nie jest właścicielem przepisu, 404 gdy nie istnieje."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "recipe_id": {"type": "string", "description": "ID przepisu do edycji."},
                "title": {"type": "string"},
                "tags": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Nadpisuje całą listę tagów.",
                },
                "meal_types": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Nadpisuje całą listę typów posiłków.",
                },
                "servings": {"type": "integer"},
                "prep_time": {"type": "integer", "description": "Minuty."},
                "cook_time": {"type": "integer", "description": "Minuty."},
                "kcal": {"type": "number", "description": "Suma dla całego przepisu (wszystkich porcji)."},
                "p": {"type": "number", "description": "Białko (g) sumarycznie dla całego przepisu."},
                "f": {"type": "number", "description": "Tłuszcz (g) sumarycznie dla całego przepisu."},
                "c": {"type": "number", "description": "Węglowodany (g) sumarycznie dla całego przepisu."},
                "hue": {"type": "integer", "description": "0–360."},
                "ingredients": {
                    "type": "array",
                    "description": "Nadpisuje całą listę składników.",
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
                "steps": {
                    "type": "array", "items": {"type": "string"},
                    "description": "Nadpisuje całą listę kroków.",
                },
            },
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="delete_recipe",
        description=(
            "Usuwa przepis ORAZ wszystkie powiązane wpisy w planach tygodni (bez pytania). "
            "Operacja nieodwracalna — zawsze potwierdź z użytkownikiem przed wywołaniem. "
            "Zwraca {\"ok\": true}. Błąd 403 gdy nie jesteś właścicielem, 404 gdy nie istnieje."
        ),
        inputSchema={
            "type": "object",
            "properties": {"recipe_id": {"type": "string", "description": "ID przepisu do usunięcia."}},
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_week_plan",
        description=(
            "Zwraca plan posiłków na podany tydzień. Format odpowiedzi: "
            "{\"week_start\": \"YYYY-MM-DD\", \"entries\": [{day: 0-6 (0=poniedziałek), "
            "meal: string (np. 'śniadanie','obiad','kolacja'), recipe_id: string, "
            "servings: int (liczba porcji do zjedzenia w tym slocie), recipe_title: string}]}. "
            "Pole `recipe_title` jest doklejone przez MCP (nie ma go w surowym API). "
            "entries może być pustą tablicą gdy nic nie zaplanowano."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {
                    "type": "string",
                    "description": "Data poniedziałku w formacie YYYY-MM-DD. Inne dni dadzą pusty plan.",
                }
            },
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_current_week_plan",
        description=(
            "Jak get_week_plan, ale week_start jest liczony automatycznie "
            "(poniedziałek bieżącego tygodnia w strefie serwera). "
            "Użyj, gdy użytkownik mówi 'ten tydzień' / 'teraz'."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="set_week_plan",
        description=(
            "DESTRUKTYWNE: usuwa wszystkie istniejące wpisy planu w tym tygodniu i zastępuje je `entries`. "
            "Pusta lista entries = wyczyszczenie planu. Wszystkie recipe_id muszą być widoczne dla "
            "użytkownika — inaczej błąd 400 z listą brakujących. "
            "Zwraca WeekPlan (jak get_week_plan, ale bez recipe_title). "
            "UWAGA: zawsze pokaż użytkownikowi podgląd i poczekaj na potwierdzenie przed wywołaniem. "
            "Do drobnych zmian preferuj add_plan_entry / remove_plan_entry."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
                "entries": {
                    "type": "array",
                    "description": "Komplet wpisów dla tygodnia — wszystko poza tą listą zostanie usunięte.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "day": {
                                "type": "integer", "minimum": 0, "maximum": 6,
                                "description": "0=poniedziałek, 6=niedziela.",
                            },
                            "meal": {
                                "type": "string",
                                "description": "Slot posiłku, np. 'śniadanie', 'obiad', 'kolacja'. (day, meal) musi być unikalne.",
                            },
                            "recipe_id": {"type": "string"},
                            "servings": {
                                "type": "integer",
                                "description": "Liczba porcji do zjedzenia w tym slocie (skalowanie względem servings przepisu).",
                            },
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
        description=(
            "Dodaje lub NADPISUJE jeden slot (day+meal) w planie tygodnia, zachowując resztę wpisów. "
            "Jeśli w tej parze (day, meal) już coś było — zostanie zastąpione. "
            "Zwraca pełen zaktualizowany WeekPlan. Błąd 400 gdy recipe_id niewidoczne dla użytkownika."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
                "day": {"type": "integer", "minimum": 0, "maximum": 6, "description": "0=poniedziałek."},
                "meal": {"type": "string", "description": "Slot, np. 'obiad'."},
                "recipe_id": {"type": "string"},
                "servings": {"type": "integer", "description": "Liczba porcji w tym slocie."},
            },
            "required": ["week_start", "day", "meal", "recipe_id", "servings"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="remove_plan_entry",
        description=(
            "Usuwa wpis pasujący do (day, meal) z planu tygodnia (no-op, jeśli go nie ma). "
            "Zwraca pełen zaktualizowany WeekPlan."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
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
            "Liczy sumy makroskładników na każdy dzień tygodnia. Wartości skalowane są w stosunku "
            "do (entry.servings / recipe.servings), więc odpowiadają faktycznej ilości do zjedzenia. "
            "Odpowiedź: obiekt {\"0\": {kcal, p, f, c}, \"1\": {...}, ... \"6\": {...}} "
            "(klucze to dni: 0=poniedziałek..6=niedziela), wartości zaokrąglone do 2 miejsc. "
            "Dni bez wpisów mają same zera."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_shopping_list",
        description=(
            "Zwraca aktualną listę zakupów dla tygodnia, posortowaną po category, name. "
            "Format: tablica obiektów {id (int, używane w check_shopping_item), week_start, "
            "name, qty (number), unit (string — znormalizowane: 'g'/'ml'/'szt' itd.), "
            "category (polska kategoria np. 'Nabiał', 'Warzywa i owoce', 'Inne'), "
            "checked (bool — czy odhaczone), is_custom (bool — true gdy użytkownik dopisał ręcznie, "
            "false gdy wygenerowane z planu)}. Pusta tablica jeśli nie wygenerowano listy."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="generate_shopping_list",
        description=(
            "DESTRUKTYWNE: usuwa wszystkie wygenerowane wpisy listy zakupów dla tygodnia "
            "(ręczne dopiski z is_custom=true są zachowane) i tworzy nową listę z planu — "
            "agreguje składniki ze wszystkich entries (skalowanie servings), normalizuje jednostki "
            "(kg→g, l→ml), kategoryzuje po nazwie. Zwraca pełną nową listę (struktura jak get_shopping_list)."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="check_shopping_item",
        description=(
            "Ustawia stan odhaczenia pojedynczej pozycji listy zakupów. "
            "Zwraca zaktualizowany obiekt pozycji (jak element z get_shopping_list)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {
                    "type": "integer",
                    "description": "Pole `id` z get_shopping_list (NIE recipe_id ani nazwa).",
                },
                "checked": {
                    "type": "boolean",
                    "description": "true = oznacz jako kupione, false = odznacz.",
                },
            },
            "required": ["item_id", "checked"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="clear_shopping_list",
        description=(
            "DESTRUKTYWNE: usuwa WSZYSTKIE pozycje (wygenerowane i ręczne) z listy zakupów tygodnia. "
            "Zwraca {\"ok\": true}. Zawsze potwierdź z użytkownikiem."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="estimate_recipe_macros",
        description=(
            "Wywołuje skonfigurowany model LLM, aby oszacować makro przepisu na podstawie składników. "
            "Zwraca {\"kcal\": number, \"p\": number, \"f\": number, \"c\": number} — SUMY dla CAŁEGO "
            "przepisu (wszystkich porcji łącznie, nie na porcję). Wartości można wprost przekazać do "
            "create_recipe / update_recipe. "
            "Wymaga skonfigurowanego LLM w add-onie (424 gdy brak), zlicza się do dziennej kwoty AI "
            "użytkownika (424 gdy wyczerpana), może zwrócić 502 jeśli LLM odpowie nie-JSON-em. "
            "Użyj przed create_recipe, gdy użytkownik nie podał makro."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Nazwa przepisu (kontekst dla modelu)."},
                "servings": {
                    "type": "integer",
                    "description": "Liczba porcji, dla której podano `ingredients` — model dostaje to w prompcie, ale wynik i tak jest sumą dla całego przepisu.",
                },
                "ingredients": {
                    "type": "array",
                    "description": "Składniki w ilości odpowiadającej `servings`.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit": {"type": "string", "description": "np. 'g', 'ml', 'szt', 'łyżka'."},
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
        meal = _normalize_meal(str(args["meal"]))
        entries = await _get_plan_entries(ws)
        entries = [
            e for e in entries
            if not (int(e["day"]) == int(args["day"]) and e["meal"].lower() == meal.lower())
        ]
        entries.append({
            "day": int(args["day"]),
            "meal": meal,
            "recipe_id": args["recipe_id"],
            "servings": int(args["servings"]),
        })
        return await _put_plan(ws, entries)

    if name == "remove_plan_entry":
        ws = args["week_start"]
        meal = _normalize_meal(str(args["meal"]))
        entries = await _get_plan_entries(ws)
        entries = [
            e for e in entries
            if not (int(e["day"]) == int(args["day"]) and e["meal"].lower() == meal.lower())
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
            f"/api/shopping/items/{args['item_id']}",
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
