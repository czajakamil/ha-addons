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
from contextvars import ContextVar
from datetime import date, timedelta
from typing import Any

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

BASE_URL = os.environ.get("MEALPILOT_BASE_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.environ.get("MEALPILOT_API_KEY", "")
SESSION_COOKIE = os.environ.get("MEALPILOT_SESSION_COOKIE", "")
COOKIE_NAME = os.environ.get("MEALPILOT_COOKIE_NAME", "mealpilot_session")

# Per-request API key override (used by SSE transport inside the add-on).
# When set, takes precedence over the global API_KEY env var.
request_api_key: ContextVar[str] = ContextVar("request_api_key", default="")

server: Server = Server("mealpilot")


def _headers() -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    key = request_api_key.get() or API_KEY
    if key:
        headers["X-MealPilot-Token"] = key
    elif SESSION_COOKIE:
        headers["Cookie"] = f"{COOKIE_NAME}={SESSION_COOKIE}"
    return headers


def _result(payload: Any) -> list[TextContent]:
    if isinstance(payload, str):
        return [TextContent(type="text", text=payload)]
    return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))]


def _error(message: str) -> list[TextContent]:
    return [TextContent(type="text", text=f"ERROR: {message}")]


async def _request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: Any | None = None,
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


_CANONICAL_MEALS = ["Śniadanie", "II Śniadanie", "Obiad", "Przekąska", "Kolacja"]


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

RECIPE_SHAPE = (
    "Recipe = { id:str, title:str, tags:str[], meal_types:str[], servings:int, "
    "prep_time:int (min), cook_time:int (min), "
    "kcal:num, p:num, f:num, c:num  (UWAGA: SUMY dla CAŁEGO przepisu = wszystkich "
    "porcji łącznie, NIE na porcję), hue:int(0-360), "
    "ingredients:[{name:str, qty:num, unit:str}], steps:str[], "
    "created_by:str, owner_name:str, image_filename:str|null, "
    "avg_rating:num|null, rating_count:int, my_rating:int(1-5)|null }"
)

WEEKPLAN_SHAPE = (
    "WeekPlan = { week_start:str(YYYY-MM-DD, zawsze poniedziałek), entries:[ "
    "{ day:int(0=pon..6=niedz), meal:str('śniadanie'|'obiad'|'kolacja'|...), "
    "recipe_id:str, servings:int (porcje do zjedzenia w tym slocie) } ] }. "
    "get_week_plan / get_current_week_plan dodatkowo doklejają recipe_title; "
    "set_/add_/remove_plan_entry zwracają WeekPlan BEZ recipe_title."
)

SHOPPING_ITEM_SHAPE = (
    "ShoppingItem = { id:int (użyj w check_shopping_item — to NIE recipe_id ani "
    "nazwa), week_start:str, name:str, qty:num, unit:str (znormalizowana: "
    "'g'/'ml'/'szt'...), category:str (PL, np. 'Nabiał'|'Warzywa i owoce'|'Inne'), "
    "checked:bool, is_custom:bool (true=dopisane ręcznie, false=wygenerowane z planu) }"
)

WEEK_START_HINT = (
    "`week_start` MUSI być poniedziałkiem (YYYY-MM-DD); inny dzień da pusty/błędny "
    "wynik. Poniedziałek = data - timedelta(days=data.weekday())."
)


TOOLS: list[Tool] = [
    # ----------------------------- PRZEPISY: ODCZYT ----------------------------- #
    Tool(
        name="list_recipes",
        description=(
            "Zwraca pełną listę przepisów widocznych dla użytkownika (własne + dzielone "
            "w household); może być pusta. Do filtrowania po tagach/typach/makro/ocenach "
            "użyj filter_recipes (nie filtruj po stronie agenta). "
            f"Każdy element: {RECIPE_SHAPE}"
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="get_recipe",
        description=(
            "Zwraca jeden pełny obiekt Recipe po ID. Błąd 404, gdy przepis nie istnieje "
            "lub użytkownik nie ma dostępu — wtedy NIE zgaduj ID, użyj list_recipes/"
            "filter_recipes. -> Recipe (kształt jak w list_recipes)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "recipe_id": {
                    "type": "string",
                    "description": "Pole `id` z list_recipes/filter_recipes.",
                }
            },
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="list_tags",
        description=(
            'Zwraca {"tags": string[]} — posortowane, unikalne tagi z widocznych '
            "przepisów (np. 'azjatyckie', 'wegetariańskie'). WYWOŁAJ PRZED filter_recipes/"
            "create_recipe: filtrowanie i spójność słownictwa wymagają DOKŁADNEGO, "
            "case-sensitive dopasowania. Nie wymyślaj tagów spoza tej listy."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="list_meal_types",
        description=(
            'Zwraca {"meal_types": string[]} — posortowane, unikalne typy posiłków '
            "(np. 'śniadanie', 'obiad', 'kolacja'). WYWOŁAJ PRZED filter_recipes/"
            "create_recipe — dopasowanie jest DOKŁADNE i case-sensitive."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="filter_recipes",
        description=(
            "Zwraca Recipe[] (kształt jak list_recipes) spełniające WSZYSTKIE podane kryteria; "
            "bez argumentów = list_recipes; może być pusta. Semantyka:\n"
            "  • tags        – AND (wymagane wszystkie), dokładne dopasowanie\n"
            "  • meal_types  – OR (wystarczy jeden), dokładne dopasowanie\n"
            "  • max_kcal    – kcal SUMY całego przepisu (nie na porcję!) ≤ wartość\n"
            "  • min_protein – białko (g) SUMY całego przepisu ≥ wartość\n"
            "  • min_my_rating  – ocena użytkownika ≥ wartość; brak oceny = WYKLUCZONY\n"
            "  • min_avg_rating – średnia ocena ≥ wartość; brak jakichkolwiek ocen = WYKLUCZONY\n"
            "Wartości tags/meal_types są case-sensitive — najpierw list_tags / list_meal_types."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Wszystkie tagi muszą wystąpić (AND). Dokładne dopasowanie.",
                },
                "meal_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": ("Przynajmniej jeden typ musi pasować (OR). Dokładne dopasowanie."),
                },
                "max_kcal": {
                    "type": "number",
                    "description": ("Górny limit kcal sumarycznie dla całego przepisu (nie na porcję)."),
                },
                "min_protein": {
                    "type": "number",
                    "description": "Dolny limit białka (g) sumarycznie dla całego przepisu.",
                },
                "min_my_rating": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5,
                    "description": ("Tylko przepisy z oceną użytkownika >= wartość. Brak oceny = wykluczony."),
                },
                "min_avg_rating": {
                    "type": "number",
                    "minimum": 1.0,
                    "maximum": 5.0,
                    "description": "Tylko przepisy ze średnią >= wartość. Brak ocen = wykluczony.",
                },
            },
            "additionalProperties": False,
        },
    ),
    # ---------------------------- PRZEPISY: ZAPIS ------------------------------ #
    Tool(
        name="create_recipe",
        description=(
            "Tworzy nowy przepis. -> Recipe (created_by/owner_* ustawione, "
            "image_filename=null). Przy 409 (zajęte `id`) wygeneruj inny slug i ponów. "
            "Pola kcal/p/f/c to SUMY dla CAŁEGO przepisu (wszystkich porcji łącznie); "
            "jeśli użytkownik nie podał makro, NAJPIERW wywołaj estimate_recipe_macros "
            "i przekaż wynik tutaj. tags/meal_types trzymaj w słownictwie z list_tags / "
            "list_meal_types."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Unikalny slug. Nowy — błąd 409 gdy zajęty.",
                },
                "title": {"type": "string", "description": "Nazwa wyświetlana w UI."},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Tagi tematyczne. Trzymaj się słownictwa z list_tags.",
                },
                "meal_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Typy posiłków. Spójne z list_meal_types.",
                },
                "servings": {"type": "integer", "description": "Liczba porcji (domyślnie 1)."},
                "prep_time": {"type": "integer", "description": "Czas przygotowania w minutach."},
                "cook_time": {"type": "integer", "description": "Czas gotowania w minutach."},
                "kcal": {
                    "type": "number",
                    "description": "Kalorie sumarycznie dla całego przepisu.",
                },
                "p": {
                    "type": "number",
                    "description": "Białko (g) sumarycznie dla całego przepisu.",
                },
                "f": {
                    "type": "number",
                    "description": "Tłuszcz (g) sumarycznie dla całego przepisu.",
                },
                "c": {
                    "type": "number",
                    "description": "Węglowodany (g) sumarycznie dla całego przepisu.",
                },
                "hue": {
                    "type": "integer",
                    "description": "Odcień karty w UI (0–360, domyślnie 40).",
                },
                "ingredients": {
                    "type": "array",
                    "description": "Składniki dla podanej liczby porcji (servings).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number", "description": "Ilość w jednostce `unit`."},
                            "unit": {
                                "type": "string",
                                "description": "np. 'g', 'ml', 'szt', 'łyżka'.",
                            },
                        },
                        "required": ["name", "qty", "unit"],
                    },
                },
                "steps": {
                    "type": "array",
                    "items": {"type": "string"},
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
            "PATCH: aktualizuje tylko przekazane pola (pomiń te, których nie zmieniasz). "
            "-> Recipe (zaktualizowany). UWAGA: tablice (ingredients, steps, tags, "
            "meal_types) są NADPISYWANE w całości — przekaż pełną nową listę, nie różnicę "
            "(aby dodać jeden składnik, pobierz get_recipe, dołóż element, odeślij całość). "
            "Błędy: 403 gdy nie jesteś właścicielem, 404 gdy nie istnieje."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "recipe_id": {"type": "string", "description": "ID przepisu do edycji."},
                "title": {"type": "string"},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Nadpisuje całą listę tagów.",
                },
                "meal_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Nadpisuje całą listę typów.",
                },
                "servings": {"type": "integer"},
                "prep_time": {"type": "integer", "description": "Minuty."},
                "cook_time": {"type": "integer", "description": "Minuty."},
                "kcal": {"type": "number", "description": "Suma dla całego przepisu."},
                "p": {"type": "number", "description": "Białko (g) sumarycznie."},
                "f": {"type": "number", "description": "Tłuszcz (g) sumarycznie."},
                "c": {"type": "number", "description": "Węglowodany (g) sumarycznie."},
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
                    "type": "array",
                    "items": {"type": "string"},
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
            "[DESTRUKCYJNE][POTWIERDŹ] Usuwa przepis ORAZ wszystkie powiązane wpisy w planach "
            'tygodni. Nieodwracalne. -> {"ok": true}. Błędy: 403 gdy nie właściciel, 404 gdy brak.'
        ),
        inputSchema={
            "type": "object",
            "properties": {"recipe_id": {"type": "string", "description": "ID przepisu do usunięcia."}},
            "required": ["recipe_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="rate_recipe",
        description=(
            "[POTWIERDŹ] Ustawia (1–5) lub usuwa (rating=0) ocenę przepisu w imieniu "
            "zalogowanego użytkownika. -> Recipe ze świeżymi avg_rating, rating_count, "
            "my_rating. (Politykę 'gdy użytkownik chwali/gani przepis, zaproponuj ocenę' "
            "trzymaj w system promptcie.)"
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "recipe_id": {"type": "string", "description": "ID przepisu do oceny."},
                "rating": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 5,
                    "description": "Ocena 1–5. Wartość 0 usuwa istniejącą ocenę.",
                },
            },
            "required": ["recipe_id", "rating"],
            "additionalProperties": False,
        },
    ),
    # ----------------------------- PLAN TYGODNIA ------------------------------- #
    Tool(
        name="get_week_plan",
        description=(
            f"Zwraca plan posiłków na podany tydzień. -> {WEEKPLAN_SHAPE} entries może być pusta. {WEEK_START_HINT}"
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_current_week_plan",
        description=(
            "Jak get_week_plan, ale week_start (poniedziałek bieżącego tygodnia w strefie "
            "serwera) liczony automatycznie. Użyj dla 'ten tydzień' / 'teraz' — "
            "nie licz daty ręcznie."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    Tool(
        name="set_week_plan",
        description=(
            "[DESTRUKCYJNE][POTWIERDŹ] Zastępuje CAŁY plan tygodnia listą `entries` "
            "(pusta lista = wyczyszczenie). Do drobnych zmian preferuj add_plan_entry / "
            "remove_plan_entry. Wszystkie recipe_id muszą być widoczne — inaczej 400 z listą "
            f"brakujących. -> WeekPlan (bez recipe_title). {WEEK_START_HINT}"
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
                "entries": {
                    "type": "array",
                    "description": "Komplet wpisów — wszystko poza tą listą zostanie usunięte.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "day": {
                                "type": "integer",
                                "minimum": 0,
                                "maximum": 6,
                                "description": "0=poniedziałek, 6=niedziela.",
                            },
                            "meal": {
                                "type": "string",
                                "description": "Slot, np. 'obiad'. (day, meal) musi być unikalne.",
                            },
                            "recipe_id": {"type": "string"},
                            "servings": {
                                "type": "integer",
                                "description": "Porcje do zjedzenia w tym slocie.",
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
            "Dodaje lub NADPISUJE jeden slot (day+meal), zachowując resztę planu (idempotentne "
            "po (day, meal)). -> WeekPlan. Błąd 400 gdy recipe_id niewidoczne dla użytkownika."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
                "day": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 6,
                    "description": "0=poniedziałek.",
                },
                "meal": {"type": "string", "description": "Slot, np. 'obiad'."},
                "recipe_id": {"type": "string"},
                "servings": {"type": "integer", "description": "Porcje w tym slocie."},
            },
            "required": ["week_start", "day", "meal", "recipe_id", "servings"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="remove_plan_entry",
        description=("Usuwa wpis (day, meal) z planu (no-op, jeśli go nie ma). -> WeekPlan."),
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
            "Sumy makro na każdy dzień tygodnia, przeskalowane wg (entry.servings / "
            "recipe.servings) — czyli faktyczna ilość do zjedzenia. "
            '-> {"0":{kcal,p,f,c}, ... "6":{...}} (0=poniedziałek..6=niedziela), '
            "wartości zaokrąglone do 2 miejsc; dni bez wpisów = same zera."
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    # ------------------------------ LISTA ZAKUPÓW ------------------------------ #
    Tool(
        name="get_shopping_list",
        description=(
            f"Aktualna lista zakupów tygodnia, posortowana po (category, name). "
            f"-> {SHOPPING_ITEM_SHAPE}[] (pusta, jeśli nie wygenerowano)."
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
            "[DESTRUKCYJNE] Usuwa wygenerowane pozycje tygodnia (ręczne is_custom=true ZOSTAJĄ) "
            "i tworzy nową listę z planu: agreguje składniki ze wszystkich entries (skalowanie "
            "servings), normalizuje jednostki (kg→g, l→ml), kategoryzuje po nazwie. "
            "-> ShoppingItem[] (kształt jak get_shopping_list)."
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
            "Odhacza/odznacza jedną pozycję listy. -> zaktualizowany ShoppingItem. "
            "item_id to pole `id` z get_shopping_list (liczba całkowita) — NIE recipe_id ani nazwa."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "integer", "description": "Pole `id` z get_shopping_list."},
                "checked": {"type": "boolean", "description": "true = kupione, false = odznacz."},
            },
            "required": ["item_id", "checked"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="add_shopping_item",
        description=(
            "Dodaje ręczną pozycję (is_custom=true) do listy zakupów tygodnia — do rzeczy "
            "spoza planu (np. 'kup masło'). Jednostka jest normalizowana (kg→g, l→ml), "
            "kategoria nadawana automatycznie po nazwie, gdy pominięta. UWAGA: jeśli istnieje "
            "już pozycja o TEJ SAMEJ (name, unit) w tym tygodniu, ilości są SUMOWANE (a nie "
            "tworzona druga pozycja). -> dodany/zaktualizowany ShoppingItem (kształt jak "
            "get_shopping_list)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."},
                "name": {"type": "string", "description": "Nazwa produktu (1–200 znaków)."},
                "qty": {"type": "number", "description": "Ilość w jednostce `unit` (domyślnie 1)."},
                "unit": {
                    "type": "string",
                    "description": "Jednostka, np. 'g', 'ml', 'szt' (domyślnie 'szt').",
                },
                "category": {
                    "type": "string",
                    "description": ("Kategoria PL (np. 'Nabiał'). Pominięta = wykryta automatycznie z nazwy."),
                },
            },
            "required": ["week_start", "name"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="delete_shopping_item",
        description=(
            "[DESTRUKCYJNE] Usuwa jedną pozycję listy zakupów (ręczną lub wygenerowaną). "
            "Nieodwracalne. item_id to pole `id` z get_shopping_list (liczba) — NIE recipe_id "
            'ani nazwa. -> {"ok": true}. Błąd 404, gdy pozycja nie istnieje / brak dostępu.'
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "integer", "description": "Pole `id` z get_shopping_list."},
            },
            "required": ["item_id"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="clear_shopping_list",
        description=(
            "[DESTRUKCYJNE][POTWIERDŹ] Usuwa WSZYSTKIE pozycje (wygenerowane i ręczne) "
            'z listy zakupów tygodnia. -> {"ok": true}.'
        ),
        inputSchema={
            "type": "object",
            "properties": {"week_start": {"type": "string", "description": "Poniedziałek (YYYY-MM-DD)."}},
            "required": ["week_start"],
            "additionalProperties": False,
        },
    ),
    # -------------------------------- POMOCNICZE ------------------------------- #
    Tool(
        name="estimate_recipe_macros",
        description=(
            "Szacuje makro przepisu modelem LLM na podstawie składników. "
            '-> {"kcal":num, "p":num, "f":num, "c":num} — SUMY dla CAŁEGO przepisu '
            "(wszystkich porcji łącznie), gotowe do przekazania do create_recipe / update_recipe. "
            "Użyj PRZED create_recipe, gdy użytkownik nie podał makro. "
            "Błędy: 424 (brak skonfigurowanego LLM lub wyczerpana dzienna kwota AI — nie ponawiaj, "
            "poproś użytkownika o makro ręcznie), 502 (LLM zwrócił nie-JSON — można ponowić raz)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Nazwa przepisu (kontekst dla modelu)."},
                "servings": {
                    "type": "integer",
                    "description": ("Liczba porcji, dla której podano ingredients (wynik i tak jest sumą całości)."),
                },
                "ingredients": {
                    "type": "array",
                    "description": "Składniki w ilości odpowiadającej servings.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit": {
                                "type": "string",
                                "description": "np. 'g', 'ml', 'szt', 'łyżka'.",
                            },
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
async def list_tools() -> list[Tool]:
    return TOOLS


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


async def _enrich_plan_with_titles(week_plan: dict[str, Any]) -> dict[str, Any]:
    recipes = await _request("GET", "/api/recipes")
    recipe_map = {r["id"]: r for r in (recipes or [])}
    for e in week_plan.get("entries", []) or []:
        r = recipe_map.get(e.get("recipe_id") or "", {})
        e["recipe_title"] = r.get("title", "")
        e["avg_rating"] = r.get("avg_rating")
        e["rating_count"] = r.get("rating_count", 0)
        e["my_rating"] = r.get("my_rating")
    return week_plan


async def _get_plan_entries(week_start: str) -> list[dict[str, Any]]:
    plan = await _request("GET", f"/api/plan/{week_start}")
    return list(plan.get("entries") or [])


async def _put_plan(week_start: str, entries: list[dict[str, Any]]) -> Any:
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


async def _dispatch(name: str, args: dict[str, Any]) -> Any:
    if name == "list_recipes":
        return await _request("GET", "/api/recipes")

    if name == "get_recipe":
        return await _request("GET", f"/api/recipes/{args['recipe_id']}")

    if name == "list_tags":
        return await _request("GET", "/api/recipes/meta/tags")

    if name == "list_meal_types":
        return await _request("GET", "/api/recipes/meta/meal_types")

    if name == "filter_recipes":
        params: dict[str, Any] = {}
        if args.get("tags"):
            params["tags"] = ",".join(args["tags"])
        if args.get("meal_types"):
            params["meal_types"] = ",".join(args["meal_types"])
        if args.get("max_kcal") is not None:
            params["max_kcal"] = args["max_kcal"]
        if args.get("min_protein") is not None:
            params["min_protein"] = args["min_protein"]
        if args.get("min_my_rating") is not None:
            params["min_my_rating"] = args["min_my_rating"]
        if args.get("min_avg_rating") is not None:
            params["min_avg_rating"] = args["min_avg_rating"]
        return await _request("GET", "/api/recipes", params=params)

    if name == "create_recipe":
        return await _request("POST", "/api/recipes", json_body=args)

    if name == "update_recipe":
        rid = args.pop("recipe_id")
        return await _request("PUT", f"/api/recipes/{rid}", json_body=args)

    if name == "delete_recipe":
        await _request("DELETE", f"/api/recipes/{args['recipe_id']}")
        return {"ok": True}

    if name == "rate_recipe":
        rid = args["recipe_id"]
        rating = int(args["rating"])
        if rating == 0:
            await _request("DELETE", f"/api/recipes/{rid}/rating")
        else:
            await _request("PUT", f"/api/recipes/{rid}/rating", json_body={"rating": rating})
        return await _request("GET", f"/api/recipes/{rid}")

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
        entries = [e for e in entries if not (int(e["day"]) == int(args["day"]) and e["meal"].lower() == meal.lower())]
        entries.append(
            {
                "day": int(args["day"]),
                "meal": meal,
                "recipe_id": args["recipe_id"],
                "servings": int(args["servings"]),
            }
        )
        return await _put_plan(ws, entries)

    if name == "remove_plan_entry":
        ws = args["week_start"]
        meal = _normalize_meal(str(args["meal"]))
        entries = await _get_plan_entries(ws)
        entries = [e for e in entries if not (int(e["day"]) == int(args["day"]) and e["meal"].lower() == meal.lower())]
        return await _put_plan(ws, entries)

    if name == "get_week_nutrition_summary":
        ws = args["week_start"]
        plan = await _request("GET", f"/api/plan/{ws}")
        recipes = await _request("GET", "/api/recipes") or []
        rmap = {r["id"]: r for r in recipes}
        out: dict[int, dict[str, float]] = {d: {"kcal": 0.0, "p": 0.0, "f": 0.0, "c": 0.0} for d in range(7)}
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

    if name == "add_shopping_item":
        body: dict[str, Any] = {"name": args["name"]}
        if args.get("qty") is not None:
            body["qty"] = args["qty"]
        if args.get("unit") is not None:
            body["unit"] = args["unit"]
        if args.get("category") is not None:
            body["category"] = args["category"]
        return await _request("POST", f"/api/shopping/{args['week_start']}/items", json_body=body)

    if name == "delete_shopping_item":
        await _request("DELETE", f"/api/shopping/items/{args['item_id']}")
        return {"ok": True}

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
async def call_tool(name: str, arguments: dict[str, Any] | None) -> list[TextContent]:
    args = dict(arguments or {})
    try:
        result = await _dispatch(name, args)
    except RuntimeError as e:
        return _error(str(e))
    except httpx.HTTPError as e:
        return _error(f"HTTP error: {e}")
    except Exception as e:
        return _error(f"{type(e).__name__}: {e}")
    return _result(result)


async def main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
