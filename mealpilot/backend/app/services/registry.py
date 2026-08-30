"""The single tool registry.

Every tool surface in MealPilot is generated from this file:

  * ``app/agent/tools/schemas.py``  -> TOOL_DEFS / TOOL_DEFS_OPENAI (in-app agent)
  * ``mcp_server.py``              -> mcp.types.Tool list (Claude Desktop etc.)
  * ``GET /api/agent/tools``       -> the "what can the agent do" modal in the UI

Adding a tool means adding one ``ToolSpec`` here. There is no second list to
keep in sync, and ``tests/unit/test_tool_registry.py`` fails if one appears.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import anyio.to_thread
from sqlalchemy.orm import Session

from .. import models
from . import macros as macros_svc
from . import plan as plan_svc
from . import recipes as recipes_svc
from . import shopping as shopping_svc
from . import templates as templates_svc
from .errors import NotFound

# --------------------------------------------------------------------------- #
# Spec
# --------------------------------------------------------------------------- #

GROUP_RECIPES = "Przepisy"
GROUP_PLAN = "Plan tygodnia"
GROUP_SHOPPING = "Lista zakupów"
GROUP_TEMPLATES = "Szablony tygodnia"
GROUP_HELPERS = "Pomocnicze"

GROUP_ORDER = [
    (GROUP_RECIPES, "📖"),
    (GROUP_PLAN, "📅"),
    (GROUP_SHOPPING, "🛒"),
    (GROUP_TEMPLATES, "🗂️"),
    (GROUP_HELPERS, "🧮"),
]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    title: str
    group: str
    summary: str
    description: str
    input_schema: dict[str, Any]
    handler: Callable[..., Any]
    output_schema: dict[str, Any] | None = None
    read_only: bool = False
    destructive: bool = False
    idempotent: bool = False
    changed: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    confirm: bool = False

    @property
    def scope(self) -> str:
        return "read" if self.read_only else "write"

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.handler)


def _obj(properties: dict[str, Any], required: list[str] | None = None, *, strict: bool = True) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    if strict:
        schema["additionalProperties"] = False
    return schema


# --------------------------------------------------------------------------- #
# Shared schema fragments
# --------------------------------------------------------------------------- #

WEEK_START = {
    "type": "string",
    "pattern": r"^\d{4}-\d{2}-\d{2}$",
    "description": "Poniedziałek tygodnia (YYYY-MM-DD). Inny dzień = błąd z podpowiedzią właściwej daty.",
}

LIMIT = {"type": "integer", "minimum": 1, "maximum": 200, "description": "Maks. liczba wyników (domyślnie 50)."}
OFFSET = {"type": "integer", "minimum": 0, "description": "Przesunięcie stronicowania (domyślnie 0)."}

STEP = {
    "type": "object",
    "properties": {
        "text": {"type": "string", "description": "Treść kroku."},
        "duration_minutes": {"type": ["integer", "null"], "description": "Czas kroku w minutach (opcjonalnie)."},
    },
    "required": ["text"],
}

INGREDIENT = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "qty": {"type": "number", "description": "Ilość w jednostce `unit`."},
        "unit": {"type": "string", "description": "np. 'g', 'ml', 'szt', 'łyżka'."},
    },
    "required": ["name", "qty", "unit"],
}

# Output-side copies without `required`: a legacy row missing a field must not
# turn a successful read into an output-validation error on the client.
INGREDIENT_OUT = {"type": "object", "properties": INGREDIENT["properties"]}
STEP_OUT = {"type": "object", "properties": STEP["properties"]}

FILTER_PROPS = {
    "tags": {"type": "array", "items": {"type": "string"}, "description": "AND — wszystkie muszą wystąpić."},
    "meal_types": {"type": "array", "items": {"type": "string"}, "description": "OR — wystarczy jeden."},
    "max_kcal": {"type": "number", "description": "kcal całego przepisu <= wartość."},
    "min_protein": {"type": "number", "description": "Białko (g) całego przepisu >= wartość."},
    "max_total_time": {"type": "number", "description": "prep_time + cook_time (min) <= wartość."},
    "is_meal_prep": {"type": "boolean", "description": "Tylko przepisy meal-prep (true) lub tylko zwykłe (false)."},
    "min_my_rating": {"type": "integer", "minimum": 1, "maximum": 5, "description": "Brak oceny = wykluczony."},
    "min_avg_rating": {"type": "number", "minimum": 1.0, "maximum": 5.0, "description": "Brak ocen = wykluczony."},
}

RECIPE_WRITE_PROPS = {
    "title": {"type": "string", "description": "Nazwa wyświetlana w UI."},
    "tags": {"type": "array", "items": {"type": "string"}, "description": "Słownictwo z list_tags."},
    "meal_types": {"type": "array", "items": {"type": "string"}, "description": "Słownictwo z list_meal_types."},
    "servings": {"type": "integer", "minimum": 1, "description": "Liczba porcji (domyślnie 1)."},
    "prep_time": {"type": "integer", "minimum": 0, "description": "Czas przygotowania w minutach."},
    "cook_time": {"type": "integer", "minimum": 0, "description": "Czas gotowania w minutach."},
    "kcal": {"type": "number", "description": "Kalorie SUMARYCZNIE dla całego przepisu."},
    "p": {"type": "number", "description": "Białko (g) sumarycznie dla całego przepisu."},
    "f": {"type": "number", "description": "Tłuszcz (g) sumarycznie dla całego przepisu."},
    "c": {"type": "number", "description": "Węglowodany (g) sumarycznie dla całego przepisu."},
    "hue": {"type": "integer", "minimum": 0, "maximum": 360, "description": "Odcień karty w UI (domyślnie 40)."},
    "ingredients": {"type": "array", "items": INGREDIENT, "description": "Składniki dla podanej liczby porcji."},
    "steps": {"type": "array", "items": STEP, "description": "Kolejne kroki przygotowania, każdy osobno."},
    "is_meal_prep": {"type": "boolean", "description": "Czy przepis jest przeznaczony do gotowania na zapas."},
    "meal_prep_days": {"type": "integer", "minimum": 1, "description": "Na ile dni starcza porcja meal-prep."},
    "meal_prep_steps": {
        "type": "array",
        "items": STEP,
        "description": "Kroki specyficzne dla batch-cookingu (porcjowanie, mrożenie, odgrzewanie).",
    },
}

# ---- output schemas ---- #

RECIPE_SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
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
        "is_meal_prep": {"type": "boolean"},
        "ingredients_count": {"type": "integer"},
        "steps_count": {"type": "integer"},
        "avg_rating": {"type": ["number", "null"]},
        "rating_count": {"type": "integer"},
        "my_rating": {"type": ["integer", "null"]},
        "my_note": {"type": ["string", "null"]},
    },
    "required": ["id", "title"],
}

RECIPE_FULL_SCHEMA = {
    "type": "object",
    "properties": {
        **{
            k: v
            for k, v in RECIPE_SUMMARY_SCHEMA["properties"].items()
            if k not in ("ingredients_count", "steps_count")
        },
        "hue": {"type": "integer"},
        "ingredients": {"type": "array", "items": INGREDIENT_OUT},
        "steps": {"type": "array", "items": STEP_OUT},
        "meal_prep_days": {"type": ["integer", "null"]},
        "meal_prep_steps": {"type": "array", "items": STEP_OUT},
        "image_filename": {"type": ["string", "null"]},
        "shared_with_household": {"type": "boolean"},
    },
    "required": ["id", "title", "ingredients", "steps"],
}


def _page_schema(item_schema: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    props = {
        "items": {"type": "array", "items": item_schema},
        "total": {"type": "integer", "description": "Liczba pasujących pozycji przed stronicowaniem."},
        "limit": {"type": "integer"},
        "offset": {"type": "integer"},
        "has_more": {"type": "boolean"},
    }
    if extra:
        props.update(extra)
    return _obj(props, ["items", "total"], strict=False)


PLAN_ENTRY_SCHEMA = {
    "type": "object",
    "properties": {
        "day": {"type": "integer", "minimum": 0, "maximum": 6},
        "meal": {"type": "string"},
        "recipe_id": {"type": "integer"},
        "servings": {"type": "integer"},
        "recipe_title": {"type": "string"},
    },
    "required": ["day", "meal", "recipe_id", "servings"],
}

WEEK_PLAN_SCHEMA = _obj(
    {"week_start": {"type": "string"}, "entries": {"type": "array", "items": PLAN_ENTRY_SCHEMA}},
    ["week_start", "entries"],
    strict=False,
)

SHOPPING_ITEM_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer", "description": "Użyj w check_shopping_item / delete_shopping_item."},
        "week_start": {"type": "string"},
        "name": {"type": "string"},
        "qty": {"type": "number"},
        "unit": {"type": "string"},
        "category": {"type": "string"},
        "checked": {"type": "boolean"},
        "is_custom": {"type": "boolean"},
        "recipe_ids": {"type": "array", "items": {"type": "integer"}},
    },
    "required": ["id", "name", "qty", "unit", "category", "checked", "is_custom"],
}

SHOPPING_LIST_SCHEMA = _obj(
    {
        "week_start": {"type": "string"},
        "items": {"type": "array", "items": SHOPPING_ITEM_SCHEMA},
        "total": {"type": "integer"},
        "checked": {"type": "integer"},
    },
    ["week_start", "items"],
    strict=False,
)

MACRO_BLOCK = {
    "type": "object",
    "properties": {
        "kcal": {"type": "number"},
        "p": {"type": "number"},
        "f": {"type": "number"},
        "c": {"type": "number"},
    },
}

TEMPLATE_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "entries": {"type": "array", "items": PLAN_ENTRY_SCHEMA},
        "entry_count": {"type": "integer"},
        "shared_with_household": {"type": "boolean"},
    },
    "required": ["id", "name"],
}

_WEEK_ARG = _obj({"week_start": WEEK_START}, ["week_start"])
_NO_ARGS = _obj({}, None)

_MACRO_NOTE = "kcal/p/f/c to SUMY dla CAŁEGO przepisu (wszystkich porcji łącznie), NIE na porcję."


# --------------------------------------------------------------------------- #
# The registry
# --------------------------------------------------------------------------- #

TOOL_SPECS: list[ToolSpec] = [
    # ------------------------------ PRZEPISY: ODCZYT ------------------------ #
    ToolSpec(
        name="search_recipes",
        title="Szukaj przepisów",
        group=GROUP_RECIPES,
        summary="Wyszukiwanie tekstowe po tytule, tagach i składnikach (np. 'kurczak', 'makaron').",
        description=(
            "Wyszukiwanie pełnotekstowe po tytule, tagach, typach posiłku i NAZWACH SKŁADNIKÓW. "
            "Nieczułe na wielkość liter i polskie znaki ('lupacz' znajdzie 'Łupacz'). "
            "Wszystkie słowa z `query` muszą wystąpić; wyniki są posortowane wg trafności "
            "(tytuł > tag > składnik). Można łączyć z filtrami (tags, max_kcal, ...). "
            "TO JEST DOMYŚLNE narzędzie, gdy użytkownik opisuje przepis słowami — nie pobieraj "
            "całej biblioteki przez list_recipes, żeby przeszukać ją samodzielnie. "
            "-> { items: RecipeSummary[], total, limit, offset, has_more, query }"
        ),
        input_schema=_obj(
            {
                "query": {"type": "string", "description": "Szukany tekst, np. 'kurczak curry' albo 'makaron'."},
                **FILTER_PROPS,
                "limit": {**LIMIT, "description": "Maks. liczba wyników (domyślnie 20)."},
            },
            ["query"],
        ),
        output_schema=_page_schema(RECIPE_SUMMARY_SCHEMA, {"query": {"type": "string"}}),
        handler=recipes_svc.search_recipes,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="list_recipes",
        title="Lista przepisów",
        group=GROUP_RECIPES,
        summary="Skróty przepisów (bez składników i kroków), stronicowane. Szczegóły: get_recipe.",
        description=(
            "Stronicowana lista SKRÓTÓW przepisów widocznych dla użytkownika (własne + dzielone "
            "w household). Nie zawiera składników ani kroków — po nie sięgnij get_recipe. "
            "Do wyszukiwania po słowie użyj search_recipes, do kryteriów — filter_recipes. "
            f"{_MACRO_NOTE} "
            "-> { items: RecipeSummary[], total, limit, offset, has_more }"
        ),
        input_schema=_obj({"limit": LIMIT, "offset": OFFSET}),
        output_schema=_page_schema(RECIPE_SUMMARY_SCHEMA),
        handler=recipes_svc.list_recipes,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="filter_recipes",
        title="Filtruj przepisy",
        group=GROUP_RECIPES,
        summary="Skróty przepisów spełniających kryteria (tagi, makro, czas, oceny, meal-prep).",
        description=(
            "Zwraca SKRÓTY przepisów spełniających WSZYSTKIE podane kryteria; bez argumentów = "
            "list_recipes. Semantyka:\n"
            "  • tags           – AND (wymagane wszystkie), dokładne dopasowanie\n"
            "  • meal_types     – OR (wystarczy jeden), dokładne dopasowanie\n"
            "  • max_kcal       – kcal SUMY całego przepisu <= wartość\n"
            "  • min_protein    – białko (g) SUMY całego przepisu >= wartość\n"
            "  • max_total_time – prep_time + cook_time (min) <= wartość\n"
            "  • is_meal_prep   – tylko przepisy do gotowania na zapas (lub tylko zwykłe)\n"
            "  • min_my_rating  – ocena użytkownika >= wartość; brak oceny = WYKLUCZONY\n"
            "  • min_avg_rating – średnia ocena >= wartość; brak ocen = WYKLUCZONY\n"
            "tags/meal_types są case-sensitive — najpierw list_tags / list_meal_types."
        ),
        input_schema=_obj({**FILTER_PROPS, "limit": LIMIT, "offset": OFFSET}),
        output_schema=_page_schema(RECIPE_SUMMARY_SCHEMA),
        handler=recipes_svc.filter_recipes,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="get_recipe",
        title="Szczegóły przepisu",
        group=GROUP_RECIPES,
        summary="Pełny przepis po id: składniki, kroki, makro, oceny, notatka.",
        description=(
            "Zwraca jeden PEŁNY przepis po id (składniki, kroki, makro, meal-prep, oceny, "
            "moja notatka). Błąd not_found, gdy przepis nie istnieje lub użytkownik nie ma dostępu — "
            f"wtedy NIE zgaduj id, użyj search_recipes. {_MACRO_NOTE}"
        ),
        input_schema=_obj(
            {"recipe_id": {"type": "integer", "description": "Pole `id` z search_recipes/list_recipes."}},
            ["recipe_id"],
        ),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.get_recipe,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="list_tags",
        title="Dostępne tagi",
        group=GROUP_RECIPES,
        summary="Unikalne tagi używane w widocznych przepisach.",
        description=(
            'Zwraca {"tags": string[]} — posortowane, unikalne tagi z widocznych przepisów. '
            "WYWOŁAJ PRZED filter_recipes/create_recipe: filtrowanie i spójność słownictwa wymagają "
            "DOKŁADNEGO, case-sensitive dopasowania. Nie wymyślaj tagów spoza tej listy."
        ),
        input_schema=_NO_ARGS,
        output_schema=_obj({"tags": {"type": "array", "items": {"type": "string"}}}, ["tags"]),
        handler=recipes_svc.list_tags,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="list_meal_types",
        title="Dostępne typy posiłków",
        group=GROUP_RECIPES,
        summary="Unikalne typy posiłków zdefiniowane w przepisach.",
        description=(
            'Zwraca {"meal_types": string[]} — posortowane, unikalne typy posiłków '
            "(np. 'śniadanie', 'obiad'). WYWOŁAJ PRZED filter_recipes/create_recipe — "
            "dopasowanie jest DOKŁADNE i case-sensitive."
        ),
        input_schema=_NO_ARGS,
        output_schema=_obj({"meal_types": {"type": "array", "items": {"type": "string"}}}, ["meal_types"]),
        handler=recipes_svc.list_meal_types,
        read_only=True,
        idempotent=True,
    ),
    # ------------------------------ PRZEPISY: ZAPIS ------------------------- #
    ToolSpec(
        name="create_recipe",
        title="Dodaj przepis",
        group=GROUP_RECIPES,
        summary="Tworzy nowy przepis. Id (liczbę) nadaje serwer — nie podawaj go.",
        description=(
            "Tworzy nowy przepis. **Id nadaje serwer** (kolejna liczba całkowita) — nie musisz go "
            "wymyślać ani obsługiwać konfliktów nazw; id nie zmienia się przy zmianie tytułu. "
            f"{_MACRO_NOTE} Jeśli użytkownik nie podał makro, wywołaj najpierw estimate_recipe_macros "
            "i przekaż wynik tutaj — nie licz makro samodzielnie. tags/meal_types trzymaj w słownictwie "
            "z list_tags / list_meal_types. Dla gotowania na zapas ustaw is_meal_prep, meal_prep_days "
            "i meal_prep_steps. -> pełny Recipe."
        ),
        input_schema=_obj(RECIPE_WRITE_PROPS, ["title"]),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.create_recipe,
        changed=("recipes",),
        confirm=True,
    ),
    ToolSpec(
        name="update_recipe",
        title="Edytuj przepis",
        group=GROUP_RECIPES,
        summary="Aktualizuje wybrane pola przepisu. Tablice są nadpisywane w całości.",
        description=(
            "PATCH: aktualizuje tylko przekazane pola. UWAGA: tablice (ingredients, steps, tags, "
            "meal_types, meal_prep_steps) są NADPISYWANE w całości — przekaż pełną nową listę, nie "
            "różnicę (aby dodać jeden składnik: get_recipe, dołóż element, odeślij całość). "
            "Błędy: forbidden gdy przepis należy do household, a nie masz prawa edycji; "
            "not_found gdy nie istnieje. -> pełny Recipe."
        ),
        input_schema=_obj(
            {"recipe_id": {"type": "integer", "description": "ID przepisu do edycji."}, **RECIPE_WRITE_PROPS},
            ["recipe_id"],
        ),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.update_recipe,
        changed=("recipes",),
    ),
    ToolSpec(
        name="delete_recipe",
        title="Usuń przepis",
        group=GROUP_RECIPES,
        summary="Trwale usuwa przepis oraz jego wpisy w planach i listach zakupów.",
        description=(
            "Usuwa przepis ORAZ wszystkie powiązane wpisy w planach tygodni, po czym regeneruje "
            "listy zakupów dotkniętych tygodni. NIEODWRACALNE — zapytaj użytkownika o potwierdzenie. "
            '-> {"deleted": id, "affected_weeks": [...]}'
        ),
        input_schema=_obj({"recipe_id": {"type": "integer"}}, ["recipe_id"]),
        output_schema=_obj(
            {"deleted": {"type": "integer"}, "affected_weeks": {"type": "array", "items": {"type": "string"}}},
            ["deleted"],
        ),
        handler=recipes_svc.delete_recipe,
        destructive=True,
        confirm=True,
        changed=("recipes", "plan", "shopping"),
    ),
    ToolSpec(
        name="rate_recipe",
        title="Oceń przepis",
        group=GROUP_RECIPES,
        summary="Ustawia (1–5) lub kasuje (0) ocenę przepisu w imieniu użytkownika.",
        description=(
            "Ustawia (1–5) lub usuwa (rating=0) ocenę przepisu w imieniu zalogowanego użytkownika. "
            "Idempotentne — ponowne ustawienie tej samej oceny nic nie zmienia. "
            "-> Recipe ze świeżymi avg_rating, rating_count, my_rating."
        ),
        input_schema=_obj(
            {
                "recipe_id": {"type": "integer"},
                "rating": {"type": "integer", "minimum": 0, "maximum": 5, "description": "1–5; 0 usuwa ocenę."},
            },
            ["recipe_id", "rating"],
        ),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.rate_recipe,
        idempotent=True,
        confirm=True,
        changed=("recipes",),
    ),
    ToolSpec(
        name="set_recipe_note",
        title="Notatka do przepisu",
        group=GROUP_RECIPES,
        summary="Zapisuje prywatną notatkę użytkownika przy przepisie (pusta = kasuje).",
        description=(
            "Zapisuje PRYWATNĄ notatkę użytkownika przy przepisie (widoczną tylko dla niego, także "
            "przy przepisach dzielonych w household). Pusty `note` kasuje notatkę. Dobre miejsce na "
            "'wyszło za słone', 'następnym razem 2x więcej czosnku'. -> Recipe z polem my_note."
        ),
        input_schema=_obj(
            {
                "recipe_id": {"type": "integer"},
                "note": {"type": "string", "description": "Pusty string kasuje notatkę."},
            },
            ["recipe_id", "note"],
        ),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.set_recipe_note,
        idempotent=True,
        changed=("recipes",),
    ),
    ToolSpec(
        name="share_recipe_with_household",
        title="Udostępnij przepis domownikom",
        group=GROUP_RECIPES,
        summary="Przepina przepis między prywatnym a wspólnym (household). Tylko twórca.",
        description=(
            "Przepina przepis między prywatnym (share=false) a wspólnym dla household (share=true). "
            "Może to zrobić WYŁĄCZNIE twórca przepisu. Po udostępnieniu widzą go wszyscy domownicy, "
            "ale edytować mogą tylko ci z prawem edycji. -> Recipe z shared_with_household."
        ),
        input_schema=_obj(
            {"recipe_id": {"type": "integer"}, "share": {"type": "boolean", "description": "true = wspólny."}},
            ["recipe_id", "share"],
        ),
        output_schema=RECIPE_FULL_SCHEMA,
        handler=recipes_svc.share_recipe_with_household,
        idempotent=True,
        confirm=True,
        changed=("recipes",),
    ),
    # -------------------------------- PLAN ---------------------------------- #
    ToolSpec(
        name="get_week_plan",
        title="Plan tygodnia",
        group=GROUP_PLAN,
        summary="Plan posiłków na wskazany tydzień (z tytułami przepisów).",
        description=(
            "Plan posiłków na podany tydzień, z doklejonym recipe_title. entries może być pusta. "
            "day: 0=poniedziałek..6=niedziela."
        ),
        input_schema=_WEEK_ARG,
        output_schema=WEEK_PLAN_SCHEMA,
        handler=plan_svc.get_week_plan,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="get_current_week_plan",
        title="Plan bieżącego tygodnia",
        group=GROUP_PLAN,
        summary="Jak get_week_plan, ale bieżący poniedziałek liczy serwer.",
        description=(
            "Jak get_week_plan, ale week_start (poniedziałek bieżącego tygodnia w strefie serwera) "
            "liczony automatycznie. Użyj dla 'ten tydzień' / 'teraz' — nie licz daty ręcznie. "
            "Zwrócone `week_start` możesz przekazać do pozostałych narzędzi tygodniowych."
        ),
        input_schema=_NO_ARGS,
        output_schema=WEEK_PLAN_SCHEMA,
        handler=plan_svc.get_current_week_plan,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="set_week_plan",
        title="Zastąp plan tygodnia",
        group=GROUP_PLAN,
        summary="Zastępuje CAŁY plan tygodnia listą entries (pusta = czyści).",
        description=(
            "Zastępuje CAŁY plan tygodnia listą `entries` (pusta lista = wyczyszczenie). "
            "Do drobnych zmian preferuj add_plan_entry / remove_plan_entry. Sloty, które już "
            "istnieją, są aktualizowane w miejscu — zachowują właściciela (prywatny/household). "
            "Wszystkie recipe_id muszą być widoczne, inaczej błąd z listą brakujących. "
            "Pokaż propozycję użytkownikowi i poczekaj na potwierdzenie."
        ),
        input_schema=_obj(
            {
                "week_start": WEEK_START,
                "entries": {
                    "type": "array",
                    "description": "Komplet wpisów — wszystko poza tą listą zostanie usunięte.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "day": {"type": "integer", "minimum": 0, "maximum": 6, "description": "0=poniedziałek."},
                            "meal": {"type": "string", "description": "Slot, np. 'Obiad'. (day, meal) unikalne."},
                            "recipe_id": {"type": "integer"},
                            "servings": {"type": "integer", "minimum": 1},
                        },
                        "required": ["day", "meal", "recipe_id", "servings"],
                    },
                },
            },
            ["week_start", "entries"],
        ),
        output_schema=WEEK_PLAN_SCHEMA,
        handler=plan_svc.set_week_plan,
        destructive=True,
        confirm=True,
        changed=("plan",),
    ),
    ToolSpec(
        name="add_plan_entry",
        title="Dodaj posiłek do planu",
        group=GROUP_PLAN,
        summary="Dodaje lub nadpisuje jeden slot (day+meal), reszta planu bez zmian.",
        description=(
            "Dodaje lub NADPISUJE jeden slot (day+meal), zachowując resztę planu. Idempotentne "
            "po (day, meal). Błąd, gdy recipe_id nie jest widoczne dla użytkownika."
        ),
        input_schema=_obj(
            {
                "week_start": WEEK_START,
                "day": {"type": "integer", "minimum": 0, "maximum": 6, "description": "0=poniedziałek."},
                "meal": {"type": "string", "description": "Slot, np. 'Obiad'."},
                "recipe_id": {"type": "integer"},
                "servings": {"type": "integer", "minimum": 1, "description": "Porcje w tym slocie."},
            },
            ["week_start", "day", "meal", "recipe_id", "servings"],
        ),
        output_schema=WEEK_PLAN_SCHEMA,
        handler=plan_svc.add_plan_entry,
        idempotent=True,
        changed=("plan",),
    ),
    ToolSpec(
        name="remove_plan_entry",
        title="Usuń posiłek z planu",
        group=GROUP_PLAN,
        summary="Usuwa wpis (day, meal) z planu; no-op, jeśli go nie ma.",
        description="Usuwa wpis (day, meal) z planu tygodnia (no-op, jeśli slot jest pusty). -> WeekPlan.",
        input_schema=_obj(
            {
                "week_start": WEEK_START,
                "day": {"type": "integer", "minimum": 0, "maximum": 6},
                "meal": {"type": "string"},
            },
            ["week_start", "day", "meal"],
        ),
        output_schema=WEEK_PLAN_SCHEMA,
        handler=plan_svc.remove_plan_entry,
        idempotent=True,
        destructive=True,
        changed=("plan",),
    ),
    ToolSpec(
        name="get_week_nutrition_summary",
        title="Makro tygodnia",
        group=GROUP_PLAN,
        summary="Sumy kcal/białka/tłuszczu/węgli na każdy dzień i na cały tydzień.",
        description=(
            "Sumy makro na każdy dzień tygodnia, przeskalowane wg (entry.servings / recipe.servings) — "
            'czyli faktyczna ilość do zjedzenia. -> { week_start, days: {"0":{kcal,p,f,c} ... "6":{...}}, '
            "week_total: {kcal,p,f,c} }; 0=poniedziałek, dni bez wpisów = zera."
        ),
        input_schema=_WEEK_ARG,
        output_schema=_obj(
            {
                "week_start": {"type": "string"},
                "days": {"type": "object", "additionalProperties": MACRO_BLOCK},
                "week_total": MACRO_BLOCK,
            },
            ["week_start", "days", "week_total"],
            strict=False,
        ),
        handler=plan_svc.get_week_nutrition_summary,
        read_only=True,
        idempotent=True,
    ),
    # ------------------------------ ZAKUPY ---------------------------------- #
    ToolSpec(
        name="get_shopping_list",
        title="Lista zakupów",
        group=GROUP_SHOPPING,
        summary="Aktualna lista zakupów tygodnia, posortowana po kategorii i nazwie.",
        description=(
            "Aktualna lista zakupów tygodnia, posortowana po (category, name). Pole `id` każdej "
            "pozycji to argument do check_shopping_item / delete_shopping_item — NIE recipe_id ani "
            "nazwa. -> { week_start, items: ShoppingItem[], total, checked }"
        ),
        input_schema=_WEEK_ARG,
        output_schema=SHOPPING_LIST_SCHEMA,
        handler=shopping_svc.get_shopping_list,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="generate_shopping_list",
        title="Wygeneruj listę zakupów",
        group=GROUP_SHOPPING,
        summary="Przelicza listę z planu tygodnia; pozycje dopisane ręcznie zostają.",
        description=(
            "Przelicza wygenerowaną część listy z planu tygodnia: agreguje składniki ze wszystkich "
            "wpisów (skalowanie servings), normalizuje jednostki (kg→g, l→ml), kategoryzuje po nazwie. "
            "Pozycje dopisane ręcznie (is_custom=true) ZOSTAJĄ nietknięte, a odhaczenia pozycji, "
            "które nadal wynikają z planu, są zachowane. Idempotentne. -> lista jak get_shopping_list."
        ),
        input_schema=_WEEK_ARG,
        output_schema=SHOPPING_LIST_SCHEMA,
        handler=shopping_svc.generate_shopping_list,
        idempotent=True,
        changed=("shopping",),
    ),
    ToolSpec(
        name="check_shopping_item",
        title="Odhacz pozycję",
        group=GROUP_SHOPPING,
        summary="Oznacza pozycję jako kupioną lub odznacza (po id z get_shopping_list).",
        description=(
            "Odhacza/odznacza jedną pozycję listy. item_id to pole `id` z get_shopping_list "
            "(liczba całkowita) — NIE recipe_id ani nazwa. Wymaga tylko widoczności pozycji: "
            "na wspólnej liście household odhaczyć może każdy domownik, także bez prawa edycji "
            "(dodawanie i usuwanie pozycji nadal go wymaga). -> zaktualizowany ShoppingItem."
        ),
        input_schema=_obj(
            {
                "item_id": {"type": "integer", "description": "Pole `id` z get_shopping_list."},
                "checked": {"type": "boolean", "description": "true = kupione."},
            },
            ["item_id", "checked"],
        ),
        output_schema=SHOPPING_ITEM_SCHEMA,
        handler=shopping_svc.check_shopping_item,
        idempotent=True,
        changed=("shopping",),
    ),
    ToolSpec(
        name="add_shopping_item",
        title="Dopisz pozycję",
        group=GROUP_SHOPPING,
        summary="Dodaje własną pozycję do listy zakupów (np. 'kup masło').",
        description=(
            "Dodaje ręczną pozycję (is_custom=true) do listy zakupów tygodnia — do rzeczy spoza planu. "
            "Jednostka jest normalizowana (kg→g, l→ml), kategoria nadawana automatycznie po nazwie, "
            "gdy pominięta. UWAGA: jeśli istnieje już pozycja o TEJ SAMEJ (name, unit) w tym tygodniu "
            "(porównanie bez względu na wielkość liter), ilości są SUMOWANE zamiast tworzenia drugiej "
            "pozycji — a scalona pozycja staje się is_custom=true, więc generate_shopping_list "
            "przestaje przeliczać jej ilość z planu (ręczna nadwyżka przeżywa regenerację). "
            "W household pozycja trafia na wspólną listę, jeśli tydzień jest wspólny; wymaga wtedy "
            "prawa edycji. -> dodany/zaktualizowany ShoppingItem."
        ),
        input_schema=_obj(
            {
                "week_start": WEEK_START,
                "name": {"type": "string", "description": "Nazwa produktu."},
                "qty": {"type": "number", "description": "Ilość w jednostce `unit` (domyślnie 1)."},
                "unit": {"type": "string", "description": "np. 'g', 'ml', 'szt' (domyślnie 'szt')."},
                "category": {"type": "string", "description": "Pominięta = wykryta automatycznie z nazwy."},
                "recipe_id": {"type": "integer", "description": "Opcjonalnie: przepis, z którego to pochodzi."},
            },
            ["week_start", "name"],
        ),
        output_schema=SHOPPING_ITEM_SCHEMA,
        handler=shopping_svc.add_shopping_item,
        changed=("shopping",),
    ),
    ToolSpec(
        name="delete_shopping_item",
        title="Usuń pozycję",
        group=GROUP_SHOPPING,
        summary="Usuwa jedną pozycję listy zakupów (po id z get_shopping_list).",
        description=(
            "Usuwa jedną pozycję listy zakupów (ręczną lub wygenerowaną). Nieodwracalne. "
            "item_id to pole `id` z get_shopping_list (liczba) — NIE recipe_id ani nazwa. "
            "Uwaga: pozycja wygenerowana wróci przy następnym generate_shopping_list, jeśli "
            'nadal wynika z planu. -> {"deleted": item_id}'
        ),
        input_schema=_obj({"item_id": {"type": "integer"}}, ["item_id"]),
        output_schema=_obj({"deleted": {"type": "integer"}}, ["deleted"]),
        handler=shopping_svc.delete_shopping_item,
        destructive=True,
        changed=("shopping",),
        aliases=("remove_shopping_item",),
    ),
    ToolSpec(
        name="clear_shopping_list",
        title="Wyczyść listę zakupów",
        group=GROUP_SHOPPING,
        summary="Usuwa WSZYSTKIE pozycje tygodnia — także dopisane ręcznie.",
        description=(
            "Usuwa WSZYSTKIE pozycje (wygenerowane i ręczne) z listy zakupów tygodnia. "
            "Ręcznych pozycji nie da się odtworzyć — zapytaj użytkownika o potwierdzenie. "
            '-> {"cleared": week_start, "removed": n}'
        ),
        input_schema=_WEEK_ARG,
        output_schema=_obj({"cleared": {"type": "string"}, "removed": {"type": "integer"}}, ["cleared"]),
        handler=shopping_svc.clear_shopping_list,
        destructive=True,
        confirm=True,
        changed=("shopping",),
    ),
    # ------------------------------ SZABLONY -------------------------------- #
    ToolSpec(
        name="list_week_templates",
        title="Szablony tygodnia",
        group=GROUP_TEMPLATES,
        summary="Zapisane szablony tygodnia (własne + dzielone w household).",
        description=(
            "Zwraca zapisane szablony tygodnia. Pole `id` przekaż do apply_week_template. "
            "-> { templates: WeekTemplate[], total }"
        ),
        input_schema=_NO_ARGS,
        output_schema=_obj(
            {"templates": {"type": "array", "items": TEMPLATE_SCHEMA}, "total": {"type": "integer"}},
            ["templates"],
        ),
        handler=templates_svc.list_week_templates,
        read_only=True,
        idempotent=True,
    ),
    ToolSpec(
        name="save_week_as_template",
        title="Zapisz tydzień jako szablon",
        group=GROUP_TEMPLATES,
        summary="Zapisuje bieżący plan tygodnia jako nazwany szablon do ponownego użycia.",
        description=(
            "Zapisuje plan wskazanego tygodnia jako nazwany szablon (do ponownego użycia w kolejnych "
            "tygodniach). Błąd, gdy plan tygodnia jest pusty. -> WeekTemplate."
        ),
        input_schema=_obj(
            {"week_start": WEEK_START, "name": {"type": "string", "description": "Nazwa szablonu."}},
            ["week_start", "name"],
        ),
        output_schema=TEMPLATE_SCHEMA,
        handler=templates_svc.save_week_as_template,
        changed=("templates",),
    ),
    ToolSpec(
        name="apply_week_template",
        title="Zastosuj szablon",
        group=GROUP_TEMPLATES,
        summary="Nadpisuje plan tygodnia zawartością szablonu.",
        description=(
            "Nadpisuje plan wskazanego tygodnia zawartością szablonu. Przepisy, które zniknęły z "
            "biblioteki, są pomijane i zwracane w `skipped_recipe_ids`. Pokaż użytkownikowi, co "
            "zostanie nadpisane, i poczekaj na potwierdzenie. -> WeekPlan + applied_template."
        ),
        input_schema=_obj(
            {
                "template_id": {"type": "integer", "description": "Pole `id` z list_week_templates."},
                "week_start": WEEK_START,
            },
            ["template_id", "week_start"],
        ),
        output_schema=WEEK_PLAN_SCHEMA,
        handler=templates_svc.apply_week_template,
        destructive=True,
        confirm=True,
        changed=("plan",),
    ),
    ToolSpec(
        name="delete_week_template",
        title="Usuń szablon",
        group=GROUP_TEMPLATES,
        summary="Trwale usuwa zapisany szablon tygodnia.",
        description='Trwale usuwa zapisany szablon tygodnia. Nieodwracalne. -> {"deleted": template_id}',
        input_schema=_obj({"template_id": {"type": "integer"}}, ["template_id"]),
        output_schema=_obj({"deleted": {"type": "integer"}}, ["deleted"]),
        handler=templates_svc.delete_week_template,
        destructive=True,
        confirm=True,
        changed=("templates",),
    ),
    # ----------------------------- POMOCNICZE ------------------------------- #
    ToolSpec(
        name="estimate_recipe_macros",
        title="Oszacuj makro",
        group=GROUP_HELPERS,
        summary="Szacuje kcal/białko/tłuszcz/węgle przepisu modelem LLM (zużywa kwotę AI).",
        description=(
            "Szacuje makro przepisu modelem LLM na podstawie składników. "
            '-> {"kcal","p","f","c"} — SUMY dla CAŁEGO przepisu, gotowe do przekazania do '
            "create_recipe / update_recipe. Użyj PRZED create_recipe, gdy użytkownik nie podał makro — "
            "i NIE licz makro samodzielnie. Błędy: unavailable (brak skonfigurowanego LLM lub "
            "wyczerpana kwota AI — nie ponawiaj, poproś użytkownika o makro), upstream_error "
            "(LLM zwrócił nie-JSON — można ponowić raz)."
        ),
        input_schema=_obj(
            {
                "title": {"type": "string", "description": "Nazwa przepisu (kontekst dla modelu)."},
                "servings": {"type": "integer", "minimum": 1, "description": "Porcje odpowiadające `ingredients`."},
                "ingredients": {"type": "array", "items": INGREDIENT},
            },
            ["title", "servings", "ingredients"],
        ),
        output_schema=_obj(
            {**MACRO_BLOCK["properties"], "note": {"type": "string"}},
            ["kcal", "p", "f", "c"],
            strict=False,
        ),
        handler=macros_svc.estimate_recipe_macros,
    ),
]


# --------------------------------------------------------------------------- #
# Lookup + dispatch
# --------------------------------------------------------------------------- #

SPECS_BY_NAME: dict[str, ToolSpec] = {}
for _spec in TOOL_SPECS:
    if _spec.name in SPECS_BY_NAME:
        raise RuntimeError(f"Duplicate tool name in registry: {_spec.name}")
    SPECS_BY_NAME[_spec.name] = _spec
    for _alias in _spec.aliases:
        if _alias in SPECS_BY_NAME:
            raise RuntimeError(f"Tool alias collides with an existing name: {_alias}")
        SPECS_BY_NAME[_alias] = _spec

TOOL_NAMES: list[str] = [s.name for s in TOOL_SPECS]
DESTRUCTIVE_TOOLS: frozenset[str] = frozenset(s.name for s in TOOL_SPECS if s.destructive)
WRITE_TOOLS: frozenset[str] = frozenset(s.name for s in TOOL_SPECS if not s.read_only)


def get_spec(name: str) -> ToolSpec:
    spec = SPECS_BY_NAME.get(name)
    if spec is None:
        close = ", ".join(sorted(TOOL_NAMES)[:8])
        raise NotFound(f"Nieznane narzędzie: {name}. Dostępne m.in.: {close}…")
    return spec


async def invoke(db: Session, user: models.User, name: str, args: dict[str, Any]) -> Any:
    """Run one tool. Raises ServiceError subclasses for expected failures.

    Synchronous handlers are dispatched to a worker thread. They talk to
    SQLAlchemy — and therefore to SQLite — synchronously, so calling them
    straight from this coroutine parked the event loop for the whole query:
    every MCP client and every agent step share that one loop, and
    ``recipes_svc.filtered_rows`` alone loads the entire visible library.

    On thread-safety: ``Session`` is not thread-safe, but nothing here shares
    one. The caller owns exactly one session, hands it to exactly one worker
    thread and awaits the result, so at most one thread touches it at a time —
    including the ``db.rollback()`` the callers run afterwards, back on the
    event loop, which only happens once this await has returned. The engine is
    built with ``check_same_thread=False``, so the underlying SQLite connection
    may legally move between threads.
    """
    spec = get_spec(name)
    payload = dict(args or {})
    if spec.is_async:
        return await spec.handler(db, user, payload)
    return await anyio.to_thread.run_sync(spec.handler, db, user, payload)


def describe(spec: ToolSpec) -> dict[str, Any]:
    """JSON-serializable description, used by GET /api/agent/tools."""
    return {
        "name": spec.name,
        "title": spec.title,
        "group": spec.group,
        "summary": spec.summary,
        "description": spec.description,
        "input_schema": spec.input_schema,
        "output_schema": spec.output_schema,
        "read_only": spec.read_only,
        "destructive": spec.destructive,
        "idempotent": spec.idempotent,
        "confirm": spec.confirm,
        "changed": list(spec.changed),
    }
