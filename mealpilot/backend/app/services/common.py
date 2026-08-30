"""Pure helpers and ownership guards shared by every service module.

This is the single home for logic that used to be copy-pasted between
``app/agent/helpers.py``, ``app/routers/shopping.py`` and ``mcp_server.py``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from .. import models
from ..ownership import can_edit, can_view, get_household_id, get_membership, visible_filter
from .errors import Forbidden, Invalid, NotFound

CANONICAL_MEALS = ["Śniadanie", "II Śniadanie", "Obiad", "Przekąska", "Kolacja"]

_PL_ASCII = str.maketrans(
    "ąćęłńóśźżĄĆĘŁŃÓŚŹŻÄÅÖÜäåöü",
    "acelnoszzACELNOSZZAAOUaaou",
)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_PL_WEEKDAYS = (
    "poniedziałek",
    "wtorek",
    "środa",
    "czwartek",
    "piątek",
    "sobota",
    "niedziela",
)


# --------------------------------------------------------------------------- #
# Week handling
# --------------------------------------------------------------------------- #


def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def current_week_start() -> str:
    """ISO date of the most recent Monday (server timezone)."""
    return monday_of(date.today()).isoformat()


def parse_week_start(value: Any) -> str:
    """Validate that `value` is an ISO date **and** a Monday.

    Every week-scoped table is keyed on the literal ``week_start`` string, so a
    Tuesday silently matches nothing and callers conclude "the plan is empty".
    Fail loudly instead, naming the Monday the caller almost certainly meant.
    """
    raw = str(value or "").strip()
    if not raw:
        raise Invalid("week_start jest wymagany (poniedziałek w formacie YYYY-MM-DD).")
    if not _ISO_DATE_RE.match(raw):
        raise Invalid(f"week_start musi mieć format YYYY-MM-DD, otrzymano: {raw!r}.")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise Invalid(f"week_start nie jest poprawną datą: {raw!r}.") from exc
    if parsed.weekday() != 0:
        monday = monday_of(parsed).isoformat()
        raise Invalid(f"week_start musi być poniedziałkiem; {raw} to {_PL_WEEKDAYS[parsed.weekday()]}. Użyj {monday}.")
    return raw


# --------------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------------- #


def strip_diacritics(text: str) -> str:
    return (text or "").translate(_PL_ASCII)


def fold(text: str) -> str:
    """Lowercase + de-accent, for accent-insensitive search and matching."""
    return strip_diacritics(str(text or "")).lower()


def normalize_meal(meal: str) -> str:
    """Return the canonical meal name (case-insensitive), else capitalize."""
    stripped = str(meal or "").strip()
    lower = stripped.lower()
    for cm in CANONICAL_MEALS:
        if cm.lower() == lower:
            return cm
    return stripped[:1].upper() + stripped[1:] if stripped else stripped


def category_of(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"kurczak|łosos|łoso|tofu|jajko|wołowin|indyk|szynk", n):
        return "Mięso, ryby, białko"
    if re.search(r"mleko|śmietan|feta|jogurt|masł|ser", n):
        return "Nabiał"
    if re.search(
        r"papryka|cebul|ogórek|pomidor|marchew|ziemniak|brokuł|pieczark|czosnek|awokado|cytryn|szczypior|koperek|dymka|imbir",
        n,
    ):
        return "Warzywa i owoce"
    if re.search(r"banan|owoc", n):
        return "Warzywa i owoce"
    if re.search(r"ryż|kasza|owsian|płatk|makaron|chleb|kromka", n):
        return "Suche i zboża"
    if re.search(r"oliw|olej|sos|miód|przyp|tymianek|cynamon|oregano|sól|papryka słodka", n):
        return "Tłuszcze i przyprawy"
    if re.search(r"bulion|passata|oliwk|orzech", n):
        return "Spiżarnia"
    return "Inne"


def normalize_unit_qty(unit: str, qty: float) -> tuple[str, float]:
    u = (unit or "").strip().lower()
    if u == "kg":
        return "g", qty * 1000.0
    if u == "l":
        return "ml", qty * 1000.0
    return u, qty


def coerce_steps(value: Any) -> list[dict[str, Any]]:
    """Normalize steps to the stored shape ``{text, duration_minutes}``.

    Accepts the plain-string form models naturally produce, and the object form
    the UI round-trips, so both surfaces write the same thing to the database.
    """
    out: list[dict[str, Any]] = []
    for step in list(value or []):
        if isinstance(step, dict):
            out.append({"text": str(step.get("text", "")), "duration_minutes": step.get("duration_minutes")})
        else:
            out.append({"text": str(step), "duration_minutes": None})
    return out


def clamp_limit(value: Any, default: int, maximum: int) -> int:
    if value is None:
        return default
    try:
        n = int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid(f"limit musi być liczbą całkowitą, otrzymano: {value!r}.") from exc
    if n < 1:
        raise Invalid("limit musi być >= 1.")
    return min(n, maximum)


def clamp_offset(value: Any) -> int:
    if value is None:
        return 0
    try:
        n = int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid(f"offset musi być liczbą całkowitą, otrzymano: {value!r}.") from exc
    if n < 0:
        raise Invalid("offset nie może być ujemny.")
    return n


_RECIPE_ID_HINT = "musi być liczbą całkowitą — użyj pola `id` z search_recipes / list_recipes, nie tytułu przepisu."


def coerce_recipe_id(value: Any, field: str = "recipe_id") -> int:
    """Recipe ids are surrogate integers; models happily send ``"42"`` instead.

    Accepting the numeric-string form keeps JSON-typed callers working, while a
    title or a legacy slug fails loudly here rather than silently matching nothing.
    """
    if isinstance(value, bool) or value is None or (isinstance(value, str) and not value.strip()):
        raise Invalid(f"{field} jest wymagane i {_RECIPE_ID_HINT}")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise Invalid(f"{field} {_RECIPE_ID_HINT} Otrzymano: {value!r}.") from exc


# --------------------------------------------------------------------------- #
# Ownership guards
# --------------------------------------------------------------------------- #

_LABELS = {
    models.Recipe: ("Przepis", "tego przepisu"),
    models.MealPlanEntry: ("Wpis planu", "tego planu"),
    models.ShoppingItem: ("Pozycja listy zakupów", "tej listy zakupów"),
    models.WeekTemplate: ("Szablon tygodnia", "tego szablonu"),
}


def visible_query(db: Session, user: models.User, model):
    """Query restricted to rows the user may see (personal + household)."""
    return db.query(model).filter(visible_filter(model, user, get_household_id(db, user.id)))


def require_visible(db: Session, user: models.User, model, pk):
    noun, _ = _LABELS.get(model, ("Zasób", "tego zasobu"))
    row = db.get(model, pk)
    if row is None or not can_view(row, user, get_household_id(db, user.id)):
        raise NotFound(f"{noun} nie istnieje lub nie masz do niego dostępu: {pk}")
    return row


def require_editable(db: Session, user: models.User, model, pk):
    """Visible **and** writable — mirrors the rule the REST layer enforces.

    Household rows are visible to every member but only writable by members
    with ``can_edit`` (or by the original creator).
    """
    noun, what = _LABELS.get(model, ("Zasób", "tego zasobu"))
    row = db.get(model, pk)
    member = get_membership(db, user.id)
    hh = member.household_id if member else None
    if row is None or not can_view(row, user, hh):
        raise NotFound(f"{noun} nie istnieje lub nie masz do niego dostępu: {pk}")
    if not can_edit(row, user, member):
        raise Forbidden(f"Brak uprawnień do edycji {what} (zasób należy do household, a nie masz prawa edycji).")
    return row


def assert_can_edit(db: Session, user: models.User, row) -> None:
    """Guard an already-loaded row (used when replacing sets of rows)."""
    member = get_membership(db, user.id)
    if not can_edit(row, user, member):
        _, what = _LABELS.get(type(row), ("Zasób", "tego zasobu"))
        raise Forbidden(f"Brak uprawnień do edycji {what}.")


def owner_kwargs_like(row) -> dict[str, Any]:
    """Ownership kwargs that keep a replacement row in the same bucket as `row`."""
    return {
        "owner_user_id": row.owner_user_id,
        "owner_household_id": row.owner_household_id,
    }


# --------------------------------------------------------------------------- #
# Serializers
# --------------------------------------------------------------------------- #


def recipe_to_dict(r: models.Recipe, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        "id": r.id,
        "title": r.title,
        "tags": list(r.tags or []),
        "meal_types": list(r.meal_types or []),
        "servings": r.servings,
        "prep_time": r.prep_time,
        "cook_time": r.cook_time,
        "kcal": r.kcal,
        "p": r.p,
        "f": r.f,
        "c": r.c,
        "hue": r.hue,
        "ingredients": list(r.ingredients or []),
        "steps": coerce_steps(r.steps),
        "is_meal_prep": bool(r.is_meal_prep),
        "meal_prep_days": r.meal_prep_days,
        "meal_prep_steps": coerce_steps(r.meal_prep_steps),
        "image_filename": r.image_filename,
        "shared_with_household": r.owner_household_id is not None,
    }
    if extra:
        out.update(extra)
    return out


def recipe_to_summary(r: models.Recipe, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        "id": r.id,
        "title": r.title,
        "tags": list(r.tags or []),
        "meal_types": list(r.meal_types or []),
        "servings": r.servings,
        "prep_time": r.prep_time,
        "cook_time": r.cook_time,
        "kcal": r.kcal,
        "p": r.p,
        "f": r.f,
        "c": r.c,
        "is_meal_prep": bool(r.is_meal_prep),
        "ingredients_count": len(r.ingredients or []),
        "steps_count": len(r.steps or []),
    }
    if extra:
        out.update(extra)
    return out


def plan_entry_to_dict(e: models.MealPlanEntry) -> dict[str, Any]:
    return {
        "day": e.day,
        "meal": e.meal,
        "recipe_id": e.recipe_id,
        "servings": e.servings,
    }


def shopping_item_to_dict(it: models.ShoppingItem) -> dict[str, Any]:
    return {
        "id": it.id,
        "week_start": it.week_start,
        "name": it.name,
        "qty": it.qty,
        "unit": it.unit,
        "category": it.category,
        "checked": bool(it.checked),
        "is_custom": bool(it.is_custom),
        "recipe_ids": it.recipe_ids,
    }


# --------------------------------------------------------------------------- #
# Shopping-item provenance
# --------------------------------------------------------------------------- #


def attach_recipe_source(db: Session, item: models.ShoppingItem, recipe_id: int | None) -> None:
    """Record that `recipe_id` contributed to `item` (no-op if already recorded)."""
    if not recipe_id:
        return
    exists = (
        db.query(models.ShoppingItemRecipe)
        .filter(
            models.ShoppingItemRecipe.item_id == item.id,
            models.ShoppingItemRecipe.recipe_id == recipe_id,
        )
        .first()
    )
    if not exists:
        db.add(models.ShoppingItemRecipe(item_id=item.id, recipe_id=recipe_id))


def delete_recipe_sources_for_items(db: Session, item_ids: Iterable[int]) -> None:
    ids = list(item_ids)
    if not ids:
        return
    db.query(models.ShoppingItemRecipe).filter(models.ShoppingItemRecipe.item_id.in_(ids)).delete(
        synchronize_session="fetch"
    )
