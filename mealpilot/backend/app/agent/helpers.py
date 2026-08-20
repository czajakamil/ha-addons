"""Pure helpers used by the agent tool handlers."""
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any, Dict, Iterable, Tuple

from sqlalchemy.orm import Session

from .. import models


CANONICAL_MEALS = ['Śniadanie', 'II Śniadanie', 'Obiad', 'Przekąska', 'Kolacja']

_PL_ASCII = str.maketrans(
    "ąćęłńóśźżÄÅÖÜäåöü",
    "acelnoszzAAOUaaou",
)


def slugify(text: str) -> str:
    """Convert a recipe title to a safe ASCII slug (lowercase, hyphens only)."""
    s = (text or "").strip().lower()
    s = s.translate(_PL_ASCII)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "przepis"


def normalize_meal(meal: str) -> str:
    """Return canonical meal name (case-insensitive match), or capitalize first char."""
    stripped = meal.strip()
    lower = stripped.lower()
    for cm in CANONICAL_MEALS:
        if cm.lower() == lower:
            return cm
    return stripped[:1].upper() + stripped[1:] if stripped else stripped


def current_week_start() -> str:
    """Return the ISO date string of the most recent Monday."""
    today = date.today()
    offset = today.weekday()  # Monday = 0
    monday = today - timedelta(days=offset)
    return monday.isoformat()


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


def normalize_unit_qty(unit: str, qty: float) -> Tuple[str, float]:
    u = (unit or "").strip().lower()
    if u == "kg":
        return "g", qty * 1000.0
    if u == "l":
        return "ml", qty * 1000.0
    return u, qty


def recipe_to_dict(r: models.Recipe) -> Dict[str, Any]:
    return {
        "id": r.id,
        "title": r.title,
        "tags": list(r.tags or []),
        "servings": r.servings,
        "prep_time": r.prep_time,
        "cook_time": r.cook_time,
        "kcal": r.kcal,
        "p": r.p,
        "f": r.f,
        "c": r.c,
        "hue": r.hue,
        "ingredients": list(r.ingredients or []),
        "steps": list(r.steps or []),
        "meal_types": list(r.meal_types or []),
        "image_filename": r.image_filename,
    }


def recipe_to_summary(r: models.Recipe) -> Dict[str, Any]:
    return {
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
        "ingredients_count": len(r.ingredients or []),
        "steps_count": len(r.steps or []),
    }


def plan_entry_to_dict(e: models.MealPlanEntry) -> Dict[str, Any]:
    return {
        "day": e.day,
        "meal": e.meal,
        "recipe_id": e.recipe_id,
        "servings": e.servings,
    }


def shopping_item_to_dict(it: models.ShoppingItem) -> Dict[str, Any]:
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


def attach_recipe_source(db: Session, item: models.ShoppingItem, recipe_id: str | None) -> None:
    """Record that `recipe_id` contributed to `item` (no-op if already recorded or absent)."""
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
    db.query(models.ShoppingItemRecipe).filter(
        models.ShoppingItemRecipe.item_id.in_(ids)
    ).delete(synchronize_session="fetch")


def safe_json_parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def json_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)
