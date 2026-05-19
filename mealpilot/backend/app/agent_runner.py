"""
Backend agent runner — executes the full agent loop server-side
using Anthropic or OpenAI API, with direct DB access for all tools.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy.orm import Session

from . import models

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MAX_STEPS = 10


def _current_week_start() -> str:
    """Return the ISO date string of the most recent Monday."""
    today = date.today()
    offset = today.weekday()  # Monday = 0
    monday = today - timedelta(days=offset)
    return monday.isoformat()


def _category_of(name: str) -> str:
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


def _normalize_unit_qty(unit: str, qty: float) -> Tuple[str, float]:
    u = (unit or "").strip().lower()
    if u == "kg":
        return "g", qty * 1000.0
    if u == "l":
        return "ml", qty * 1000.0
    return u, qty


def _recipe_to_dict(r: models.Recipe) -> Dict[str, Any]:
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


def _plan_entry_to_dict(e: models.MealPlanEntry) -> Dict[str, Any]:
    return {
        "day": e.day,
        "meal": e.meal,
        "recipe_id": e.recipe_id,
        "servings": e.servings,
    }


def _shopping_item_to_dict(it: models.ShoppingItem) -> Dict[str, Any]:
    return {
        "id": it.id,
        "week_start": it.week_start,
        "name": it.name,
        "qty": it.qty,
        "unit": it.unit,
        "category": it.category,
        "checked": bool(it.checked),
        "is_custom": bool(it.is_custom),
    }


# ---------------------------------------------------------------------------
# Tool implementations (direct DB access)
# ---------------------------------------------------------------------------

def tool_list_recipes(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    rows = db.query(models.Recipe).filter(models.Recipe.user_id == user.id).all()
    return [_recipe_to_dict(r) for r in rows]


def tool_get_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, models.Recipe.user_id == user.id)
        .one_or_none()
    )
    if not r:
        raise ValueError(f"Recipe not found: {recipe_id}")
    return _recipe_to_dict(r)


def tool_list_tags(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    rows = db.query(models.Recipe).filter(models.Recipe.user_id == user.id).all()
    out: set = set()
    for r in rows:
        for t in (r.tags or []):
            if isinstance(t, str) and t:
                out.add(t)
    return {"tags": sorted(out)}


def tool_list_meal_types(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    rows = db.query(models.Recipe).filter(models.Recipe.user_id == user.id).all()
    out: set = set()
    for r in rows:
        for m in (r.meal_types or []):
            if isinstance(m, str) and m:
                out.add(m)
    return {"meal_types": sorted(out)}


def tool_filter_recipes(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    rows = db.query(models.Recipe).filter(models.Recipe.user_id == user.id).all()
    tags_filter = list(args.get("tags") or [])
    mt_filter = list(args.get("meal_types") or [])
    max_kcal = args.get("max_kcal")
    min_protein = args.get("min_protein")

    result = []
    for r in rows:
        r_tags = list(r.tags or [])
        r_mts = list(r.meal_types or [])
        if tags_filter and not all(t in r_tags for t in tags_filter):
            continue
        if mt_filter and not any(m in r_mts for m in mt_filter):
            continue
        if max_kcal is not None and (r.kcal or 0) > float(max_kcal):
            continue
        if min_protein is not None and (r.p or 0) < float(min_protein):
            continue
        result.append(_recipe_to_dict(r))
    return result


def tool_create_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("id", ""))
    if not recipe_id:
        raise ValueError("id is required")
    if db.get(models.Recipe, recipe_id):
        raise ValueError(f"Recipe with id '{recipe_id}' already exists")
    ingredients = [
        i if isinstance(i, dict) else i for i in (args.get("ingredients") or [])
    ]
    r = models.Recipe(
        user_id=user.id,
        id=recipe_id,
        title=str(args.get("title", "")),
        tags=list(args.get("tags") or []),
        servings=int(args.get("servings") or 1),
        prep_time=int(args.get("prep_time") or 0),
        cook_time=int(args.get("cook_time") or 0),
        kcal=float(args.get("kcal") or 0),
        p=float(args.get("p") or 0),
        f=float(args.get("f") or 0),
        c=float(args.get("c") or 0),
        hue=int(args.get("hue") or 40),
        ingredients=ingredients,
        steps=list(args.get("steps") or []),
        meal_types=list(args.get("meal_types") or []),
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return _recipe_to_dict(r)


def tool_update_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, models.Recipe.user_id == user.id)
        .one_or_none()
    )
    if not r:
        raise ValueError(f"Recipe not found: {recipe_id}")
    updatable = ["title", "tags", "servings", "prep_time", "cook_time",
                 "kcal", "p", "f", "c", "hue", "ingredients", "steps", "meal_types"]
    for key in updatable:
        if key in args and args[key] is not None:
            value = args[key]
            if key == "ingredients":
                value = [i if isinstance(i, dict) else i for i in value]
            setattr(r, key, value)
    db.commit()
    db.refresh(r)
    return _recipe_to_dict(r)


def tool_delete_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, models.Recipe.user_id == user.id)
        .one_or_none()
    )
    if not r:
        raise ValueError(f"Recipe not found: {recipe_id}")
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.recipe_id == recipe_id,
        models.MealPlanEntry.user_id == user.id,
    ).delete(synchronize_session=False)
    db.delete(r)
    db.commit()
    return {"deleted": recipe_id}


def _get_plan_entries(db: Session, user: models.User, week_start: str) -> List[Dict[str, Any]]:
    rows = (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.user_id == user.id,
            models.MealPlanEntry.week_start == week_start,
        )
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )
    return [_plan_entry_to_dict(e) for e in rows]


def _enrich_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    recipe_ids = {e["recipe_id"] for e in entries}
    recipes = {}
    if recipe_ids:
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids), models.Recipe.user_id == user.id
        ).all():
            recipes[r.id] = r.title
    enriched = [
        {**e, "recipe_title": recipes.get(e["recipe_id"], "")}
        for e in entries
    ]
    return {"week_start": week_start, "entries": enriched}


def tool_get_week_plan(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, entries)


def tool_get_current_week_plan(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    week_start = _current_week_start()
    entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, entries)


def _replace_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> None:
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.user_id == user.id,
        models.MealPlanEntry.week_start == week_start,
    ).delete(synchronize_session=False)
    for e in entries:
        db.add(
            models.MealPlanEntry(
                user_id=user.id,
                week_start=week_start,
                day=int(e["day"]),
                meal=str(e["meal"]),
                recipe_id=str(e["recipe_id"]),
                servings=int(e.get("servings") or 1),
            )
        )
    db.commit()


def tool_set_week_plan(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    entries = list(args.get("entries") or [])
    _replace_plan(db, user, week_start, entries)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


def tool_add_plan_entry(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    day = int(args.get("day", 0))
    meal = str(args.get("meal", ""))
    recipe_id = str(args.get("recipe_id", ""))
    servings = int(args.get("servings") or 1)

    entries = _get_plan_entries(db, user, week_start)
    filtered = [e for e in entries if not (e["day"] == day and e["meal"] == meal)]
    filtered.append({"day": day, "meal": meal, "recipe_id": recipe_id, "servings": servings})
    _replace_plan(db, user, week_start, filtered)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


def tool_remove_plan_entry(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    day = int(args.get("day", 0))
    meal = str(args.get("meal", ""))

    entries = _get_plan_entries(db, user, week_start)
    filtered = [e for e in entries if not (e["day"] == day and e["meal"] == meal)]
    _replace_plan(db, user, week_start, filtered)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


def tool_get_week_nutrition_summary(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    plan_entries = _get_plan_entries(db, user, week_start)
    recipe_ids = {e["recipe_id"] for e in plan_entries}
    recipes: Dict[str, models.Recipe] = {}
    if recipe_ids:
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids), models.Recipe.user_id == user.id
        ).all():
            recipes[r.id] = r

    out: Dict[int, Dict[str, float]] = {d: {"kcal": 0, "p": 0, "f": 0, "c": 0} for d in range(7)}
    for e in plan_entries:
        r = recipes.get(e["recipe_id"])
        if not r or not r.servings:
            continue
        scale = e["servings"] / float(r.servings)
        out[e["day"]]["kcal"] += (r.kcal or 0) * scale
        out[e["day"]]["p"] += (r.p or 0) * scale
        out[e["day"]]["f"] += (r.f or 0) * scale
        out[e["day"]]["c"] += (r.c or 0) * scale

    def rnd(v: float) -> float:
        return round(v * 100) / 100

    return {
        str(d): {"kcal": rnd(v["kcal"]), "p": rnd(v["p"]), "f": rnd(v["f"]), "c": rnd(v["c"])}
        for d, v in out.items()
    }


def tool_get_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    rows = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .order_by(models.ShoppingItem.category, models.ShoppingItem.name)
        .all()
    )
    return [_shopping_item_to_dict(it) for it in rows]


def tool_generate_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))

    plan_rows = (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.user_id == user.id,
            models.MealPlanEntry.week_start == week_start,
        )
        .all()
    )

    if not plan_rows:
        db.query(models.ShoppingItem).filter(
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.is_custom == 0,
        ).delete(synchronize_session=False)
        db.commit()
        return tool_get_shopping_list(db, user, args)

    recipe_ids = {p.recipe_id for p in plan_rows}
    recipes: Dict[str, models.Recipe] = {
        r.id: r
        for r in db.query(models.Recipe)
        .filter(models.Recipe.id.in_(recipe_ids), models.Recipe.user_id == user.id)
        .all()
    }

    aggregate: Dict[Tuple[str, str], float] = {}
    display_name: Dict[Tuple[str, str], str] = {}

    for entry in plan_rows:
        rec = recipes.get(entry.recipe_id)
        if not rec or not rec.servings:
            continue
        scale = (entry.servings or 0) / float(rec.servings)
        for ing in (rec.ingredients or []):
            name = (ing.get("name") or "").strip()
            if not name:
                continue
            qty = float(ing.get("qty") or 0) * scale
            unit_in = (ing.get("unit") or "").strip()
            unit, qty = _normalize_unit_qty(unit_in, qty)
            key = (name.lower(), unit)
            aggregate[key] = aggregate.get(key, 0.0) + qty
            display_name.setdefault(key, name)

    existing = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .all()
    )
    prev_checked: Dict[Tuple[str, str], int] = {
        (it.name.lower(), it.unit): it.checked for it in existing if not it.is_custom
    }

    db.query(models.ShoppingItem).filter(
        models.ShoppingItem.user_id == user.id,
        models.ShoppingItem.week_start == week_start,
        models.ShoppingItem.is_custom == 0,
    ).delete(synchronize_session=False)

    for (name_key, unit), qty in aggregate.items():
        name = display_name[(name_key, unit)]
        db.add(
            models.ShoppingItem(
                user_id=user.id,
                week_start=week_start,
                name=name,
                qty=round(qty, 3),
                unit=unit,
                category=_category_of(name),
                checked=prev_checked.get((name_key, unit), 0),
                is_custom=0,
            )
        )
    db.commit()
    return tool_get_shopping_list(db, user, args)


def tool_check_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    item_id = int(args.get("item_id", 0))
    checked = bool(args.get("checked", False))

    item = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .one_or_none()
    )
    if not item:
        raise ValueError(f"Shopping item not found: {item_id}")
    item.checked = 1 if checked else 0
    db.commit()
    db.refresh(item)
    return _shopping_item_to_dict(item)


def tool_add_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    name = str(args.get("name", "")).strip()
    if not name:
        raise ValueError("name is required")
    unit_in = str(args.get("unit") or "").strip()
    qty_raw = float(args.get("qty") or 0)
    unit, qty = _normalize_unit_qty(unit_in, qty_raw)
    category_arg = args.get("category")
    category = str(category_arg) if category_arg else _category_of(name)

    existing = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.name == name,
            models.ShoppingItem.unit == unit,
        )
        .one_or_none()
    )
    if existing:
        existing.qty = round((existing.qty or 0.0) + qty, 3)
        existing.category = category
        db.commit()
        db.refresh(existing)
        return _shopping_item_to_dict(existing)

    item = models.ShoppingItem(
        user_id=user.id,
        week_start=week_start,
        name=name,
        qty=round(qty, 3),
        unit=unit,
        category=category,
        checked=0,
        is_custom=1,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return _shopping_item_to_dict(item)


def tool_remove_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    item_id = int(args.get("item_id", 0))

    deleted = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .delete(synchronize_session=False)
    )
    if not deleted:
        raise ValueError(f"Shopping item not found: {item_id}")
    db.commit()
    return {"deleted": item_id}


def tool_clear_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    db.query(models.ShoppingItem).filter(
        models.ShoppingItem.user_id == user.id,
        models.ShoppingItem.week_start == week_start,
    ).delete(synchronize_session=False)
    db.commit()
    return {"cleared": week_start}


# ---------------------------------------------------------------------------
# Tool dispatch table
# ---------------------------------------------------------------------------

# Maps tool name → function(db, user, args) → result
TOOL_HANDLERS = {
    "list_recipes": tool_list_recipes,
    "get_recipe": tool_get_recipe,
    "list_tags": tool_list_tags,
    "list_meal_types": tool_list_meal_types,
    "filter_recipes": tool_filter_recipes,
    "create_recipe": tool_create_recipe,
    "update_recipe": tool_update_recipe,
    "delete_recipe": tool_delete_recipe,
    "get_week_plan": tool_get_week_plan,
    "get_current_week_plan": tool_get_current_week_plan,
    "set_week_plan": tool_set_week_plan,
    "add_plan_entry": tool_add_plan_entry,
    "remove_plan_entry": tool_remove_plan_entry,
    "get_week_nutrition_summary": tool_get_week_nutrition_summary,
    "get_shopping_list": tool_get_shopping_list,
    "generate_shopping_list": tool_generate_shopping_list,
    "check_shopping_item": tool_check_shopping_item,
    "add_shopping_item": tool_add_shopping_item,
    "remove_shopping_item": tool_remove_shopping_item,
    "clear_shopping_list": tool_clear_shopping_list,
}

# Tools that mutate data and which "changed" categories they affect
TOOL_CHANGED: Dict[str, List[str]] = {
    "create_recipe": ["recipes"],
    "update_recipe": ["recipes"],
    "delete_recipe": ["recipes", "plan"],
    "set_week_plan": ["plan"],
    "add_plan_entry": ["plan"],
    "remove_plan_entry": ["plan"],
    "generate_shopping_list": ["shopping"],
    "check_shopping_item": ["shopping"],
    "add_shopping_item": ["shopping"],
    "remove_shopping_item": ["shopping"],
    "clear_shopping_list": ["shopping"],
}

# ---------------------------------------------------------------------------
# Tool schema definitions (for LLM)
# ---------------------------------------------------------------------------

TOOL_DEFS = [
    {
        "name": "list_recipes",
        "description": "Zwraca wszystkie przepisy zalogowanego użytkownika.",
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
        "description": "Zwraca przepisy spełniające kryteria.",
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
            "required": ["id", "title"],
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
                "week_start": {"type": "string"},
                "item_id": {"type": "integer"},
                "checked": {"type": "boolean"},
            },
            "required": ["week_start", "item_id", "checked"],
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
                "week_start": {"type": "string"},
                "item_id": {"type": "integer"},
            },
            "required": ["week_start", "item_id"],
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

# OpenAI-style tool definitions
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


# ---------------------------------------------------------------------------
# Helpers for calling tools
# ---------------------------------------------------------------------------

def _call_tool(
    db: Session,
    user: models.User,
    name: str,
    input_args: Dict[str, Any],
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> Tuple[str, bool]:
    """Execute a single tool call. Returns (result_text, is_error)."""
    handler = TOOL_HANDLERS.get(name)
    is_error = False
    if not handler:
        result_text = f"Unknown tool: {name}"
        is_error = True
    else:
        try:
            out = handler(db, user, input_args)
            result_text = json.dumps(out, ensure_ascii=False)
            # Track changed domains
            for domain in TOOL_CHANGED.get(name, []):
                changed_set.add(domain)
        except Exception as exc:
            result_text = str(exc)
            is_error = True

    tool_events.append({
        "tool_use_id": str(uuid.uuid4()),
        "name": name,
        "input": input_args,
        "output": None if is_error else json.loads(result_text) if result_text else None,
        "error": result_text if is_error else None,
    })
    return result_text, is_error


def _is_anthropic(endpoint: str) -> bool:
    return "anthropic.com" in endpoint or "/v1/messages" in endpoint


# ---------------------------------------------------------------------------
# Anthropic agent loop
# ---------------------------------------------------------------------------

async def _run_anthropic(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> str:
    messages = list(history)  # already in Anthropic format (role/content)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    final_text = ""

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": messages,
                "tools": TOOL_DEFS,
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("content"):
                err = data.get("error", {})
                err_msg = err.get("message", JSON_str(data)) if isinstance(err, dict) else str(err)
                raise RuntimeError(f"Anthropic: brak pola 'content' — {err_msg}")

            blocks = data["content"]
            text_blocks = [b["text"] for b in blocks if b.get("type") == "text" and b.get("text")]
            if text_blocks:
                final_text = "\n".join(text_blocks).strip()

            tool_uses = [b for b in blocks if b.get("type") == "tool_use"]
            if data.get("stop_reason") != "tool_use" or not tool_uses:
                break

            messages.append({"role": "assistant", "content": blocks})
            tool_results = []
            for tu in tool_uses:
                tu_id = tu.get("id", str(uuid.uuid4()))
                name = tu.get("name", "")
                input_args = tu.get("input") or {}
                # Override tool_use_id in events with the real one from Anthropic
                # so we can reference it
                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)
                # Re-use same event but fix tool_use_id
                tool_events.append({
                    "tool_use_id": tu_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu_id,
                    "content": result_text,
                    "is_error": is_error,
                })
            messages.append({"role": "user", "content": tool_results})

    return final_text or "(agent przekroczył limit kroków)"


# ---------------------------------------------------------------------------
# OpenAI agent loop
# ---------------------------------------------------------------------------

async def _run_openai(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    history: List[Dict[str, Any]],
    db: Session,
    user: models.User,
    tool_events: List[Dict[str, Any]],
    changed_set: set,
) -> str:
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        *history,
    ]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    final_text = ""

    async with httpx.AsyncClient(timeout=120.0) as client:
        for _step in range(MAX_STEPS):
            body = {
                "model": model,
                "messages": messages,
                "tools": TOOL_DEFS_OPENAI,
            }
            resp = await client.post(endpoint, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()

            choices = data.get("choices") or []
            if not choices:
                err = data.get("error", {})
                err_msg = err.get("message", json.dumps(data)) if isinstance(err, dict) else str(err)
                raise RuntimeError(f"OpenAI: brak pola 'choices' — {err_msg}")

            msg = choices[0]["message"]
            if msg.get("content"):
                final_text = msg["content"].strip()

            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                break

            messages.append(msg)
            for call in tool_calls:
                call_id = call.get("id", str(uuid.uuid4()))
                name = call.get("function", {}).get("name", "")
                try:
                    input_args = json.loads(call.get("function", {}).get("arguments", "{}") or "{}")
                except json.JSONDecodeError:
                    input_args = {}

                result_text, is_error = _call_tool(db, user, name, input_args, [], changed_set)
                tool_events.append({
                    "tool_use_id": call_id,
                    "name": name,
                    "input": input_args,
                    "output": None if is_error else _safe_json_parse(result_text),
                    "error": result_text if is_error else None,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": result_text,
                })

    return final_text or "(agent przekroczył limit kroków)"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_json_parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def JSON_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return str(obj)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_agent(
    db: Session,
    user: models.User,
    settings: models.AgentSettings,
    history: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Run the full agent loop.

    Args:
        db: SQLAlchemy session.
        user: The authenticated user.
        settings: AgentSettings row (for model + system_prompt).
        history: List of messages in provider format: [{"role": ..., "content": ...}].

    Returns:
        {"reply": str, "tool_events": list, "changed": list}
    """
    endpoint = os.environ.get("MEALPILOT_AI_API_URL", "").strip()
    api_key = os.environ.get("MEALPILOT_AI_API_KEY", "").strip()

    if not endpoint:
        return {
            "reply": "❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_URL w ustawieniach Home Assistant.",
            "tool_events": [],
            "changed": [],
        }
    if not api_key:
        return {
            "reply": "❗ Błąd: Brak konfiguracji MEALPILOT_AI_API_KEY w ustawieniach Home Assistant.",
            "tool_events": [],
            "changed": [],
        }

    model = settings.model or ""
    system_prompt = settings.system_prompt or ""

    tool_events: List[Dict[str, Any]] = []
    changed_set: set = set()

    try:
        if _is_anthropic(endpoint):
            reply = await _run_anthropic(
                endpoint, api_key, model, system_prompt,
                history, db, user, tool_events, changed_set,
            )
        else:
            reply = await _run_openai(
                endpoint, api_key, model, system_prompt,
                history, db, user, tool_events, changed_set,
            )
    except httpx.HTTPStatusError as exc:
        resp_text = exc.response.text[:300]
        if resp_text.lstrip().lower().startswith("<!doctype") or resp_text.lstrip().startswith("<html"):
            current_url = os.environ.get("MEALPILOT_AI_API_URL", "")
            reply = (
                f"❗ Błąd konfiguracji: serwer AI zwrócił kod {exc.response.status_code} ze stroną HTML zamiast odpowiedzi API.\n\n"
                f"Skonfigurowany adres URL: {current_url!r}\n\n"
                f"Sprawdź ustawienia dodatku w Home Assistant. Przykładowe poprawne adresy:\n"
                f"• Anthropic: https://api.anthropic.com/v1/messages\n"
                f"• OpenAI: https://api.openai.com/v1/chat/completions\n"
                f"• OpenRouter: https://openrouter.ai/api/v1/chat/completions"
            )
        else:
            reply = f"❗ Błąd: {exc.response.status_code} {resp_text}"
    except Exception as exc:
        reply = f"❗ Błąd: {exc}"

    return {
        "reply": reply,
        "tool_events": tool_events,
        "changed": sorted(changed_set),
    }
