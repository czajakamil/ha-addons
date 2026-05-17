from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from sqlalchemy.orm import Session

from ... import models
from ...ownership import get_household_id, visible_filter
from ..registry import tool

# ---------------------------------------------------------------------------
# Date / unit / category helpers
# ---------------------------------------------------------------------------

def _current_week_start() -> str:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
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


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

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
# Internal plan helpers
# ---------------------------------------------------------------------------

def _get_plan_entries(db: Session, user: models.User, week_start: str) -> List[Dict[str, Any]]:
    rows = (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.created_by == user.id,
            models.MealPlanEntry.week_start == week_start,
        )
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )
    return [_plan_entry_to_dict(e) for e in rows]


def _enrich_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    recipe_ids = {e["recipe_id"] for e in entries}
    recipes: Dict[str, str] = {}
    if recipe_ids:
        hh = get_household_id(db, user.id)
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids),
            visible_filter(models.Recipe, user, hh),
        ).all():
            recipes[r.id] = r.title
    enriched = [
        {**e, "recipe_title": recipes.get(e["recipe_id"], "")}
        for e in entries
    ]
    return {"week_start": week_start, "entries": enriched}


def _replace_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> None:
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.created_by == user.id,
        models.MealPlanEntry.week_start == week_start,
    ).delete(synchronize_session=False)
    for e in entries:
        db.add(
            models.MealPlanEntry(
                created_by=user.id,
                owner_user_id=user.id,
                owner_household_id=None,
                week_start=week_start,
                day=int(e["day"]),
                meal=str(e["meal"]),
                recipe_id=str(e["recipe_id"]),
                servings=int(e.get("servings") or 1),
            )
        )
    db.commit()


# ---------------------------------------------------------------------------
# Shared schema fragments
# ---------------------------------------------------------------------------

_PLAN_ENTRY_SCHEMA = {
    "type": "object",
    "properties": {
        "day": {"type": "integer", "minimum": 0, "maximum": 6},
        "meal": {"type": "string"},
        "recipe_id": {"type": "string"},
        "servings": {"type": "integer"},
    },
    "required": ["day", "meal", "recipe_id", "servings"],
}


# ---------------------------------------------------------------------------
# Plan tools
# ---------------------------------------------------------------------------

@tool(
    name="get_week_plan",
    description="Plan posiłków na dany tydzień. week_start = poniedziałek (YYYY-MM-DD).",
    input_schema={
        "type": "object",
        "properties": {"week_start": {"type": "string"}},
        "required": ["week_start"],
    },
)
def tool_get_week_plan(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, entries)


@tool(
    name="get_current_week_plan",
    description="Plan na bieżący tydzień (week_start liczony automatycznie).",
    input_schema={"type": "object", "properties": {}},
)
def tool_get_current_week_plan(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    week_start = _current_week_start()
    entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, entries)


@tool(
    name="set_week_plan",
    description="Zastępuje cały plan tygodnia.",
    input_schema={
        "type": "object",
        "properties": {
            "week_start": {"type": "string"},
            "entries": {"type": "array", "items": _PLAN_ENTRY_SCHEMA},
        },
        "required": ["week_start", "entries"],
    },
    changes=["plan"],
)
def tool_set_week_plan(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    entries = list(args.get("entries") or [])
    _replace_plan(db, user, week_start, entries)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


@tool(
    name="add_plan_entry",
    description="Dodaje jeden slot do planu.",
    input_schema={
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
    changes=["plan"],
)
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


@tool(
    name="remove_plan_entry",
    description="Usuwa slot (dzień + posiłek) z planu.",
    input_schema={
        "type": "object",
        "properties": {
            "week_start": {"type": "string"},
            "day": {"type": "integer", "minimum": 0, "maximum": 6},
            "meal": {"type": "string"},
        },
        "required": ["week_start", "day", "meal"],
    },
    changes=["plan"],
)
def tool_remove_plan_entry(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    day = int(args.get("day", 0))
    meal = str(args.get("meal", ""))

    entries = _get_plan_entries(db, user, week_start)
    filtered = [e for e in entries if not (e["day"] == day and e["meal"] == meal)]
    _replace_plan(db, user, week_start, filtered)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


@tool(
    name="get_week_nutrition_summary",
    description="Suma kcal/białka/tłuszczu/węglowodanów dla każdego dnia tygodnia.",
    input_schema={
        "type": "object",
        "properties": {"week_start": {"type": "string"}},
        "required": ["week_start"],
    },
)
def tool_get_week_nutrition_summary(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    plan_entries = _get_plan_entries(db, user, week_start)
    recipe_ids = {e["recipe_id"] for e in plan_entries}
    recipes: Dict[str, models.Recipe] = {}
    if recipe_ids:
        hh = get_household_id(db, user.id)
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids),
            visible_filter(models.Recipe, user, hh),
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


# ---------------------------------------------------------------------------
# Shopping tools
# ---------------------------------------------------------------------------

@tool(
    name="get_shopping_list",
    description="Aktualna lista zakupów na dany tydzień.",
    input_schema={
        "type": "object",
        "properties": {"week_start": {"type": "string"}},
        "required": ["week_start"],
    },
)
def tool_get_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    rows = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.created_by == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .order_by(models.ShoppingItem.category, models.ShoppingItem.name)
        .all()
    )
    return [_shopping_item_to_dict(it) for it in rows]


@tool(
    name="generate_shopping_list",
    description="Regeneruje listę zakupów z planu tygodnia.",
    input_schema={
        "type": "object",
        "properties": {"week_start": {"type": "string"}},
        "required": ["week_start"],
    },
    changes=["shopping"],
)
def tool_generate_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))

    plan_rows = (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.created_by == user.id,
            models.MealPlanEntry.week_start == week_start,
        )
        .all()
    )

    if not plan_rows:
        db.query(models.ShoppingItem).filter(
            models.ShoppingItem.created_by == user.id,
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.is_custom == 0,
        ).delete(synchronize_session=False)
        db.commit()
        return tool_get_shopping_list(db, user, args)

    recipe_ids = {p.recipe_id for p in plan_rows}
    hh = get_household_id(db, user.id)
    recipes: Dict[str, models.Recipe] = {
        r.id: r
        for r in db.query(models.Recipe)
        .filter(
            models.Recipe.id.in_(recipe_ids),
            visible_filter(models.Recipe, user, hh),
        )
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
            models.ShoppingItem.created_by == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .all()
    )
    prev_checked: Dict[Tuple[str, str], int] = {
        (it.name.lower(), it.unit): it.checked for it in existing if not it.is_custom
    }

    db.query(models.ShoppingItem).filter(
        models.ShoppingItem.created_by == user.id,
        models.ShoppingItem.week_start == week_start,
        models.ShoppingItem.is_custom == 0,
    ).delete(synchronize_session=False)

    for (name_key, unit), qty in aggregate.items():
        name = display_name[(name_key, unit)]
        db.add(
            models.ShoppingItem(
                created_by=user.id,
                owner_user_id=user.id,
                owner_household_id=None,
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


@tool(
    name="check_shopping_item",
    description="Oznacza pozycję jako kupioną lub odznacza.",
    input_schema={
        "type": "object",
        "properties": {
            "week_start": {"type": "string"},
            "item_id": {"type": "integer"},
            "checked": {"type": "boolean"},
        },
        "required": ["week_start", "item_id", "checked"],
    },
    changes=["shopping"],
)
def tool_check_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    item_id = int(args.get("item_id", 0))
    checked = bool(args.get("checked", False))

    item = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            models.ShoppingItem.created_by == user.id,
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


@tool(
    name="add_shopping_item",
    description="Dodaje własną pozycję do listy zakupów.",
    input_schema={
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
    changes=["shopping"],
)
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
            models.ShoppingItem.created_by == user.id,
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
        created_by=user.id,
        owner_user_id=user.id,
        owner_household_id=None,
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


@tool(
    name="remove_shopping_item",
    description="Usuwa pojedynczą pozycję z listy zakupów.",
    input_schema={
        "type": "object",
        "properties": {
            "week_start": {"type": "string"},
            "item_id": {"type": "integer"},
        },
        "required": ["week_start", "item_id"],
    },
    changes=["shopping"],
)
def tool_remove_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    item_id = int(args.get("item_id", 0))

    deleted = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            models.ShoppingItem.created_by == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .delete(synchronize_session=False)
    )
    if not deleted:
        raise ValueError(f"Shopping item not found: {item_id}")
    db.commit()
    return {"deleted": item_id}


@tool(
    name="clear_shopping_list",
    description="Usuwa wszystkie pozycje listy zakupów danego tygodnia.",
    input_schema={
        "type": "object",
        "properties": {"week_start": {"type": "string"}},
        "required": ["week_start"],
    },
    changes=["shopping"],
)
def tool_clear_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    db.query(models.ShoppingItem).filter(
        models.ShoppingItem.created_by == user.id,
        models.ShoppingItem.week_start == week_start,
    ).delete(synchronize_session=False)
    db.commit()
    return {"cleared": week_start}
