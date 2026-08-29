"""Shopping-list domain services.

Regeneration reconciles rows in place rather than deleting and re-inserting, so
a household list stays a household list and manual check marks survive.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .. import models
from ..ownership import default_owner_kwargs
from .common import (
    assert_can_edit,
    attach_recipe_source,
    category_of,
    delete_recipe_sources_for_items,
    normalize_unit_qty,
    parse_week_start,
    require_editable,
    shopping_item_to_dict,
    visible_query,
)
from .errors import Invalid


def _items(db: Session, user: models.User, week_start: str) -> list[models.ShoppingItem]:
    return (
        visible_query(db, user, models.ShoppingItem)
        .filter(models.ShoppingItem.week_start == week_start)
        .order_by(models.ShoppingItem.category, models.ShoppingItem.name)
        .all()
    )


def _serialized(db: Session, user: models.User, week_start: str) -> dict[str, Any]:
    rows = _items(db, user, week_start)
    return {
        "week_start": week_start,
        "items": [shopping_item_to_dict(it) for it in rows],
        "total": len(rows),
        "checked": sum(1 for it in rows if it.checked),
    }


def _aggregate_from_plan(db: Session, user: models.User, week_start: str) -> tuple[dict, dict, dict]:
    plan_rows = (
        visible_query(db, user, models.MealPlanEntry).filter(models.MealPlanEntry.week_start == week_start).all()
    )
    aggregate: dict[tuple[str, str], float] = {}
    display_name: dict[tuple[str, str], str] = {}
    sources: dict[tuple[str, str], set[str]] = {}
    if not plan_rows:
        return aggregate, display_name, sources

    recipe_ids = {p.recipe_id for p in plan_rows}
    recipes = {r.id: r for r in visible_query(db, user, models.Recipe).filter(models.Recipe.id.in_(recipe_ids)).all()}

    for entry in plan_rows:
        rec = recipes.get(entry.recipe_id)
        if not rec or not rec.servings:
            continue
        scale = (entry.servings or 0) / float(rec.servings)
        for ing in rec.ingredients or []:
            if not isinstance(ing, dict):
                continue
            name = (ing.get("name") or "").strip()
            if not name:
                continue
            qty = float(ing.get("qty") or 0) * scale
            unit, qty = normalize_unit_qty((ing.get("unit") or "").strip(), qty)
            key = (name.lower(), unit)
            aggregate[key] = aggregate.get(key, 0.0) + qty
            display_name.setdefault(key, name)
            sources.setdefault(key, set()).add(rec.id)
    return aggregate, display_name, sources


def regenerate_auto_shopping(db: Session, user: models.User, week_start: str) -> None:
    """Rebuild the generated (``is_custom=0``) part of a week's list from the plan.

    Manual items are never touched. Generated rows that survive keep their id,
    owner and checked state; only quantity, category and provenance are rewritten.
    """
    aggregate, display_name, sources = _aggregate_from_plan(db, user, week_start)

    generated = [it for it in _items(db, user, week_start) if not it.is_custom]
    by_key: dict[tuple[str, str], models.ShoppingItem] = {}
    stale: list[models.ShoppingItem] = []
    for it in generated:
        key = (it.name.lower(), it.unit)
        if key in by_key or key not in aggregate:
            stale.append(it)
        else:
            by_key[key] = it

    if stale:
        for it in stale:
            assert_can_edit(db, user, it)
            db.delete(it)  # cascades to shopping_item_recipes
        db.flush()

    for key, qty in aggregate.items():
        name = display_name[key]
        item = by_key.get(key)
        if item is None:
            item = models.ShoppingItem(
                created_by=user.id,
                **default_owner_kwargs(user),
                week_start=week_start,
                name=name,
                qty=round(qty, 3),
                unit=key[1],
                category=category_of(name),
                checked=0,
                is_custom=0,
            )
            db.add(item)
            db.flush()
        else:
            assert_can_edit(db, user, item)
            item.name = name
            item.qty = round(qty, 3)
            item.category = category_of(name)
            delete_recipe_sources_for_items(db, [item.id])
            db.flush()
        for recipe_id in sorted(sources.get(key, ())):
            attach_recipe_source(db, item, recipe_id)
    db.commit()


# --------------------------------------------------------------------------- #
# Tool-facing operations
# --------------------------------------------------------------------------- #


def get_shopping_list(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    return _serialized(db, user, week_start)


def generate_shopping_list(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    regenerate_auto_shopping(db, user, week_start)
    return _serialized(db, user, week_start)


def check_shopping_item(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    item_id = _coerce_item_id(args.get("item_id"))
    item = require_editable(db, user, models.ShoppingItem, item_id)
    item.checked = 1 if bool(args.get("checked", False)) else 0
    db.commit()
    db.refresh(item)
    return shopping_item_to_dict(item)


def add_shopping_item(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    name = str(args.get("name", "")).strip()
    if not name:
        raise Invalid("name jest wymagane.")
    try:
        qty_raw = float(args.get("qty") if args.get("qty") is not None else 1)
    except (TypeError, ValueError) as exc:
        raise Invalid("qty musi być liczbą.") from exc
    unit, qty = normalize_unit_qty(str(args.get("unit") or "szt").strip(), qty_raw)
    category = str(args["category"]) if args.get("category") else category_of(name)
    recipe_id = str(args.get("recipe_id") or "").strip() or None

    existing = (
        visible_query(db, user, models.ShoppingItem)
        .filter(
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.name == name,
            models.ShoppingItem.unit == unit,
        )
        .first()
    )
    if existing:
        assert_can_edit(db, user, existing)
        existing.qty = round((existing.qty or 0.0) + qty, 3)
        existing.category = category
        attach_recipe_source(db, existing, recipe_id)
        db.commit()
        db.refresh(existing)
        return shopping_item_to_dict(existing)

    item = models.ShoppingItem(
        created_by=user.id,
        **default_owner_kwargs(user),
        week_start=week_start,
        name=name,
        qty=round(qty, 3),
        unit=unit,
        category=category,
        checked=0,
        is_custom=1,
    )
    db.add(item)
    db.flush()
    attach_recipe_source(db, item, recipe_id)
    db.commit()
    db.refresh(item)
    return shopping_item_to_dict(item)


def delete_shopping_item(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    item_id = _coerce_item_id(args.get("item_id"))
    item = require_editable(db, user, models.ShoppingItem, item_id)
    db.delete(item)  # cascades to shopping_item_recipes
    db.commit()
    return {"deleted": item_id}


def clear_shopping_list(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    rows = _items(db, user, week_start)
    for it in rows:
        assert_can_edit(db, user, it)
    for it in rows:
        db.delete(it)  # cascades to shopping_item_recipes
    db.commit()
    return {"cleared": week_start, "removed": len(rows)}


def _coerce_item_id(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid("item_id musi być liczbą całkowitą — użyj pola `id` z get_shopping_list.") from exc
