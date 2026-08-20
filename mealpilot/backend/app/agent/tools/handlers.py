"""Tool handler implementations — direct DB access."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from sqlalchemy.orm import Session

from ... import models
from ...ownership import visible_filter, get_household_id, default_owner_kwargs
from ..helpers import (
    attach_recipe_source,
    category_of,
    current_week_start,
    delete_recipe_sources_for_items,
    normalize_meal,
    normalize_unit_qty,
    plan_entry_to_dict,
    recipe_to_dict,
    recipe_to_summary,
    shopping_item_to_dict,
    slugify,
)


def tool_list_recipes(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
    return [recipe_to_summary(r) for r in rows]


def tool_get_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    hh = get_household_id(db, user.id)
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, visible_filter(models.Recipe, user, hh))
        .one_or_none()
    )
    if not r:
        raise ValueError(f"Recipe not found: {recipe_id}")
    return recipe_to_dict(r)


def tool_list_tags(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
    out: set = set()
    for r in rows:
        for t in (r.tags or []):
            if isinstance(t, str) and t:
                out.add(t)
    return {"tags": sorted(out)}


def tool_list_meal_types(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
    out: set = set()
    for r in rows:
        for m in (r.meal_types or []):
            if isinstance(m, str) and m:
                out.add(m)
    return {"meal_types": sorted(out)}


def tool_filter_recipes(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    hh = get_household_id(db, user.id)
    rows = db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()
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
        result.append(recipe_to_summary(r))
    return result


def tool_create_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    title = str(args.get("title", ""))
    recipe_id = slugify(str(args.get("id", ""))) or slugify(title)
    if not recipe_id:
        raise ValueError("title is required to generate a recipe id")
    if db.get(models.Recipe, recipe_id):
        base = recipe_id
        n = 2
        while db.get(models.Recipe, f"{base}-{n}"):
            n += 1
        recipe_id = f"{base}-{n}"
    ingredients = list(args.get("ingredients") or [])
    r = models.Recipe(
        created_by=user.id,
        **default_owner_kwargs(user),
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
    return recipe_to_dict(r)


def tool_update_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    hh = get_household_id(db, user.id)
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, visible_filter(models.Recipe, user, hh))
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
                value = list(value)
            setattr(r, key, value)
    db.commit()
    db.refresh(r)
    return recipe_to_dict(r)


def tool_delete_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    hh = get_household_id(db, user.id)
    r = (
        db.query(models.Recipe)
        .filter(models.Recipe.id == recipe_id, visible_filter(models.Recipe, user, hh))
        .one_or_none()
    )
    if not r:
        raise ValueError(f"Recipe not found: {recipe_id}")
    affected_weeks = {
        w for (w,) in db.query(models.MealPlanEntry.week_start).filter(
            models.MealPlanEntry.recipe_id == recipe_id,
            visible_filter(models.MealPlanEntry, user, hh),
        ).distinct().all()
    }
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.recipe_id == recipe_id,
        visible_filter(models.MealPlanEntry, user, hh),
    ).delete(synchronize_session=False)
    db.query(models.ShoppingItemRecipe).filter(
        models.ShoppingItemRecipe.recipe_id == recipe_id
    ).delete(synchronize_session=False)
    db.delete(r)
    db.commit()
    for week_start in affected_weeks:
        regenerate_auto_shopping(db, user, week_start)
    return {"deleted": recipe_id}


def _get_plan_entries(db: Session, user: models.User, week_start: str) -> List[Dict[str, Any]]:
    hh = get_household_id(db, user.id)
    rows = (
        db.query(models.MealPlanEntry)
        .filter(
            visible_filter(models.MealPlanEntry, user, hh),
            models.MealPlanEntry.week_start == week_start,
        )
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )
    return [plan_entry_to_dict(e) for e in rows]


def _enrich_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    hh = get_household_id(db, user.id)
    recipe_ids = {e["recipe_id"] for e in entries}
    recipes = {}
    if recipe_ids:
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids), visible_filter(models.Recipe, user, hh)
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
    week_start = current_week_start()
    entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, entries)


def _replace_plan(db: Session, user: models.User, week_start: str, entries: List[Dict[str, Any]]) -> None:
    hh = get_household_id(db, user.id)
    recipe_ids = {str(e["recipe_id"]) for e in entries if e.get("recipe_id")}
    if recipe_ids:
        visible_ids = {
            r.id
            for r in db.query(models.Recipe.id).filter(
                models.Recipe.id.in_(recipe_ids),
                visible_filter(models.Recipe, user, hh),
            ).all()
        }
        missing = recipe_ids - visible_ids
        if missing:
            raise ValueError(f"Recipe not found or not visible: {', '.join(sorted(missing))}")
    db.query(models.MealPlanEntry).filter(
        visible_filter(models.MealPlanEntry, user, hh),
        models.MealPlanEntry.week_start == week_start,
    ).delete(synchronize_session=False)
    for e in entries:
        db.add(
            models.MealPlanEntry(
                created_by=user.id,
                **default_owner_kwargs(user),
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
    raw_entries = list(args.get("entries") or [])
    entries = [{**e, "meal": normalize_meal(str(e.get("meal", "")))} for e in raw_entries]
    _replace_plan(db, user, week_start, entries)
    result_entries = _get_plan_entries(db, user, week_start)
    return _enrich_plan(db, user, week_start, result_entries)


def tool_add_plan_entry(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    day = int(args.get("day", 0))
    meal = normalize_meal(str(args.get("meal", "")))
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
    meal = normalize_meal(str(args.get("meal", "")))

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
        hh = get_household_id(db, user.id)
        for r in db.query(models.Recipe).filter(
            models.Recipe.id.in_(recipe_ids), visible_filter(models.Recipe, user, hh)
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
    hh = get_household_id(db, user.id)
    rows = (
        db.query(models.ShoppingItem)
        .filter(
            visible_filter(models.ShoppingItem, user, hh),
            models.ShoppingItem.week_start == week_start,
        )
        .order_by(models.ShoppingItem.category, models.ShoppingItem.name)
        .all()
    )
    return [shopping_item_to_dict(it) for it in rows]


def regenerate_auto_shopping(db: Session, user: models.User, week_start: str) -> None:
    hh = get_household_id(db, user.id)
    plan_rows = (
        db.query(models.MealPlanEntry)
        .filter(
            visible_filter(models.MealPlanEntry, user, hh),
            models.MealPlanEntry.week_start == week_start,
        )
        .all()
    )

    def _delete_generated_items() -> None:
        old_ids = [
            i
            for (i,) in db.query(models.ShoppingItem.id)
            .filter(
                visible_filter(models.ShoppingItem, user, hh),
                models.ShoppingItem.week_start == week_start,
                models.ShoppingItem.is_custom == 0,
            )
            .all()
        ]
        delete_recipe_sources_for_items(db, old_ids)
        db.query(models.ShoppingItem).filter(
            visible_filter(models.ShoppingItem, user, hh),
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.is_custom == 0,
        ).delete(synchronize_session="fetch")

    if not plan_rows:
        _delete_generated_items()
        db.commit()
        return

    recipe_ids = {p.recipe_id for p in plan_rows}
    recipes: Dict[str, models.Recipe] = {
        r.id: r
        for r in db.query(models.Recipe)
        .filter(models.Recipe.id.in_(recipe_ids), visible_filter(models.Recipe, user, hh))
        .all()
    }

    aggregate: Dict[Tuple[str, str], float] = {}
    display_name: Dict[Tuple[str, str], str] = {}
    sources: Dict[Tuple[str, str], set] = {}

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
            unit, qty = normalize_unit_qty(unit_in, qty)
            key = (name.lower(), unit)
            aggregate[key] = aggregate.get(key, 0.0) + qty
            display_name.setdefault(key, name)
            sources.setdefault(key, set()).add(rec.id)

    existing = (
        db.query(models.ShoppingItem)
        .filter(
            visible_filter(models.ShoppingItem, user, hh),
            models.ShoppingItem.week_start == week_start,
        )
        .all()
    )
    prev_checked: Dict[Tuple[str, str], int] = {
        (it.name.lower(), it.unit): it.checked for it in existing if not it.is_custom
    }

    _delete_generated_items()

    for key, qty in aggregate.items():
        name = display_name[key]
        item = models.ShoppingItem(
            created_by=user.id,
            **default_owner_kwargs(user),
            week_start=week_start,
            name=name,
            qty=round(qty, 3),
            unit=key[1],
            category=category_of(name),
            checked=prev_checked.get(key, 0),
            is_custom=0,
        )
        db.add(item)
        db.flush()
        for recipe_id in sources.get(key, ()):
            attach_recipe_source(db, item, recipe_id)
    db.commit()


def tool_generate_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    regenerate_auto_shopping(db, user, week_start)
    return tool_get_shopping_list(db, user, args)


def tool_check_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    item_id = int(args.get("item_id", 0))
    checked = bool(args.get("checked", False))

    hh = get_household_id(db, user.id)
    item = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            visible_filter(models.ShoppingItem, user, hh),
        )
        .one_or_none()
    )
    if not item:
        raise ValueError(f"Shopping item not found: {item_id}")
    item.checked = 1 if checked else 0
    db.commit()
    db.refresh(item)
    return shopping_item_to_dict(item)


def tool_add_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    name = str(args.get("name", "")).strip()
    if not name:
        raise ValueError("name is required")
    unit_in = str(args.get("unit") or "").strip()
    qty_raw = float(args.get("qty") or 0)
    unit, qty = normalize_unit_qty(unit_in, qty_raw)
    category_arg = args.get("category")
    category = str(category_arg) if category_arg else category_of(name)

    hh = get_household_id(db, user.id)
    existing = (
        db.query(models.ShoppingItem)
        .filter(
            visible_filter(models.ShoppingItem, user, hh),
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
    db.commit()
    db.refresh(item)
    return shopping_item_to_dict(item)


def tool_remove_shopping_item(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    item_id = int(args.get("item_id", 0))

    hh = get_household_id(db, user.id)
    item = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.id == item_id,
            visible_filter(models.ShoppingItem, user, hh),
        )
        .one_or_none()
    )
    if not item:
        raise ValueError(f"Shopping item not found: {item_id}")
    delete_recipe_sources_for_items(db, [item_id])
    db.delete(item)
    db.commit()
    return {"deleted": item_id}


def tool_clear_shopping_list(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    week_start = str(args.get("week_start", ""))
    hh = get_household_id(db, user.id)
    ids = [
        i
        for (i,) in db.query(models.ShoppingItem.id)
        .filter(
            visible_filter(models.ShoppingItem, user, hh),
            models.ShoppingItem.week_start == week_start,
        )
        .all()
    ]
    delete_recipe_sources_for_items(db, ids)
    db.query(models.ShoppingItem).filter(
        visible_filter(models.ShoppingItem, user, hh),
        models.ShoppingItem.week_start == week_start,
    ).delete(synchronize_session=False)
    db.commit()
    return {"cleared": week_start}
