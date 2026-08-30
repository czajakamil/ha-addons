"""Shopping-list domain services.

Regeneration reconciles rows in place rather than deleting and re-inserting, so
a household list stays a household list and manual check marks survive.

Ownership of *new* rows is inherited from the plan the list is generated from,
not from whoever pressed "generate":

  * a line is **household-owned** when at least one plan entry that contributed
    to it is household-owned (``owner_household_id`` set). One shared recipe in
    the week is enough — a shared plan must produce one shared list, otherwise
    every member silently builds a private copy of the same shopping list and
    nobody's check marks are visible to anyone else.
  * a line is **personal** when every contributing plan entry is personal, and
    for users without a household (unchanged behaviour).
  * a manual item joins whichever bucket the rest of the week is in: household
    if any visible shopping row or plan entry for that week is household-owned.

Ownership of rows that already exist is never rewritten here — re-pinning a
resource between personal and household is the creator's call
(``ownership.can_change_owner``), so a list generated before a plan was shared
stays where it is until somebody deletes those rows.

Because a household row can now be created by one member and merged into by
another, the old ``uq_shop_user_week_name_unit`` unique constraint (keyed on
``created_by``) stopped being the real identity of a line and was dropped by
Alembic revision ``0002_drop_dead_schema``. Deduplication is done in this module
instead: lookups go through ``visible_query``, so a member finds and merges with
a household row another member inserted. There is no database-level guard any
more — every insert path here has to go through this module.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from ..ownership import default_owner_kwargs, get_household_id, get_membership, household_owner_kwargs
from .common import (
    assert_can_edit,
    attach_recipe_source,
    category_of,
    coerce_recipe_id,
    delete_recipe_sources_for_items,
    normalize_unit_qty,
    parse_week_start,
    require_editable,
    require_visible,
    shopping_item_to_dict,
    visible_query,
)
from .errors import Forbidden, Invalid, NotFound


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


def _item_key(item: models.ShoppingItem) -> tuple[str, str]:
    return (item.name.lower(), item.unit)


def _week_household_id(db: Session, user: models.User, week_start: str) -> int | None:
    """Household id when this week's list is a shared one, else ``None``.

    A week counts as shared as soon as one visible shopping row *or* one visible
    plan entry for it belongs to the household — that is the bucket a manually
    added item has to land in to stay on the same list as everything else.
    """
    hh = get_household_id(db, user.id)
    if hh is None:
        return None
    shared_item = (
        visible_query(db, user, models.ShoppingItem)
        .filter(
            models.ShoppingItem.week_start == week_start,
            models.ShoppingItem.owner_household_id.isnot(None),
        )
        .first()
    )
    if shared_item is not None:
        return hh
    shared_entry = (
        visible_query(db, user, models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.week_start == week_start,
            models.MealPlanEntry.owner_household_id.isnot(None),
        )
        .first()
    )
    return hh if shared_entry is not None else None


def _owner_kwargs(user: models.User, household_id: int | None) -> dict[str, Any]:
    return household_owner_kwargs(household_id) if household_id is not None else default_owner_kwargs(user)


def _assert_may_write_to_bucket(db: Session, user: models.User, household_id: int | None) -> None:
    """A new row on a *shared* week is a write to the household's list.

    Before ownership inheritance every insert was private, so creating one never
    needed a permission check. Now it can land on everyone's list, which is
    exactly what ``can_edit`` gates.
    """
    if household_id is None:
        return
    member = get_membership(db, user.id)
    if member is None or not member.can_edit:
        raise Forbidden(
            "Brak uprawnień do edycji tej listy zakupów (lista należy do household, a nie masz prawa edycji)."
        )


def _aggregate_from_plan(db: Session, user: models.User, week_start: str) -> tuple[dict, dict, dict, set]:
    """Aggregate the week's plan into shopping lines.

    Returns ``(qty_by_key, display_name_by_key, source_recipe_ids_by_key,
    keys_fed_by_a_household_plan_entry)``.
    """
    plan_rows = (
        visible_query(db, user, models.MealPlanEntry).filter(models.MealPlanEntry.week_start == week_start).all()
    )
    aggregate: dict[tuple[str, str], float] = {}
    display_name: dict[tuple[str, str], str] = {}
    sources: dict[tuple[str, str], set[int]] = {}
    household_keys: set[tuple[str, str]] = set()
    if not plan_rows:
        return aggregate, display_name, sources, household_keys

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
            if entry.owner_household_id is not None:
                household_keys.add(key)
    return aggregate, display_name, sources, household_keys


def regenerate_auto_shopping(db: Session, user: models.User, week_start: str) -> None:
    """Rebuild the generated (``is_custom=0``) part of a week's list from the plan.

    Manual items are never touched, and a line already held by a manual row is
    skipped entirely: that row owns the line (see ``add_shopping_item``), so
    re-creating a generated twin would clobber the manual quantity and leave two
    rows describing the same line.

    Generated rows that survive keep their id, owner and checked state; only
    quantity, category and provenance are rewritten. New rows inherit the plan's
    ownership — household when a household plan entry fed the line.
    """
    aggregate, display_name, sources, household_keys = _aggregate_from_plan(db, user, week_start)
    week_household_id = get_household_id(db, user.id)
    if household_keys and week_household_id is not None:
        _assert_may_write_to_bucket(db, user, week_household_id)

    rows = _items(db, user, week_start)
    manual_keys = {_item_key(it) for it in rows if it.is_custom}
    # Household rows first, then oldest: when a personal leftover and a shared
    # row describe the same line, the shared one is the survivor.
    generated = sorted(
        (it for it in rows if not it.is_custom),
        key=lambda it: (1 if it.owner_household_id is None else 0, it.id),
    )
    by_key: dict[tuple[str, str], models.ShoppingItem] = {}
    stale: list[models.ShoppingItem] = []
    for it in generated:
        key = _item_key(it)
        if key in by_key or key in manual_keys or key not in aggregate:
            stale.append(it)
        else:
            by_key[key] = it

    if stale:
        for it in stale:
            assert_can_edit(db, user, it)
            db.delete(it)  # cascades to shopping_item_recipes
        db.flush()

    for key, qty in aggregate.items():
        if key in manual_keys:
            continue
        name = display_name[key]
        item = by_key.get(key)
        if item is None:
            item = models.ShoppingItem(
                created_by=user.id,
                **_owner_kwargs(user, week_household_id if key in household_keys else None),
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
    """Tick a line off. Seeing the list is enough — ``can_edit`` is not required.

    Checking is a shopping-trip gesture, not an edit of what the household
    decided to buy: a member who may only read the plan still walks the aisles.
    Adding, deleting and clearing keep requiring edit rights.
    """
    item_id = _coerce_item_id(args.get("item_id"))
    item = require_visible(db, user, models.ShoppingItem, item_id)
    _assert_in_week(item, args.get("week_start"))
    item.checked = 1 if bool(args.get("checked", False)) else 0
    db.commit()
    db.refresh(item)
    return shopping_item_to_dict(item)


def add_shopping_item(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    """Add a manual item, or top up the quantity of the line that already exists.

    Topping up a *generated* line turns it into a manual one (``is_custom=1``).
    The quantity column is the only place a manual surplus can live — there is no
    separate "extra" field — so leaving the row generated would let the next
    ``generate_shopping_list`` overwrite ``qty`` with the plan's number and drop
    the surplus without a trace. The trade-off is deliberate: from then on the
    line is the user's and stops being recalculated from the plan; delete it and
    regenerate to get automatic quantities back.
    """
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
    # Provenance is optional here: absent, null and "" all mean "no source recipe".
    recipe_id = coerce_recipe_id(args["recipe_id"]) if args.get("recipe_id") else None

    # Matched case-insensitively, the way regeneration keys its lines — otherwise
    # "Ryż" and "ryż" become two rows describing the same purchase.
    existing = (
        visible_query(db, user, models.ShoppingItem)
        .filter(
            models.ShoppingItem.week_start == week_start,
            func.lower(models.ShoppingItem.name) == name.lower(),
            models.ShoppingItem.unit == unit,
        )
        .first()
    )
    if existing:
        assert_can_edit(db, user, existing)
        existing.qty = round((existing.qty or 0.0) + qty, 3)
        existing.category = category
        existing.is_custom = 1
        attach_recipe_source(db, existing, recipe_id)
        db.commit()
        db.refresh(existing)
        return shopping_item_to_dict(existing)

    bucket = _week_household_id(db, user, week_start)
    _assert_may_write_to_bucket(db, user, bucket)
    item = models.ShoppingItem(
        created_by=user.id,
        **_owner_kwargs(user, bucket),
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
    _assert_in_week(item, args.get("week_start"))
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


def _assert_in_week(item: models.ShoppingItem, week_start: Any) -> None:
    """Guard the week-scoped REST routes: ``/{week_start}/items/{item_id}``.

    Those routes used to ignore the week entirely, so an item from week X could
    be ticked off or deleted through week Y's URL. The path is part of the
    resource's identity, so a mismatch is a 404 rather than a silent success.
    ``week_start=None`` means "caller did not scope the request" (the
    ``/items/{item_id}`` routes and the tool layer) and skips the check.
    """
    if week_start is None:
        return
    expected = parse_week_start(week_start)
    if item.week_start != expected:
        raise NotFound(f"Pozycja {item.id} nie należy do tygodnia {expected} (jest w {item.week_start}).")


def _coerce_item_id(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid("item_id musi być liczbą całkowitą — użyj pola `id` z get_shopping_list.") from exc
