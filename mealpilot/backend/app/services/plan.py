"""Week-plan domain services.

Replacing a week used to be "delete every visible entry, insert fresh personal
rows", which silently converted a household plan into a private one. Here the
week is *reconciled* slot by slot, so each row keeps the owner it already had
and household rows are only touched by members allowed to edit them.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .. import models
from ..ownership import default_owner_kwargs
from .common import (
    assert_can_edit,
    current_week_start,
    normalize_meal,
    parse_week_start,
    plan_entry_to_dict,
    visible_query,
)
from .errors import Invalid

MEALS_PER_DAY_HINT = "day musi być liczbą 0–6 (0=poniedziałek, 6=niedziela)."


def _coerce_day(value: Any) -> int:
    try:
        day = int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid(MEALS_PER_DAY_HINT) from exc
    if not 0 <= day <= 6:
        raise Invalid(MEALS_PER_DAY_HINT)
    return day


def _coerce_servings(value: Any) -> int:
    if value is None:
        return 1
    try:
        servings = int(value)
    except (TypeError, ValueError) as exc:
        raise Invalid("servings musi być dodatnią liczbą całkowitą.") from exc
    if servings < 1:
        raise Invalid("servings musi być >= 1.")
    return servings


def _entry_rows(db: Session, user: models.User, week_start: str) -> list[models.MealPlanEntry]:
    return (
        visible_query(db, user, models.MealPlanEntry)
        .filter(models.MealPlanEntry.week_start == week_start)
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )


def _enrich(db: Session, user: models.User, week_start: str, rows: list[models.MealPlanEntry]) -> dict[str, Any]:
    entries = [plan_entry_to_dict(e) for e in rows]
    recipe_ids = {e["recipe_id"] for e in entries}
    titles: dict[str, str] = {}
    if recipe_ids:
        titles = {
            r.id: r.title for r in visible_query(db, user, models.Recipe).filter(models.Recipe.id.in_(recipe_ids)).all()
        }
    return {
        "week_start": week_start,
        "entries": [{**e, "recipe_title": titles.get(e["recipe_id"], "")} for e in entries],
    }


def _assert_recipes_visible(db: Session, user: models.User, recipe_ids: set[str]) -> None:
    if not recipe_ids:
        return
    found = {r.id for r in visible_query(db, user, models.Recipe).filter(models.Recipe.id.in_(recipe_ids)).all()}
    missing = recipe_ids - found
    if missing:
        raise Invalid(
            "Nieznane lub niewidoczne recipe_id: "
            + ", ".join(sorted(missing))
            + ". Pobierz aktualną listę (search_recipes / list_recipes) i użyj id z odpowiedzi."
        )


def _stored_entries(rows: list[models.MealPlanEntry]) -> list[dict[str, Any]]:
    """Existing rows as desired-state dicts, without re-validating stored data."""
    return [
        {
            "day": e.day,
            "meal": normalize_meal(e.meal),
            "recipe_id": e.recipe_id,
            "servings": max(1, e.servings or 1),
        }
        for e in rows
    ]


def normalize_entries(raw_entries: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for e in list(raw_entries or []):
        recipe_id = str(e.get("recipe_id") or "").strip()
        if not recipe_id:
            raise Invalid("Każdy wpis planu musi mieć recipe_id.")
        out.append(
            {
                "day": _coerce_day(e.get("day")),
                "meal": normalize_meal(str(e.get("meal", ""))),
                "recipe_id": recipe_id,
                "servings": _coerce_servings(e.get("servings")),
            }
        )
    return out


def reconcile_week(db: Session, user: models.User, week_start: str, desired: list[dict[str, Any]]) -> None:
    """Make the visible week match `desired`, preserving each slot's ownership."""
    _assert_recipes_visible(db, user, {e["recipe_id"] for e in desired})

    existing: dict[tuple[int, str], models.MealPlanEntry] = {}
    duplicates: list[models.MealPlanEntry] = []
    for row in _entry_rows(db, user, week_start):
        key = (row.day, normalize_meal(row.meal).lower())
        if key in existing:
            duplicates.append(row)
        else:
            existing[key] = row

    wanted: dict[tuple[int, str], dict[str, Any]] = {(e["day"], e["meal"].lower()): e for e in desired}

    for row in duplicates:
        assert_can_edit(db, user, row)
        db.delete(row)

    for key, row in existing.items():
        want = wanted.get(key)
        if want is None:
            assert_can_edit(db, user, row)
            db.delete(row)
            continue
        if row.recipe_id != want["recipe_id"] or row.servings != want["servings"] or row.meal != want["meal"]:
            assert_can_edit(db, user, row)
            row.recipe_id = want["recipe_id"]
            row.servings = want["servings"]
            row.meal = want["meal"]

    for key, want in wanted.items():
        if key in existing:
            continue
        db.add(
            models.MealPlanEntry(
                created_by=user.id,
                **default_owner_kwargs(user),
                week_start=week_start,
                day=want["day"],
                meal=want["meal"],
                recipe_id=want["recipe_id"],
                servings=want["servings"],
            )
        )
    db.commit()


# --------------------------------------------------------------------------- #
# Tool-facing operations
# --------------------------------------------------------------------------- #


def get_week_plan(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    return _enrich(db, user, week_start, _entry_rows(db, user, week_start))


def get_current_week_plan(db: Session, user: models.User, _args: dict[str, Any]) -> dict[str, Any]:
    week_start = current_week_start()
    return _enrich(db, user, week_start, _entry_rows(db, user, week_start))


def set_week_plan(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    reconcile_week(db, user, week_start, normalize_entries(args.get("entries")))
    return _enrich(db, user, week_start, _entry_rows(db, user, week_start))


def add_plan_entry(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    day = _coerce_day(args.get("day"))
    meal = normalize_meal(str(args.get("meal", "")))
    recipe_id = str(args.get("recipe_id") or "").strip()
    if not recipe_id:
        raise Invalid("recipe_id jest wymagane.")
    servings = _coerce_servings(args.get("servings"))

    current = _stored_entries(_entry_rows(db, user, week_start))
    kept = [e for e in current if not (e["day"] == day and e["meal"].lower() == meal.lower())]
    kept.append({"day": day, "meal": meal, "recipe_id": recipe_id, "servings": servings})
    reconcile_week(db, user, week_start, kept)
    return _enrich(db, user, week_start, _entry_rows(db, user, week_start))


def remove_plan_entry(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    day = _coerce_day(args.get("day"))
    meal = normalize_meal(str(args.get("meal", "")))

    current = _stored_entries(_entry_rows(db, user, week_start))
    kept = [e for e in current if not (e["day"] == day and e["meal"].lower() == meal.lower())]
    reconcile_week(db, user, week_start, kept)
    return _enrich(db, user, week_start, _entry_rows(db, user, week_start))


def get_week_nutrition_summary(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    rows = _entry_rows(db, user, week_start)
    recipe_ids = {e.recipe_id for e in rows}
    recipes: dict[str, models.Recipe] = {}
    if recipe_ids:
        recipes = {
            r.id: r for r in visible_query(db, user, models.Recipe).filter(models.Recipe.id.in_(recipe_ids)).all()
        }

    days: dict[int, dict[str, float]] = {d: {"kcal": 0.0, "p": 0.0, "f": 0.0, "c": 0.0} for d in range(7)}
    for e in rows:
        r = recipes.get(e.recipe_id)
        if not r or not r.servings:
            continue
        scale = (e.servings or 0) / float(r.servings)
        for macro in ("kcal", "p", "f", "c"):
            days[e.day][macro] += (getattr(r, macro) or 0) * scale

    per_day = {str(d): {k: round(v, 2) for k, v in vals.items()} for d, vals in days.items()}
    week_total = {macro: round(sum(vals[macro] for vals in days.values()), 2) for macro in ("kcal", "p", "f", "c")}
    return {"week_start": week_start, "days": per_day, "week_total": week_total}
