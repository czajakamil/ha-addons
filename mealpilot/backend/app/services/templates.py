"""Week-template domain services (save a week, list templates, apply one)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .. import models
from ..ownership import default_owner_kwargs
from .common import parse_week_start, plan_entry_to_dict, require_editable, require_visible, visible_query
from .errors import Invalid
from .plan import normalize_entries, reconcile_week


def _to_dict(t: models.WeekTemplate) -> dict[str, Any]:
    return {
        "id": t.id,
        "name": t.name,
        "entries": list(t.entries or []),
        "entry_count": len(t.entries or []),
        "shared_with_household": t.owner_household_id is not None,
    }


def list_week_templates(db: Session, user: models.User, _args: dict[str, Any]) -> dict[str, Any]:
    rows = visible_query(db, user, models.WeekTemplate).order_by(models.WeekTemplate.created_at.desc()).all()
    return {"templates": [_to_dict(t) for t in rows], "total": len(rows)}


def save_week_as_template(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    week_start = parse_week_start(args.get("week_start"))
    name = str(args.get("name", "")).strip()
    if not name:
        raise Invalid("name jest wymagane — nazwij szablon (np. 'Tydzień wysokobiałkowy').")
    rows = (
        visible_query(db, user, models.MealPlanEntry)
        .filter(models.MealPlanEntry.week_start == week_start)
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )
    if not rows:
        raise Invalid(f"Plan na {week_start} jest pusty — nie ma czego zapisać jako szablon.")
    tpl = models.WeekTemplate(
        created_by=user.id,
        **default_owner_kwargs(user),
        name=name,
        entries=[plan_entry_to_dict(e) for e in rows],
    )
    db.add(tpl)
    db.commit()
    db.refresh(tpl)
    return _to_dict(tpl)


def apply_week_template(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    from .plan import get_week_plan

    week_start = parse_week_start(args.get("week_start"))
    try:
        template_id = int(args.get("template_id"))
    except (TypeError, ValueError) as exc:
        raise Invalid("template_id musi być liczbą całkowitą (pole `id` z list_week_templates).") from exc
    tpl = require_visible(db, user, models.WeekTemplate, template_id)

    entries = normalize_entries(tpl.entries or [])
    visible_ids = {
        r.id
        for r in visible_query(db, user, models.Recipe)
        .filter(models.Recipe.id.in_({e["recipe_id"] for e in entries} or {""}))
        .all()
    }
    skipped = sorted({e["recipe_id"] for e in entries if e["recipe_id"] not in visible_ids})
    entries = [e for e in entries if e["recipe_id"] in visible_ids]

    reconcile_week(db, user, week_start, entries)
    plan = get_week_plan(db, user, {"week_start": week_start})
    plan["applied_template"] = tpl.name
    plan["skipped_recipe_ids"] = skipped
    return plan


def delete_week_template(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    try:
        template_id = int(args.get("template_id"))
    except (TypeError, ValueError) as exc:
        raise Invalid("template_id musi być liczbą całkowitą.") from exc
    tpl = require_editable(db, user, models.WeekTemplate, template_id)
    db.delete(tpl)
    db.commit()
    return {"deleted": template_id}
