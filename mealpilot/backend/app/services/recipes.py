"""Recipe domain services — the single implementation behind REST, agent and MCP."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from ..ownership import default_owner_kwargs, get_household_id
from .common import (
    clamp_limit,
    clamp_offset,
    coerce_steps,
    fold,
    recipe_to_dict,
    recipe_to_summary,
    require_editable,
    require_visible,
    slugify,
    unique_slug,
    visible_query,
)
from .errors import Invalid, NotFound, ServiceError

LIST_LIMIT_DEFAULT = 50
LIST_LIMIT_MAX = 200
SEARCH_LIMIT_DEFAULT = 20

WRITABLE_FIELDS = (
    "title",
    "tags",
    "meal_types",
    "servings",
    "prep_time",
    "cook_time",
    "kcal",
    "p",
    "f",
    "c",
    "hue",
    "ingredients",
    "steps",
    "is_meal_prep",
    "meal_prep_days",
    "meal_prep_steps",
)

_LIST_FIELDS = ("tags", "meal_types")
_STEP_FIELDS = ("steps", "meal_prep_steps")


# --------------------------------------------------------------------------- #
# Rating / note enrichment
# --------------------------------------------------------------------------- #


def rating_maps(db: Session, user_id: int, recipe_ids: list[str]) -> dict[str, dict]:
    """avg/count/my-rating/my-note for exactly the recipes being returned."""
    avg_map: dict[str, float] = {}
    count_map: dict[str, int] = {}
    my_map: dict[str, int] = {}
    note_map: dict[str, str] = {}
    if recipe_ids:
        for rid, avg_r, cnt in (
            db.query(
                models.RecipeRating.recipe_id,
                func.avg(models.RecipeRating.rating),
                func.count(models.RecipeRating.id),
            )
            .filter(models.RecipeRating.recipe_id.in_(recipe_ids))
            .group_by(models.RecipeRating.recipe_id)
            .all()
        ):
            avg_map[rid] = round(float(avg_r), 2)
            count_map[rid] = cnt
        for rid, r in (
            db.query(models.RecipeRating.recipe_id, models.RecipeRating.rating)
            .filter(
                models.RecipeRating.recipe_id.in_(recipe_ids),
                models.RecipeRating.user_id == user_id,
            )
            .all()
        ):
            my_map[rid] = r
        for rid, note in (
            db.query(models.RecipeNote.recipe_id, models.RecipeNote.note)
            .filter(
                models.RecipeNote.recipe_id.in_(recipe_ids),
                models.RecipeNote.user_id == user_id,
            )
            .all()
        ):
            note_map[rid] = note
    return {"avg": avg_map, "count": count_map, "mine": my_map, "note": note_map}


def _ratings_for(maps: dict[str, dict], rid: str) -> dict[str, Any]:
    return {
        "avg_rating": maps["avg"].get(rid),
        "rating_count": maps["count"].get(rid, 0),
        "my_rating": maps["mine"].get(rid),
        "my_note": maps["note"].get(rid),
    }


def serialize(db: Session, user: models.User, rows: list[models.Recipe], *, full: bool) -> list[dict[str, Any]]:
    maps = rating_maps(db, user.id, [r.id for r in rows])
    render = recipe_to_dict if full else recipe_to_summary
    return [render(r, _ratings_for(maps, r.id)) for r in rows]


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #


def _matches(r: models.Recipe, f: dict[str, Any]) -> bool:
    if f["tags"] and not all(t in (r.tags or []) for t in f["tags"]):
        return False
    if f["meal_types"] and not any(m in (r.meal_types or []) for m in f["meal_types"]):
        return False
    if f["max_kcal"] is not None and (r.kcal or 0) > f["max_kcal"]:
        return False
    if f["min_protein"] is not None and (r.p or 0) < f["min_protein"]:
        return False
    if f["max_total_time"] is not None and (r.prep_time or 0) + (r.cook_time or 0) > f["max_total_time"]:
        return False
    if f["is_meal_prep"] is not None and bool(r.is_meal_prep) is not f["is_meal_prep"]:  # noqa: SIM103
        return False
    return True


def _as_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise Invalid(f"{field} musi być liczbą, otrzymano: {value!r}.") from exc


def _filter_spec(args: dict[str, Any]) -> dict[str, Any]:
    return {
        "tags": [str(t) for t in (args.get("tags") or [])],
        "meal_types": [str(m) for m in (args.get("meal_types") or [])],
        "max_kcal": _as_float(args.get("max_kcal"), "max_kcal"),
        "min_protein": _as_float(args.get("min_protein"), "min_protein"),
        "max_total_time": _as_float(args.get("max_total_time"), "max_total_time"),
        "is_meal_prep": args.get("is_meal_prep"),
    }


def _paginate(items: list, limit: int | None, offset: int) -> dict[str, Any]:
    total = len(items)
    page = items[offset:] if limit is None else items[offset : offset + limit]
    return {
        "items": page,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(page) < total,
    }


def filtered_rows(db: Session, user: models.User, args: dict[str, Any]) -> list[models.Recipe]:
    """Visible recipes matching every filter, sorted by title.

    Shared by the REST endpoint and the tool layer so the two can never drift;
    each caller serializes the rows in its own shape afterwards.
    """
    spec = _filter_spec(args)
    rows = [r for r in visible_query(db, user, models.Recipe).all() if _matches(r, spec)]
    rows.sort(key=lambda r: fold(r.title))

    min_my = args.get("min_my_rating")
    min_avg = _as_float(args.get("min_avg_rating"), "min_avg_rating")
    if min_my is None and min_avg is None:
        return rows

    maps = rating_maps(db, user.id, [r.id for r in rows])
    kept = []
    for r in rows:
        my = maps["mine"].get(r.id)
        avg = maps["avg"].get(r.id)
        if min_my is not None and (my is None or my < int(min_my)):
            continue
        if min_avg is not None and (avg is None or avg < min_avg):
            continue
        kept.append(r)
    return kept


def query_recipes(
    db: Session,
    user: models.User,
    args: dict[str, Any],
    *,
    full: bool = False,
) -> dict[str, Any]:
    """Paginated tool-facing view over `filtered_rows`."""
    limit = clamp_limit(args.get("limit"), LIST_LIMIT_DEFAULT, LIST_LIMIT_MAX)
    offset = clamp_offset(args.get("offset"))
    page = _paginate(filtered_rows(db, user, args), limit, offset)
    page["items"] = serialize(db, user, page["items"], full=full)
    return page


def search_recipes(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    """Free-text search over title, tags, meal types and ingredient names."""
    raw = str(args.get("query") or "").strip()
    if not raw:
        raise Invalid("query jest wymagane — podaj szukany tekst (np. 'kurczak', 'makaron').")
    tokens = [t for t in fold(raw).split() if t]
    if not tokens:
        raise Invalid("query nie zawiera żadnego szukanego słowa.")
    limit = clamp_limit(args.get("limit"), SEARCH_LIMIT_DEFAULT, LIST_LIMIT_MAX)

    scored: list[tuple[float, str, models.Recipe]] = []
    for r in filtered_rows(db, user, args):
        title = fold(r.title)
        labels = " ".join(fold(x) for x in list(r.tags or []) + list(r.meal_types or []))
        ings = " ".join(fold(i.get("name", "")) for i in (r.ingredients or []) if isinstance(i, dict))
        haystack = f"{title} {labels} {ings}"
        if not all(tok in haystack for tok in tokens):
            continue
        score = 0.0
        for tok in tokens:
            if tok in title:
                score += 3.0
            elif tok in labels:
                score += 1.5
            else:
                score += 1.0
        if title.startswith(tokens[0]):
            score += 1.0
        scored.append((-score, title, r))

    scored.sort(key=lambda t: (t[0], t[1]))
    page = _paginate([r for _, _, r in scored], limit, 0)
    page["items"] = serialize(db, user, page["items"], full=False)
    page["query"] = raw
    return page


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #


def list_recipes(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    return query_recipes(db, user, args, full=False)


def filter_recipes(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    return query_recipes(db, user, args, full=False)


def get_recipe(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    recipe_id = str(args.get("recipe_id", "")).strip()
    if not recipe_id:
        raise Invalid("recipe_id jest wymagane.")
    r = require_visible(db, user, models.Recipe, recipe_id)
    return serialize(db, user, [r], full=True)[0]


def _distinct_values(db: Session, user: models.User, attr: str) -> list[str]:
    out: set[str] = set()
    for r in visible_query(db, user, models.Recipe).all():
        for v in getattr(r, attr) or []:
            if isinstance(v, str) and v:
                out.add(v)
    return sorted(out)


def list_tags(db: Session, user: models.User, _args: dict[str, Any]) -> dict[str, Any]:
    return {"tags": _distinct_values(db, user, "tags")}


def list_meal_types(db: Session, user: models.User, _args: dict[str, Any]) -> dict[str, Any]:
    return {"meal_types": _distinct_values(db, user, "meal_types")}


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #


def _coerce_writable(args: dict[str, Any], *, partial: bool) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key in WRITABLE_FIELDS:
        if key not in args or args[key] is None:
            continue
        value = args[key]
        if key in _LIST_FIELDS:
            value = [str(v) for v in value]
        elif key in _STEP_FIELDS:
            value = coerce_steps(value)
        elif key == "ingredients":
            value = [dict(i) for i in value]
        elif key in ("servings", "prep_time", "cook_time", "hue", "meal_prep_days"):
            value = int(value)
        elif key in ("kcal", "p", "f", "c"):
            value = float(value)
        elif key == "is_meal_prep":
            value = bool(value)
        elif key == "title":
            value = str(value)
        data[key] = value
    if not partial:
        data.setdefault("servings", 1)
        data.setdefault("prep_time", 0)
        data.setdefault("cook_time", 0)
        for m in ("kcal", "p", "f", "c"):
            data.setdefault(m, 0.0)
        data.setdefault("hue", 40)
        for lf in _LIST_FIELDS + _STEP_FIELDS:
            data.setdefault(lf, [])
        data.setdefault("ingredients", [])
        data.setdefault("is_meal_prep", False)
    return data


def create_recipe(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    """Create a recipe. The slug is derived server-side and de-duplicated."""
    title = str(args.get("title", "")).strip()
    if not title:
        raise Invalid("title jest wymagane.")
    recipe_id = unique_slug(db, slugify(title))

    data = _coerce_writable({**args, "title": title}, partial=False)
    r = models.Recipe(created_by=user.id, **default_owner_kwargs(user), id=recipe_id, **data)
    db.add(r)
    db.commit()
    db.refresh(r)
    return serialize(db, user, [r], full=True)[0]


def update_recipe(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    recipe_id = str(args.get("recipe_id", "")).strip()
    if not recipe_id:
        raise Invalid("recipe_id jest wymagane.")
    r = require_editable(db, user, models.Recipe, recipe_id)
    for key, value in _coerce_writable(args, partial=True).items():
        setattr(r, key, value)
    db.commit()
    db.refresh(r)
    return serialize(db, user, [r], full=True)[0]


def delete_recipe(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    from .shopping import regenerate_auto_shopping

    recipe_id = str(args.get("recipe_id", "")).strip()
    if not recipe_id:
        raise Invalid("recipe_id jest wymagane.")
    require_editable(db, user, models.Recipe, recipe_id)

    affected_weeks = {
        w
        for (w,) in db.query(models.MealPlanEntry.week_start)
        .filter(models.MealPlanEntry.recipe_id == recipe_id)
        .distinct()
        .all()
    }
    db.query(models.MealPlanEntry).filter(models.MealPlanEntry.recipe_id == recipe_id).delete(synchronize_session=False)
    db.query(models.ShoppingItemRecipe).filter(models.ShoppingItemRecipe.recipe_id == recipe_id).delete(
        synchronize_session=False
    )
    db.delete(db.get(models.Recipe, recipe_id))
    db.commit()
    regenerated = []
    for week_start in sorted(affected_weeks):
        # The delete is already committed; a week this user may not edit must not
        # abort the cleanup of the weeks they can.
        try:
            regenerate_auto_shopping(db, user, week_start)
        except ServiceError:
            db.rollback()
            continue
        regenerated.append(week_start)
    return {"deleted": recipe_id, "affected_weeks": regenerated}


def rate_recipe(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    recipe_id = str(args.get("recipe_id", "")).strip()
    r = require_visible(db, user, models.Recipe, recipe_id)
    try:
        rating = int(args.get("rating"))
    except (TypeError, ValueError) as exc:
        raise Invalid("rating musi być liczbą 0–5 (0 usuwa ocenę).") from exc
    if not 0 <= rating <= 5:
        raise Invalid("rating musi mieścić się w zakresie 0–5 (0 usuwa ocenę).")

    existing = (
        db.query(models.RecipeRating)
        .filter(models.RecipeRating.recipe_id == recipe_id, models.RecipeRating.user_id == user.id)
        .one_or_none()
    )
    if rating == 0:
        if existing:
            db.delete(existing)
    elif existing:
        existing.rating = rating
    else:
        db.add(models.RecipeRating(recipe_id=recipe_id, user_id=user.id, rating=rating))
    db.commit()
    db.refresh(r)
    return serialize(db, user, [r], full=True)[0]


def set_recipe_note(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    recipe_id = str(args.get("recipe_id", "")).strip()
    r = require_visible(db, user, models.Recipe, recipe_id)
    note = str(args.get("note") or "")
    existing = (
        db.query(models.RecipeNote)
        .filter(models.RecipeNote.recipe_id == recipe_id, models.RecipeNote.user_id == user.id)
        .one_or_none()
    )
    if not note.strip():
        if existing:
            db.delete(existing)
    elif existing:
        existing.note = note
    else:
        db.add(models.RecipeNote(recipe_id=recipe_id, user_id=user.id, note=note))
    db.commit()
    db.refresh(r)
    return serialize(db, user, [r], full=True)[0]


def share_recipe_with_household(db: Session, user: models.User, args: dict[str, Any]) -> dict[str, Any]:
    """Re-pin a recipe between personal and household. Creator-only, per ownership rules."""
    recipe_id = str(args.get("recipe_id", "")).strip()
    r = db.get(models.Recipe, recipe_id)
    if r is None:
        raise NotFound(f"Przepis nie istnieje: {recipe_id}")
    if r.created_by != user.id:
        raise NotFound(f"Przepis nie istnieje lub nie jesteś jego twórcą: {recipe_id}")
    share = bool(args.get("share", True))
    if share:
        hh = get_household_id(db, user.id)
        if hh is None:
            raise Invalid("Nie należysz do żadnego household — nie ma z kim dzielić przepisu.")
        r.owner_user_id = None
        r.owner_household_id = hh
    else:
        r.owner_user_id = user.id
        r.owner_household_id = None
    db.commit()
    db.refresh(r)
    return serialize(db, user, [r], full=True)[0]
