from __future__ import annotations

from typing import Any, Dict, List

from sqlalchemy.orm import Session

from ... import models
from ...ownership import can_edit, can_view, default_owner_kwargs, get_household_id, get_membership, visible_filter
from ..registry import tool

# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

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
        "rating": r.rating,
    }


def _visible_recipes(db: Session, user: models.User) -> List[models.Recipe]:
    hh = get_household_id(db, user.id)
    return db.query(models.Recipe).filter(visible_filter(models.Recipe, user, hh)).all()


# ---------------------------------------------------------------------------
# Shared schemas
# ---------------------------------------------------------------------------

_INGREDIENT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "qty": {"type": "number"},
        "unit": {"type": "string"},
    },
    "required": ["name", "qty", "unit"],
}

_RECIPE_FIELDS_SCHEMA = {
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
    "ingredients": {"type": "array", "items": _INGREDIENT_SCHEMA},
    "steps": {"type": "array", "items": {"type": "string"}},
    "rating": {"type": "integer", "minimum": 1, "maximum": 5, "description": "Ocena przepisu 1-5 gwiazdek"},
}


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool(
    name="list_recipes",
    description="Zwraca wszystkie przepisy zalogowanego użytkownika.",
    input_schema={"type": "object", "properties": {}},
)
def tool_list_recipes(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    return [_recipe_to_dict(r) for r in _visible_recipes(db, user)]


@tool(
    name="get_recipe",
    description="Szczegóły jednego przepisu (składniki, kroki, makro).",
    input_schema={
        "type": "object",
        "properties": {"recipe_id": {"type": "string"}},
        "required": ["recipe_id"],
    },
)
def tool_get_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = db.get(models.Recipe, recipe_id)
    hh = get_household_id(db, user.id)
    if not r or not can_view(r, user, hh):
        raise ValueError(f"Recipe not found: {recipe_id}")
    return _recipe_to_dict(r)


@tool(
    name="list_tags",
    description="Wszystkie unikalne tagi używane w bibliotece przepisów.",
    input_schema={"type": "object", "properties": {}},
)
def tool_list_tags(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    out: set = set()
    for r in _visible_recipes(db, user):
        for t in (r.tags or []):
            if isinstance(t, str) and t:
                out.add(t)
    return {"tags": sorted(out)}


@tool(
    name="list_meal_types",
    description="Wszystkie unikalne typy posiłków zdefiniowane w przepisach.",
    input_schema={"type": "object", "properties": {}},
)
def tool_list_meal_types(db: Session, user: models.User, _args: Dict[str, Any]) -> Any:
    out: set = set()
    for r in _visible_recipes(db, user):
        for m in (r.meal_types or []):
            if isinstance(m, str) and m:
                out.add(m)
    return {"meal_types": sorted(out)}


@tool(
    name="filter_recipes",
    description="Zwraca przepisy spełniające kryteria.",
    input_schema={
        "type": "object",
        "properties": {
            "tags": {"type": "array", "items": {"type": "string"}},
            "meal_types": {"type": "array", "items": {"type": "string"}},
            "max_kcal": {"type": "number"},
            "min_protein": {"type": "number"},
            "min_rating": {"type": "integer", "minimum": 1, "maximum": 5, "description": "Minimalna ocena (1-5). Użyj 4 lub 5 by planować tylko ulubione przepisy."},
        },
    },
)
def tool_filter_recipes(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    rows = _visible_recipes(db, user)
    tags_filter = list(args.get("tags") or [])
    mt_filter = list(args.get("meal_types") or [])
    max_kcal = args.get("max_kcal")
    min_protein = args.get("min_protein")
    min_rating = args.get("min_rating")

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
        if min_rating is not None and (r.rating or 0) < float(min_rating):
            continue
        result.append(_recipe_to_dict(r))
    return result


@tool(
    name="create_recipe",
    description="Dodaje nowy przepis do biblioteki.",
    input_schema={
        "type": "object",
        "properties": {"id": {"type": "string"}, "title": {"type": "string"}, **_RECIPE_FIELDS_SCHEMA},
        "required": ["id", "title"],
    },
    changes=["recipes"],
)
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
        rating=int(args["rating"]) if args.get("rating") is not None else None,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return _recipe_to_dict(r)


@tool(
    name="update_recipe",
    description="Aktualizuje wybrane pola istniejącego przepisu.",
    input_schema={
        "type": "object",
        "properties": {"recipe_id": {"type": "string"}, **_RECIPE_FIELDS_SCHEMA},
        "required": ["recipe_id"],
    },
    changes=["recipes"],
)
def tool_update_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = db.get(models.Recipe, recipe_id)
    hh = get_household_id(db, user.id)
    if not r or not can_view(r, user, hh):
        raise ValueError(f"Recipe not found: {recipe_id}")
    member = get_membership(db, user.id)
    if not can_edit(r, user, member):
        raise ValueError("Nie masz uprawnień do edycji tego przepisu")
    updatable = ["title", "tags", "servings", "prep_time", "cook_time",
                 "kcal", "p", "f", "c", "hue", "ingredients", "steps", "meal_types", "rating"]
    for key in updatable:
        if key in args and args[key] is not None:
            value = args[key]
            if key == "ingredients":
                value = [i if isinstance(i, dict) else i for i in value]
            setattr(r, key, value)
    db.commit()
    db.refresh(r)
    return _recipe_to_dict(r)


@tool(
    name="delete_recipe",
    description="Usuwa przepis i powiązane wpisy w planie tygodnia.",
    input_schema={
        "type": "object",
        "properties": {"recipe_id": {"type": "string"}},
        "required": ["recipe_id"],
    },
    changes=["recipes", "plan"],
)
def tool_delete_recipe(db: Session, user: models.User, args: Dict[str, Any]) -> Any:
    recipe_id = str(args.get("recipe_id", ""))
    r = db.get(models.Recipe, recipe_id)
    hh = get_household_id(db, user.id)
    if not r or not can_view(r, user, hh):
        raise ValueError(f"Recipe not found: {recipe_id}")
    member = get_membership(db, user.id)
    if not can_edit(r, user, member):
        raise ValueError("Nie masz uprawnień do usunięcia tego przepisu")
    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.recipe_id == recipe_id,
        models.MealPlanEntry.created_by == user.id,
    ).delete(synchronize_session=False)
    db.delete(r)
    db.commit()
    return {"deleted": recipe_id}
