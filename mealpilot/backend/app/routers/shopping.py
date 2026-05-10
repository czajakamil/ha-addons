import re
from typing import Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user

router = APIRouter(prefix="/api/shopping", tags=["shopping"])


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


@router.get("/{week_start}", response_model=List[schemas.ShoppingItemOut])
def get_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    rows = (
        db.query(models.ShoppingItem)
        .filter(
            models.ShoppingItem.user_id == user.id,
            models.ShoppingItem.week_start == week_start,
        )
        .order_by(models.ShoppingItem.category, models.ShoppingItem.name)
        .all()
    )
    return rows


@router.post(
    "/{week_start}/generate",
    response_model=List[schemas.ShoppingItemOut],
)
def generate_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
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
        return get_shopping_list(week_start, db, user)

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

    return get_shopping_list(week_start, db, user)


@router.patch("/{week_start}/items/{item_id}", response_model=schemas.ShoppingItemOut)
def patch_shopping_item(
    week_start: str,
    item_id: int,
    payload: schemas.ShoppingItemPatch,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
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
        raise HTTPException(404, "Shopping item not found")
    item.checked = 1 if payload.checked else 0
    db.commit()
    db.refresh(item)
    return item


@router.post("/{week_start}/items", response_model=schemas.ShoppingItemOut)
def add_shopping_item(
    week_start: str,
    payload: schemas.ShoppingItemCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    name = payload.name.strip()
    unit_in = (payload.unit or "").strip()
    unit, qty = _normalize_unit_qty(unit_in, float(payload.qty or 0))
    category = payload.category or _category_of(name)

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
        return existing

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
    return item


@router.delete(
    "/{week_start}/items/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_shopping_item(
    week_start: str,
    item_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
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
        raise HTTPException(404, "Shopping item not found")
    db.commit()
    return None


@router.delete("/{week_start}", status_code=status.HTTP_204_NO_CONTENT)
def clear_shopping_list(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    db.query(models.ShoppingItem).filter(
        models.ShoppingItem.user_id == user.id,
        models.ShoppingItem.week_start == week_start,
    ).delete(synchronize_session=False)
    db.commit()
    return None
