from typing import List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user

router = APIRouter(prefix="/api/plan", tags=["plan"])


@router.get("/{week_start}", response_model=schemas.WeekPlan)
def get_week_plan(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    rows = (
        db.query(models.MealPlanEntry)
        .filter(
            models.MealPlanEntry.user_id == user.id,
            models.MealPlanEntry.week_start == week_start,
        )
        .order_by(models.MealPlanEntry.day, models.MealPlanEntry.meal)
        .all()
    )
    entries = [
        schemas.PlanEntry(day=r.day, meal=r.meal, recipe_id=r.recipe_id, servings=r.servings)
        for r in rows
    ]
    return schemas.WeekPlan(week_start=week_start, entries=entries)


@router.put("/{week_start}", response_model=schemas.WeekPlan)
def replace_week_plan(
    week_start: str,
    entries: List[schemas.PlanEntry],
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    recipe_ids = {e.recipe_id for e in entries}
    if recipe_ids:
        existing = {
            r.id
            for r in db.query(models.Recipe)
            .filter(models.Recipe.id.in_(recipe_ids), models.Recipe.user_id == user.id)
            .all()
        }
        missing = recipe_ids - existing
        if missing:
            raise HTTPException(400, f"Unknown recipe ids: {sorted(missing)}")

    db.query(models.MealPlanEntry).filter(
        models.MealPlanEntry.user_id == user.id,
        models.MealPlanEntry.week_start == week_start,
    ).delete(synchronize_session=False)

    for e in entries:
        db.add(
            models.MealPlanEntry(
                user_id=user.id,
                week_start=week_start,
                day=e.day,
                meal=e.meal,
                recipe_id=e.recipe_id,
                servings=e.servings,
            )
        )
    db.commit()

    return get_week_plan(week_start, db, user)
