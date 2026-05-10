from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..routers.plan import get_week_plan

router = APIRouter(prefix="/api/templates", tags=["templates"])


@router.get("", response_model=List[schemas.WeekTemplateOut])
def list_templates(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return (
        db.query(models.WeekTemplate)
        .filter(models.WeekTemplate.user_id == user.id)
        .order_by(models.WeekTemplate.created_at.desc())
        .all()
    )


@router.post("", response_model=schemas.WeekTemplateOut, status_code=201)
def create_template(
    body: schemas.WeekTemplateCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    recipe_ids = {e.recipe_id for e in body.entries}
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

    tpl = models.WeekTemplate(
        user_id=user.id,
        name=body.name,
        entries=[e.model_dump() for e in body.entries],
    )
    db.add(tpl)
    db.commit()
    db.refresh(tpl)
    return tpl


@router.delete("/{template_id}", status_code=204)
def delete_template(
    template_id: int,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    tpl = (
        db.query(models.WeekTemplate)
        .filter(models.WeekTemplate.id == template_id, models.WeekTemplate.user_id == user.id)
        .first()
    )
    if not tpl:
        raise HTTPException(404, "Template not found")
    db.delete(tpl)
    db.commit()


@router.post("/{template_id}/apply/{week_start}", response_model=schemas.WeekPlan)
def apply_template(
    template_id: int,
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    tpl = (
        db.query(models.WeekTemplate)
        .filter(models.WeekTemplate.id == template_id, models.WeekTemplate.user_id == user.id)
        .first()
    )
    if not tpl:
        raise HTTPException(404, "Template not found")

    entries = [schemas.PlanEntry(**e) for e in tpl.entries]
    recipe_ids = {e.recipe_id for e in entries}
    if recipe_ids:
        existing = {
            r.id
            for r in db.query(models.Recipe)
            .filter(models.Recipe.id.in_(recipe_ids), models.Recipe.user_id == user.id)
            .all()
        }
        entries = [e for e in entries if e.recipe_id in existing]

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
