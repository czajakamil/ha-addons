from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..ownership import (
    can_edit,
    can_view,
    default_owner_kwargs,
    get_household_id,
    get_membership,
    visible_filter,
)
from ..services import templates as templates_svc

router = APIRouter(prefix="/api/templates", tags=["templates"])


@router.get("", response_model=list[schemas.WeekTemplateOut])
def list_templates(
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    hh = get_household_id(db, user.id)
    return (
        db.query(models.WeekTemplate)
        .filter(visible_filter(models.WeekTemplate, user, hh))
        .order_by(models.WeekTemplate.created_at.desc())
        .all()
    )


@router.post("", response_model=schemas.WeekTemplateOut, status_code=201)
def create_template(
    body: schemas.WeekTemplateCreate,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    hh = get_household_id(db, user.id)
    recipe_ids = {e.recipe_id for e in body.entries}
    if recipe_ids:
        existing = {
            r.id
            for r in db.query(models.Recipe)
            .filter(models.Recipe.id.in_(recipe_ids), visible_filter(models.Recipe, user, hh))
            .all()
        }
        missing = recipe_ids - existing
        if missing:
            raise HTTPException(400, f"Unknown recipe ids: {sorted(missing)}")

    tpl = models.WeekTemplate(
        created_by=user.id,
        **default_owner_kwargs(user),
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
    tpl = db.get(models.WeekTemplate, template_id)
    member = get_membership(db, user.id)
    hh = member.household_id if member else None
    if not tpl or not can_view(tpl, user, hh):
        raise HTTPException(404, "Template not found")
    if not can_edit(tpl, user, member):
        raise HTTPException(403, "Brak uprawnień do usunięcia szablonu")
    db.delete(tpl)
    db.commit()


@router.put("/{template_id}/ownership", response_model=schemas.WeekTemplateOut)
def update_template_ownership(
    template_id: int,
    payload: schemas.OwnershipPatch,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    tpl = db.get(models.WeekTemplate, template_id)
    if not tpl or tpl.created_by != user.id:
        raise HTTPException(404, "Template not found")
    if payload.share_with_household:
        hh = get_household_id(db, user.id)
        if hh is None:
            raise HTTPException(400, "Nie należysz do żadnego household")
        tpl.owner_user_id = None
        tpl.owner_household_id = hh
    else:
        tpl.owner_user_id = user.id
        tpl.owner_household_id = None
    db.commit()
    db.refresh(tpl)
    return tpl


@router.post("/{template_id}/apply/{week_start}", response_model=schemas.WeekPlan)
def apply_template(
    template_id: int,
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return templates_svc.apply_week_template(db, user, {"template_id": template_id, "week_start": week_start})
