from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import update
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_admin

router = APIRouter(prefix="/api/admin/households", tags=["admin"])


def _to_out(db: Session, hh: models.Household) -> schemas.HouseholdOut:
    count = (
        db.query(models.HouseholdMember)
        .filter(models.HouseholdMember.household_id == hh.id)
        .count()
    )
    return schemas.HouseholdOut(
        id=hh.id, name=hh.name, created_at=hh.created_at, member_count=count,
    )


@router.get("", response_model=List[schemas.HouseholdOut])
def list_households(
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    return [_to_out(db, h) for h in db.query(models.Household).order_by(models.Household.id).all()]


@router.post("", response_model=schemas.HouseholdOut, status_code=status.HTTP_201_CREATED)
def create_household(
    payload: schemas.HouseholdCreate,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    hh = models.Household(name=payload.name.strip())
    db.add(hh)
    db.commit()
    db.refresh(hh)
    return _to_out(db, hh)


@router.patch("/{household_id}", response_model=schemas.HouseholdOut)
def update_household(
    household_id: int,
    payload: schemas.HouseholdUpdate,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    hh = db.get(models.Household, household_id)
    if not hh:
        raise HTTPException(404, "Household not found")
    hh.name = payload.name.strip()
    db.commit()
    db.refresh(hh)
    return _to_out(db, hh)


@router.delete("/{household_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_household(
    household_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    hh = db.get(models.Household, household_id)
    if not hh:
        raise HTTPException(404, "Household not found")
    # Reassign household-owned resources back to their creators (personal)
    for model in (models.Recipe, models.WeekTemplate, models.MealPlanEntry, models.ShoppingItem):
        db.execute(
            update(model)
            .where(model.owner_household_id == household_id)
            .values(owner_household_id=None, owner_user_id=model.created_by)
        )
    db.query(models.HouseholdMember).filter(
        models.HouseholdMember.household_id == household_id
    ).delete(synchronize_session=False)
    db.delete(hh)
    db.commit()
    return None


@router.get("/{household_id}/members", response_model=List[schemas.HouseholdMemberOut])
def list_members(
    household_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    hh = db.get(models.Household, household_id)
    if not hh:
        raise HTTPException(404, "Household not found")
    rows = (
        db.query(models.HouseholdMember, models.User)
        .join(models.User, models.User.id == models.HouseholdMember.user_id)
        .filter(models.HouseholdMember.household_id == household_id)
        .all()
    )
    return [
        schemas.HouseholdMemberOut(
            user_id=m.user_id,
            username=u.username,
            household_id=m.household_id,
            can_edit=bool(m.can_edit),
            joined_at=m.joined_at,
        )
        for m, u in rows
    ]
