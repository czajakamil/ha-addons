from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user

router = APIRouter(prefix="/api/households", tags=["households"])


def _get_membership(db: Session, user_id: int, household_id: int) -> models.HouseholdMember:
    member = (
        db.query(models.HouseholdMember)
        .filter(
            models.HouseholdMember.user_id == user_id,
            models.HouseholdMember.household_id == household_id,
        )
        .one_or_none()
    )
    if member is None:
        raise HTTPException(status_code=404, detail="Household not found")
    return member


def _get_or_create_settings(db: Session, household_id: int) -> models.HouseholdSettings:
    row = db.get(models.HouseholdSettings, household_id)
    if row is None:
        row = models.HouseholdSettings(household_id=household_id)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def _memory_from_row(row: models.HouseholdSettings) -> schemas.HouseholdMemoryOut:
    raw = row.memory or {}
    return schemas.HouseholdMemoryOut(
        shared_restrictions=raw.get("shared_restrictions") or [],
        shared_dislikes=raw.get("shared_dislikes") or [],
        planning_notes=raw.get("planning_notes"),
        servings_default=raw.get("servings_default"),
    )


@router.get("/{household_id}/memory", response_model=schemas.HouseholdMemoryOut)
def get_household_memory(
    household_id: int,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _get_membership(db, user.id, household_id)
    row = _get_or_create_settings(db, household_id)
    return _memory_from_row(row)


@router.patch("/{household_id}/memory", response_model=schemas.HouseholdMemoryOut)
def patch_household_memory(
    household_id: int,
    payload: schemas.HouseholdMemoryPatch,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    member = _get_membership(db, user.id, household_id)
    if not member.can_edit:
        raise HTTPException(status_code=403, detail="Brak uprawnień do edycji ustawień household")
    row = _get_or_create_settings(db, household_id)
    mem = dict(row.memory or {})
    if payload.shared_restrictions is not None:
        mem["shared_restrictions"] = payload.shared_restrictions
    if payload.shared_dislikes is not None:
        mem["shared_dislikes"] = payload.shared_dislikes
    if payload.planning_notes is not None:
        mem["planning_notes"] = payload.planning_notes
    if payload.servings_default is not None:
        mem["servings_default"] = payload.servings_default
    row.memory = mem
    db.commit()
    db.refresh(row)
    return _memory_from_row(row)
