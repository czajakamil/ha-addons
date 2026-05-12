"""Ownership & permission helpers.

Resources (Recipe, MealPlanEntry, WeekTemplate, ShoppingItem) carry:
  - created_by (immutable, audit trail)
  - owner_user_id XOR owner_household_id (exactly one)

Visibility / edit rules:
  - personal resource (owner_user_id set): only owner sees & edits.
  - household resource (owner_household_id set): all members see; only
    members with can_edit=True edit. Creator can always edit their own
    creation regardless of can_edit.
  - Only created_by can change owner (re-pin personal <-> household).
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import or_

from . import models


def get_household_id(db: Session, user_id: int) -> Optional[int]:
    row = db.get(models.HouseholdMember, user_id)
    return row.household_id if row else None


def get_membership(db: Session, user_id: int) -> Optional[models.HouseholdMember]:
    return db.get(models.HouseholdMember, user_id)


def visible_filter(model, user: models.User, household_id: Optional[int]):
    """SQL filter: resources the user can see."""
    if household_id is not None:
        return or_(
            model.owner_user_id == user.id,
            model.owner_household_id == household_id,
        )
    return model.owner_user_id == user.id


def can_view(resource, user: models.User, household_id: Optional[int]) -> bool:
    if resource.owner_user_id is not None:
        return resource.owner_user_id == user.id
    return household_id is not None and resource.owner_household_id == household_id


def can_edit(resource, user: models.User, member: Optional[models.HouseholdMember]) -> bool:
    if resource.owner_user_id is not None:
        return resource.owner_user_id == user.id
    # household-owned
    if member is None or resource.owner_household_id != member.household_id:
        return False
    if resource.created_by == user.id:
        return True
    return bool(member.can_edit)


def can_change_owner(resource, user: models.User) -> bool:
    """Only the original creator may re-pin a resource between personal and household."""
    return resource.created_by == user.id


def default_owner_kwargs(user: models.User) -> dict:
    """Default ownership for newly created resources: personal."""
    return {"owner_user_id": user.id, "owner_household_id": None}
