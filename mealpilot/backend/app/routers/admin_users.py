from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import models, oauth, schemas
from ..db import get_db
from ..dependencies import get_current_admin
from ..security import hash_password

router = APIRouter(prefix="/api/admin/users", tags=["admin"])


def _user_admin_out(db: Session, user: models.User) -> schemas.UserAdminOut:
    m = db.get(models.HouseholdMember, user.id)
    return schemas.UserAdminOut(
        id=user.id,
        username=user.username,
        role=user.role,
        is_active=bool(user.is_active),
        created_at=user.created_at,
        can_use_ai=bool(user.can_use_ai),
        ai_monthly_token_limit=user.ai_monthly_token_limit,
        ai_monthly_cost_limit_cents=user.ai_monthly_cost_limit_cents,
        ai_used_tokens_this_month=user.ai_used_tokens_this_month or 0,
        ai_used_cost_cents_this_month=user.ai_used_cost_cents_this_month or 0,
        household_id=m.household_id if m else None,
        can_edit_in_household=bool(m.can_edit) if m else False,
    )


def _admin_count(db: Session) -> int:
    return db.query(models.User).filter(models.User.role == "admin", models.User.is_active == 1).count()


@router.get("", response_model=list[schemas.UserAdminOut])
def list_users(
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    users = db.query(models.User).order_by(models.User.id).all()
    return [_user_admin_out(db, u) for u in users]


@router.put("/{user_id}/ai-limits", response_model=schemas.UserAdminOut)
def update_ai_limits(
    user_id: int,
    payload: schemas.UserAiLimitsPatch,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    user = db.get(models.User, user_id)
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if payload.can_use_ai is not None:
        user.can_use_ai = payload.can_use_ai
    if payload.clear_token_limit:
        user.ai_monthly_token_limit = None
    elif payload.ai_monthly_token_limit is not None:
        user.ai_monthly_token_limit = payload.ai_monthly_token_limit
    if payload.clear_cost_limit:
        user.ai_monthly_cost_limit_cents = None
    elif payload.ai_monthly_cost_limit_cents is not None:
        user.ai_monthly_cost_limit_cents = payload.ai_monthly_cost_limit_cents
    db.commit()
    db.refresh(user)
    return _user_admin_out(db, user)


@router.post("/{user_id}/ai-usage/reset", status_code=status.HTTP_204_NO_CONTENT)
def reset_ai_usage(
    user_id: int,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    user = db.get(models.User, user_id)
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    from datetime import datetime

    user.ai_used_tokens_this_month = 0
    user.ai_used_cost_cents_this_month = 0
    user.ai_usage_period_start = datetime.now(UTC)
    db.commit()
    return None


@router.put("/{user_id}/household", response_model=schemas.UserAdminOut)
def assign_to_household(
    user_id: int,
    payload: schemas.HouseholdAssignRequest,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    user = db.get(models.User, user_id)
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    existing = db.get(models.HouseholdMember, user_id)
    if payload.household_id is None:
        if existing:
            db.delete(existing)
            db.commit()
    else:
        hh = db.get(models.Household, payload.household_id)
        if not hh:
            raise HTTPException(404, "Household not found")
        if existing:
            existing.household_id = payload.household_id
            existing.can_edit = payload.can_edit
        else:
            db.add(
                models.HouseholdMember(
                    user_id=user_id,
                    household_id=payload.household_id,
                    can_edit=payload.can_edit,
                )
            )
        db.commit()
    db.refresh(user)
    return _user_admin_out(db, user)


@router.post("", response_model=schemas.UserAdminOut, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: schemas.CreateUserRequest,
    db: Session = Depends(get_db),
    _: models.User = Depends(get_current_admin),
):
    user = models.User(
        username=payload.username.strip(),
        password_hash=hash_password(payload.password),
        role=payload.role,
        is_active=1,
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "Username already exists") from None
    db.refresh(user)
    return _user_admin_out(db, user)


@router.patch("/{user_id}", response_model=schemas.UserAdminOut)
def update_user(
    user_id: int,
    payload: schemas.UpdateUserRequest,
    db: Session = Depends(get_db),
    admin: models.User = Depends(get_current_admin),
):
    user = db.get(models.User, user_id)
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")

    would_lose_last_admin = (
        user.role == "admin"
        and user.is_active
        and ((payload.role is not None and payload.role != "admin") or (payload.is_active is False))
    )
    if would_lose_last_admin and _admin_count(db) <= 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot demote/disable the last admin")

    if payload.username is not None:
        user.username = payload.username.strip()
    if payload.password is not None:
        user.password_hash = hash_password(payload.password)
        # Unieważnij istniejące sesje i API keys gdy admin rotuje hasło.
        user.session_version = (user.session_version or 0) + 1
        db.query(models.ApiKey).filter(models.ApiKey.user_id == user.id).delete()
    if payload.role is not None:
        user.role = payload.role
    if payload.is_active is not None:
        user.is_active = 1 if payload.is_active else 0
        if not payload.is_active:
            # Dezaktywacja: też wybij sesje.
            user.session_version = (user.session_version or 0) + 1

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "Username already exists") from None
    db.refresh(user)
    return _user_admin_out(db, user)


def _household_successor(db: Session, user_id: int) -> int | None:
    """Lowest-numbered *other* member of the leaving user's household, if any.

    Recipes shared with a household outlive whoever happened to type them in, so
    they need a new `created_by` rather than a tombstone.
    """
    membership = db.get(models.HouseholdMember, user_id)
    if membership is None:
        return None
    return (
        db.query(func.min(models.HouseholdMember.user_id))
        .filter(
            models.HouseholdMember.household_id == membership.household_id,
            models.HouseholdMember.user_id != user_id,
        )
        .scalar()
    )


def _purge_user_data(db: Session, user_id: int) -> None:
    """Remove every row that belongs to `user_id`, in foreign-key-safe order.

    This has to be exhaustive. SQLite reuses the id of a deleted row whenever it
    was the highest one (the primary keys here are plain INTEGER PRIMARY KEY, not
    AUTOINCREMENT), so anything left behind is not merely orphaned garbage — the
    next user created inherits it. A surviving `api_keys` row means a leaked key
    keeps working against a brand-new account; surviving `agent_conversations`
    means that account can read a stranger's chat history.
    """
    successor = _household_successor(db, user_id)

    # --- Agent: conversations -> messages -> tool uses ---------------------
    conv_ids = [
        i for (i,) in db.query(models.AgentConversation.id).filter(models.AgentConversation.user_id == user_id).all()
    ]
    if conv_ids:
        msg_ids = [
            i
            for (i,) in db.query(models.AgentMessage.id).filter(models.AgentMessage.conversation_id.in_(conv_ids)).all()
        ]
        if msg_ids:
            db.query(models.AgentToolUse).filter(models.AgentToolUse.message_id.in_(msg_ids)).delete(
                synchronize_session=False
            )
        db.query(models.AgentMessage).filter(models.AgentMessage.conversation_id.in_(conv_ids)).delete(
            synchronize_session=False
        )
        db.query(models.AgentConversation).filter(models.AgentConversation.id.in_(conv_ids)).delete(
            synchronize_session=False
        )
    db.query(models.AgentSettings).filter(models.AgentSettings.user_id == user_id).delete(synchronize_session=False)

    # --- Credentials -------------------------------------------------------
    db.query(models.ApiKey).filter(models.ApiKey.user_id == user_id).delete(synchronize_session=False)
    oauth.revoke_all_for_user(db, user_id)

    # --- This user's ratings/notes on anybody's recipes --------------------
    db.query(models.RecipeRating).filter(models.RecipeRating.user_id == user_id).delete(synchronize_session=False)
    db.query(models.RecipeNote).filter(models.RecipeNote.user_id == user_id).delete(synchronize_session=False)

    # --- Recipes: hand the shared ones over, destroy the private ones ------
    owned_recipes = (
        db.query(models.Recipe.id, models.Recipe.owner_household_id)
        .filter(or_(models.Recipe.created_by == user_id, models.Recipe.owner_user_id == user_id))
        .all()
    )
    reassign_ids = [rid for rid, household_id in owned_recipes if household_id is not None and successor is not None]
    doomed_ids = [rid for rid, household_id in owned_recipes if household_id is None or successor is None]

    if reassign_ids:
        db.query(models.Recipe).filter(models.Recipe.id.in_(reassign_ids)).update(
            {models.Recipe.created_by: successor}, synchronize_session=False
        )
    if doomed_ids:
        # Other members can only ever have touched a household recipe, and those
        # were reassigned above — but clear these by recipe id anyway, because
        # the rows must be gone before the recipe itself can be.
        db.query(models.RecipeRating).filter(models.RecipeRating.recipe_id.in_(doomed_ids)).delete(
            synchronize_session=False
        )
        db.query(models.RecipeNote).filter(models.RecipeNote.recipe_id.in_(doomed_ids)).delete(
            synchronize_session=False
        )
        db.query(models.ShoppingItemRecipe).filter(models.ShoppingItemRecipe.recipe_id.in_(doomed_ids)).delete(
            synchronize_session=False
        )
        db.query(models.MealPlanEntry).filter(models.MealPlanEntry.recipe_id.in_(doomed_ids)).delete(
            synchronize_session=False
        )

    # --- Plan / shopping / templates --------------------------------------
    db.query(models.MealPlanEntry).filter(
        or_(models.MealPlanEntry.created_by == user_id, models.MealPlanEntry.owner_user_id == user_id)
    ).delete(synchronize_session=False)

    item_ids = [
        i
        for (i,) in db.query(models.ShoppingItem.id)
        .filter(or_(models.ShoppingItem.created_by == user_id, models.ShoppingItem.owner_user_id == user_id))
        .all()
    ]
    if item_ids:
        db.query(models.ShoppingItemRecipe).filter(models.ShoppingItemRecipe.item_id.in_(item_ids)).delete(
            synchronize_session=False
        )
        db.query(models.ShoppingItem).filter(models.ShoppingItem.id.in_(item_ids)).delete(synchronize_session=False)

    db.query(models.WeekTemplate).filter(
        or_(models.WeekTemplate.created_by == user_id, models.WeekTemplate.owner_user_id == user_id)
    ).delete(synchronize_session=False)

    # Recipes last: nothing may still point at them.
    if doomed_ids:
        db.query(models.Recipe).filter(models.Recipe.id.in_(doomed_ids)).delete(synchronize_session=False)

    db.query(models.HouseholdMember).filter(models.HouseholdMember.user_id == user_id).delete(synchronize_session=False)


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    admin: models.User = Depends(get_current_admin),
):
    user = db.get(models.User, user_id)
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "User not found")
    if user.id == admin.id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot delete yourself")
    if user.role == "admin" and user.is_active and _admin_count(db) <= 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cannot delete the last admin")

    _purge_user_data(db, user_id)
    db.delete(user)
    db.commit()
    return None
