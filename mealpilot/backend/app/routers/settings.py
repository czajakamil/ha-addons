from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user

router = APIRouter(prefix="/api/settings", tags=["settings"])


def _get_or_create(db: Session, user_id: int) -> models.AgentSettings:
    row = db.get(models.AgentSettings, user_id)
    if row is None:
        row = models.AgentSettings(user_id=user_id)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


@router.get("/agent", response_model=schemas.AgentSettingsOut)
def get_agent_settings(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_or_create(db, user.id)
    return schemas.AgentSettingsOut(
        model=row.model,
        system_prompt=row.system_prompt,
    )


@router.put("/agent", response_model=schemas.AgentSettingsOut)
def update_agent_settings(
    payload: schemas.AgentSettingsUpdate,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_or_create(db, user.id)
    row.model = payload.model
    row.system_prompt = payload.system_prompt
    db.commit()
    db.refresh(row)
    return schemas.AgentSettingsOut(
        model=row.model,
        system_prompt=row.system_prompt,
    )


def _prefs_from_row(row: models.AgentSettings) -> schemas.UiPrefsOut:
    raw = row.ui_prefs or {}
    mt = raw.get("macro_targets") or {}
    return schemas.UiPrefsOut(
        recipes_grouped=bool(raw.get("recipes_grouped", False)),
        macro_targets=schemas.MacroTargets(
            kcal=mt.get("kcal", 2200),
            p=mt.get("p", 130),
            f=mt.get("f", 70),
            c=mt.get("c", 260),
        ),
        favorite_recipe_ids=list(raw.get("favorite_recipe_ids") or []),
    )


@router.get("/ui", response_model=schemas.UiPrefsOut)
def get_ui_prefs(
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_or_create(db, user.id)
    return _prefs_from_row(row)


@router.patch("/ui", response_model=schemas.UiPrefsOut)
def patch_ui_prefs(
    payload: schemas.UiPrefsPatch,
    user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = _get_or_create(db, user.id)
    prefs = dict(row.ui_prefs or {})
    if payload.recipes_grouped is not None:
        prefs["recipes_grouped"] = payload.recipes_grouped
    if payload.macro_targets is not None:
        prefs["macro_targets"] = payload.macro_targets.model_dump()
    if payload.favorite_recipe_ids is not None:
        prefs["favorite_recipe_ids"] = payload.favorite_recipe_ids
    row.ui_prefs = prefs
    db.commit()
    db.refresh(row)
    return _prefs_from_row(row)
