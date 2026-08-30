from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from .. import models, schemas
from ..db import get_db
from ..dependencies import get_current_user
from ..services import plan as plan_svc

router = APIRouter(prefix="/api/plan", tags=["plan"])


@router.get("/{week_start}", response_model=schemas.WeekPlan)
def get_week_plan(
    week_start: str,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return plan_svc.get_week_plan(db, user, {"week_start": week_start})


@router.put("/{week_start}", response_model=schemas.WeekPlan)
def replace_week_plan(
    week_start: str,
    entries: list[schemas.PlanEntry],
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    return plan_svc.set_week_plan(
        db,
        user,
        {"week_start": week_start, "entries": [e.model_dump() for e in entries]},
    )
