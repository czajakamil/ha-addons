"""Budget endpoints.

Thin HTTP layer over :mod:`finance_app.services.budget`.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import (
    BudgetAssignmentUpdate,
    BudgetMonthResponse,
)
from ..services import budget as budget_service

router = APIRouter()


@router.get("/budget/month", response_model=BudgetMonthResponse)
def get_budget_month(
    month: str = Query(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return budget_service.get_budget_month(db, month, current_user.id)


@router.post("/budget/assignments", response_model=BudgetAssignmentUpdate)
def upsert_budget_assignment(
    payload: BudgetAssignmentUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return budget_service.upsert_budget_assignment(db, payload, current_user.id)
