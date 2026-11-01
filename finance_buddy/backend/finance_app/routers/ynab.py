"""YNAB integration endpoints (settings, budgets, import).

Thin HTTP layer over :mod:`finance_app.services.ynab`.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import (
    YnabAuth,
    YnabBudgetSummary,
    YnabImportRequest,
    YnabImportResponse,
    YnabSettingsRead,
    YnabSettingsUpdate,
)
from ..services import ynab as ynab_service

router = APIRouter()


@router.get("/ynab/settings", tags=["ynab"], response_model=YnabSettingsRead)
def get_ynab_settings(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return ynab_service.get_settings(db, current_user)


@router.put("/ynab/settings", tags=["ynab"], response_model=YnabSettingsRead)
def update_ynab_settings(
    payload: YnabSettingsUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return ynab_service.update_settings(db, payload, current_user)


@router.post("/ynab/budgets", tags=["ynab"], response_model=list[YnabBudgetSummary])
def list_ynab_budgets(
    payload: YnabAuth,
    current_user: User = Depends(get_current_user),
):
    return ynab_service.list_budgets(payload.api_key, current_user)


@router.post("/ynab/import", tags=["ynab"], response_model=YnabImportResponse)
def import_ynab_budget(
    payload: YnabImportRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return ynab_service.import_budget(db, payload, current_user)
