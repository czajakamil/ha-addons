"""Ekran startowy — cienka warstwa HTTP nad :mod:`finance_app.services.home`."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import HomeSummary
from ..services import home as home_service

router = APIRouter()


@router.get("/home/summary", response_model=HomeSummary)
def get_home_summary(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return home_service.get_home_summary(db, current_user.id)
