"""Ładowanie przykładowego budżetu dla ekranu powitalnego."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import DemoLoadResponse
from ..services.demo import load_demo_data

router = APIRouter()


@router.post("/demo/load", response_model=DemoLoadResponse, status_code=status.HTTP_201_CREATED)
def load_demo(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return load_demo_data(db, current_user.id)
