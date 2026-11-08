"""Połączenia z bankiem przez Enable Banking — cienka warstwa HTTP nad services/bank_sync.py."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import (
    BankAspspRead,
    BankConnectionAccountUpdate,
    BankConnectionComplete,
    BankConnectionRead,
    BankConnectionStart,
    BankConnectionStartResponse,
    BankSyncResult,
    BankSyncSettingsRead,
    BankSyncSettingsUpdate,
)
from ..services import bank_sync

router = APIRouter(prefix="/bank-sync", tags=["bank-sync"])


def _psu(request: Request) -> dict[str, str]:
    return bank_sync.psu_headers(
        request.headers.get("x-forwarded-for"),
        request.client.host if request.client else None,
        request.headers.get("user-agent"),
    )


@router.get("/settings", response_model=BankSyncSettingsRead)
def get_settings(current_user: User = Depends(get_current_user)):
    return bank_sync.get_settings(current_user.id)


@router.put("/settings", response_model=BankSyncSettingsRead)
def update_settings(
    payload: BankSyncSettingsUpdate, current_user: User = Depends(get_current_user)
):
    return bank_sync.update_settings(current_user.id, payload)


@router.delete("/settings", status_code=status.HTTP_204_NO_CONTENT)
def delete_settings(current_user: User = Depends(get_current_user)):
    bank_sync.delete_settings(current_user.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/banks", response_model=list[BankAspspRead])
def list_banks(country: str = "PL", current_user: User = Depends(get_current_user)):
    return bank_sync.list_banks(current_user.id, country)


@router.get("/connections", response_model=list[BankConnectionRead])
def list_connections(
    db: Session = Depends(get_db_session), current_user: User = Depends(get_current_user)
):
    return bank_sync.list_connections(db, current_user.id)


@router.post("/connections", response_model=BankConnectionStartResponse)
def start_connection(
    payload: BankConnectionStart,
    request: Request,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return bank_sync.start_connection(db, current_user.id, payload, _psu(request))


@router.post("/connections/{connection_id}/complete", response_model=BankConnectionRead)
def complete_connection(
    connection_id: str,
    payload: BankConnectionComplete,
    request: Request,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return bank_sync.complete_connection(
        db, current_user.id, connection_id, payload.redirect, _psu(request)
    )


@router.post("/connections/{connection_id}/sync", response_model=BankSyncResult)
def sync_connection(
    connection_id: str,
    request: Request,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return bank_sync.sync_connection_by_id(db, current_user.id, connection_id, _psu(request))


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connection(
    connection_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    bank_sync.delete_connection(db, current_user.id, connection_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/accounts/{link_id}", response_model=BankConnectionRead)
def update_account_link(
    link_id: str,
    payload: BankConnectionAccountUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return bank_sync.update_account_link(db, current_user.id, link_id, payload)
