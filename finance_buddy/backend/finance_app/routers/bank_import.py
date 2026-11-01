"""Import wyciągu bankowego z CSV — rozpoznanie, podgląd, zapis, profile, historia.

Bezstanowo: frontend wysyła plik przy każdym kroku (multipart), a parametry
jako JSON w polu formularza. Projekt: docs/design/import-csv.md §7.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import BaseModel, ValidationError
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user, read_upload_capped
from ..models import User
from ..schemas import (
    BankCommitRequest,
    BankImportBatchRead,
    BankImportProfileRead,
    BankImportProfileUpdate,
    BankImportSpec,
    BankImportUndoResult,
    BankInspectResponse,
    BankParseOptions,
    BankPreviewResponse,
)
from ..services import bank_import as service

router = APIRouter(prefix="/bank-import", tags=["bank-import"])

# Wyciąg roczny to ułamek tego; limit chroni przed pomyłką (np. wgraniem PDF-a).
MAX_CSV_BYTES = 5 * 1024 * 1024


def _read(upload: UploadFile) -> bytes:
    try:
        return read_upload_capped(upload, MAX_CSV_BYTES)
    finally:
        upload.file.close()


def _parse_json[M: BaseModel](model: type[M], raw: str | None) -> M:
    try:
        return model.model_validate_json(raw or "{}")
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=exc.errors(include_url=False, include_context=False),
        ) from exc


@router.post("/inspect", response_model=BankInspectResponse)
def inspect_file(
    file: UploadFile = File(...),
    options: str | None = Form(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    content = _read(file)
    return service.inspect(db, current_user.id, content, _parse_json(BankParseOptions, options))


@router.post("/preview", response_model=BankPreviewResponse)
def preview_file(
    file: UploadFile = File(...),
    spec: str = Form(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    content = _read(file)
    return service.preview(db, current_user.id, content, _parse_json(BankImportSpec, spec))


@router.post("/commit", response_model=BankImportBatchRead, status_code=status.HTTP_201_CREATED)
def commit_file(
    file: UploadFile = File(...),
    request: str = Form(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    content = _read(file)
    return service.commit(
        db,
        current_user.id,
        content,
        file.filename or "wyciag.csv",
        _parse_json(BankCommitRequest, request),
    )


@router.get("/profiles", response_model=list[BankImportProfileRead])
def list_profiles(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return service.list_profiles(db, current_user.id)


@router.patch("/profiles/{profile_id}", response_model=BankImportProfileRead)
def update_profile(
    profile_id: str,
    payload: BankImportProfileUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return service.update_profile(db, current_user.id, profile_id, payload)


@router.delete("/profiles/{profile_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_profile(
    profile_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    service.delete_profile(db, current_user.id, profile_id)


@router.get("/batches", response_model=list[BankImportBatchRead])
def list_batches(
    account_id: str | None = Query(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return service.list_batches(db, current_user.id, account_id)


@router.delete("/batches/{batch_id}", response_model=BankImportUndoResult)
def undo_batch(
    batch_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return service.undo_batch(db, current_user.id, batch_id)
