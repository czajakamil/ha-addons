"""Skrzynka paragonów — szkice OCR i ich zatwierdzanie na transakcje."""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user, read_upload_capped
from ..models import User
from ..receipt_import import ReceiptImportDependencyError, ReceiptImportError
from ..schemas import (
    BankMatchInfo,
    ReceiptDraftCommit,
    ReceiptDraftCommitResult,
    ReceiptDraftDetail,
    ReceiptDraftListResponse,
    ReceiptDraftUpdate,
    UntaggedCount,
)
from ..services import receipt_drafts as drafts_service

router = APIRouter()


@router.post(
    "/receipt-drafts",
    response_model=ReceiptDraftListResponse,
    status_code=status.HTTP_201_CREATED,
)
def upload_receipt_drafts(
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Wgranie plików: OCR i szkice w skrzynce, bez tworzenia transakcji.

    Celowo zwykłe `def`, nie `async def`: OCR trwa sekundy na stronę i jest
    synchroniczny, więc w `async def` zatrzymałby pętlę zdarzeń — a z nią
    wszystkie inne żądania. FastAPI uruchamia `def` w puli wątków.
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Brak plików do przetworzenia.",
        )

    payloads: list[tuple[str | None, str | None, bytes]] = []
    for upload in files:
        content = read_upload_capped(upload)
        payloads.append((upload.filename, upload.content_type, content))
        upload.file.close()

    try:
        drafts_service.create_drafts_from_uploads(db, current_user.id, payloads)
    except ReceiptImportDependencyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except ReceiptImportError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return list_receipt_drafts(db=db, current_user=current_user)


@router.get("/receipt-drafts", response_model=ReceiptDraftListResponse)
def list_receipt_drafts(
    filter: str = Query(  # noqa: A002 — nazwa parametru query stringa, część kontraktu API
        drafts_service.FILTER_ALL, pattern="^(all|todo|done|skipped)$"
    ),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    drafts, counts = drafts_service.list_drafts(db, current_user.id, filter)
    return ReceiptDraftListResponse(
        drafts=drafts, counts=counts, rules=drafts_service.check_rules()
    )


@router.get("/receipt-drafts/todo/count", response_model=UntaggedCount)
def count_todo_receipt_drafts(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Sama liczba paragonów do przejrzenia — tania, bo woła ją pasek boczny."""
    return UntaggedCount(count=drafts_service.count_todo(db, current_user.id))


@router.get("/receipt-drafts/{draft_id}", response_model=ReceiptDraftDetail)
def get_receipt_draft(
    draft_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    draft = drafts_service.get_draft(db, draft_id, current_user.id)
    return drafts_service.serialize_detail(draft)


@router.get("/receipt-drafts/{draft_id}/image")
def get_receipt_draft_image(
    draft_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    draft = drafts_service.get_draft(db, draft_id, current_user.id)
    path = drafts_service.draft_image_path(draft)
    if not path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Szkic nie ma zapisanego obrazu."
        )
    return FileResponse(path, media_type=draft.image_mime or "image/png")


@router.patch("/receipt-drafts/{draft_id}", response_model=ReceiptDraftDetail)
def update_receipt_draft(
    draft_id: str,
    payload: ReceiptDraftUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    draft = drafts_service.update_draft(
        db, draft_id, current_user.id, payload.payload, payload.status
    )
    return drafts_service.serialize_detail(draft)


@router.get("/receipt-drafts/{draft_id}/bank-matches", response_model=list[BankMatchInfo])
def receipt_draft_bank_matches(
    draft_id: str,
    account_id: str = Query(...),
    date: dt.date = Query(...),
    total_cents: int = Query(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Transakcje z wyciągu, pod które można podpiąć paragon zamiast tworzyć nową.

    Data i suma przychodzą z edytora, a nie ze szkicu — użytkownik mógł je
    właśnie poprawić i jeszcze nie zapisać.
    """
    drafts_service.get_draft(db, draft_id, current_user.id)
    return drafts_service.bank_matches(db, current_user.id, account_id, date, total_cents)


@router.post("/receipt-drafts/{draft_id}/commit", response_model=ReceiptDraftCommitResult)
def commit_receipt_draft(
    draft_id: str,
    payload: ReceiptDraftCommit,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Szkic → transakcja. 422 z listą zastrzeżeń, gdy blokada nie została przyjęta."""
    transaction, draft = drafts_service.commit_draft(db, draft_id, current_user.id, payload)
    return ReceiptDraftCommitResult(
        transaction_id=transaction.id, draft=drafts_service.serialize_detail(draft)
    )


@router.delete("/receipt-drafts/{draft_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_receipt_draft(
    draft_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    drafts_service.delete_draft(db, draft_id, current_user.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
