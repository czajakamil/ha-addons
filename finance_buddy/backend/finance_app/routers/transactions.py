"""Transaction and attachment endpoints.

These handlers only parse the request and serialize the response; the domain
logic lives in :mod:`finance_app.services.transactions`.
"""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, File, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from ..db import get_db_session
from ..dependencies import get_current_user
from ..models import User
from ..schemas import (
    TagStoreRequest,
    TagStoreResult,
    TagSuggestionRead,
    TransactionAttachmentRead,
    TransactionCreate,
    TransactionDetail,
    TransactionPage,
    TransactionRead,
    TransactionUpdate,
    UntaggedCount,
    UntaggedTransactionRead,
    UntaggedTransactionsPage,
)
from ..services import tagging as tagging_service
from ..services import transactions as transactions_service

router = APIRouter()


# Musi być zarejestrowana przed `/transactions/{transaction_id}`, inaczej
# „page” trafiłoby do tamtej trasy jako identyfikator.
@router.get("/transactions/page", response_model=TransactionPage)
def list_transactions_page(
    start_date: dt.date | None = Query(None),
    end_date: dt.date | None = Query(None),
    account_id: str | None = Query(None),
    account_ids: list[str] | None = Query(None),
    store_ids: list[str] | None = Query(None),
    search: str | None = Query(None, max_length=200),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    """Lista transakcji do widoku Transakcje: strona wyników + liczba wszystkich pasujących."""
    return transactions_service.list_transactions_page(
        db,
        current_user.id,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        account_ids=account_ids,
        store_ids=store_ids,
        search=search,
        offset=offset,
        limit=limit,
    )


# Kolejka „do otagowania" — przed `/transactions/{transaction_id}`, bo FastAPI
# dopasowuje trasy w kolejności rejestracji i „untagged" wziąłby za id.
@router.get("/transactions/untagged", response_model=UntaggedTransactionsPage)
def list_untagged_transactions(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    total, entries = tagging_service.list_untagged(db, current_user.id, offset=offset, limit=limit)
    items = [
        UntaggedTransactionRead(
            id=entry.transaction.id,
            date=entry.transaction.date,
            total_cents=entry.transaction.total_cents,
            currency=entry.transaction.currency,
            comment=entry.transaction.comment,
            account_id=entry.account.id,
            account_name=entry.account.name,
            store_id=entry.store.id if entry.store else None,
            store_name=entry.store.name if entry.store else None,
            store_untagged_count=entry.store_untagged_count,
            suggestion=(
                TagSuggestionRead.model_validate(entry.suggestion) if entry.suggestion else None
            ),
        )
        for entry in entries
    ]
    return UntaggedTransactionsPage(total=total, items=items)


@router.get("/transactions/untagged/count", response_model=UntaggedCount)
def count_untagged_transactions(
    store_id: str | None = Query(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return UntaggedCount(
        count=tagging_service.count_untagged(db, current_user.id, store_id=store_id)
    )


@router.post("/transactions/untagged/tag-store", response_model=TagStoreResult)
def tag_store_untagged_transactions(
    payload: TagStoreRequest,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    updated = tagging_service.tag_store_queue(
        db, current_user.id, payload.store_id, payload.category_id
    )
    return TagStoreResult(updated=updated)


@router.get("/transactions/{transaction_id}", response_model=TransactionDetail)
def get_transaction(
    transaction_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return transactions_service.get_transaction_detail(db, transaction_id, current_user.id)


@router.post("/transactions", response_model=TransactionRead, status_code=status.HTTP_201_CREATED)
def create_transaction(
    payload: TransactionCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return transactions_service.create_transaction(db, payload, current_user.id)


@router.put("/transactions/{transaction_id}", response_model=TransactionRead)
@router.patch("/transactions/{transaction_id}", response_model=TransactionRead)
def update_transaction(
    transaction_id: str,
    payload: TransactionUpdate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return transactions_service.update_transaction(db, transaction_id, payload, current_user.id)


@router.delete("/transactions/{transaction_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_transaction(
    transaction_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    transactions_service.delete_transaction(db, transaction_id, current_user.id)
    return


@router.post(
    "/transactions/{transaction_id}/attachments",
    response_model=TransactionAttachmentRead,
    status_code=status.HTTP_201_CREATED,
)
def upload_transaction_attachment(
    transaction_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return transactions_service.add_attachment(db, transaction_id, file, current_user.id)


@router.delete(
    "/transactions/{transaction_id}/attachments/{attachment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_transaction_attachment(
    transaction_id: str,
    attachment_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    transactions_service.delete_attachment(db, transaction_id, attachment_id, current_user.id)
    return


@router.get("/transactions/{transaction_id}/attachments/{attachment_id}")
def download_transaction_attachment(
    transaction_id: str,
    attachment_id: str,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    path, attachment = transactions_service.resolve_attachment_download(
        db, transaction_id, attachment_id, current_user.id
    )
    media_type, disposition = transactions_service.attachment_serving(attachment.content_type)
    # `filename=` instead of a hand-built header: Starlette emits the RFC 5987
    # `filename*` form, so names like "paragon_żabka.pdf" don't crash on latin-1.
    return FileResponse(
        path,
        media_type=media_type,
        filename=attachment.original_name,
        content_disposition_type=disposition,
        headers={"X-Content-Type-Options": "nosniff"},
    )


@router.get("/transactions", response_model=list[TransactionRead])
def list_transactions(
    start_date: dt.date | None = Query(None),
    end_date: dt.date | None = Query(None),
    account_id: str | None = Query(None),
    account_ids: list[str] | None = Query(None),
    store_id: str | None = Query(None),
    store_ids: list[str] | None = Query(None),
    category_id: str | None = Query(None),
    search: str | None = Query(None, max_length=200),
    exclude_internal: bool = Query(False),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_user),
):
    return transactions_service.list_transactions(
        db,
        current_user.id,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        account_ids=account_ids,
        store_id=store_id,
        store_ids=store_ids,
        category_id=category_id,
        search=search,
        exclude_internal=exclude_internal,
        offset=offset,
        limit=limit,
    )
