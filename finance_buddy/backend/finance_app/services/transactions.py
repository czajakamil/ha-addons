"""Business logic for transactions and their attachments.

Handlers in ``routers/transactions.py`` parse the request, delegate to the
functions here, and serialize the response. All persistence, validation, and
domain rules (transfer references, ``recompute_total``, attachment files) live
in this module.
"""

from __future__ import annotations

import datetime as dt
import re
import sqlite3
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import ColumnElement, Select, func, or_, select
from sqlalchemy.orm import InstrumentedAttribute, Session, joinedload

from ..dependencies import (
    ATTACHMENTS_DIR,
    _get_owned_or_404,
    apply_pagination,
    copy_upload_capped,
)
from ..helpers import _is_internal_report_transaction
from ..models import (
    Account,
    Category,
    Product,
    Store,
    Transaction,
    TransactionAttachment,
    TransactionItem,
)
from ..schemas import (
    TransactionAttachmentRead,
    TransactionCreate,
    TransactionDetail,
    TransactionItemCreate,
    TransactionItemDetail,
    TransactionRead,
    TransactionUpdate,
)
from .categories import build_path_map

# --- Attachment helpers ---------------------------------------------------


def _make_attachment_download_url(transaction_id: str, attachment_id: str) -> str:
    return f"/transactions/{transaction_id}/attachments/{attachment_id}"


def attachment_file_path(attachment: TransactionAttachment) -> Path:
    return ATTACHMENTS_DIR / attachment.stored_name


# Types the browser may render in the app's origin. The stored content type is
# whatever the uploading browser claimed, and the UI opens attachments as
# same-origin `blob:` URLs — an HTML or SVG file served as such would run
# script next to the session token in localStorage. Everything else downloads.
INLINE_ATTACHMENT_TYPES = frozenset(
    {
        "application/pdf",
        "image/gif",
        "image/jpeg",
        "image/png",
        "image/webp",
        "text/plain",
    }
)


def attachment_serving(content_type: str | None) -> tuple[str, str]:
    """Return ``(media_type, content_disposition_type)`` for a stored attachment."""
    media_type = (content_type or "").split(";")[0].strip().lower()
    if media_type in INLINE_ATTACHMENT_TYPES:
        return media_type, "inline"
    return "application/octet-stream", "attachment"


def _sanitize_filename(raw_name: str | None) -> str:
    name = (raw_name or "").strip()
    if not name:
        return "attachment"
    return Path(name).name or "attachment"


def _persist_attachment_file(transaction_id: str, upload: UploadFile) -> TransactionAttachment:
    filename = _sanitize_filename(upload.filename)
    stored_name = f"{uuid.uuid4().hex}{Path(filename).suffix}"
    target_path = ATTACHMENTS_DIR / stored_name

    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        size_bytes = copy_upload_capped(upload, target_path)
    finally:
        upload.file.close()

    return TransactionAttachment(
        transaction_id=transaction_id,
        original_name=filename,
        stored_name=stored_name,
        content_type=upload.content_type,
        size_bytes=size_bytes,
    )


# --- Serialization --------------------------------------------------------


def serialize_attachment(attachment: TransactionAttachment) -> TransactionAttachmentRead:
    return TransactionAttachmentRead(
        id=attachment.id,
        transaction_id=attachment.transaction_id,
        original_name=attachment.original_name,
        content_type=attachment.content_type,
        size_bytes=attachment.size_bytes,
        created_at=attachment.created_at,
        download_url=_make_attachment_download_url(attachment.transaction_id, attachment.id),
    )


def serialize_transaction_detail(tx: Transaction) -> TransactionDetail:
    base_data = TransactionRead.model_validate(tx).model_dump(exclude={"items"})
    item_details = [
        TransactionItemDetail(
            id=item.id,
            product_id=item.product_id,
            unit_price_cents=item.unit_price_cents,
            quantity=item.quantity,
            unit=item.unit,
            comment=item.comment,
            raw_name=item.raw_name,
            product_name=item.product.name if item.product else None,
        )
        for item in tx.items
    ]
    attachments = [serialize_attachment(attachment) for attachment in tx.attachments]

    return TransactionDetail(
        **base_data,
        account_name=tx.account.name if tx.account else None,
        store_name=tx.store.name if tx.store else None,
        category_name=tx.category.name if tx.category else None,
        items=item_details,
        attachments=attachments,
    )


# --- Domain rules ---------------------------------------------------------


def _apply_transaction_payload(tx: Transaction, data: dict) -> None:
    for field in (
        "account_id",
        "store_id",
        "category_id",
        "transfer_account_id",
        "transfer_transaction_id",
        "receipt_number",
        "recorded_at",
        "date",
        "currency",
        "payment_method",
        "comment",
        "cleared",
        "reconciled",
    ):
        if field in data:
            setattr(tx, field, data[field])


def _ensure_transaction_dates(tx: Transaction, changed: dict) -> None:
    """Keep ``recorded_at`` and ``date`` in sync, honouring whichever one ``changed`` sets.

    The UI edits only ``date``; deriving it back from the stored ``recorded_at``
    would silently undo that edit. A new date keeps the recorded time of day.
    ``recorded_at`` must be timezone-aware (``UTCDateTime`` rejects naive values).
    """
    if changed.get("recorded_at"):
        tx.date = tx.recorded_at.date()
        return

    if changed.get("date"):
        time_of_day = tx.recorded_at.timetz() if tx.recorded_at else dt.time.min
        if time_of_day.tzinfo is None:
            time_of_day = time_of_day.replace(tzinfo=dt.UTC)
        tx.recorded_at = dt.datetime.combine(tx.date, time_of_day)
        return

    if tx.recorded_at:
        tx.date = tx.recorded_at.date()
        return

    now = dt.datetime.now(dt.UTC)
    tx.recorded_at = now
    tx.date = now.date()


def _sync_transaction_items(tx: Transaction, items_payload: list[TransactionItemCreate]) -> None:
    tx.items.clear()
    for item_data in items_payload:
        tx.items.append(TransactionItem(**item_data.model_dump()))


def _resolve_total_with_items(tx: Transaction, manual_total: int | None) -> int:
    """Total dla transakcji z pozycjami: jawny ``manual_total`` wygrywa z sumą pozycji.

    Pozycje mają dodatnie ceny (CheckConstraint), więc ``recompute_total`` zwraca
    magnitudę bez znaku i bez rabatów. Import paragonu (wydatek) podaje jawny,
    ujemny ``total_cents`` uwzględniający rabaty — i on jest źródłem prawdy o
    obciążeniu. Bez jawnego totala odtwarzamy sumę z pozycji (zgodnie z dotychczasowym
    zachowaniem dla wywołań budujących transakcję wyłącznie z pozycji).
    """
    if manual_total is not None:
        return manual_total
    return tx.recompute_total()


def _validate_transaction_refs(db: Session, data: dict, user_id: str) -> None:
    if data.get("account_id"):
        _get_owned_or_404(db, Account, data["account_id"], user_id, "Account")
    if data.get("store_id"):
        _get_owned_or_404(db, Store, data["store_id"], user_id, "Store")
    if data.get("category_id"):
        _get_owned_or_404(db, Category, data["category_id"], user_id, "Category")
    if data.get("transfer_account_id"):
        _get_owned_or_404(db, Account, data["transfer_account_id"], user_id, "Account")


def _validate_item_products(
    db: Session, items_payload: list[TransactionItemCreate], user_id: str
) -> None:
    if not items_payload:
        return
    product_ids = {item.product_id for item in items_payload}
    rows = db.execute(
        select(func.count(Product.id)).where(
            Product.user_id == user_id, Product.id.in_(product_ids)
        )
    ).scalar_one()
    if rows != len(product_ids):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid product selection."
        )


# --- Use cases ------------------------------------------------------------


def get_transaction_detail(db: Session, transaction_id: str, user_id: str) -> TransactionDetail:
    stmt = (
        select(Transaction)
        .options(
            joinedload(Transaction.account),
            joinedload(Transaction.store),
            joinedload(Transaction.category),
            joinedload(Transaction.items).joinedload(TransactionItem.product),
            joinedload(Transaction.attachments),
        )
        .where(Transaction.id == transaction_id, Transaction.user_id == user_id)
    )
    tx = db.scalars(stmt).unique().one_or_none()
    if not tx:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    return serialize_transaction_detail(tx)


def create_transaction(db: Session, payload: TransactionCreate, user_id: str) -> Transaction:
    payload_dict = payload.model_dump()
    items_payload = [TransactionItemCreate(**item) for item in payload_dict.pop("items", [])]
    manual_total = payload_dict.pop("total_cents", None)
    data = payload_dict

    tx = Transaction()
    tx.user_id = user_id
    _validate_transaction_refs(db, data, user_id)
    _validate_item_products(db, items_payload, user_id)
    _apply_transaction_payload(tx, data)

    _ensure_transaction_dates(tx, data)

    _sync_transaction_items(tx, items_payload)

    if items_payload:
        tx.total_cents = _resolve_total_with_items(tx, manual_total)
    elif manual_total is not None:
        tx.total_cents = manual_total
    else:
        tx.total_cents = 0

    db.add(tx)
    db.commit()
    db.refresh(tx)
    return tx


def update_transaction(
    db: Session, transaction_id: str, payload: TransactionUpdate, user_id: str
) -> Transaction:
    tx = _get_owned_or_404(db, Transaction, transaction_id, user_id, "Transaction")

    data = payload.model_dump(exclude_unset=True)
    if "account_id" in data and not data["account_id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="account_id cannot be null",
        )
    items_payload_raw = data.pop("items", None)
    manual_total = data.pop("total_cents", None)
    items_payload = (
        [TransactionItemCreate(**item) for item in items_payload_raw]
        if items_payload_raw is not None
        else None
    )

    _validate_transaction_refs(db, data, user_id)
    if items_payload is not None:
        _validate_item_products(db, items_payload, user_id)

    _apply_transaction_payload(tx, data)

    _ensure_transaction_dates(tx, data)

    if items_payload is not None:
        _sync_transaction_items(tx, items_payload)
        if items_payload:
            tx.total_cents = _resolve_total_with_items(tx, manual_total)
        else:
            tx.total_cents = 0
    elif manual_total is not None:
        tx.total_cents = manual_total

    db.add(tx)
    db.commit()
    db.refresh(tx)
    return tx


def delete_transaction(db: Session, transaction_id: str, user_id: str) -> None:
    tx = _get_owned_or_404(db, Transaction, transaction_id, user_id, "Transaction")

    for attachment in list(tx.attachments):
        path = attachment_file_path(attachment)
        if path.exists():
            path.unlink(missing_ok=True)

    db.delete(tx)
    db.commit()


def add_attachment(
    db: Session, transaction_id: str, upload: UploadFile, user_id: str
) -> TransactionAttachmentRead:
    _get_owned_or_404(db, Transaction, transaction_id, user_id, "Transaction")

    attachment = _persist_attachment_file(transaction_id, upload)
    db.add(attachment)
    db.commit()
    db.refresh(attachment)
    return serialize_attachment(attachment)


def _get_owned_attachment(
    db: Session, transaction_id: str, attachment_id: str, user_id: str
) -> TransactionAttachment:
    _get_owned_or_404(db, Transaction, transaction_id, user_id, "Transaction")
    attachment = db.get(TransactionAttachment, attachment_id)
    if not attachment or attachment.transaction_id != transaction_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment not found")
    return attachment


def delete_attachment(db: Session, transaction_id: str, attachment_id: str, user_id: str) -> None:
    attachment = _get_owned_attachment(db, transaction_id, attachment_id, user_id)

    path = attachment_file_path(attachment)
    if path.exists():
        path.unlink(missing_ok=True)

    db.delete(attachment)
    db.commit()


def resolve_attachment_download(
    db: Session, transaction_id: str, attachment_id: str, user_id: str
) -> tuple[Path, TransactionAttachment]:
    """Validate ownership and return the on-disk path + attachment metadata.

    The router builds the ``FileResponse`` from the returned values.
    """
    attachment = _get_owned_attachment(db, transaction_id, attachment_id, user_id)

    path = attachment_file_path(attachment)
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Attachment file missing")
    return path, attachment


# --- Listing and search -----------------------------------------------------

# Kwota w wyszukiwarce: „12”, „12,5”, „-12.50”, „1 234,56 zł”. Cyfry są przycięte,
# żeby nie przekroczyć zakresu INTEGER w SQLite przy przeliczaniu na grosze.
_AMOUNT_QUERY_RE = re.compile(
    r"^[-+\u2212]?\s*(\d{1,3}(?:[ \u00a0]\d{3}){1,3}|\d{1,12})(?:[.,](\d{1,2}))?\s*(?:zł|pln)?$",
    re.IGNORECASE,
)


def parse_amount_query(text: str) -> tuple[int, int] | None:
    """Zamienia frazę wyszukiwania na zakres groszy (wartość bezwzględna) albo ``None``.

    Liczba bez części ułamkowej oznacza całe złote, więc „12” trafia od 12,00
    do 12,99; z groszami („12,5”, „12,50”) — dokładnie tę kwotę. Znak jest
    pomijany: wydatki są zapisane ujemnie, a nikt nie wpisuje minusa, szukając
    paragonu.
    """
    match = _AMOUNT_QUERY_RE.match(text.strip())
    if not match:
        return None
    whole = int(re.sub(r"[ \u00a0]", "", match.group(1)))
    fraction = match.group(2)
    if fraction is None:
        return whole * 100, whole * 100 + 99
    cents = whole * 100 + int(fraction.ljust(2, "0"))
    return cents, cents


def _casefold(value: str | None) -> str | None:
    return value.casefold() if value is not None else None


def _register_casefold(db: Session) -> None:
    """Rejestruje ``py_casefold`` na połączeniu SQLite tej sesji.

    Wbudowane ``lower()`` i ``LIKE`` w SQLite zmieniają wielkość liter tylko w
    ASCII — „żabka” nie trafiłaby w „Żabka”. Rejestracja na bieżącym połączeniu
    (a nie w ``make_engine``) działa też z silnikami tworzonymi w testach.
    """
    driver_connection = db.connection().connection.driver_connection
    if isinstance(driver_connection, sqlite3.Connection):
        driver_connection.create_function("py_casefold", 1, _casefold, deterministic=True)


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _search_clause(db: Session, user_id: str, search: str) -> ColumnElement[bool]:
    """Warunek wyszukiwania: sklep, komentarz, konto, tag (ze ścieżką), produkt, kwota."""
    needle = search.strip().casefold()
    _register_casefold(db)
    pattern = f"%{_escape_like(needle)}%"

    def matches(column: ColumnElement[str | None] | InstrumentedAttribute) -> ColumnElement[bool]:
        return func.py_casefold(column).like(pattern, escape="\\")

    # Ścieżki tagów są wyliczane z drzewa, więc dopasowujemy je w Pythonie.
    # Fraza „jedzenie” trafia też w potomków („Jedzenie/Restauracje”).
    tag_needle = needle.lstrip("#")
    tag_ids = [
        category_id
        for category_id, path in build_path_map(db, user_id).items()
        if tag_needle and tag_needle in path.casefold()
    ]
    matching_accounts = select(Account.id).where(Account.user_id == user_id, matches(Account.name))
    item_match = matches(Product.name)
    if tag_ids:
        item_match = or_(item_match, Product.category_id.in_(tag_ids))

    clauses = [
        matches(Transaction.comment),
        matches(Store.name),
        matches(Store.group_name),
        Transaction.account_id.in_(matching_accounts),
        # Przelew pokazujemy jako „Transfer: <konto docelowe>”.
        Transaction.transfer_account_id.in_(matching_accounts),
        select(TransactionItem.id)
        .join(Product, TransactionItem.product_id == Product.id)
        .where(TransactionItem.transaction_id == Transaction.id, item_match)
        .exists(),
    ]
    if tag_ids:
        clauses.append(Transaction.category_id.in_(tag_ids))
    amount = parse_amount_query(search)
    if amount is not None:
        clauses.append(func.abs(Transaction.total_cents).between(*amount))
    return or_(*clauses)


def _filtered_transactions(
    db: Session,
    user_id: str,
    *,
    start_date: dt.date | None,
    end_date: dt.date | None,
    account_id: str | None,
    store_id: str | None,
    category_id: str | None,
    search: str | None,
    exclude_internal: bool,
    store_ids: Sequence[str] | None = None,
    account_ids: Sequence[str] | None = None,
    category_ids: Sequence[str] | None = None,
) -> Select[tuple[Transaction]]:
    stmt = (
        select(Transaction)
        # joiny na potrzeby filtrów po nazwach (wyszukiwarka, exclude_internal);
        # obie relacje są wiele-do-jednego, więc nie mnożą wierszy przy liczeniu
        .outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(Transaction.user_id == user_id)
    )

    if start_date is not None:
        stmt = stmt.where(Transaction.date >= start_date)
    if end_date is not None:
        stmt = stmt.where(Transaction.date <= end_date)
    if account_id is not None:
        _get_owned_or_404(db, Account, account_id, user_id, "Account")
        stmt = stmt.where(Transaction.account_id == account_id)
    if account_ids:
        # Bez sprawdzania właściciela: wyżej zawężamy do transakcji użytkownika.
        stmt = stmt.where(Transaction.account_id.in_(account_ids))
    if store_id is not None:
        _get_owned_or_404(db, Store, store_id, user_id, "Store")
        stmt = stmt.where(Transaction.store_id == store_id)
    if store_ids:
        # Bez sprawdzania właściciela: wyżej zawężamy do transakcji użytkownika.
        stmt = stmt.where(Transaction.store_id.in_(store_ids))
    if category_ids:
        # Bez sprawdzania właściciela: wyżej zawężamy do transakcji użytkownika.
        stmt = stmt.where(Transaction.category_id.in_(category_ids))
    if category_id is not None:
        if category_id == "__uncategorized__":
            stmt = stmt.where(Transaction.category_id.is_(None))
        else:
            _get_owned_or_404(db, Category, category_id, user_id, "Category")
            stmt = stmt.where(Transaction.category_id == category_id)

    if exclude_internal:
        stmt = stmt.where(~_is_internal_report_transaction())

    if search and search.strip():
        stmt = stmt.where(_search_clause(db, user_id, search))

    return stmt


def _page_of(
    db: Session, filtered: Select[tuple[Transaction]], offset: int, limit: int
) -> list[Transaction]:
    # `id` rozstrzyga remisy (import z YNAB daje wiele transakcji o północy);
    # bez niego kolejne strony mogłyby powtarzać lub gubić wiersze.
    stmt = filtered.options(joinedload(Transaction.items)).order_by(
        Transaction.recorded_at.desc(), Transaction.id.desc()
    )
    return list(db.scalars(apply_pagination(stmt, offset, limit)).unique().all())


def list_transactions(
    db: Session,
    user_id: str,
    *,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    account_id: str | None = None,
    account_ids: Sequence[str] | None = None,
    store_id: str | None = None,
    store_ids: Sequence[str] | None = None,
    category_id: str | None = None,
    search: str | None = None,
    exclude_internal: bool = False,
    offset: int = 0,
    limit: int = 50,
) -> list[Transaction]:
    filtered = _filtered_transactions(
        db,
        user_id,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        account_ids=account_ids,
        store_id=store_id,
        store_ids=store_ids,
        category_id=category_id,
        search=search,
        exclude_internal=exclude_internal,
    )
    return _page_of(db, filtered, offset, limit)


def list_transactions_page(
    db: Session,
    user_id: str,
    *,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    account_id: str | None = None,
    account_ids: Sequence[str] | None = None,
    store_ids: Sequence[str] | None = None,
    category_ids: Sequence[str] | None = None,
    search: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """Strona wyników listy transakcji wraz z liczbą wszystkich pasujących."""
    filtered = _filtered_transactions(
        db,
        user_id,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        account_ids=account_ids,
        store_id=None,
        store_ids=store_ids,
        category_id=None,
        category_ids=category_ids,
        search=search,
        exclude_internal=False,
    )
    ids = filtered.with_only_columns(Transaction.id).subquery()
    total = db.scalar(select(func.count()).select_from(ids)) or 0
    return {
        "items": _page_of(db, filtered, offset, limit),
        "total": total,
        "offset": offset,
        "limit": limit,
    }
