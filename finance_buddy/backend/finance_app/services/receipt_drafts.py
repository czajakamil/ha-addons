"""Skrzynka paragonów: szkice między wgraniem pliku a transakcją.

Wgranie plików uruchamia OCR i tworzy **szkice**, nie transakcje. Dzięki temu
wsad przegląda się na raty, a nie w jednej sesji, i każdy paragon dostaje pełny
ekran zamiast kafelka. Transakcja powstaje dopiero w `commit_draft` — i tylko
przez bramkę `receipt_checks`, która jest tu ostatnim słowem: formularz liczy te
same reguły u siebie, żeby nie pytać serwera przy każdym naciśnięciu klawisza,
ale to nie znaczy, że można je ominąć.

Obraz strony trzymamy jako PNG obok załączników. Przy zatwierdzeniu **kopiujemy**
go na załącznik transakcji, zamiast przekazywać ten sam plik: inaczej usunięcie
szkicu zabierałoby plik należący już do transakcji.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
import shutil
import threading
import uuid
from pathlib import Path

from fastapi import HTTPException, status
from PIL import Image
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import receipt_checks
from ..config import settings
from ..dependencies import _get_owned_or_404
from ..models import (
    Account,
    Category,
    Product,
    ReceiptDraft,
    Store,
    Transaction,
    TransactionAttachment,
    TransactionItem,
)
from ..receipt_extraction import ReceiptExtraction, create_extractor
from ..receipt_import import (
    ReceiptImportDependencyError,
    build_store_index,
    iter_receipt_pages,
    match_store,
)
from ..schemas import (
    BankMatchInfo,
    ReceiptCheckRules,
    ReceiptDraftCommit,
    ReceiptDraftCounts,
    ReceiptDraftDetail,
    ReceiptDraftPayload,
    ReceiptDraftSummary,
    TransactionCreate,
    TransactionItemCreate,
)
from . import bank_import, product_matching, transaction_matching
from . import categories as categories_service
from . import transactions as transactions_service
from .store_aliases import register_store_name

log = logging.getLogger(__name__)

DRAFTS_DIR = Path(settings.attachments_dir) / "receipt_drafts"

# Wgrania obsługuje pula wątków, więc dwa naraz liczyłyby OCR równolegle. Na
# Raspberry Pi to podwaja szczyt pamięci przy zerowym zysku czasu (modele i tak
# zajmują wszystkie rdzenie) — kolejkujemy strony jedną za drugą.
_OCR_LOCK = threading.Lock()

# Filtry kolejki dzielą szkice po statusie i tylko po nim — każdy szkic wpada
# dokładnie do jednego kubełka, więc liczniki sumują się do całości. Zastrzeżenia
# to cecha wiersza, nie osobny kubełek: widać je na liście po etykiecie.
FILTER_ALL = "all"
FILTER_TODO = "todo"
FILTER_DONE = "done"
FILTER_SKIPPED = "skipped"


def check_rules() -> ReceiptCheckRules:
    return ReceiptCheckRules(
        scan_window_days=receipt_checks.SCAN_WINDOW_DAYS,
        item_sum_tolerance_ratio=receipt_checks.ITEM_SUM_TOLERANCE_RATIO,
        item_sum_tolerance_min_cents=receipt_checks.ITEM_SUM_TOLERANCE_MIN_CENTS,
    )


# --- Tworzenie szkiców z wgranych plików ----------------------------------


def _payload_from_extraction(extraction: ReceiptExtraction) -> dict:
    happened_at = (
        extraction.happened_at.strftime("%Y-%m-%dT%H:%M") if extraction.happened_at else None
    )
    return ReceiptDraftPayload(
        store_name=extraction.store_name,
        nip=extraction.nip,
        happened_at=happened_at,
        total_cents=extraction.total_cents,
        currency=extraction.currency or "PLN",
        items=[
            {
                "raw_name": item.raw_name,
                "quantity": item.quantity,
                "unit": item.unit,
                "unit_price_cents": item.unit_price_cents,
                "line_total_cents": item.line_total_cents,
                "discount_cents": item.discount_cents,
                "discount_label": item.discount_label,
            }
            for item in extraction.items
        ],
    ).model_dump()


def _resolve_scan_date(source_name: str, uploaded_on: dt.date) -> dt.date:
    """Granica dla daty zakupu: z nazwy pliku, a gdy jej tam nie ma — dzień wgrania.

    Paragon nie może być późniejszy niż moment, w którym trafił do systemu. Dla
    skanów zbiorczych (`Converted_2024_06_11.pdf`) nazwa pliku jest znacznie
    lepszą granicą niż „dzisiaj", bo pod skaner trafiają paragony sprzed tygodni.
    """
    return receipt_checks.scan_date(source_name) or uploaded_on


def _store_page_image(image: Image.Image) -> tuple[str, int]:
    DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    stored_name = f"{uuid.uuid4().hex}.png"
    target = DRAFTS_DIR / stored_name
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    target.write_bytes(buffer.getvalue())
    return stored_name, target.stat().st_size


def draft_image_path(draft: ReceiptDraft) -> Path | None:
    if not draft.image_name:
        return None
    path = DRAFTS_DIR / draft.image_name
    return path if path.exists() else None


def create_drafts_from_uploads(
    db: Session,
    user_id: str,
    files: list[tuple[str | None, str | None, bytes]],
) -> list[ReceiptDraft]:
    extractor = create_extractor()
    extractor.ensure_ready()

    store_index = build_store_index(db, user_id)
    today = dt.datetime.now(dt.UTC).date()
    drafts: list[ReceiptDraft] = []

    for page in iter_receipt_pages(files):
        scan_date = _resolve_scan_date(page.source_name, today)

        if page.image is None:
            drafts.append(
                ReceiptDraft(
                    user_id=user_id,
                    source_name=page.source_name,
                    page_number=page.page_number,
                    scan_date=scan_date,
                    payload=ReceiptDraftPayload().model_dump(),
                    error=page.error,
                )
            )
            continue

        try:
            with _OCR_LOCK:
                extraction = extractor.extract(page.image)
            store = match_store(store_index, extraction.nip)
            image_name, image_bytes = _store_page_image(page.image)
            payload = _payload_from_extraction(extraction)
            drafts.append(
                ReceiptDraft(
                    user_id=user_id,
                    source_name=page.source_name,
                    page_number=page.page_number,
                    scan_date=scan_date,
                    image_name=image_name,
                    image_mime="image/png",
                    image_bytes=image_bytes,
                    raw_text=extraction.raw_text,
                    # Odczyt silnika zostaje nietknięty obok edytowalnej etykiety:
                    # różnica między nimi to darmowy pomiar jakości ekstrakcji.
                    ocr_payload=payload,
                    payload=dict(payload),
                    confidence=extraction.confidence,
                    matched_store_id=store.id if store else None,
                )
            )
        except ReceiptImportDependencyError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("OCR paragonu wysypał się na %s s.%s", page.source_name, page.page_number)
            drafts.append(
                ReceiptDraft(
                    user_id=user_id,
                    source_name=page.source_name,
                    page_number=page.page_number,
                    scan_date=scan_date,
                    payload=ReceiptDraftPayload().model_dump(),
                    error=str(exc),
                )
            )
        finally:
            page.image.close()

    for draft in drafts:
        db.add(draft)
    db.commit()
    for draft in drafts:
        db.refresh(draft)
    return drafts


# --- Odczyt ----------------------------------------------------------------


def _label_for_checks(draft: ReceiptDraft, payload: dict | None = None) -> dict:
    label = dict(payload if payload is not None else (draft.payload or {}))
    label["scan_date"] = draft.scan_date.isoformat() if draft.scan_date else None
    label["source"] = draft.source_name
    return label


def draft_issues(draft: ReceiptDraft, payload: dict | None = None) -> list[dict]:
    """Zastrzeżenia do szkicu; zatwierdzony już ich nie zbiera.

    Po zatwierdzeniu etykieta jest zamknięta — trzymanie jej dalej na liście
    „do przejrzenia" kazałoby poprawiać coś, czego poprawienie i tak nie ruszy
    powstałej transakcji.
    """
    if draft.status == ReceiptDraft.STATUS_COMMITTED:
        return []
    return receipt_checks.label_issues(_label_for_checks(draft, payload))


def _summary_fields(draft: ReceiptDraft) -> dict:
    payload = draft.payload or {}
    return {
        "id": draft.id,
        "status": draft.status,
        "source_name": draft.source_name,
        "page_number": draft.page_number,
        "scan_date": draft.scan_date,
        "store_name": payload.get("store_name"),
        "happened_at": payload.get("happened_at"),
        "total_cents": payload.get("total_cents"),
        "currency": payload.get("currency") or "PLN",
        "item_count": len(payload.get("items") or []),
        "issues": draft_issues(draft),
        "transaction_id": draft.transaction_id,
        "error": draft.error,
        "created_at": draft.created_at,
    }


def serialize_summary(draft: ReceiptDraft) -> ReceiptDraftSummary:
    return ReceiptDraftSummary(**_summary_fields(draft))


def serialize_detail(draft: ReceiptDraft) -> ReceiptDraftDetail:
    return ReceiptDraftDetail(
        **_summary_fields(draft),
        payload=ReceiptDraftPayload(**(draft.payload or {})),
        ocr_payload=ReceiptDraftPayload(**draft.ocr_payload) if draft.ocr_payload else None,
        raw_text=draft.raw_text,
        matched_store_id=draft.matched_store_id,
        matched_store_name=draft.matched_store.name if draft.matched_store else None,
        confidence=draft.confidence,
        image_url=f"/receipt-drafts/{draft.id}/image" if draft.image_name else None,
    )


def _matches_filter(draft: ReceiptDraft, mode: str) -> bool:
    if mode == FILTER_TODO:
        return draft.status == ReceiptDraft.STATUS_TODO
    if mode == FILTER_DONE:
        return draft.status == ReceiptDraft.STATUS_COMMITTED
    if mode == FILTER_SKIPPED:
        return draft.status == ReceiptDraft.STATUS_SKIPPED
    return True


def list_drafts(
    db: Session, user_id: str, mode: str = FILTER_ALL
) -> tuple[list[ReceiptDraftSummary], ReceiptDraftCounts]:
    drafts = db.scalars(
        select(ReceiptDraft)
        .where(ReceiptDraft.user_id == user_id)
        .order_by(ReceiptDraft.created_at, ReceiptDraft.source_name, ReceiptDraft.page_number)
    ).all()

    summaries = [(draft, serialize_summary(draft)) for draft in drafts]
    counts = ReceiptDraftCounts(
        total=len(summaries),
        todo=sum(1 for d, _ in summaries if d.status == ReceiptDraft.STATUS_TODO),
        # Poprzeczny do statusów, dlatego nie jest filtrem: sygnalizuje „jest tu
        # coś do obejrzenia", a liczy się z reguł, żeby zmiana reguły nie
        # wymagała przeliczania kolumny w bazie.
        review=sum(1 for _, s in summaries if s.issues),
        skipped=sum(1 for d, _ in summaries if d.status == ReceiptDraft.STATUS_SKIPPED),
        committed=sum(1 for d, _ in summaries if d.status == ReceiptDraft.STATUS_COMMITTED),
    )
    visible = [s for d, s in summaries if _matches_filter(d, mode)]
    return visible, counts


def get_draft(db: Session, draft_id: str, user_id: str) -> ReceiptDraft:
    return _get_owned_or_404(db, ReceiptDraft, draft_id, user_id, "Receipt draft")


# --- Zapis szkicu ----------------------------------------------------------


def _reject_if_committed(draft: ReceiptDraft) -> None:
    if draft.status == ReceiptDraft.STATUS_COMMITTED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Szkic jest już zatwierdzony — popraw powstałą transakcję.",
        )


def update_draft(
    db: Session,
    draft_id: str,
    user_id: str,
    payload: ReceiptDraftPayload | None,
    new_status: str | None,
) -> ReceiptDraft:
    draft = get_draft(db, draft_id, user_id)
    _reject_if_committed(draft)

    if payload is not None:
        draft.payload = payload.model_dump()
    if new_status is not None:
        if new_status not in (ReceiptDraft.STATUS_TODO, ReceiptDraft.STATUS_SKIPPED):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Status szkicu można ustawić tylko na 'todo' albo 'skipped'.",
            )
        draft.status = new_status

    db.add(draft)
    db.commit()
    db.refresh(draft)
    return draft


def delete_draft(db: Session, draft_id: str, user_id: str) -> None:
    draft = get_draft(db, draft_id, user_id)
    path = draft_image_path(draft)
    if path:
        path.unlink(missing_ok=True)
    db.delete(draft)
    db.commit()


# --- Zatwierdzenie ---------------------------------------------------------


def _parse_happened_at(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = value.strip().replace(" ", "T")
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=dt.UTC)
        except ValueError:
            continue
    return None


def _gate_or_422(draft: ReceiptDraft, request: ReceiptDraftCommit) -> None:
    """Ostatnie słowo przed zapisem — blokady, których użytkownik nie przyjął imiennie.

    Front liczy te same reguły u siebie, więc w normalnym przebiegu ta bramka
    milczy. Kiedy jednak odezwie się na czymś, czego formularz nie pokazał,
    znaczy to, że wspólne fikstury (`shared/receipt_checks_cases.json`) nie
    objęły przypadku — dlatego logujemy to głośno, zamiast tylko odesłać 422.
    """
    label = _label_for_checks(draft, request.payload.model_dump())
    blocking = receipt_checks.blocking_issues(label)
    acknowledged = set(request.acknowledged_issues)
    unacknowledged = [issue for issue in blocking if issue["code"] not in acknowledged]
    if not unacknowledged:
        return

    log.warning(
        "Bramka zapisu zatrzymała szkic %s na %s — formularz tego nie pokazał?",
        draft.id,
        [issue["code"] for issue in unacknowledged],
    )
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail={"issues": unacknowledged},
    )


def _resolve_store(
    db: Session, draft: ReceiptDraft, request: ReceiptDraftCommit, user_id: str
) -> str | None:
    if request.store_id:
        _get_owned_or_404(db, Store, request.store_id, user_id, "Store")
        return request.store_id
    if draft.matched_store_id:
        return draft.matched_store_id

    name = (request.payload.store_name or "").strip()
    nip = (request.payload.nip or "").strip()
    if not name and not nip:
        return None
    store = Store(user_id=user_id, name=name or "Sklep z paragonu", nip=nip or None)
    db.add(store)
    register_store_name(db, store)
    return store.id


def _resolve_items(
    db: Session, request: ReceiptDraftCommit, user_id: str
) -> tuple[list[TransactionItemCreate], list[tuple[str, str]]]:
    """Pozycje transakcji + aliasy do nauczenia po zapisie."""
    items: list[TransactionItemCreate] = []
    alias_intents: list[tuple[str, str]] = []

    for row in request.items:
        product_id = row.product_id
        if not product_id:
            name = (row.new_product_name or row.raw_name).strip()
            if not name:
                continue  # pozycja bez produktu (np. linia rabatu) — pomijamy
            category_id = None
            if row.category_path and row.category_path.strip():
                category_id = categories_service.ensure_path(
                    db, user_id, row.category_path.strip()
                ).id
            product = Product(user_id=user_id, name=name, category_id=category_id)
            db.add(product)
            db.flush()
            product_id = product.id
        else:
            _get_owned_or_404(db, Product, product_id, user_id, "Product")

        quantity = row.quantity if row.quantity and row.quantity > 0 else 1
        unit_price = row.unit_price_cents
        if (unit_price is None or unit_price < 0) and row.line_total_cents is not None:
            unit_price = round(abs(row.line_total_cents) / quantity)
        if unit_price is None or unit_price < 0:
            continue  # bez sensownej ceny nie ma czego zapisać

        items.append(
            TransactionItemCreate(
                product_id=product_id,
                unit_price_cents=abs(unit_price),
                quantity=quantity,
                unit=row.unit,
            )
        )
        if row.learn_alias and row.raw_name.strip():
            alias_intents.append((product_id, row.raw_name))

    return items, alias_intents


def _attach_receipt_image(db: Session, draft: ReceiptDraft, transaction_id: str) -> None:
    source = draft_image_path(draft)
    if not source:
        return
    stored_name = f"{uuid.uuid4().hex}.png"
    target = Path(settings.attachments_dir) / stored_name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    db.add(
        TransactionAttachment(
            transaction_id=transaction_id,
            original_name=f"{Path(draft.source_name).stem}-s{draft.page_number}.png",
            stored_name=stored_name,
            content_type="image/png",
            size_bytes=target.stat().st_size,
        )
    )


def commit_draft(
    db: Session, draft_id: str, user_id: str, request: ReceiptDraftCommit
) -> tuple[Transaction, ReceiptDraft]:
    draft = get_draft(db, draft_id, user_id)
    _reject_if_committed(draft)
    _gate_or_422(draft, request)

    account = _get_owned_or_404(db, Account, request.account_id, user_id, "Account")

    total_cents = request.payload.total_cents
    if total_cents is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Podaj sumę paragonu przed zatwierdzeniem.",
        )

    store_id = _resolve_store(db, draft, request, user_id)
    items, alias_intents = _resolve_items(db, request, user_id)
    recorded_at = _parse_happened_at(request.payload.happened_at) or dt.datetime.now(dt.UTC)

    if request.merge_into_transaction_id:
        transaction = _merge_into_bank_transaction(
            db, request, user_id, store_id, items, recorded_at, total_cents
        )
    else:
        transaction = _create_receipt_transaction(
            db, request, user_id, account, store_id, items, recorded_at, total_cents
        )

    _attach_receipt_image(db, draft, transaction.id)
    for product_id, raw_name in alias_intents:
        product_matching.save_alias(db, user_id, product_id, raw_name, store_id)

    draft.payload = request.payload.model_dump()
    draft.status = ReceiptDraft.STATUS_COMMITTED
    draft.transaction_id = transaction.id
    db.add(draft)
    db.commit()
    db.refresh(draft)
    db.refresh(transaction)
    return transaction, draft


def _create_receipt_transaction(
    db: Session,
    request: ReceiptDraftCommit,
    user_id: str,
    account: Account,
    store_id: str | None,
    items: list[TransactionItemCreate],
    recorded_at: dt.datetime,
    total_cents: int,
) -> Transaction:
    payload = TransactionCreate(
        account_id=request.account_id,
        store_id=store_id,
        category_id=request.category_id,
        recorded_at=recorded_at,
        date=recorded_at.date(),
        currency=account.currency or request.payload.currency or "PLN",
        comment=request.comment or request.payload.notes or request.payload.store_name,
        cleared=False,
        reconciled=False,
        items=items,
        # Wydatek jest ujemny, a jawna suma z paragonu wygrywa z odtworzeniem
        # z pozycji: to ona uwzględnia rabaty.
        total_cents=-abs(total_cents),
    )
    return transactions_service.create_transaction(db, payload, user_id)


def bank_matches(
    db: Session, user_id: str, account_id: str, day: dt.date, total_cents: int
) -> list[BankMatchInfo]:
    """Transakcje z wyciągu, pod które można podpiąć ten paragon (§5.5)."""
    _get_owned_or_404(db, Account, account_id, user_id, "Account")
    found = transaction_matching.bank_transactions_for_receipt(
        db, user_id, account_id, day, total_cents
    )
    result = []
    for candidate in found:
        tx = db.get(Transaction, candidate.id)
        if tx is not None:
            result.append(bank_import.match_info(db, tx))
    return result


def _merge_into_bank_transaction(
    db: Session,
    request: ReceiptDraftCommit,
    user_id: str,
    store_id: str | None,
    items: list[TransactionItemCreate],
    recorded_at: dt.datetime,
    total_cents: int,
) -> Transaction:
    """Paragon dopięty do transakcji z banku zamiast nowej transakcji (§5.5).

    Z paragonu: sklep, pozycje, kategoria, komentarz i godzina zakupu. Z banku
    zostają kwota (to faktycznie zeszło z konta), data, tożsamość wiersza
    wyciągu i `cleared` — dzięki temu kolejny import wyciągu nadal rozpoznaje
    tę transakcję jako już zaimportowaną.
    """
    allowed = transaction_matching.bank_transactions_for_receipt(
        db, user_id, request.account_id, recorded_at.date(), total_cents
    )
    if request.merge_into_transaction_id not in {c.id for c in allowed}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Ta transakcja z banku nie pasuje do paragonu (konto, kwota lub data).",
        )
    tx = _get_owned_or_404(
        db, Transaction, request.merge_into_transaction_id, user_id, "Transaction"
    )
    tx.store_id = store_id
    if request.category_id:
        _get_owned_or_404(db, Category, request.category_id, user_id, "Category")
        tx.category_id = request.category_id
    tx.comment = (
        request.comment or request.payload.notes or request.payload.store_name or tx.comment
    )
    # Data zostaje bankowa, z paragonu bierzemy tylko porę dnia.
    tx.recorded_at = dt.datetime.combine(tx.date, recorded_at.timetz())
    tx.items = [TransactionItem(**item.model_dump()) for item in items]
    db.flush()
    return tx
