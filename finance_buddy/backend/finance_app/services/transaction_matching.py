"""Dopasowanie „to ta sama transakcja z innego źródła" — wspólne dla CSV i paragonów.

Jedna reguła dla obu stron, żeby import wyciągu i Skrzynka paragonów liczyły
„to samo" tak samo: to samo konto, identyczna kwota, data w oknie ±4 dni,
przydział jeden-do-jednego od najbliższej daty. Projekt:
docs/design/import-csv.md §5.2–5.5.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass

from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..models import Account, Transaction, TransactionAttachment, TransactionItem

# Płatność kartą księguje się 1–3 dni robocze, przez weekend wychodzi 4.
MATCH_WINDOW_DAYS = 4


@dataclass(frozen=True)
class RowKey:
    """Wiersz do dopasowania: indeks wiersza pliku, data, kwota, opis."""

    index: int
    date: dt.date
    amount_cents: int
    description: str = ""


@dataclass(frozen=True)
class Candidate:
    id: str
    date: dt.date
    amount_cents: int
    label: str = ""
    account_id: str | None = None


def assign(
    rows: Iterable[RowKey],
    candidates: Iterable[Candidate],
    *,
    opposite: bool = False,
    window_days: int = MATCH_WINDOW_DAYS,
) -> dict[int, Candidate]:
    """Przydział wiersz → kandydat, każdy kandydat najwyżej raz.

    Pary sortujemy po odległości dat, remis rozstrzyga podobieństwo opisu, a
    potem bierzemy zachłannie. Dzięki temu tydzień identycznych kaw paruje się
    dzień do dnia, a nie wszystkie wiersze z jedną transakcją.
    `opposite=True` szuka kwoty przeciwnej (druga połówka przelewu).
    """
    by_amount: dict[int, list[Candidate]] = {}
    for cand in candidates:
        by_amount.setdefault(cand.amount_cents, []).append(cand)

    pairs: list[tuple[int, float, int, str, RowKey, Candidate]] = []
    for row in rows:
        wanted = -row.amount_cents if opposite else row.amount_cents
        for cand in by_amount.get(wanted, ()):
            distance = abs((cand.date - row.date).days)
            if distance > window_days:
                continue
            similarity = _similarity(row.description, cand.label)
            pairs.append((distance, -similarity, row.index, cand.id, row, cand))
    pairs.sort(key=lambda p: p[:4])

    taken_rows: set[int] = set()
    taken_cands: set[str] = set()
    result: dict[int, Candidate] = {}
    for _, _, _, _, row, cand in pairs:
        if row.index in taken_rows or cand.id in taken_cands:
            continue
        taken_rows.add(row.index)
        taken_cands.add(cand.id)
        result[row.index] = cand
    return result


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_set_ratio(a.lower(), b.lower())


def _label(tx: Transaction) -> str:
    parts = [tx.store.name if tx.store else "", tx.comment or ""]
    return " ".join(p for p in parts if p)


def _candidate(tx: Transaction) -> Candidate:
    return Candidate(
        id=tx.id,
        date=tx.date,
        amount_cents=tx.total_cents,
        label=_label(tx),
        account_id=tx.account_id,
    )


def _window(dates: Iterable[dt.date]) -> tuple[dt.date, dt.date] | None:
    values = list(dates)
    if not values:
        return None
    pad = dt.timedelta(days=MATCH_WINDOW_DAYS)
    return min(values) - pad, max(values) + pad


def same_source_candidates(
    db: Session, user_id: str, account_id: str, dates: Iterable[dt.date]
) -> list[Transaction]:
    """Sito 2: transakcje na tym koncie, które NIE przyszły z wyciągu.

    Tylko bez `external_id` — transakcje z CSV rozpoznaje sito 1. Gdyby tu
    trafiały, codzienna kawa z nowego wyciągu „dublowałaby" kawę z poprzedniego.
    Połówki przelewów z YNAB-a są kandydatami: to ten sam ruch na tym koncie.
    """
    window = _window(dates)
    if window is None:
        return []
    stmt = (
        select(Transaction)
        .options(selectinload(Transaction.store))
        .where(
            Transaction.user_id == user_id,
            Transaction.account_id == account_id,
            Transaction.external_id.is_(None),
            Transaction.date >= window[0],
            Transaction.date <= window[1],
        )
    )
    return list(db.scalars(stmt))


def transfer_candidates(
    db: Session, user_id: str, account: Account, dates: Iterable[dt.date]
) -> list[Transaction]:
    """Sito 3: niesparowane transakcje na INNYCH kontach w tej samej walucie.

    Bez kategorii i bez pozycji — przelew nie jest zakupem. Bez tego warunku
    zwrot 50 zł na ING parowałby się z zakupem za 50 zł na Revolucie.
    """
    window = _window(dates)
    if window is None:
        return []
    has_items = select(TransactionItem.id).where(TransactionItem.transaction_id == Transaction.id)
    stmt = (
        select(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .options(selectinload(Transaction.store))
        .where(
            Transaction.user_id == user_id,
            Transaction.account_id != account.id,
            Account.currency == account.currency,
            Account.is_deleted.is_(False),
            Transaction.transfer_account_id.is_(None),
            Transaction.transfer_transaction_id.is_(None),
            Transaction.category_id.is_(None),
            ~has_items.exists(),
            Transaction.date >= window[0],
            Transaction.date <= window[1],
        )
    )
    return list(db.scalars(stmt))


def bank_transactions_for_receipt(
    db: Session, user_id: str, account_id: str, day: dt.date, total_cents: int
) -> list[Candidate]:
    """§5.5: transakcje z banku, pod które można podpiąć paragon.

    Z `external_id` (przyszły z wyciągu), bez pozycji i bez załącznika (nikt
    jeszcze nie podpiął tu paragonu), kwota = wydatek z paragonu, data ±4 dni.
    Najbliższa data pierwsza.
    """
    pad = dt.timedelta(days=MATCH_WINDOW_DAYS)
    has_items = select(TransactionItem.id).where(TransactionItem.transaction_id == Transaction.id)
    has_attachment = select(TransactionAttachment.id).where(
        TransactionAttachment.transaction_id == Transaction.id
    )
    stmt = (
        select(Transaction)
        .options(selectinload(Transaction.store))
        .where(
            Transaction.user_id == user_id,
            Transaction.account_id == account_id,
            Transaction.external_id.is_not(None),
            Transaction.total_cents == -abs(total_cents),
            Transaction.date >= day - pad,
            Transaction.date <= day + pad,
            ~has_items.exists(),
            ~has_attachment.exists(),
        )
    )
    found = [_candidate(tx) for tx in db.scalars(stmt)]
    found.sort(key=lambda c: (abs((c.date - day).days), c.id))
    return found


def candidates_from(transactions: Iterable[Transaction]) -> list[Candidate]:
    return [_candidate(tx) for tx in transactions]
