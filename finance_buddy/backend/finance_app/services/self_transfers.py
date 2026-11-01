"""Zamiana transakcji „przelewu własnego” na przelewy między kontami, tylko na żądanie.

Podgląd niczego nie zmienia i proponuje drugą stronę wyłącznie wtedy, gdy jest pewna:
ta sama kwota z przeciwnym znakiem, inne konto w tej samej walucie, data w oknie ±3 dni
i dokładnie jedna kandydatka. Inne waluty zawsze wybiera użytkownik, bo bez kursów nie
da się porównać kwot i nigdy ich nie przeliczamy. Transakcje, które już są przelewami,
pomijamy.

Bez drugiej strony wpis zostaje jednostronny (konto nieśledzone): nowa transakcja
zmieniłaby saldo konta docelowego.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..dependencies import _get_owned_or_404
from ..models import Account, StoreTransferConversion, Transaction, utcnow

WINDOW = dt.timedelta(days=3)
UNDO_WINDOW = dt.timedelta(days=30)
_SNAPSHOT_FIELDS = ("store_id", "category_id", "transfer_account_id", "transfer_transaction_id")


def _is_transfer(tx: Transaction) -> bool:
    return bool(tx.transfer_account_id or tx.transfer_transaction_id)


def _bad_request(message: str) -> HTTPException:
    return HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, message)


def _candidates(
    db: Session, user_id: str, tx: Transaction
) -> tuple[list[Transaction], list[Transaction]]:
    """Kandydatki na drugą stronę: w tej samej walucie i w innych."""
    rows = db.scalars(
        select(Transaction)
        .options(selectinload(Transaction.items))
        .join(Account, Transaction.account_id == Account.id)
        .where(
            Transaction.user_id == user_id,
            Transaction.id != tx.id,
            Transaction.account_id != tx.account_id,
            Transaction.date.between(tx.date - WINDOW, tx.date + WINDOW),
            Transaction.transfer_account_id.is_(None),
            Transaction.transfer_transaction_id.is_(None),
            Account.is_deleted.is_(False),
        )
        .order_by(Transaction.date, Transaction.id)
    ).all()
    opposite = [row for row in rows if row.total_cents * tx.total_cents < 0 and not row.items]
    same = [
        row
        for row in opposite
        if row.currency == tx.currency and row.total_cents == -tx.total_cents
    ]
    other = [row for row in opposite if row.currency != tx.currency]
    return same, other


def _describe(tx: Transaction, accounts: dict[str, str]) -> dict[str, object]:
    return {
        "id": tx.id,
        "date": tx.date,
        "amount_cents": tx.total_cents,
        "currency": tx.currency,
        "account_id": tx.account_id,
        "account_name": accounts.get(tx.account_id, ""),
        "description": tx.bank_description or tx.comment,
    }


def preview(db: Session, user_id: str, store_ids: Sequence[str]) -> dict[str, object]:
    accounts = dict(
        db.execute(select(Account.id, Account.name).where(Account.user_id == user_id))
        .tuples()
        .all()
    )
    transactions = db.scalars(
        select(Transaction)
        .options(selectinload(Transaction.items))
        .where(Transaction.user_id == user_id, Transaction.store_id.in_(store_ids))
        .order_by(Transaction.date.desc(), Transaction.id)
    ).all()
    convertible = [tx for tx in transactions if not _is_transfer(tx)]
    found = {tx.id: _candidates(db, user_id, tx) for tx in convertible if not tx.items}
    # Jedna kandydatka dla kilku transakcji nie jest pewna, więc wybiera użytkownik.
    sole = Counter(same[0].id for same, _ in found.values() if len(same) == 1)

    rows = []
    taken: set[str] = set()
    # Wydatki najpierw: gdy obie strony są w tym sklepie, wiersz wpływu jest już sparowany.
    for tx in sorted(convertible, key=lambda row: row.total_cents):
        if tx.items:
            rows.append(
                {
                    **_describe(tx, accounts),
                    "status": "has_items",
                    "candidates": [],
                    "suggested": None,
                }
            )
            continue
        same, other = found[tx.id]
        suggested = None
        if tx.id in taken:
            state = "covered"
        elif len(same) == 1 and sole[same[0].id] == 1 and same[0].id not in taken:
            state = "paired"
            suggested = same[0].id
            taken.update({tx.id, same[0].id})
        elif same:
            state = "ambiguous"
        elif other:
            state = "other_currency"
        else:
            state = "no_partner"
        rows.append(
            {
                **_describe(tx, accounts),
                "status": state,
                "suggested": suggested,
                "candidates": [_describe(row, accounts) for row in [*same, *other]],
            }
        )
    rows.sort(key=lambda row: (row["date"], row["id"]), reverse=True)
    return {"rows": rows, "already_transfers": len(transactions) - len(convertible)}


@dataclass(frozen=True)
class Decision:
    transaction_id: str
    partner_id: str | None
    account_id: str | None


def _validate_partner(db: Session, user_id: str, tx: Transaction, partner_id: str) -> Transaction:
    partner = _get_owned_or_404(db, Transaction, partner_id, user_id, "Transaction")
    if _is_transfer(partner) or partner.items:
        raise _bad_request("Wybrana druga strona jest już przelewem albo ma pozycje paragonu.")
    if partner.account_id == tx.account_id:
        raise _bad_request("Druga strona przelewu musi być na innym koncie.")
    if abs(partner.date - tx.date) > WINDOW:
        raise _bad_request("Druga strona przelewu musi być w oknie 3 dni.")
    opposite = partner.total_cents * tx.total_cents < 0
    if not opposite or (partner.currency == tx.currency and partner.total_cents != -tx.total_cents):
        raise _bad_request("Kwota drugiej strony nie pasuje do tego przelewu.")
    return partner


def convert(db: Session, user_id: str, decisions: Sequence[Decision]) -> dict[str, object]:
    snapshots: list[dict[str, object]] = []
    store_ids: set[str] = set()
    touched: set[str] = set()

    def clear(tx: Transaction) -> None:
        if tx.id in touched:
            raise _bad_request("Ta sama transakcja jest wybrana dwa razy.")
        touched.add(tx.id)
        snapshots.append({"id": tx.id, **{f: getattr(tx, f) for f in _SNAPSHOT_FIELDS}})
        if tx.store_id:
            store_ids.add(tx.store_id)
        tx.store_id = None
        tx.category_id = None

    for decision in decisions:
        tx = _get_owned_or_404(db, Transaction, decision.transaction_id, user_id, "Transaction")
        if _is_transfer(tx):
            raise _bad_request("Ta transakcja jest już przelewem.")
        if tx.items:
            raise _bad_request("Transakcji z pozycjami paragonu nie można zamienić na przelew.")
        if bool(decision.partner_id) == bool(decision.account_id):
            raise _bad_request("Wskaż drugą stronę albo konto docelowe.")
        if decision.partner_id:
            partner = _validate_partner(db, user_id, tx, decision.partner_id)
            clear(tx)
            clear(partner)
            tx.transfer_account_id = partner.account_id
            tx.transfer_transaction_id = partner.id
            partner.transfer_account_id = tx.account_id
            partner.transfer_transaction_id = tx.id
        else:
            account = _get_owned_or_404(db, Account, decision.account_id, user_id, "Account")
            if account.id == tx.account_id:
                raise _bad_request("Konto docelowe musi być inne niż konto transakcji.")
            clear(tx)
            tx.transfer_account_id = account.id

    conversion = StoreTransferConversion(
        user_id=user_id,
        payload={
            "store_ids": sorted(store_ids),
            "transactions": snapshots,
            "count": len(decisions),
        },
    )
    db.add(conversion)
    db.commit()
    return {
        "conversion_id": conversion.id,
        "converted": len(decisions),
        "expires_at": conversion.created_at + UNDO_WINDOW,
    }


def list_conversions(
    db: Session, user_id: str, store_ids: Sequence[str]
) -> list[dict[str, object]]:
    wanted = set(store_ids)
    rows = db.scalars(
        select(StoreTransferConversion)
        .where(
            StoreTransferConversion.user_id == user_id,
            StoreTransferConversion.undone_at.is_(None),
            StoreTransferConversion.created_at >= utcnow() - UNDO_WINDOW,
        )
        .order_by(StoreTransferConversion.created_at.desc())
    )
    return [
        {
            "id": row.id,
            "converted": row.payload["count"],
            "created_at": row.created_at,
            "expires_at": row.created_at + UNDO_WINDOW,
        }
        for row in rows
        if wanted.intersection(row.payload["store_ids"])
    ]


def undo(db: Session, user_id: str, conversion_id: str) -> dict[str, int]:
    conversion = _get_owned_or_404(
        db, StoreTransferConversion, conversion_id, user_id, "Transfer conversion"
    )
    if conversion.undone_at is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "Ta zamiana została już cofnięta.")
    if utcnow() - conversion.created_at > UNDO_WINDOW:
        raise HTTPException(status.HTTP_409_CONFLICT, "Minęło 30 dni, zamiany nie można cofnąć.")

    restored = kept = 0
    for snapshot in conversion.payload["transactions"]:
        tx = db.get(Transaction, snapshot["id"])
        # Usuniętej albo później zmienionej transakcji nie ruszamy.
        if tx is None or not _is_transfer(tx) or tx.store_id or tx.category_id:
            kept += 1
            continue
        for field in _SNAPSHOT_FIELDS:
            setattr(tx, field, snapshot[field])
        restored += 1
    conversion.undone_at = utcnow()
    db.commit()
    return {"restored": restored, "kept": kept}
