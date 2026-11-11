"""Przelew między własnymi kontami widać na obu kontach."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Store, Transaction
from backend.finance_app.schemas import TransactionCreate, TransactionUpdate
from backend.finance_app.services.transactions import (
    create_transaction,
    delete_transaction,
    update_transaction,
)

USER = "user-1"
DAY = dt.date(2026, 11, 11)


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                Account(id="pko", user_id=USER, name="PKO", currency="PLN"),
                Account(id="rev", user_id=USER, name="Revolut", currency="PLN"),
                Account(id="eur", user_id=USER, name="Revolut EUR", currency="EUR"),
                Store(id="s1", user_id=USER, name="Biedronka"),
            ]
        )
        session.flush()
        yield session


def _bank_tx(db: Session, account: str, cents: int, *, ext: str | None = None) -> Transaction:
    tx = Transaction(
        user_id=USER,
        account_id=account,
        date=DAY,
        recorded_at=dt.datetime(2026, 11, 11, 12, tzinfo=dt.UTC),
        currency="PLN",
        total_cents=cents,
        external_id=ext,
    )
    db.add(tx)
    db.commit()
    return tx


def _on(db: Session, account: str) -> list[Transaction]:
    return list(db.scalars(select(Transaction).where(Transaction.account_id == account)))


def test_marking_bank_row_as_transfer_adds_other_half(db: Session) -> None:
    tx = _bank_tx(db, "pko", -20000, ext="id:1")

    update_transaction(
        db,
        tx.id,
        TransactionUpdate(transfer_account_id="rev", transfer_transaction_id=None, store_id=None),
        USER,
    )

    [partner] = _on(db, "rev")
    assert partner.total_cents == 20000
    assert partner.transfer_account_id == "pko"
    assert partner.transfer_transaction_id == tx.id
    assert tx.transfer_transaction_id == partner.id
    assert partner.external_id is None


def test_pairs_with_existing_row_instead_of_duplicating(db: Session) -> None:
    other = _bank_tx(db, "rev", 20000, ext="id:rev")
    tx = _bank_tx(db, "pko", -20000, ext="id:pko")

    update_transaction(db, tx.id, TransactionUpdate(transfer_account_id="rev"), USER)

    assert _on(db, "rev") == [other]
    assert other.transfer_transaction_id == tx.id
    assert tx.transfer_transaction_id == other.id


def test_create_transfer_adds_other_half(db: Session) -> None:
    tx = create_transaction(
        db,
        TransactionCreate(
            account_id="pko", transfer_account_id="rev", total_cents=-5000, date=DAY, items=[]
        ),
        USER,
    )

    [partner] = _on(db, "rev")
    assert partner.total_cents == 5000
    assert partner.transfer_transaction_id == tx.id


def test_switching_back_to_store_removes_added_half(db: Session) -> None:
    tx = _bank_tx(db, "pko", -20000, ext="id:1")
    update_transaction(db, tx.id, TransactionUpdate(transfer_account_id="rev"), USER)

    update_transaction(
        db,
        tx.id,
        TransactionUpdate(store_id="s1", transfer_account_id=None, transfer_transaction_id=None),
        USER,
    )

    assert _on(db, "rev") == []
    assert tx.transfer_transaction_id is None


def test_unlinking_keeps_bank_half(db: Session) -> None:
    other = _bank_tx(db, "rev", 20000, ext="id:rev")
    tx = _bank_tx(db, "pko", -20000, ext="id:pko")
    update_transaction(db, tx.id, TransactionUpdate(transfer_account_id="rev"), USER)

    delete_transaction(db, tx.id, USER)

    assert _on(db, "rev") == [other]
    assert other.transfer_account_id is None
    assert other.transfer_transaction_id is None


def test_amount_edit_follows_to_added_half(db: Session) -> None:
    tx = _bank_tx(db, "pko", -20000)
    update_transaction(db, tx.id, TransactionUpdate(transfer_account_id="rev"), USER)

    update_transaction(db, tx.id, TransactionUpdate(total_cents=-25000), USER)

    [partner] = _on(db, "rev")
    assert partner.total_cents == 25000


def test_other_currency_stays_one_sided(db: Session) -> None:
    tx = _bank_tx(db, "pko", -20000)

    update_transaction(db, tx.id, TransactionUpdate(transfer_account_id="eur"), USER)

    assert _on(db, "eur") == []
    assert tx.transfer_account_id == "eur"


def test_comment_edit_does_not_touch_one_sided_legacy_transfer(db: Session) -> None:
    tx = _bank_tx(db, "pko", -20000)
    tx.transfer_account_id = "rev"
    db.commit()

    update_transaction(db, tx.id, TransactionUpdate(comment="spłata"), USER)

    assert _on(db, "rev") == []
