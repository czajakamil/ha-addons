from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import (
    Account,
    Base,
    Category,
    Product,
    Store,
    StoreTransferConversion,
    Transaction,
    TransactionItem,
    utcnow,
)
from backend.finance_app.services.self_transfers import (
    Decision,
    convert,
    list_conversions,
    preview,
    undo,
)

USER = "user-1"
DAY = dt.date(2026, 10, 10)


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                Account(id="a", user_id=USER, name="Main", currency="PLN"),
                Account(id="b", user_id=USER, name="Revolut", currency="PLN"),
                Account(id="c", user_id=USER, name="Oszczednosci", currency="PLN"),
                Account(id="e", user_id=USER, name="Euro", currency="EUR"),
                Store(id="s", user_id=USER, name="JAN KOWALSKI SENT FROM"),
                Category(id="k", user_id=USER, name="Inne"),
            ]
        )
        session.flush()
        yield session


def tx(
    db: Session,
    tx_id: str,
    account: str,
    cents: int,
    *,
    store: str | None = None,
    day: dt.date = DAY,
    currency: str = "PLN",
    **extra: object,
) -> Transaction:
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    item = Transaction(
        id=tx_id,
        user_id=USER,
        account_id=account,
        store_id=store,
        date=day,
        recorded_at=moment,
        created_at=moment,
        total_cents=cents,
        currency=currency,
        **extra,
    )
    db.add(item)
    db.flush()
    return item


def row(result: dict, tx_id: str) -> dict:
    return next(entry for entry in result["rows"] if entry["id"] == tx_id)


def test_single_exact_candidate_is_paired_and_preview_changes_nothing(db: Session) -> None:
    source = tx(db, "t", "a", -5000, store="s", category_id="k")
    tx(db, "p", "b", 5000, day=DAY + dt.timedelta(days=3))
    tx(db, "far", "b", 5000, day=DAY + dt.timedelta(days=4))
    tx(db, "wrong", "c", 4999)
    tx(db, "same-account", "a", 5000)

    entry = row(preview(db, USER, ["s"]), "t")

    assert entry["status"] == "paired"
    assert entry["suggested"] == "p"
    assert source.store_id == "s"
    assert source.transfer_account_id is None


def test_two_candidates_need_a_choice(db: Session) -> None:
    tx(db, "t", "a", -5000, store="s")
    tx(db, "p1", "b", 5000)
    tx(db, "p2", "c", 5000)
    entry = row(preview(db, USER, ["s"]), "t")
    assert entry["status"] == "ambiguous"
    assert entry["suggested"] is None
    assert len(entry["candidates"]) == 2


def test_one_candidate_for_two_transactions_is_not_certain(db: Session) -> None:
    tx(db, "t1", "a", -5000, store="s")
    tx(db, "t2", "c", -5000, store="s")
    tx(db, "p", "b", 5000)
    result = preview(db, USER, ["s"])
    assert {entry["status"] for entry in result["rows"]} == {"ambiguous"}


def test_existing_transfers_are_skipped_as_rows_and_candidates(db: Session) -> None:
    tx(db, "done", "a", -100, store="s", transfer_account_id="b")
    tx(db, "t", "a", -5000, store="s")
    tx(db, "p", "b", 5000, transfer_account_id="a")

    result = preview(db, USER, ["s"])

    assert result["already_transfers"] == 1
    assert [entry["id"] for entry in result["rows"]] == ["t"]
    assert result["rows"][0]["status"] == "no_partner"
    assert result["rows"][0]["candidates"] == []


def test_other_currency_is_never_suggested_nor_converted_in_amount(db: Session) -> None:
    source = tx(db, "t", "a", -43000, store="s")
    partner = tx(db, "p", "e", 10000, currency="EUR")

    entry = row(preview(db, USER, ["s"]), "t")
    assert entry["status"] == "other_currency"
    assert entry["suggested"] is None
    assert [item["id"] for item in entry["candidates"]] == ["p"]

    convert(db, USER, [Decision("t", "p", None)])
    assert source.total_cents == -43000
    assert partner.total_cents == 10000
    assert source.transfer_transaction_id == "p"


def test_both_sides_in_one_store_become_one_row_to_convert(db: Session) -> None:
    tx(db, "out", "a", -5000, store="s")
    tx(db, "in", "b", 5000, store="s")
    result = preview(db, USER, ["s"])
    assert row(result, "out")["status"] == "paired"
    assert row(result, "in")["status"] == "covered"


def test_items_block_conversion(db: Session) -> None:
    db.add(Product(id="m", user_id=USER, name="Mleko"))
    source = tx(db, "t", "a", -5000, store="s")
    db.add(TransactionItem(transaction_id="t", product_id="m", unit_price_cents=5000, quantity=1))
    db.flush()
    db.refresh(source)
    assert row(preview(db, USER, ["s"]), "t")["status"] == "has_items"
    with pytest.raises(HTTPException):
        convert(db, USER, [Decision("t", None, "b")])


def test_convert_pair_links_both_sides_and_undo_restores(db: Session) -> None:
    source = tx(db, "t", "a", -5000, store="s", category_id="k")
    partner = tx(db, "p", "b", 5000, category_id="k")

    result = convert(db, USER, [Decision("t", "p", None)])

    assert (source.store_id, source.category_id) == (None, None)
    assert (source.transfer_account_id, source.transfer_transaction_id) == ("b", "p")
    assert (partner.transfer_account_id, partner.transfer_transaction_id) == ("a", "t")
    assert source.total_cents == -5000
    assert len(list_conversions(db, USER, ["s"])) == 1

    assert undo(db, USER, result["conversion_id"]) == {"restored": 2, "kept": 0}
    assert (source.store_id, source.category_id, source.transfer_account_id) == ("s", "k", None)
    assert (partner.category_id, partner.transfer_transaction_id) == ("k", None)
    assert list_conversions(db, USER, ["s"]) == []
    with pytest.raises(HTTPException):
        undo(db, USER, result["conversion_id"])


def test_one_sided_creates_no_transaction(db: Session) -> None:
    source = tx(db, "t", "a", -5000, store="s")
    convert(db, USER, [Decision("t", None, "b")])
    assert source.transfer_account_id == "b"
    assert source.transfer_transaction_id is None
    assert db.query(Transaction).count() == 1


@pytest.mark.parametrize(
    "decisions",
    [
        [Decision("t", None, None)],
        [Decision("t", "p", "b")],
        [Decision("t", None, "a")],
        [Decision("t", "bad-amount", None)],
        [Decision("t", "too-far", None)],
        [Decision("t", "p", None), Decision("t2", "p", None)],
    ],
)
def test_invalid_decisions_are_rejected(db: Session, decisions: list[Decision]) -> None:
    tx(db, "t", "a", -5000, store="s")
    tx(db, "t2", "c", -5000, store="s")
    tx(db, "p", "b", 5000)
    tx(db, "bad-amount", "b", 4000)
    tx(db, "too-far", "b", 5000, day=DAY + dt.timedelta(days=9))
    with pytest.raises(HTTPException):
        convert(db, USER, decisions)


def test_undo_leaves_later_changes_and_expires(db: Session) -> None:
    source = tx(db, "t", "a", -5000, store="s")
    other = tx(db, "o", "a", -100, store="s")
    first = convert(db, USER, [Decision("t", None, "b"), Decision("o", None, "b")])
    other.transfer_account_id = None

    assert undo(db, USER, first["conversion_id"]) == {"restored": 1, "kept": 1}
    assert source.store_id == "s"

    second = convert(db, USER, [Decision("t", None, "b")])
    db.get(StoreTransferConversion, second["conversion_id"]).created_at = utcnow() - dt.timedelta(
        days=31
    )
    db.flush()
    assert list_conversions(db, USER, ["s"]) == []
    with pytest.raises(HTTPException) as exc:
        undo(db, USER, second["conversion_id"])
    assert "30 dni" in exc.value.detail
