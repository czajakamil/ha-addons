from __future__ import annotations

import datetime as dt
import itertools
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import (
    Account,
    Base,
    Category,
    Product,
    Store,
    Transaction,
    TransactionItem,
)
from backend.finance_app.services.product_stats import item_monthly_spend, item_stats

TODAY = dt.date(2026, 11, 15)
USER = "user-1"
_ids = itertools.count()


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.add(Store(id="s1", user_id=USER, name="Lidl"))
        session.add(Product(id="milk", user_id=USER, name="Mleko"))
        session.add(Product(id="apples", user_id=USER, name="Jabłka"))
        session.add(Product(id="never", user_id=USER, name="Nigdy"))
        session.add(Product(id="theirs", user_id="user-2", name="Cudze"))
        session.flush()
        yield session


def add_tx(
    db: Session,
    day: dt.date,
    lines: list[tuple[str, int, float]],
    *,
    total_cents: int | None = None,
    **extra: object,
) -> None:
    """Transakcja z pozycjami (produkt, cena jednostkowa, ilość)."""
    tx_id = f"tx-{next(_ids)}"
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    if total_cents is None:
        total_cents = -sum(round(price * qty) for _, price, qty in lines)
    db.add(
        Transaction(
            id=tx_id,
            user_id=extra.pop("user_id", USER),
            account_id="a",
            store_id="s1",
            date=day,
            recorded_at=moment,
            created_at=moment,
            total_cents=total_cents,
            currency=extra.pop("currency", "PLN"),
            **extra,
        )
    )
    db.flush()
    for product, price, qty in lines:
        db.add(
            TransactionItem(
                transaction_id=tx_id, product_id=product, unit_price_cents=price, quantity=qty
            )
        )
    db.flush()


def stats_by_item(db: Session, **kwargs: object) -> dict[str, dict]:
    return {row["item_id"]: row for row in item_stats(db, USER, today=TODAY, **kwargs)}


def test_count_is_transactions_and_spend_is_price_times_quantity(db: Session) -> None:
    add_tx(db, dt.date(2026, 11, 1), [("milk", 399, 2), ("apples", 650, 1.5)])
    # Ten sam przedmiot dwa razy na jednym paragonie to nadal jeden zakup
    add_tx(db, dt.date(2025, 12, 1), [("milk", 399, 1), ("milk", 349, 1)])
    add_tx(db, dt.date(2025, 11, 30), [("milk", 10000, 1)])

    rows = stats_by_item(db)
    assert rows["milk"] == {
        "item_id": "milk",
        "count": 3,
        "spent_cents_12m": 798 + 399 + 349,
        "last_date": dt.date(2026, 11, 1),
    }
    assert rows["apples"]["spent_cents_12m"] == 975
    assert "never" not in rows


def test_refund_reduces_and_exclusions_are_counted_but_not_spent(db: Session) -> None:
    day = dt.date(2026, 10, 1)
    db.add(Category(id="adj", user_id=USER, name="Reconciliation Balance Adjustment"))
    db.flush()
    add_tx(db, day, [("milk", 1000, 1)])
    add_tx(db, day, [("milk", 300, 1)], total_cents=300)
    add_tx(db, day, [("milk", 5000, 1)], currency="EUR")
    add_tx(db, day, [("milk", 8000, 1)], category_id="adj")

    row = stats_by_item(db)["milk"]
    assert (row["count"], row["spent_cents_12m"]) == (4, 700)


def test_other_users_and_filter(db: Session) -> None:
    day = dt.date(2026, 10, 1)
    add_tx(db, day, [("milk", 100, 1), ("apples", 200, 1)])
    add_tx(db, day, [("theirs", 300, 1)], user_id="user-2")

    assert set(stats_by_item(db)) == {"milk", "apples"}
    assert set(stats_by_item(db, item_ids=["apples", "theirs"])) == {"apples"}


def test_monthly_fills_gaps_and_matches_stats(db: Session) -> None:
    add_tx(db, dt.date(2026, 11, 2), [("milk", 400, 2)])
    add_tx(db, dt.date(2026, 11, 3), [("apples", 200, 1)])
    add_tx(db, dt.date(2025, 12, 1), [("milk", 100, 1)])
    add_tx(db, dt.date(2025, 11, 30), [("milk", 999, 1)])

    months = item_monthly_spend(db, USER, ["milk"], today=TODAY)

    assert len(months) == 12
    assert months[0] == {"month": "2025-12", "cents": 100}
    assert months[1] == {"month": "2026-01", "cents": 0}
    assert months[11] == {"month": "2026-11", "cents": 800}
    spent = stats_by_item(db, item_ids=["milk"])["milk"]["spent_cents_12m"]
    assert sum(month["cents"] for month in months) == spent
