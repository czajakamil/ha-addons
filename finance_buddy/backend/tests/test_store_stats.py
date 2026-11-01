from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Category, Store, Transaction
from backend.finance_app.services.store_stats import spend_window, store_monthly_spend, store_stats
from backend.finance_app.services.transactions import list_transactions, list_transactions_page

TODAY = dt.date(2026, 11, 15)
USER = "user-1"


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.add(Store(id="s1", user_id=USER, name="Lidl"))
        session.add(Store(id="s2", user_id=USER, name="Lidl 2"))
        session.add(Store(id="other", user_id="user-2", name="Cudzy"))
        session.flush()
        yield session


def add_tx(db: Session, store: str | None, day: dt.date, cents: int, **extra: object) -> None:
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    db.add(
        Transaction(
            user_id=extra.pop("user_id", USER),
            account_id="a",
            store_id=store,
            date=day,
            recorded_at=moment,
            created_at=moment,
            total_cents=cents,
            currency=extra.pop("currency", "PLN"),
            **extra,
        )
    )
    db.flush()


def stats_by_store(db: Session, **kwargs: object) -> dict[str, dict]:
    rows = store_stats(db, USER, today=TODAY, **kwargs)
    return {row["store_id"]: row for row in rows}


def test_window_is_current_month_and_eleven_before() -> None:
    assert spend_window(TODAY) == (dt.date(2025, 12, 1), dt.date(2026, 11, 30))
    assert spend_window(dt.date(2026, 1, 31)) == (dt.date(2025, 2, 1), dt.date(2026, 1, 31))
    assert spend_window(dt.date(2026, 12, 1))[1] == dt.date(2026, 12, 31)


def test_count_and_last_date_cover_whole_history_but_spend_only_window(db: Session) -> None:
    add_tx(db, "s1", dt.date(2026, 11, 1), -1000)
    add_tx(db, "s1", dt.date(2025, 12, 1), -500)
    add_tx(db, "s1", dt.date(2025, 11, 30), -9900)

    assert stats_by_store(db)["s1"] == {
        "store_id": "s1",
        "count": 3,
        "spent_cents_12m": 1500,
        "last_date": dt.date(2026, 11, 1),
    }


def test_refund_reduces_and_exclusions_are_counted_but_not_spent(db: Session) -> None:
    day = dt.date(2026, 10, 1)
    db.add(Category(id="adj", user_id=USER, name="Reconciliation Balance Adjustment"))
    db.flush()
    add_tx(db, "s1", day, -1000)
    add_tx(db, "s1", day, 300)
    add_tx(db, "s1", day, -5000, transfer_account_id="a")
    add_tx(db, "s1", day, -7000, currency="EUR")
    add_tx(db, "s1", day, -8000, category_id="adj")

    row = stats_by_store(db)["s1"]
    assert (row["count"], row["spent_cents_12m"]) == (5, 700)


def test_other_users_and_filter(db: Session) -> None:
    day = dt.date(2026, 10, 1)
    add_tx(db, "s1", day, -100)
    add_tx(db, "s2", day, -200)
    add_tx(db, "other", day, -300, user_id="user-2")

    assert set(stats_by_store(db)) == {"s1", "s2"}
    assert set(stats_by_store(db, store_ids=["s2", "other"])) == {"s2"}


def test_monthly_fills_gaps_and_matches_stats(db: Session) -> None:
    add_tx(db, "s1", dt.date(2026, 11, 2), -800)
    add_tx(db, "s2", dt.date(2026, 11, 3), -200)
    add_tx(db, "s1", dt.date(2025, 12, 1), -100)
    add_tx(db, "s1", dt.date(2025, 11, 30), -999)

    months = store_monthly_spend(db, USER, ["s1", "s2"], today=TODAY)

    assert len(months) == 12
    assert months[0] == {"month": "2025-12", "cents": 100}
    assert months[1] == {"month": "2026-01", "cents": 0}
    assert months[11] == {"month": "2026-11", "cents": 1000}
    spent = sum(row["spent_cents_12m"] for row in store_stats(db, USER, today=TODAY))
    assert sum(month["cents"] for month in months) == spent


def test_store_ids_filters_lists_with_total(db: Session) -> None:
    day = dt.date(2026, 10, 1)
    add_tx(db, "s1", day, -100)
    add_tx(db, "s2", day, -200)
    add_tx(db, "other", day, -300, user_id="user-2")
    add_tx(db, None, day, -400)

    assert len(list_transactions(db, USER, store_ids=["s1", "s2", "other"])) == 2
    page = list_transactions_page(db, USER, store_ids=["s1"], limit=10)
    assert page["total"] == 1
    assert len(list_transactions_page(db, USER, limit=10)["items"]) == 3
