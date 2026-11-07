"""Podział paragonu na kategorie produktów: reguła, budżet, raporty i statystyki kategorii."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from types import SimpleNamespace

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
from backend.finance_app.routers.reports import monthly_category_report, reports_dashboard
from backend.finance_app.services.budget import get_budget_month
from backend.finance_app.services.category_split import split_lines
from backend.finance_app.services.category_stats import category_stats, category_top_stores

TODAY = dt.date(2026, 11, 15)
USER = "user-1"
ME = SimpleNamespace(id=USER)


# --- Reguła -------------------------------------------------------------------


def test_items_go_to_product_category_rest_to_transaction_category() -> None:
    lines = split_lines("food", -1000, [("junk", 300), (None, 200)])
    assert dict(lines) == {"junk": -300, "food": -700}


def test_without_items_whole_amount_stays_in_transaction_category() -> None:
    assert split_lines("food", -1000, None) == [("food", -1000)]
    assert split_lines(None, -1000, []) == [(None, -1000)]


def test_items_over_total_are_scaled_so_no_part_flips_sign() -> None:
    # Rabat 2 zł na cały paragon: pozycje 12 zł, zapłacono 10 zł
    lines = dict(split_lines("food", -1000, [("junk", 600), ("alco", 600)]))
    assert lines == {"junk": -500, "alco": -500}
    assert all(cents < 0 for cents in lines.values())


def test_negative_item_reduces_the_others() -> None:
    # Zwrot kaucji 0,50 zł w linii paragonu
    lines = dict(split_lines("food", -950, [("junk", 500), ("alco", 500), ("deposit", -50)]))
    assert sum(lines.values()) == -950
    assert lines["junk"] == lines["alco"] == -475


def test_refund_keeps_positive_sign() -> None:
    assert dict(split_lines("food", 500, [("junk", 300)])) == {"junk": 300, "food": 200}


# --- Agregaty ------------------------------------------------------------------


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN", is_on_budget=True))
        session.add(Store(id="s1", user_id=USER, name="Kaufland"))
        for cid, name in (("food", "Jedzenie"), ("junk", "Słodycze"), ("alco", "Alkohol")):
            session.add(Category(id=cid, user_id=USER, name=name))
        session.add(Product(id="chips", user_id=USER, name="Chipsy", category_id="junk"))
        session.add(Product(id="wine", user_id=USER, name="Wino", category_id="alco"))
        session.add(Product(id="bread", user_id=USER, name="Chleb", category_id=None))
        session.flush()
        add_receipt(session, "food")
        yield session


def add_receipt(db: Session, category: str | None, tx_id: str = "t1") -> None:
    """Paragon na 60 zł: chipsy 10 zł, wino 20 zł, chleb bez kategorii 5 zł, reszta niewpisana."""
    day = dt.date(2026, 11, 10)
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    db.add(
        Transaction(
            id=tx_id,
            user_id=USER,
            account_id="a",
            store_id="s1",
            category_id=category,
            date=day,
            recorded_at=moment,
            created_at=moment,
            total_cents=-6000,
            currency="PLN",
        )
    )
    for product, cents in (("chips", 1000), ("wine", 2000), ("bread", 500)):
        db.add(
            TransactionItem(
                transaction_id=tx_id, product_id=product, unit_price_cents=cents, quantity=1
            )
        )
    db.flush()


EXPECTED = {"junk": -1000, "alco": -2000, "food": -3000}


def test_budget_activity_follows_items(db: Session) -> None:
    month = get_budget_month(db, "2026-11", USER)
    activity = {row.category_id: row.activity_cents for row in month.categories}
    assert {key: activity[key] for key in EXPECTED} == EXPECTED


def test_items_count_even_without_transaction_category(db: Session) -> None:
    db.query(Transaction).filter_by(id="t1").update({"category_id": None})
    month = get_budget_month(db, "2026-11", USER)
    activity = {row.category_id: row.activity_cents for row in month.categories}
    assert activity["junk"] == -1000
    assert activity["alco"] == -2000
    assert activity["food"] == 0


def test_monthly_category_report_follows_items(db: Session) -> None:
    rows = monthly_category_report(year=2026, month=11, db=db, current_user=ME)
    assert {row.category_id: row.total_cents for row in rows} == EXPECTED


def test_dashboard_category_filter_shows_only_matching_part(db: Session) -> None:
    report = reports_dashboard(
        start_date=dt.date(2026, 11, 1),
        end_date=dt.date(2026, 11, 30),
        age_of_money_window_days=90,
        account_ids=None,
        category_ids=["alco"],
        db=db,
        current_user=ME,
    )
    entries = report.spending_breakdown.entries
    assert [entry.category_id for entry in entries] == ["alco"]
    assert entries[0].total_cents == 2000


def test_category_stats_follow_items(db: Session) -> None:
    stats = {row["category_id"]: row for row in category_stats(db, USER, today=TODAY)}
    assert {key: stats[key]["spent_cents_12m"] for key in EXPECTED} == {
        key: -cents for key, cents in EXPECTED.items()
    }
    assert stats["junk"]["count"] == 1


def test_top_stores_count_only_the_category_part(db: Session) -> None:
    stores = category_top_stores(db, USER, ["alco"], today=TODAY)
    assert stores == [{"name": "Kaufland", "store_id": "s1", "count": 1, "spent_cents_12m": 2000}]
