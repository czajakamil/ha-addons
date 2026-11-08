"""Liczby z ekranu Start: co liczy się do „Wydanych", „Możesz wydać" i kolejki."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.helpers import BALANCE_ADJUSTMENT_COMMENT
from backend.finance_app.models import Account, Base, Budget, Category, Store, Transaction
from backend.finance_app.services.home import get_home_summary

USER = "user-1"
TODAY = dt.datetime.now(dt.UTC).date()
MONTH = TODAY.strftime("%Y-%m")


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                Account(id="main", user_id=USER, name="Konto", currency="PLN", is_on_budget=True),
                Category(id="food", user_id=USER, name="Jedzenie"),
                Category(id="fuel", user_id=USER, name="Paliwo"),
                Store(id="lidl", user_id=USER, name="Lidl"),
            ]
        )
        session.flush()
        yield session


def _tx(db: Session, cents: int, **fields) -> Transaction:
    tx = Transaction(
        user_id=USER,
        account_id=fields.pop("account_id", "main"),
        date=fields.pop("date", TODAY),
        recorded_at=dt.datetime.now(dt.UTC),
        total_cents=cents,
        currency="PLN",
        **fields,
    )
    db.add(tx)
    db.flush()
    return tx


def _assign(db: Session, category_id: str, cents: int) -> None:
    db.add(
        Budget(
            entry_type="assignment",
            user_id=USER,
            category_id=category_id,
            month=MONTH,
            assigned_cents=cents,
        )
    )
    db.flush()


def test_uncategorized_expense_without_store_counts_as_spent(db: Session) -> None:
    # Wiersz z wyciągu z nieznanym odbiorcą: bez sklepu i bez kategorii.
    _tx(db, -8_000, comment="BLIK do osoby")
    _tx(db, -1_500, store_id="lidl", category_id="food")

    summary = get_home_summary(db, USER)

    assert summary.spent_month_cents == 9_500
    assert summary.untagged_count == 1


def test_balance_adjustment_is_not_spending_nor_todo(db: Session) -> None:
    _tx(db, -2_000, comment=BALANCE_ADJUSTMENT_COMMENT)

    summary = get_home_summary(db, USER)

    assert summary.spent_month_cents == 0
    assert summary.untagged_count == 0
    assert summary.recent == []


def test_transfer_between_own_accounts_is_not_spending(db: Session) -> None:
    db.add(Account(id="save", user_id=USER, name="Oszczędności", currency="PLN", is_on_budget=True))
    db.flush()
    _tx(db, -50_000, transfer_account_id="save")
    _tx(db, 50_000, account_id="save", transfer_account_id="main")

    assert get_home_summary(db, USER).spent_month_cents == 0


def test_spendable_is_envelopes_when_budget_is_covered(db: Session) -> None:
    _tx(db, 100_000, comment="Wpłata")
    _assign(db, "food", 30_000)
    _assign(db, "fuel", 20_000)
    _tx(db, -5_000, store_id="lidl", category_id="food")

    summary = get_home_summary(db, USER)

    # 250 + 200 zł w kopertach; 500 zł bez przeznaczenia nie wlicza się.
    assert summary.spendable_cents == 45_000
    assert summary.ready_to_assign_cents == 50_000
    assert summary.on_budget_balance_cents == 95_000


def test_overspent_envelope_reduces_spendable(db: Session) -> None:
    _tx(db, 100_000, comment="Wpłata")
    _assign(db, "food", 10_000)
    _assign(db, "fuel", 20_000)
    _tx(db, -15_000, store_id="lidl", category_id="food")

    summary = get_home_summary(db, USER)

    # Paliwo 200 zł, Jedzenie -50 zł.
    assert summary.spendable_cents == 15_000
    assert summary.overspent_count == 1


def test_over_assigned_budget_caps_spendable_at_real_money(db: Session) -> None:
    _tx(db, 40_000, comment="Wpłata")
    _assign(db, "food", 30_000)
    _assign(db, "fuel", 30_000)

    summary = get_home_summary(db, USER)

    assert summary.ready_to_assign_cents == -20_000
    # Przypisane 600 zł, ale na koncie jest 400 zł — tyle realnie można wydać.
    assert summary.spendable_cents == 40_000
    assert summary.spendable_cents == summary.on_budget_balance_cents


def test_credit_card_payment_envelope_is_not_spendable(db: Session) -> None:
    db.add(
        Account(
            id="card",
            user_id=USER,
            name="Karta",
            currency="PLN",
            is_on_budget=True,
            account_kind="creditCard",
        )
    )
    db.flush()
    _tx(db, 100_000, comment="Wpłata")
    _assign(db, "food", 30_000)
    # Zakup kartą z pokrytej koperty przenosi 100 zł do koperty spłaty karty.
    _tx(db, -10_000, account_id="card", store_id="lidl", category_id="food")

    summary = get_home_summary(db, USER)

    assert summary.spendable_cents == 20_000
    assert summary.on_budget_account_count == 2


def test_fully_spent_envelope_is_not_near_limit(db: Session) -> None:
    _tx(db, 100_000, comment="Wpłata")
    _assign(db, "food", 10_000)
    _assign(db, "fuel", 10_000)
    _tx(db, -10_000, store_id="lidl", category_id="food")  # opłacone co do grosza
    _tx(db, -9_000, store_id="lidl", category_id="fuel")  # 90%, zostało 10 zł

    summary = get_home_summary(db, USER)

    assert [env.category_id for env in summary.envelopes] == ["fuel"]


def _month_ago(months: int, day: int) -> dt.date:
    index = TODAY.year * 12 + TODAY.month - 1 - months
    return dt.date(index // 12, index % 12 + 1, day)


def test_top_categories_and_uncategorized_add_up_to_spent(db: Session) -> None:
    _tx(db, -30_000, store_id="lidl", category_id="fuel")
    _tx(db, -12_000, store_id="lidl", category_id="food")
    _tx(db, -3_000, store_id="lidl", category_id="food")
    _tx(db, -8_000, comment="BLIK do osoby")
    _tx(db, 5_000, store_id="lidl", category_id="food")  # zwrot nie zmniejsza wydatków

    summary = get_home_summary(db, USER)

    assert [(c.category_id, c.spent_cents) for c in summary.top_categories] == [
        ("fuel", 30_000),
        ("food", 15_000),
    ]
    assert summary.uncategorized_spent_cents == 8_000
    total = sum(c.spent_cents for c in summary.top_categories) + summary.uncategorized_spent_cents
    assert total == summary.spent_month_cents


def test_top_category_path_includes_parent(db: Session) -> None:
    db.add(Category(id="home", user_id=USER, name="Dom"))
    db.add(Category(id="furniture", user_id=USER, name="Wyposażenie", parent_id="home"))
    db.flush()
    _tx(db, -24_999, store_id="lidl", category_id="furniture")

    assert get_home_summary(db, USER).top_categories[0].path == "Dom/Wyposażenie"


def test_pace_is_cumulative_up_to_today(db: Session) -> None:
    _tx(db, -1_000, store_id="lidl", category_id="food", date=TODAY.replace(day=1))
    _tx(db, -500, store_id="lidl", category_id="food")

    pace = get_home_summary(db, USER).pace

    assert len(pace.current_cents) == TODAY.day
    assert pace.current_cents[-1] == 1_500
    assert pace.current_cents[0] == (1_000 if TODAY.day > 1 else 1_500)
    assert pace.average_cents == []  # brak wcześniejszych miesięcy z danymi
    assert pace.average_months == 0


def test_pace_average_skips_months_before_first_data(db: Session) -> None:
    # Dane od dwóch miesięcy wstecz: średnia z dwóch miesięcy, nie z sześciu.
    _tx(db, -10_000, store_id="lidl", category_id="food", date=_month_ago(2, 1))
    _tx(db, -30_000, store_id="lidl", category_id="food", date=_month_ago(1, 1))
    _tx(db, -2_000, store_id="lidl", category_id="food", date=_month_ago(1, 28))

    pace = get_home_summary(db, USER).pace

    assert pace.average_months == 2
    assert len(pace.average_cents) == pace.days_in_month
    assert pace.average_cents[0] == 20_000  # (100 + 300) / 2
    assert pace.average_cents[-1] == 21_000  # (100 + 320) / 2
