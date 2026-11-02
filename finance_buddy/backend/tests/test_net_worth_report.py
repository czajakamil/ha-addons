from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Transaction
from backend.finance_app.routers.reports import _build_net_worth, _earliest_transaction_date

USER = "user-1"
MONTHS = [dt.date(2026, 9, 1), dt.date(2026, 10, 1)]
END = dt.date(2026, 10, 31)


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="bank", user_id=USER, name="Bank", currency="PLN"))
        session.add(
            Account(
                id="card",
                user_id=USER,
                name="Karta",
                currency="PLN",
                account_kind="creditCard",
            )
        )
        session.add(Account(id="old", user_id=USER, name="Stare", currency="PLN", is_closed=True))
        session.add(Account(id="empty", user_id=USER, name="Puste", currency="PLN"))
        session.flush()
        yield session


def add_tx(db: Session, account: str, day: dt.date, cents: int) -> None:
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    db.add(
        Transaction(
            user_id=USER,
            account_id=account,
            date=day,
            recorded_at=moment,
            created_at=moment,
            total_cents=cents,
            currency="PLN",
        )
    )
    db.flush()


def test_account_balances_add_up_to_latest_net_worth(db: Session) -> None:
    add_tx(db, "bank", dt.date(2026, 9, 5), 1000)
    add_tx(db, "card", dt.date(2026, 10, 5), -300)
    add_tx(db, "old", dt.date(2026, 9, 5), 50)
    add_tx(db, "empty", dt.date(2026, 9, 5), 70)
    add_tx(db, "empty", dt.date(2026, 9, 6), -70)

    report = _build_net_worth(db, MONTHS, END, USER)

    balances = {row.account_id: row.balance_cents for row in report.accounts}
    assert balances == {"bank": 1000, "card": -300, "old": 50}
    assert sum(balances.values()) == report.latest_net_worth_cents == 750


def test_balances_are_taken_at_end_of_period(db: Session) -> None:
    add_tx(db, "bank", dt.date(2026, 10, 31), 1000)
    add_tx(db, "bank", dt.date(2026, 11, 1), 5000)

    report = _build_net_worth(db, MONTHS, END, USER)

    assert [row.balance_cents for row in report.accounts] == [1000]


def test_account_filter_limits_balances(db: Session) -> None:
    add_tx(db, "bank", dt.date(2026, 9, 5), 1000)
    add_tx(db, "old", dt.date(2026, 9, 5), 50)

    report = _build_net_worth(db, MONTHS, END, USER, {"old"})

    assert [row.account_id for row in report.accounts] == ["old"]
    assert report.latest_net_worth_cents == 50


def test_earliest_transaction_date_is_per_user(db: Session) -> None:
    assert _earliest_transaction_date(db, USER) is None
    add_tx(db, "bank", dt.date(2024, 3, 9), 100)
    add_tx(db, "bank", dt.date(2025, 1, 2), 100)
    db.flush()
    assert _earliest_transaction_date(db, USER) == dt.date(2024, 3, 9)
    assert _earliest_transaction_date(db, "someone-else") is None
