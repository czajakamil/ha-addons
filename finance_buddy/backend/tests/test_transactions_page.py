from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Transaction
from backend.finance_app.services.transactions import list_transactions_page

USER = "user-1"
DAY = dt.date(2026, 11, 1)


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Main", currency="PLN"))
        session.flush()
        for index, cleared in enumerate([True, False, False, True]):
            session.add(
                Transaction(
                    id=f"t{index}",
                    user_id=USER,
                    account_id="a",
                    date=DAY,
                    recorded_at=dt.datetime(2026, 11, 1, 12, index, tzinfo=dt.UTC),
                    currency="PLN",
                    total_cents=-100 * (index + 1),
                    cleared=cleared,
                )
            )
        session.flush()
        yield session


def test_uncleared_only_returns_pending_transactions(db: Session) -> None:
    page = list_transactions_page(db, USER, uncleared_only=True)

    assert page["total"] == 2
    assert {tx.id for tx in page["items"]} == {"t1", "t2"}


def test_without_flag_returns_everything(db: Session) -> None:
    assert list_transactions_page(db, USER)["total"] == 4
