from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Category, Store, Transaction
from backend.finance_app.routers.categories import (
    create_category,
    delete_category,
    list_categories,
    reorder_categories,
    update_category,
)
from backend.finance_app.schemas import CategoryCreate, CategoryReorder, CategoryUpdate
from backend.finance_app.services.category_stats import category_stats, category_top_stores

TODAY = dt.date(2026, 11, 15)
USER = "user-1"
ME = SimpleNamespace(id=USER)


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.add(Store(id="s1", user_id=USER, name="Lidl"))
        session.add(Store(id="s2", user_id=USER, name="Lidl Plac", group_name="Lidl"))
        session.add(Store(id="s3", user_id=USER, name="Biedronka"))
        session.flush()
        yield session


def add_cat(db: Session, cid: str, name: str, parent: str | None = None, order: int = 0) -> None:
    db.add(Category(id=cid, user_id=USER, name=name, parent_id=parent, sort_order=order))
    db.flush()


def add_tx(db: Session, category: str | None, day: dt.date, cents: int, store: str | None = None):
    moment = dt.datetime.combine(day, dt.time.min, tzinfo=dt.UTC)
    db.add(
        Transaction(
            user_id=USER,
            account_id="a",
            store_id=store,
            category_id=category,
            date=day,
            recorded_at=moment,
            created_at=moment,
            total_cents=cents,
            currency="PLN",
        )
    )
    db.flush()


def names(db: Session, parent_id: str | None = None) -> list[str]:
    rows = list_categories(
        parent_id=parent_id,
        for_accounts=None,
        path_prefix=None,
        offset=0,
        limit=100,
        db=db,
        current_user=ME,
    )
    return [row.name for row in rows if row.parent_id == parent_id]


def test_list_follows_sort_order_and_new_ones_go_last(db: Session) -> None:
    add_cat(db, "b", "Bez", order=0)
    add_cat(db, "a", "Auto", order=1)
    create_category(CategoryCreate(name="Czynsz"), db=db, current_user=ME)
    create_category(CategoryCreate(path="Auto/Paliwo"), db=db, current_user=ME)
    assert names(db) == ["Bez", "Auto", "Czynsz"]


def test_reorder_sets_positions_and_keeps_omitted_siblings_after(db: Session) -> None:
    add_cat(db, "a", "A", order=0)
    add_cat(db, "b", "B", order=1)
    add_cat(db, "c", "C", order=2)
    reorder_categories(
        CategoryReorder(parent_id=None, category_ids=["c", "a"]), db=db, current_user=ME
    )
    assert names(db) == ["C", "A", "B"]


def test_reorder_moves_category_under_new_parent(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_cat(db, "home", "Dom", order=1)
    add_cat(db, "snacks", "Przekąski", "food")
    reorder_categories(
        CategoryReorder(parent_id="home", category_ids=["snacks"]), db=db, current_user=ME
    )
    assert db.get(Category, "snacks").parent_id == "home"


def test_reorder_refuses_cycles_and_duplicate_names(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_cat(db, "out", "Na mieście", "food")
    add_cat(db, "home", "Dom", order=1)
    add_cat(db, "home-out", "Na mieście", "home")
    with pytest.raises(HTTPException) as cycle:
        reorder_categories(
            CategoryReorder(parent_id="out", category_ids=["food"]), db=db, current_user=ME
        )
    assert cycle.value.status_code == 409
    with pytest.raises(HTTPException) as duplicate:
        reorder_categories(
            CategoryReorder(parent_id="food", category_ids=["out", "home-out"]),
            db=db,
            current_user=ME,
        )
    assert duplicate.value.status_code == 409


def test_moving_by_patch_puts_category_at_the_end(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_cat(db, "x", "X", "food", order=0)
    add_cat(db, "y", "Y", "food", order=1)
    add_cat(db, "z", "Z")
    update_category("z", CategoryUpdate(parent_id="food"), db=db, current_user=ME)
    assert names(db, "food") == ["X", "Y", "Z"]


def test_delete_only_without_transactions_in_subtree(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_cat(db, "snacks", "Przekąski", "food")
    add_tx(db, "snacks", TODAY, -500)
    with pytest.raises(HTTPException) as refused:
        delete_category("food", db=db, current_user=ME)
    assert refused.value.status_code == 409

    add_cat(db, "empty", "Pusta")
    delete_category("empty", db=db, current_user=ME)
    assert db.get(Category, "empty") is None


def test_stats_count_own_transactions_and_months(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_tx(db, "food", TODAY, -1000)
    add_tx(db, "food", dt.date(2026, 10, 3), -2000)
    add_tx(db, "food", dt.date(2026, 10, 9), 500)  # zwrot pomniejsza sumę
    add_tx(db, "food", dt.date(2025, 1, 1), -9900)  # poza oknem, ale liczy się do liczby
    [row] = category_stats(db, USER, today=TODAY)
    assert row["count"] == 4
    assert row["spent_cents_12m"] == 2500
    assert row["last_date"] == TODAY
    assert row["monthly_cents"][-1] == 1000
    assert row["monthly_cents"][-2] == 1500
    assert len(row["monthly_cents"]) == 12


def test_top_stores_group_by_store_group(db: Session) -> None:
    add_cat(db, "food", "Jedzenie")
    add_tx(db, "food", TODAY, -1000, "s1")
    add_tx(db, "food", TODAY, -1500, "s2")
    add_tx(db, "food", TODAY, -2000, "s3")
    rows = category_top_stores(db, USER, ["food"], today=TODAY)
    assert [(row["name"], row["count"], row["spent_cents_12m"]) for row in rows] == [
        ("Lidl", 2, 2500),
        ("Biedronka", 1, 2000),
    ]
