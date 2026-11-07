"""Statystyki kategorii liczone w bazie, na tych samych zasadach co statystyki sklepów.

Liczba i ostatnia transakcja obejmują całą historię, a wydatki 12 miesięcy
kalendarzowych razem z bieżącym (przelewy, korekty salda i waluty inne niż PLN
pomijamy, jak w raportach). Każdy wiersz dotyczy WŁASNYCH transakcji kategorii:
sumy gałęzi składa UI, bo zna drzewo i zwinięte węzły.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from sqlalchemy import Select, case, func, select
from sqlalchemy.orm import Session

from ..models import Category, Store, Transaction
from .store_stats import MONTHS_IN_WINDOW, _counts_as_spending, _month_start

TOP_STORES_LIMIT = 5


def _scoped(stmt: Select, user_id: str) -> Select:
    # Joiny dla warunków z raportów, które patrzą na nazwy sklepu i kategorii.
    return (
        stmt.outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(Transaction.user_id == user_id, Transaction.category_id.is_not(None))
    )


def _window_months(today: dt.date) -> list[str]:
    return [
        _month_start(today, back).strftime("%Y-%m") for back in range(MONTHS_IN_WINDOW - 1, -1, -1)
    ]


def category_stats(
    db: Session, user_id: str, today: dt.date | None = None
) -> list[dict[str, object]]:
    """Wiersz na kategorię z transakcjami: liczba, wydatki 12 mies., ostatnia data, miesiące."""
    today = today or dt.datetime.now(dt.UTC).date()
    spending = _counts_as_spending(today)
    totals_stmt = _scoped(
        select(
            Transaction.category_id,
            func.count(Transaction.id),
            func.coalesce(func.sum(case((spending, -Transaction.total_cents), else_=0)), 0),
            func.max(Transaction.date),
        ),
        user_id,
    ).group_by(Transaction.category_id)

    month = func.strftime("%Y-%m", Transaction.date)
    monthly_stmt = (
        _scoped(select(Transaction.category_id, month, func.sum(-Transaction.total_cents)), user_id)
        .where(spending)
        .group_by(Transaction.category_id, month)
    )
    monthly: dict[str, dict[str, int]] = {}
    for category_id, entry, cents in db.execute(monthly_stmt):
        monthly.setdefault(category_id, {})[entry] = cents

    months = _window_months(today)
    return [
        {
            "category_id": category_id,
            "count": count,
            "spent_cents_12m": cents,
            "last_date": last,
            "monthly_cents": [monthly.get(category_id, {}).get(entry, 0) for entry in months],
        }
        for category_id, count, cents, last in db.execute(totals_stmt)
    ]


def category_top_stores(
    db: Session,
    user_id: str,
    category_ids: Sequence[str],
    today: dt.date | None = None,
    limit: int = TOP_STORES_LIMIT,
) -> list[dict[str, object]]:
    """Sklepy z największymi wydatkami w podanych kategoriach (12 mies.), malejąco.

    Sklepy z jednej grupy liczymy razem, pod nazwą grupy: tak samo widzi je ekran Sklepy.
    """
    if not category_ids:
        return []
    today = today or dt.datetime.now(dt.UTC).date()
    name = func.coalesce(func.nullif(Store.group_name, ""), Store.name)
    spent = func.sum(-Transaction.total_cents)
    stmt = (
        _scoped(select(name, func.min(Store.id), func.count(Transaction.id), spent), user_id)
        .where(
            Transaction.category_id.in_(category_ids),
            Transaction.store_id.is_not(None),
            _counts_as_spending(today),
        )
        .group_by(name)
        .having(spent > 0)
        .order_by(spent.desc(), name)
        .limit(limit)
    )
    return [
        {"name": store_name, "store_id": store_id, "count": count, "spent_cents_12m": cents}
        for store_name, store_id, count, cents in db.execute(stmt)
    ]


def subtree_transaction_count(db: Session, user_id: str, category_ids: Sequence[str]) -> int:
    return (
        db.scalar(
            select(func.count())
            .select_from(Transaction)
            .where(Transaction.user_id == user_id, Transaction.category_id.in_(category_ids))
        )
        or 0
    )
