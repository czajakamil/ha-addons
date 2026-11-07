"""Statystyki przedmiotów liczone w bazie, na tych samych zasadach co sklepy.

Liczba zakupów (transakcji z przedmiotem) i ostatni zakup obejmują całą
historię, a wydatki 12 miesięcy kalendarzowych razem z bieżącym. Wydatki to
cena jednostkowa × ilość, bez przelewów, korekt salda i walut innych niż PLN.
Pozycja na transakcji-wpływie (zwrot) pomniejsza sumę.

Ilości nie sumujemy: ten sam przedmiot bywa w kilogramach i w sztukach.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from sqlalchemy import Select, case, func, select
from sqlalchemy.orm import Session

from ..models import Category, Product, Store, Transaction, TransactionItem
from .store_stats import MONTHS_IN_WINDOW, _counts_as_spending, _month_start

# Pozycja na transakcji z dodatnią kwotą to zwrot, więc odejmujemy ją od wydatków.
_line_spend = case(
    (Transaction.total_cents > 0, -1),
    else_=1,
) * func.round(TransactionItem.unit_price_cents * TransactionItem.quantity)


def _scoped(stmt: Select, user_id: str, item_ids: Sequence[str] | None) -> Select:
    # Sklep i kategoria są potrzebne warunkom z raportów, które patrzą na ich nazwy.
    stmt = (
        stmt.select_from(TransactionItem)
        .join(Transaction, Transaction.id == TransactionItem.transaction_id)
        .join(Product, Product.id == TransactionItem.product_id)
        .outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(Product.user_id == user_id, Transaction.user_id == user_id)
    )
    return stmt.where(TransactionItem.product_id.in_(item_ids)) if item_ids else stmt


def item_stats(
    db: Session,
    user_id: str,
    item_ids: Sequence[str] | None = None,
    today: dt.date | None = None,
) -> list[dict[str, object]]:
    today = today or dt.datetime.now(dt.UTC).date()
    spent = func.sum(case((_counts_as_spending(today), _line_spend), else_=0))
    stmt = _scoped(
        select(
            TransactionItem.product_id,
            func.count(func.distinct(Transaction.id)),
            func.coalesce(spent, 0),
            func.max(Transaction.date),
        ),
        user_id,
        item_ids,
    ).group_by(TransactionItem.product_id)
    return [
        {"item_id": item_id, "count": count, "spent_cents_12m": int(cents), "last_date": last}
        for item_id, count, cents, last in db.execute(stmt)
    ]


def item_monthly_spend(
    db: Session,
    user_id: str,
    item_ids: Sequence[str],
    today: dt.date | None = None,
) -> list[dict[str, object]]:
    """Wydatki na przedmioty w 12 miesiącach, od najstarszego; miesiąc bez zakupów to 0."""
    today = today or dt.datetime.now(dt.UTC).date()
    month = func.strftime("%Y-%m", Transaction.date)
    stmt = _scoped(
        select(month, func.sum(_line_spend)).where(_counts_as_spending(today)),
        user_id,
        item_ids,
    ).group_by(month)
    totals = {entry: int(cents) for entry, cents in db.execute(stmt).tuples().all()}
    months = [
        _month_start(today, back).strftime("%Y-%m") for back in range(MONTHS_IN_WINDOW - 1, -1, -1)
    ]
    return [{"month": entry, "cents": totals.get(entry, 0)} for entry in months]
