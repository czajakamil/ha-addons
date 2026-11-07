"""Statystyki kategorii liczone w bazie, na tych samych zasadach co statystyki sklepów.

Liczba i ostatnia transakcja obejmują całą historię, a wydatki 12 miesięcy
kalendarzowych razem z bieżącym (przelewy, korekty salda i waluty inne niż PLN
pomijamy, jak w raportach). Każdy wiersz dotyczy WŁASNYCH transakcji kategorii:
sumy gałęzi składa UI, bo zna drzewo i zwinięte węzły.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from collections.abc import Iterator, Sequence

from sqlalchemy import Row, Select, case, func, select
from sqlalchemy.orm import Session

from ..models import Category, Store, Transaction
from .category_split import load_item_shares, split_lines
from .store_stats import MONTHS_IN_WINDOW, _counts_as_spending, _month_start

TOP_STORES_LIMIT = 5


def _scoped(stmt: Select, user_id: str) -> Select:
    # Joiny dla warunków z raportów, które patrzą na nazwy sklepu i kategorii.
    return (
        stmt.outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(Transaction.user_id == user_id)
    )


def _window_months(today: dt.date) -> list[str]:
    return [
        _month_start(today, back).strftime("%Y-%m") for back in range(MONTHS_IN_WINDOW - 1, -1, -1)
    ]


def _category_lines(db: Session, stmt: Select) -> Iterator[tuple[Row, str, int]]:
    """(wiersz transakcji, kategoria, grosze) — pozycje paragonu idą do kategorii produktów.

    `stmt` zwraca kolumny: id, category_id, total_cents i dowolne dalsze.
    Transakcja bez głównej kategorii też trafia tu, jeśli jej pozycje mają kategorie.
    """
    rows = db.execute(stmt).all()
    shares = load_item_shares(db, [row[0] for row in rows])
    for row in rows:
        for category_id, cents in split_lines(row[1], int(row[2]), shares.get(row[0])):
            if category_id:
                yield row, category_id, cents


def category_stats(
    db: Session, user_id: str, today: dt.date | None = None
) -> list[dict[str, object]]:
    """Wiersz na kategorię z transakcjami: liczba, wydatki 12 mies., ostatnia data, miesiące."""
    today = today or dt.datetime.now(dt.UTC).date()
    stmt = _scoped(
        select(
            Transaction.id,
            Transaction.category_id,
            Transaction.total_cents,
            Transaction.date,
            case((_counts_as_spending(today), True), else_=False),
        ),
        user_id,
    )
    counts: dict[str, int] = defaultdict(int)
    spent: dict[str, int] = defaultdict(int)
    last: dict[str, dt.date] = {}
    monthly: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row, category_id, cents in _category_lines(db, stmt):
        tx_date, in_window = row[3], row[4]
        counts[category_id] += 1
        if category_id not in last or tx_date > last[category_id]:
            last[category_id] = tx_date
        if in_window:
            spent[category_id] -= cents
            monthly[category_id][tx_date.strftime("%Y-%m")] -= cents

    months = _window_months(today)
    return [
        {
            "category_id": category_id,
            "count": count,
            "spent_cents_12m": spent[category_id],
            "last_date": last[category_id],
            "monthly_cents": [monthly[category_id].get(entry, 0) for entry in months],
        }
        for category_id, count in counts.items()
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
    Z paragonu liczy się tylko część przypadająca na te kategorie.
    """
    if not category_ids:
        return []
    today = today or dt.datetime.now(dt.UTC).date()
    wanted = set(category_ids)
    name = func.coalesce(func.nullif(Store.group_name, ""), Store.name)
    stmt = _scoped(
        select(Transaction.id, Transaction.category_id, Transaction.total_cents, name, Store.id),
        user_id,
    ).where(Transaction.store_id.is_not(None), _counts_as_spending(today))
    spent: dict[str, int] = defaultdict(int)
    first_store_id: dict[str, str] = {}
    transactions: dict[str, set[str]] = defaultdict(set)
    for row, category_id, cents in _category_lines(db, stmt):
        if category_id not in wanted:
            continue
        tx_id, store_name, store_id = row[0], row[3], row[4]
        spent[store_name] -= cents
        transactions[store_name].add(tx_id)
        first_store_id[store_name] = min(first_store_id.get(store_name, store_id), store_id)
    ranked = sorted((store for store in spent if spent[store] > 0), key=lambda s: (-spent[s], s))
    return [
        {
            "name": store_name,
            "store_id": first_store_id[store_name],
            "count": len(transactions[store_name]),
            "spent_cents_12m": spent[store_name],
        }
        for store_name in ranked[:limit]
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
