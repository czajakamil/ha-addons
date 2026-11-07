"""Podział kwoty transakcji na kategorie według pozycji paragonu.

Zasada (ta sama co w szczegółach transakcji w UI):
- pozycja liczy się do kategorii swojego produktu, a produkt bez kategorii — do
  głównej kategorii transakcji;
- część kwoty nieobjęta pozycjami (np. niewpisane produkty) zostaje w głównej
  kategorii transakcji;
- gdy pozycje przekraczają kwotę (rabat na cały paragon), kwotę rozkładamy
  proporcjonalnie do pozycji, żeby żadna część nie zmieniła znaku — inaczej
  rabat wyglądałby w raportach jak przychód.

Transakcja bez pozycji to jedna linia: jej kategoria i cała kwota. Budżet,
raporty i statystyki kategorii biorą linie stąd zamiast `Transaction.category_id`.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Category, Product, TransactionItem

# Wiersze IN (...) dzielimy na paczki: SQLite ma limit parametrów w zapytaniu.
_CHUNK = 500

ItemShare = tuple[str | None, int]  # (kategoria produktu albo None, grosze dodatnie)


def _item_cents(unit_price_cents: int, quantity: float) -> int:
    return round(unit_price_cents * quantity)


def load_item_shares(db: Session, transaction_ids: Iterable[str]) -> dict[str, list[ItemShare]]:
    """Pozycje transakcji jako (kategoria produktu, kwota). Transakcje bez pozycji nie mają wpisu.

    Kategoria usunięta albo oznaczona jako kategoria kont nie liczy się jako kategoria produktu.
    """
    ids = list(dict.fromkeys(transaction_ids))
    shares: dict[str, list[ItemShare]] = defaultdict(list)
    for start in range(0, len(ids), _CHUNK):
        chunk = ids[start : start + _CHUNK]
        rows = db.execute(
            select(
                TransactionItem.transaction_id,
                TransactionItem.unit_price_cents,
                TransactionItem.quantity,
                Category.id,
            )
            .join(Product, Product.id == TransactionItem.product_id)
            .outerjoin(
                Category,
                (Category.id == Product.category_id) & Category.is_for_accounts.is_(False),
            )
            .where(TransactionItem.transaction_id.in_(chunk))
        ).all()
        for transaction_id, unit_price_cents, quantity, category_id in rows:
            shares[transaction_id].append(
                (category_id, _item_cents(int(unit_price_cents), float(quantity)))
            )
    return dict(shares)


def _allocate(total: int, weights: list[int]) -> list[int]:
    """Rozkłada `total` (>= 0) proporcjonalnie do wag; reszty groszy dostają największe ułamki."""
    weight_sum = sum(weights)
    raw = [total * weight / weight_sum for weight in weights]
    parts = [int(value) for value in raw]
    remainder = total - sum(parts)
    order = sorted(range(len(raw)), key=lambda index: raw[index] - parts[index], reverse=True)
    for index in order[:remainder]:
        parts[index] += 1
    return parts


def split_lines(
    category_id: str | None, total_cents: int, items: list[ItemShare] | None
) -> list[tuple[str | None, int]]:
    """Linie (kategoria, grosze ze znakiem transakcji) sumujące się do `total_cents`."""
    if not items:
        return [(category_id, total_cents)]
    positive = [
        (item_category or category_id, cents) for item_category, cents in items if cents > 0
    ]
    # Ujemne pozycje (zwrot kaucji, rabat w linii) pomniejszają pozycje — tak jak na paragonie.
    negative = sum(-cents for _, cents in items if cents < 0)
    items_sum = sum(cents for _, cents in positive) - negative
    if items_sum <= 0 or not positive:
        return [(category_id, total_cents)]

    sign = -1 if total_cents < 0 else 1
    magnitude = abs(total_cents)
    lines: dict[str | None, int] = defaultdict(int)
    if items_sum <= magnitude and negative == 0:
        for item_category, cents in positive:
            lines[item_category] += cents
        lines[category_id] += magnitude - items_sum
    else:
        # Rabaty albo pozycje ponad kwotę: cała kwota rozłożona proporcjonalnie do pozycji
        covered = min(items_sum, magnitude)
        parts = _allocate(covered, [cents for _, cents in positive])
        for (item_category, _), part in zip(positive, parts, strict=True):
            lines[item_category] += part
        lines[category_id] += magnitude - covered
    result = [(key, sign * cents) for key, cents in lines.items() if cents]
    return result or [(category_id, total_cents)]
