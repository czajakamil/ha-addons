"""Scalanie produktów: kilka produktów staje się jednym.

Paragony każdego sklepu nazywają ten sam produkt inaczej, więc w słowniku łatwo
narobić duplikatów („Zup.chin.vifon kurcz”, „Vifon kurczak”). Scalanie przepina
pozycje transakcji i aliasy na produkt docelowy, a nazwy scalanych produktów
zapisuje jako aliasy globalne, żeby kolejny paragon z tą nazwą trafił w cel.

Bez cofania, inaczej niż przy sklepach: produkt nie ma zewnętrznego id (YNAB),
a pozycje i aliasy da się przepiąć z powrotem ręcznie.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from ..dependencies import _get_owned_or_404
from ..models import Product, ProductAlias, TransactionItem
from .product_matching import normalize_alias

# Puste pola celu uzupełniamy z pierwszego scalanego produktu, który je ma.
_FILLABLE_FIELDS = (
    "group_name",
    "name_simplified",
    "unit_of_measurement",
    "category_id",
    "tax_rate",
    "manufacturer",
    "comment",
)


@dataclass
class ProductMergeOutcome:
    target_id: str
    merged_count: int
    items_moved: int
    aliases_moved: int
    aliases_added: int


def _load(
    db: Session, user_id: str, source_ids: Sequence[str], target_id: str
) -> tuple[Product, list[Product]]:
    unique_ids = list(dict.fromkeys(source_ids))
    if not unique_ids:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Wybierz produkty do scalenia.")
    if target_id in unique_ids:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "Produkt docelowy nie może być też scalany."
        )
    target = _get_owned_or_404(db, Product, target_id, user_id, "Item")
    sources = [_get_owned_or_404(db, Product, pid, user_id, "Item") for pid in unique_ids]
    return target, sources


def _names_to_learn(target: Product, sources: list[Product]) -> list[str]:
    """Znormalizowane nazwy scalanych produktów, których cel jeszcze nie rozpoznaje."""
    known = {normalize_alias(target.name), normalize_alias(target.name_simplified)}
    names: list[str] = []
    for product in sources:
        for raw in (product.name, product.name_simplified):
            norm = normalize_alias(raw)
            if norm and norm not in known and norm not in names:
                names.append(norm)
    return names


def _user_global_aliases(db: Session, user_id: str) -> dict[str, ProductAlias]:
    rows = db.scalars(
        select(ProductAlias)
        .join(Product, Product.id == ProductAlias.product_id)
        .where(Product.user_id == user_id, ProductAlias.store_id.is_(None))
    )
    return {row.alias: row for row in rows}


def merge_products(
    db: Session,
    user_id: str,
    source_ids: Sequence[str],
    target_id: str,
    *,
    preview: bool = False,
) -> ProductMergeOutcome:
    """Scala `source_ids` w `target_id`. Z `preview` tylko liczy skutki, nic nie zmienia."""
    target, sources = _load(db, user_id, source_ids, target_id)
    ids = [product.id for product in sources]

    items_count = db.scalar(
        select(func.count()).select_from(TransactionItem).where(TransactionItem.product_id.in_(ids))
    )
    source_aliases = list(db.scalars(select(ProductAlias).where(ProductAlias.product_id.in_(ids))))
    global_aliases = _user_global_aliases(db, user_id)
    # Nazwa zajęta przez alias globalny: jeśli wskazuje scalany produkt, i tak przejdzie
    # do celu; jeśli inny produkt, zostawiamy go w spokoju.
    added = [name for name in _names_to_learn(target, sources) if name not in global_aliases]

    outcome = ProductMergeOutcome(
        target_id=target.id,
        merged_count=len(sources),
        items_moved=items_count or 0,
        aliases_moved=len(source_aliases),
        aliases_added=len(added),
    )
    if preview:
        return outcome

    db.execute(
        update(TransactionItem)
        .where(TransactionItem.product_id.in_(ids))
        .values(product_id=target.id)
    )

    # Alias globalny o tej samej nazwie może już wskazywać cel — wtedy duplikat usuwamy.
    target_globals = set(
        db.scalars(
            select(ProductAlias.alias).where(
                ProductAlias.product_id == target.id, ProductAlias.store_id.is_(None)
            )
        )
    )
    for alias in source_aliases:
        if alias.store_id is None and alias.alias in target_globals:
            db.delete(alias)
            outcome.aliases_moved -= 1
            continue
        alias.product_id = target.id
        if alias.store_id is None:
            target_globals.add(alias.alias)

    for name in added:
        db.add(ProductAlias(product_id=target.id, store_id=None, alias=name))

    for field in _FILLABLE_FIELDS:
        if getattr(target, field) not in (None, ""):
            continue
        for product in sources:
            value = getattr(product, field)
            if value not in (None, ""):
                setattr(target, field, value)
                break

    db.flush()
    for product in sources:
        db.expire(product, ["items"])
        db.delete(product)
    db.flush()
    return outcome
