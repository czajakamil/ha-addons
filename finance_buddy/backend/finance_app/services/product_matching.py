"""Dopasowanie pozycji paragonu do produktów (Etap 3 pipeline'u OCR).

Nazwy na paragonach to skróty specyficzne dla sklepu ("MLEKO ŁAC.UHT 3.2%"),
więc mapujemy je na produkty kaskadą:

1. alias dokładny w tym sklepie   → pewne trafienie (auto),
2. alias globalny / nazwa produktu → pewne trafienie (auto),
3. fuzzy (rapidfuzz, próg 85) po nazwach i aliasach z innych sklepów
                                   → top-3 propozycje do potwierdzenia,
4. brak dopasowania               → sygnał "nowy produkt" z prefillem nazwy.

Każde potwierdzenie użytkownika zapisuje alias (`save_alias`) — system uczy się
i kolejny paragon z tego samego sklepu mapuje się automatycznie.

Sugestia tagu (Etap 4): przy pewnym trafieniu bierzemy `Product.category_id`
dopasowanego produktu; przy fuzzy/nowym — tag najbardziej podobnego kandydata.

Projekt: docs/design/ocr-pipeline.md.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from rapidfuzz import fuzz
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Product, ProductAlias, Store
from .categories import build_path_map

# Próg podobieństwa fuzzy (rapidfuzz WRatio, skala 0–100). 85 dobrane pod krótkie
# polskie nazwy produktów: literówki OCR i drobne skróty ("MLEKO UHT" vs
# "Mleko UHT 3,2%") zostają nad progiem, a wyraźnie inne produkty ("MLEKO" vs
# "MASŁO") spadają poniżej. Niżej sypią się fałszywe trafienia, wyżej gubimy
# oczywiste dopasowania wymagające skrótu.
FUZZY_THRESHOLD = 85.0
MAX_CANDIDATES = 3

_WHITESPACE = re.compile(r"\s+")


# Typy dopasowania (kolejność kaskady). "none" = brak, do założenia nowego produktu.
MATCH_ALIAS_STORE = "alias_store"
MATCH_ALIAS_GLOBAL = "alias_global"
MATCH_NAME = "name"
MATCH_FUZZY = "fuzzy"
MATCH_NONE = "none"

# Trafienia pewne (bez potwierdzenia): zapisujemy transakcję bez pytania.
CERTAIN_MATCHES = frozenset({MATCH_ALIAS_STORE, MATCH_ALIAS_GLOBAL, MATCH_NAME})


@dataclass
class ProductCandidate:
    product_id: str
    name: str
    score: float
    category_id: str | None = None
    category_path: str | None = None


@dataclass
class ItemMatch:
    raw_name: str
    match_type: str
    matched_product_id: str | None = None
    matched_product_name: str | None = None
    candidates: list[ProductCandidate] = field(default_factory=list)
    suggested_category_id: str | None = None
    suggested_category_path: str | None = None
    prefill_name: str = ""

    @property
    def is_certain(self) -> bool:
        return self.match_type in CERTAIN_MATCHES


def normalize_alias(raw: str | None) -> str:
    """Normalizuje nazwę z paragonu do postaci porównywalnej (małe litery, pojedyncze spacje)."""
    if not raw:
        return ""
    return _WHITESPACE.sub(" ", raw.strip()).lower()


def _prefill_name(raw: str | None) -> str:
    """Nazwa do prefillu formularza nowego produktu.

    `raw_name` zostaje osobno, drukowaną wielkością liter — to on jedzie do
    aliasów i dopasowania kolejnych paragonów. Tu chodzi o nazwę *produktu*,
    czyli innego bytu: paragon fiskalny drukuje CAŁYMI WIELKIMI, a to się nie
    powinno przenosić na kartotekę produktów. Cofamy do zdania, tylko gdy
    cały tekst krzyczy wielkimi — mieszaną wielkość liter (już czyjaś ręczna
    poprawka) zostawiamy bez zmian.
    """
    text = _WHITESPACE.sub(" ", (raw or "").strip())
    if text.isupper():
        return text[:1] + text[1:].lower()
    return text


class _MatchIndex:
    """Zbuforowane indeksy produktów/aliasów użytkownika — budowane raz na żądanie."""

    def __init__(self, db: Session, user_id: str) -> None:
        self.products: list[Product] = list(
            db.scalars(select(Product).where(Product.user_id == user_id)).all()
        )
        self.by_id: dict[str, Product] = {p.id: p for p in self.products}
        self.paths: dict[str, str] = build_path_map(db, user_id)

        # Aliasy zawężone do produktów użytkownika (join po user_id).
        alias_rows = db.execute(
            select(ProductAlias.alias, ProductAlias.store_id, ProductAlias.product_id)
            .join(Product, Product.id == ProductAlias.product_id)
            .where(Product.user_id == user_id)
        ).all()
        self.alias_by_store: dict[tuple[str, str], str] = {}
        self.alias_global: dict[str, str] = {}
        # Wszystkie znane nazwy z paragonów per produkt — do fuzzy. Paragon z innego
        # sklepu rzadko przypomina nazwę produktu („Zupka chińska”), ale często
        # przypomina to, co ten produkt nazywał już inny sklep („VIFON KURCZAK 70G”).
        self.aliases_by_product: dict[str, list[str]] = {}
        for alias, store_id, product_id in alias_rows:
            self.aliases_by_product.setdefault(product_id, []).append(alias)
            if store_id:
                self.alias_by_store[(store_id, alias)] = product_id
            else:
                self.alias_global[alias] = product_id

        # Dokładna nazwa produktu (name / name_simplified) → produkt.
        self.name_index: dict[str, str] = {}
        for product in self.products:
            for candidate in (product.name, product.name_simplified):
                norm = normalize_alias(candidate)
                if norm:
                    self.name_index.setdefault(norm, product.id)

    def category_path(self, category_id: str | None) -> str | None:
        if not category_id:
            return None
        return self.paths.get(category_id)


def _fuzzy_candidates(raw_norm: str, index: _MatchIndex) -> list[ProductCandidate]:
    """Top-N produktów powyżej progu podobieństwa.

    Wynik produktu to najlepszy z jego nazw i aliasów (z dowolnego sklepu).
    """
    scored: list[ProductCandidate] = []
    for product in index.products:
        best = 0.0
        names = [
            normalize_alias(product.name),
            normalize_alias(product.name_simplified),
            *index.aliases_by_product.get(product.id, ()),
        ]
        for norm in names:
            if not norm:
                continue
            best = max(best, fuzz.WRatio(raw_norm, norm))
        if best >= FUZZY_THRESHOLD:
            scored.append(
                ProductCandidate(
                    product_id=product.id,
                    name=product.name,
                    score=round(best, 1),
                    category_id=product.category_id,
                    category_path=index.category_path(product.category_id),
                )
            )
    scored.sort(key=lambda c: c.score, reverse=True)
    return scored[:MAX_CANDIDATES]


def _match_one(raw_name: str, store_id: str | None, index: _MatchIndex) -> ItemMatch:
    raw_norm = normalize_alias(raw_name)
    prefill = _prefill_name(raw_name)

    def certain(product_id: str, match_type: str) -> ItemMatch:
        product = index.by_id.get(product_id)
        category_id = product.category_id if product else None
        return ItemMatch(
            raw_name=raw_name,
            match_type=match_type,
            matched_product_id=product_id,
            matched_product_name=product.name if product else None,
            suggested_category_id=category_id,
            suggested_category_path=index.category_path(category_id),
            prefill_name=prefill,
        )

    if not raw_norm:
        return ItemMatch(raw_name=raw_name, match_type=MATCH_NONE, prefill_name=prefill)

    # 1. alias dokładny w tym sklepie
    if (
        store_id
        and (product_id := index.alias_by_store.get((store_id, raw_norm)))
        and product_id in index.by_id
    ):
        return certain(product_id, MATCH_ALIAS_STORE)

    # 2. alias globalny
    if (product_id := index.alias_global.get(raw_norm)) and product_id in index.by_id:
        return certain(product_id, MATCH_ALIAS_GLOBAL)

    # 2b. dokładna nazwa produktu (name / name_simplified)
    if product_id := index.name_index.get(raw_norm):
        return certain(product_id, MATCH_NAME)

    # 3. fuzzy → propozycje do potwierdzenia; tag z najlepszego kandydata
    candidates = _fuzzy_candidates(raw_norm, index)
    if candidates:
        top = candidates[0]
        return ItemMatch(
            raw_name=raw_name,
            match_type=MATCH_FUZZY,
            candidates=candidates,
            suggested_category_id=top.category_id,
            suggested_category_path=top.category_path,
            prefill_name=prefill,
        )

    # 4. brak dopasowania → nowy produkt z prefillem nazwy
    return ItemMatch(raw_name=raw_name, match_type=MATCH_NONE, prefill_name=prefill)


def match_items(
    db: Session,
    user_id: str,
    raw_names: list[str],
    store_id: str | None = None,
) -> list[ItemMatch]:
    """Dopasowuje listę nazw pozycji paragonu do produktów użytkownika.

    Kolejność wyników odpowiada `raw_names`. `store_id` (opcjonalny) włącza aliasy
    per-sklep w kroku 1; gdy go brak, działa tylko dopasowanie globalne/fuzzy.
    """
    index = _MatchIndex(db, user_id)
    return [_match_one(raw_name, store_id, index) for raw_name in raw_names]


def save_alias(
    db: Session,
    user_id: str,
    product_id: str,
    raw_name: str,
    store_id: str | None = None,
) -> ProductAlias | None:
    """Zapisuje (lub przepina) alias z potwierdzenia użytkownika — pętla uczenia.

    Zwraca `None`, gdy nazwa jest pusta albo produkt/sklep nie należy do użytkownika.
    Idempotentne: istniejący alias o tej samej znormalizowanej nazwie i sklepie
    zostaje przepięty na wskazany produkt zamiast tworzyć duplikat.
    """
    alias = normalize_alias(raw_name)
    if not alias:
        return None

    product = db.get(Product, product_id)
    if not product or product.user_id != user_id:
        return None

    if store_id is not None:
        store = db.get(Store, store_id)
        if not store or store.user_id != user_id:
            return None

    # Istniejący alias (zawężony do produktów użytkownika) — przepinamy zamiast dublować.
    existing = db.scalars(
        select(ProductAlias)
        .join(Product, Product.id == ProductAlias.product_id)
        .where(
            Product.user_id == user_id,
            ProductAlias.alias == alias,
            ProductAlias.store_id.is_(None)
            if store_id is None
            else ProductAlias.store_id == store_id,
        )
    ).first()
    if existing is not None:
        existing.product_id = product_id
        db.flush()
        return existing

    obj = ProductAlias(product_id=product_id, store_id=store_id, alias=alias)
    db.add(obj)
    db.flush()
    return obj
