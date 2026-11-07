"""Aliasy nazw z banku: po nich importer przypisuje transakcję do sklepu.

Dopasowanie bez zgadywania: najpierw dokładny alias po normalizacji, potem prefiks,
ale tylko dla aliasów z ``*`` na końcu (najdłuższy wygrywa), a bez trafienia
transakcja zostaje bez sklepu. Tylko końcowa gwiazdka coś znaczy: ``PayU*Allegro``
to zwykła nazwa z banku.

Ta sama nazwa kilku aktywnych sklepów daje aliasy niejednoznaczne. Importer ich nie
używa, bo wybór sklepu byłby zgadywaniem.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Store, StoreAlias, Transaction

EXACT = "exact"
PREFIX = "prefix"
MIN_PREFIX_LENGTH = 4
# Tak łączy opis `bank_csv.ParsedRow.description`: nazwa · tytuł przelewu.
_DESCRIPTION_SEPARATOR = " · "

_LEGAL_FORMS = (
    re.compile(r"\bsp\.?\s*z\s*o\.?\s*o\.?", re.IGNORECASE),
    re.compile(r"\bs\.\s*a\.?(?=\s|$)", re.IGNORECASE),
    re.compile(r"\bsa$", re.IGNORECASE),
)


def normalize_alias(value: str) -> str:
    """Bez wielkości liter, nadmiarowych spacji i form prawnych („Sp. z o.o.”, „S.A.”)."""
    stripped = value
    for form in _LEGAL_FORMS:
        stripped = form.sub(" ", stripped)
    result = " ".join(stripped.casefold().replace(".", " ").replace(",", " ").split())
    return result or " ".join(value.casefold().split())


@dataclass(frozen=True)
class ParsedAlias:
    pattern: str
    normalized: str
    match_type: str


def parse_alias(raw: str) -> ParsedAlias:
    pattern = raw.strip()
    is_prefix = pattern.endswith("*")
    normalized = normalize_alias(pattern[:-1] if is_prefix else pattern)
    if not normalized:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "Podaj nazwę z banku.")
    if is_prefix and len(normalized) < MIN_PREFIX_LENGTH:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"Przed gwiazdką potrzeba co najmniej {MIN_PREFIX_LENGTH} znaków.",
        )
    return ParsedAlias(pattern, normalized, PREFIX if is_prefix else EXACT)


class StoreMatcher:
    """Reguły importera: dokładny alias, potem najdłuższy prefiks z gwiazdką, inaczej nic."""

    def __init__(self, aliases: Iterable[tuple[StoreAlias, Store]]) -> None:
        self._exact: dict[str, Store] = {}
        prefixes: list[tuple[str, Store]] = []
        for alias, store in aliases:
            if alias.match_type == EXACT:
                self._exact[alias.normalized] = store
            else:
                prefixes.append((alias.normalized, store))
        self._prefixes = sorted(prefixes, key=lambda entry: len(entry[0]), reverse=True)

    def match(self, name: str | None) -> Store | None:
        if not name:
            return None
        normalized = normalize_alias(name)
        exact = self._exact.get(normalized)
        if exact is not None:
            return exact
        return next((s for prefix, s in self._prefixes if normalized.startswith(prefix)), None)


def load_matcher(db: Session, user_id: str) -> StoreMatcher:
    rows = db.execute(
        select(StoreAlias, Store)
        .join(Store, StoreAlias.store_id == Store.id)
        .where(
            StoreAlias.user_id == user_id,
            StoreAlias.is_ambiguous.is_(False),
            Store.is_deleted.is_(False),
        )
    )
    return StoreMatcher(rows.tuples().all())


def _resolve_ambiguity(db: Session, user_id: str | None, normalized: str) -> None:
    """Przelicza, kto ma nazwę: jedyny aktywny sklep, jedyny w ogóle, inaczej nikt."""
    rows = db.execute(
        select(StoreAlias, Store)
        .join(Store, StoreAlias.store_id == Store.id)
        .where(
            StoreAlias.user_id == user_id,
            StoreAlias.normalized == normalized,
            StoreAlias.match_type == EXACT,
        )
    ).all()
    active = [alias for alias, store in rows if not store.is_deleted]
    owner = active[0] if len(active) == 1 else rows[0][0] if len(rows) == 1 else None
    # Najpierw zdejmujemy właściciela, bo indeks unikalny sprawdza każdy zapis osobno.
    for alias, _ in rows:
        alias.is_ambiguous = True
    db.flush()
    if owner is not None:
        owner.is_ambiguous = False
        db.flush()


def register_store_name(db: Session, store: Store, source: str = "store_name") -> None:
    """Alias z nazwy sklepu. Bez niego importer nie znalazłby nowego sklepu."""
    db.flush()
    normalized = normalize_alias(store.name)
    exists = db.scalar(
        select(StoreAlias.id).where(
            StoreAlias.store_id == store.id,
            StoreAlias.normalized == normalized,
            StoreAlias.match_type == EXACT,
        )
    )
    if not exists:
        db.add(
            StoreAlias(
                user_id=store.user_id,
                store_id=store.id,
                pattern=store.name,
                normalized=normalized,
                match_type=EXACT,
                is_ambiguous=True,
                source=source,
            )
        )
    db.flush()
    _resolve_ambiguity(db, store.user_id, normalized)


def find_store_by_name(db: Session, user_id: str | None, name: str) -> Store | None:
    """Aktywny sklep, który ma już tę nazwę jako dokładny alias (po normalizacji).

    Gdy nazwę ma kilka sklepów, wygrywa właściciel aliasu, a bez niego najstarszy alias.
    """
    normalized = normalize_alias(name)
    if not normalized:
        return None
    return db.scalars(
        select(Store)
        .join(StoreAlias, StoreAlias.store_id == Store.id)
        .where(
            StoreAlias.user_id == user_id,
            StoreAlias.normalized == normalized,
            StoreAlias.match_type == EXACT,
            Store.is_deleted.is_(False),
            Store.merged_into_id.is_(None),
        )
        .order_by(StoreAlias.is_ambiguous, StoreAlias.created_at)
        .limit(1)
    ).first()


def exact_names_of(db: Session, store: Store) -> list[str]:
    return list(
        db.scalars(
            select(StoreAlias.normalized).where(
                StoreAlias.store_id == store.id, StoreAlias.match_type == EXACT
            )
        )
    )


def resolve_names(db: Session, user_id: str | None, names: Iterable[str]) -> None:
    """Po archiwizacji, przywróceniu lub usunięciu sklepu nazwa może zmienić właściciela."""
    db.flush()
    for normalized in names:
        _resolve_ambiguity(db, user_id, normalized)


def list_aliases(
    db: Session,
    user_id: str,
    store_ids: Sequence[str] | None = None,
    *,
    ambiguous_only: bool = False,
) -> list[StoreAlias]:
    stmt = select(StoreAlias).where(StoreAlias.user_id == user_id)
    if store_ids:
        stmt = stmt.where(StoreAlias.store_id.in_(store_ids))
    if ambiguous_only:
        stmt = stmt.where(StoreAlias.is_ambiguous.is_(True))
    return list(db.scalars(stmt.order_by(StoreAlias.created_at, StoreAlias.pattern)))


def _conflict(db: Session, user_id: str, parsed: ParsedAlias) -> tuple[Store, bool] | None:
    """Sklep, który już ma ten alias, i czy to alias niejednoznaczny."""
    row = db.execute(
        select(Store, StoreAlias.is_ambiguous)
        .join(StoreAlias, StoreAlias.store_id == Store.id)
        .where(
            StoreAlias.user_id == user_id,
            StoreAlias.normalized == parsed.normalized,
            StoreAlias.match_type == parsed.match_type,
        )
        .order_by(StoreAlias.is_ambiguous)
    ).first()
    return (row[0], row[1]) if row else None


def _conflict_message(store: Store, ambiguous: bool) -> str:
    label = (store.group_name or "").strip() or store.name
    if ambiguous:
        return f"Ta nazwa jest niejednoznaczna: ma ją kilka sklepów, m.in. „{label}”."
    return f"Ten alias należy już do sklepu „{label}”."


def add_alias(db: Session, user_id: str, store: Store, raw: str) -> StoreAlias:
    parsed = parse_alias(raw)
    conflict = _conflict(db, user_id, parsed)
    if conflict:
        raise HTTPException(status.HTTP_409_CONFLICT, _conflict_message(*conflict))
    alias = StoreAlias(
        user_id=user_id,
        store_id=store.id,
        pattern=parsed.pattern,
        normalized=parsed.normalized,
        match_type=parsed.match_type,
        source="manual",
    )
    db.add(alias)
    db.commit()
    db.refresh(alias)
    return alias


def remove_alias(db: Session, alias: StoreAlias) -> None:
    user_id, normalized, match_type = alias.user_id, alias.normalized, alias.match_type
    db.delete(alias)
    db.flush()
    if match_type == EXACT:
        _resolve_ambiguity(db, user_id, normalized)
    db.commit()


def preview_alias(
    db: Session, user_id: str, raw: str, store_ids: Sequence[str]
) -> dict[str, object]:
    """Ile dotychczasowych transakcji pasuje do aliasu.

    To tylko zapowiedź: alias działa na przyszłe importy i nie przenosi transakcji.
    Porównujemy z nazwą sklepu transakcji i nazwą z opisu z importu CSV.
    """
    parsed = parse_alias(raw)
    conflict = _conflict(db, user_id, parsed)

    def matches(text: str | None) -> bool:
        if not text:
            return False
        normalized = normalize_alias(text)
        if parsed.match_type == EXACT:
            return normalized == parsed.normalized
        return normalized.startswith(parsed.normalized)

    own = set(store_ids)
    total = other = 0
    rows = db.execute(
        select(Transaction.store_id, Store.name, Transaction.bank_description, func.count())
        .outerjoin(Store, Transaction.store_id == Store.id)
        .where(Transaction.user_id == user_id)
        .group_by(Transaction.store_id, Store.name, Transaction.bank_description)
    )
    for store_id, store_name, description, count in rows:
        payee = description.split(_DESCRIPTION_SEPARATOR)[0] if description else None
        if matches(store_name) or matches(payee):
            total += count
            if store_id is not None and store_id not in own:
                other += count
    return {
        "match_type": parsed.match_type,
        "matches": total,
        "matches_other_stores": other,
        "conflict": _conflict_message(*conflict) if conflict else None,
    }
