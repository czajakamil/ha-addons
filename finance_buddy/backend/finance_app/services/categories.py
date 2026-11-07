"""Logika ścieżek tagów.

Kategorie działają jak zagnieżdżone tagi z Obsidiana: tożsamością tagu jest
ścieżka („dom/remont/lazienka"). Źródłem prawdy pozostaje drzewo
(`parent_id` + `name`) — ścieżki są wyliczane, więc nie mogą się rozjechać,
a wszystkie miejsca tworzące kategorie (router, budżet, import YNAB)
pozostają spójne bez dodatkowej dyscypliny.

Projekt: docs/design/zagniezdzone-tagi.md
"""

from __future__ import annotations

import re

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import Category

_WHITESPACE = re.compile(r"\s+")

# Nazwa tagu przychodu, który zasila „Do przypisania" (flaga is_ready_to_assign).
# Taką nazwę dostaje YNAB-owe „Inflow: Ready to Assign" przy imporcie.
READY_TO_ASSIGN_TAG_NAME = "Przychód (Do przypisania)"


def normalize_segment(name: str) -> str:
    """Klucz porównawczy dla segmentu ścieżki (nazwy tagu).

    Służy do dopasowań bez rozróżniania wielkości liter. Nie używać do zapisu: zapisana nazwa ma
    zachować wielkość liter tak, jak wpisał ją użytkownik.
    """
    collapsed = _WHITESPACE.sub(" ", name.strip())
    return collapsed.replace("/", "-").lower()


def sanitize_segment(name: str) -> str:
    """Porządkuje segment do zapisu.

    Pojedyncze spacje, bez skrajnych białych znaków; '/' jest zarezerwowany jako
    separator, więc w nazwie zamieniamy go
    na '-'. Wielkość liter pozostaje taka, jaką wpisał użytkownik.
    """
    collapsed = _WHITESPACE.sub(" ", name.strip())
    return collapsed.replace("/", "-")


def split_path(path: str) -> list[str]:
    """Dzieli ścieżkę na segmenty (z zachowaną wielkością liter), pomijając puste (np. 'a//b')."""
    raw_segments = path.lstrip("#").split("/")
    segments = [sanitize_segment(segment) for segment in raw_segments]
    return [segment for segment in segments if segment]


def build_path_map(db: Session, user_id: str) -> dict[str, str]:
    """Zwraca mapę id → znormalizowana ścieżka dla wszystkich kategorii użytkownika."""
    categories = db.scalars(select(Category).where(Category.user_id == user_id)).all()
    by_id = {category.id: category for category in categories}
    paths: dict[str, str] = {}

    def resolve(category: Category) -> str:
        cached = paths.get(category.id)
        if cached is not None:
            return cached
        segment = sanitize_segment(category.name)
        parent = by_id.get(category.parent_id) if category.parent_id else None
        path = f"{resolve(parent)}/{segment}" if parent else segment
        paths[category.id] = path
        return path

    for category in categories:
        resolve(category)
    return paths


def category_path(db: Session, category: Category) -> str:
    """Ścieżka pojedynczej kategorii (wspina się po rodzicach)."""
    segments: list[str] = []
    current: Category | None = category
    visited: set[str] = set()
    while current is not None:
        if current.id in visited:  # obrona przed cyklem w uszkodzonych danych
            break
        visited.add(current.id)
        segments.append(sanitize_segment(current.name))
        current = db.get(Category, current.parent_id) if current.parent_id else None
    return "/".join(reversed(segments))


def find_child(db: Session, user_id: str, parent_id: str | None, name: str) -> Category | None:
    """Szuka dziecka o tej nazwie (bez rozróżniania wielkości liter)."""
    stmt = select(Category).where(
        Category.user_id == user_id,
        Category.parent_id == parent_id if parent_id else Category.parent_id.is_(None),
        func.lower(Category.name) == normalize_segment(name),
    )
    return db.scalar(stmt)


def ensure_path(
    db: Session,
    user_id: str,
    path: str,
    *,
    is_for_accounts: bool = False,
) -> Category:
    """Zwraca tag o podanej ścieżce, tworząc brakujące segmenty (jak w Obsidianie).

    Wpisanie „dom/remont/lazienka" tworzy po drodze „dom" i „dom/remont",
    jeśli nie istnieją. Dopasowanie segmentów nie rozróżnia wielkości liter.
    Flaga is_for_accounts trafia na każdy nowo tworzony segment ścieżki — cała
    gałąź tagu konta należy do przestrzeni kont. Gdyby flaga siedziała tylko na
    liściu, przodkowie (is_for_accounts=False) wypadaliby z widoku „Tagi kont"
    (filtrowanego po fladze), a liść stawał się sierotą bez rodzica w drzewie.
    """
    segments = split_path(path)
    if not segments:
        raise ValueError("Ścieżka kategorii nie może być pusta.")

    parent: Category | None = None
    node: Category | None = None
    for segment in segments:
        parent_id = parent.id if parent else None
        node = find_child(db, user_id, parent_id, segment)
        if node is None:
            node = Category(
                name=segment,
                parent_id=parent_id,
                is_for_accounts=is_for_accounts,
                user_id=user_id,
            )
            db.add(node)
            db.flush()
        parent = node

    assert node is not None  # noqa: S101 — zawężenie typu; pętla zawsze ustawia node
    return node


def descendant_ids(db: Session, user_id: str, root_id: str) -> list[str]:
    """Id wszystkich potomków (bez korzenia) — do rollupów i potwierdzeń usunięcia."""
    children_by_parent: dict[str | None, list[str]] = {}
    rows = db.execute(
        select(Category.id, Category.parent_id).where(Category.user_id == user_id)
    ).all()
    for category_id, parent_id in rows:
        children_by_parent.setdefault(parent_id, []).append(category_id)

    result: list[str] = []
    queue = list(children_by_parent.get(root_id, []))
    while queue:
        current = queue.pop()
        result.append(current)
        queue.extend(children_by_parent.get(current, []))
    return result


def is_descendant_or_self(db: Session, user_id: str, candidate_id: str, root_id: str) -> bool:
    """Sprawdza, czy candidate_id leży w pod-drzewie root_id (łącznie z nim samym).

    Używane przy przenoszeniu tagu, żeby nie dało się wpiąć gałęzi w samą siebie.
    """
    if candidate_id == root_id:
        return True
    return candidate_id in set(descendant_ids(db, user_id, root_id))
