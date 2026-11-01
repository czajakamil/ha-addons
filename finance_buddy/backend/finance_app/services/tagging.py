"""Kolejka „do otagowania": transakcje bez tagu i podpowiedź tagu dla każdej z nich.

Do kolejki trafia transakcja bez tagu na koncie budżetowym, która nie jest
przelewem między własnymi kontami ani korektą salda. Konta śledzone (poza
budżetem) pomijamy celowo: YNAB w ogóle nie pozwala ich kategoryzować, budżet
ich nie liczy, a ich „bez tagu" to zmiany wyceny i salda otwarcia, a nie
zaległość do nadrobienia.

Podpowiedź to kaskada, od najmocniejszego dowodu:

1. reguła sklepu — `automatic_category_id`, jak w podpowiedzi przy paragonie
   (ui/src/pages/receipts/tagSuggestion.ts; tamta działa na pozycjach szkicu
   w przeglądarce, ta potrzebuje historii całej bazy, więc mieszka tutaj),
2. tag najczęściej nadawany transakcjom z tego samego sklepu,
3. to samo dla sieci (`Store.group_name` łączy placówki jednej sieci),
4. podobne opisy — nazwa sklepu albo komentarz zbliżony do już otagowanych
   („Rossmann SDP SP, Z 0.0" ~ „Rossmann").

Historię liczymy raz na żądanie — to kilka tysięcy wierszy, a nie powód do
osobnej tabeli, która mogłaby rozjechać się z transakcjami.
"""

from __future__ import annotations

import datetime as dt
import re
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from rapidfuzz import fuzz, process
from sqlalchemy import Select, func, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql._typing import _ColumnsClauseArgument

from ..dependencies import _get_owned_or_404
from ..helpers import _is_internal_label
from ..models import Account, Category, Store, Transaction

SOURCE_STORE_RULE = "store_rule"
SOURCE_STORE = "store"
SOURCE_STORE_GROUP = "store_group"
SOURCE_SIMILAR = "similar"

# Próg podobieństwa opisów (rapidfuzz token_set_ratio, 0–100). token_set daje
# 100, gdy krótsza etykieta zawiera się w dłuższej („rossmann" w „rossmann
# sdp") — to jest właśnie przypadek, o który chodzi, a 90 przepuszcza jeszcze
# literówkę w jednym słowie, odcinając etykiety, które dzielą tylko część słów.
SIMILAR_THRESHOLD = 90.0
SIMILAR_CANDIDATES = 20
# Krótsze etykiety („pl", „sp") pasowałyby do wszystkiego, co je zawiera.
MIN_LABEL_LENGTH = 4

_NON_WORD = re.compile(r"[^\w]+")
_HAS_DIGIT = re.compile(r"\d")


@dataclass(frozen=True)
class TagSuggestion:
    category_id: str
    source: str
    # Ile otagowanych transakcji stoi za podpowiedzią (0 dla reguły sklepu).
    matches: int = 0
    # Dla „similar": opis najbliższej otagowanej transakcji — do pokazania w UI.
    similar_to: str | None = None


def normalize_label(raw: str | None) -> str:
    """Opis transakcji jako zestaw słów do porównań.

    Bez polskich znaków (bank pisze „Zabka", paragon „Żabka"), bez interpunkcji
    i bez słów z cyframi — to numery placówek i terminali („ZD908", „0.0"),
    które różnią każdą transakcję z tej samej sieci.
    """
    if not raw:
        return ""
    text = raw.lower().replace("ł", "l")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    words = [
        word
        for word in _NON_WORD.sub(" ", text).replace("_", " ").split()
        if len(word) > 1 and not _HAS_DIGIT.search(word)
    ]
    return " ".join(words)


class _Votes:
    """Licznik tagów z rozstrzyganiem remisów na korzyść ostatnio używanego."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.last_used: dict[str, dt.date] = {}

    def add(self, category_id: str, when: dt.date, weight: int = 1) -> None:
        self.counts[category_id] += weight
        if when > self.last_used.get(category_id, dt.date.min):
            self.last_used[category_id] = when

    def winner(self) -> tuple[str, int] | None:
        if not self.counts:
            return None
        category_id = max(
            self.counts,
            key=lambda cid: (self.counts[cid], self.last_used.get(cid, dt.date.min)),
        )
        return category_id, self.counts[category_id]


class TagHistory:
    """Dotychczasowe tagowanie użytkownika, zindeksowane pod podpowiedzi."""

    def __init__(self) -> None:
        self.by_store: defaultdict[str, _Votes] = defaultdict(_Votes)
        self.by_group: defaultdict[str, _Votes] = defaultdict(_Votes)
        self.by_label: defaultdict[str, _Votes] = defaultdict(_Votes)
        # Znormalizowana etykieta → pierwszy oryginalny zapis, do pokazania w UI.
        self.display: dict[str, str] = {}

    @classmethod
    def load(cls, db: Session, user_id: str) -> TagHistory:
        history = cls()
        rows = db.execute(
            select(
                Transaction.store_id,
                Store.name,
                Store.group_name,
                Transaction.comment,
                Transaction.category_id,
                Transaction.date,
            )
            .outerjoin(Store, Transaction.store_id == Store.id)
            .where(
                Transaction.user_id == user_id,
                Transaction.category_id.is_not(None),
                Transaction.transfer_account_id.is_(None),
                Transaction.transfer_transaction_id.is_(None),
            )
        ).all()
        for store_id, store_name, group_name, comment, category_id, when in rows:
            if store_id:
                history.by_store[store_id].add(category_id, when)
            group = normalize_label(group_name)
            if group:
                history.by_group[group].add(category_id, when)
            for raw in {store_name, comment}:
                label = normalize_label(raw)
                if len(label) < MIN_LABEL_LENGTH:
                    continue
                history.by_label[label].add(category_id, when)
                history.display.setdefault(label, (raw or "").strip())
        return history

    def similar(self, *raw_labels: str | None) -> TagSuggestion | None:
        """Głosowanie podobnych opisów: liczba transakcji × podobieństwo."""
        weights: defaultdict[str, float] = defaultdict(float)
        counts: Counter[str] = Counter()
        closest: dict[str, tuple[float, str]] = {}
        keys = list(self.by_label)
        seen: set[str] = set()
        for raw in raw_labels:
            query = normalize_label(raw)
            if len(query) < MIN_LABEL_LENGTH or query in seen:
                continue
            seen.add(query)
            for key, score, _ in process.extract(
                query,
                keys,
                scorer=fuzz.token_set_ratio,
                score_cutoff=SIMILAR_THRESHOLD,
                limit=SIMILAR_CANDIDATES,
            ):
                for category_id, count in self.by_label[key].counts.items():
                    weights[category_id] += count * score / 100
                    counts[category_id] += count
                    if score > closest.get(category_id, (0.0, ""))[0]:
                        closest[category_id] = (score, key)
        if not weights:
            return None
        category_id = max(weights, key=lambda cid: (weights[cid], counts[cid]))
        return TagSuggestion(
            category_id=category_id,
            source=SOURCE_SIMILAR,
            matches=counts[category_id],
            similar_to=self.display.get(closest[category_id][1]),
        )

    def suggest(self, store: Store | None, comment: str | None) -> TagSuggestion | None:
        if store is not None:
            if store.automatically_categorize_payee and store.automatic_category_id:
                return TagSuggestion(store.automatic_category_id, SOURCE_STORE_RULE)
            votes = self.by_store.get(store.id)
            winner = votes.winner() if votes else None
            if winner:
                return TagSuggestion(winner[0], SOURCE_STORE, matches=winner[1])
            group = normalize_label(store.group_name)
            votes = self.by_group.get(group) if group else None
            winner = votes.winner() if votes else None
            if winner:
                return TagSuggestion(winner[0], SOURCE_STORE_GROUP, matches=winner[1])
        return self.similar(store.name if store else None, comment)


# --- Kolejka ---------------------------------------------------------------


def _queue_select(columns: Sequence[_ColumnsClauseArgument[Any]], user_id: str) -> Select[Any]:
    """SELECT po transakcjach z kolejki; joiny są potrzebne regułom wykluczeń."""
    return (
        select(*columns)
        .select_from(Transaction)
        .join(Account, Transaction.account_id == Account.id)
        .outerjoin(Store, Transaction.store_id == Store.id)
        .outerjoin(Category, Transaction.category_id == Category.id)
        .where(
            Transaction.user_id == user_id,
            Transaction.category_id.is_(None),
            Transaction.transfer_account_id.is_(None),
            Transaction.transfer_transaction_id.is_(None),
            Account.is_on_budget.is_(True),
            ~_is_internal_label(),
        )
    )


def count_untagged(db: Session, user_id: str, store_id: str | None = None) -> int:
    stmt = _queue_select([func.count(Transaction.id)], user_id)
    if store_id is not None:
        stmt = stmt.where(Transaction.store_id == store_id)
    return db.scalar(stmt) or 0


def untagged_among(db: Session, user_id: str, transaction_ids: Sequence[str]) -> set[str]:
    """Które z podanych transakcji należą do kolejki — ta sama reguła co licznik."""
    if not transaction_ids:
        return set()
    stmt = _queue_select([Transaction.id], user_id).where(Transaction.id.in_(transaction_ids))
    return set(db.scalars(stmt).all())


@dataclass
class QueueEntry:
    transaction: Transaction
    account: Account
    store: Store | None
    store_untagged_count: int
    suggestion: TagSuggestion | None


def list_untagged(
    db: Session, user_id: str, *, offset: int = 0, limit: int = 50
) -> tuple[int, list[QueueEntry]]:
    """Strona kolejki (najnowsze najpierw) z podpowiedzią dla każdej pozycji."""
    total = count_untagged(db, user_id)
    rows = db.execute(
        _queue_select([Transaction, Account, Store], user_id)
        # `id` na końcu: stała kolejność przy tej samej dacie, inaczej
        # stronicowanie gubiłoby i dublowało pozycje.
        .order_by(Transaction.date.desc(), Transaction.recorded_at.desc(), Transaction.id)
        .offset(offset)
        .limit(limit)
    ).all()
    if not rows:
        return total, []

    store_ids = {store.id for _, _, store in rows if store is not None}
    per_store: dict[str, int] = {}
    if store_ids:
        grouped = db.execute(
            _queue_select([Transaction.store_id, func.count(Transaction.id)], user_id)
            .where(Transaction.store_id.in_(store_ids))
            .group_by(Transaction.store_id)
        ).all()
        per_store = {row[0]: row[1] for row in grouped}

    history = TagHistory.load(db, user_id)
    entries = [
        QueueEntry(
            transaction=tx,
            account=account,
            store=store,
            store_untagged_count=per_store.get(store.id, 0) if store else 0,
            suggestion=history.suggest(store, tx.comment),
        )
        for tx, account, store in rows
    ]
    return total, entries


def tag_store_queue(db: Session, user_id: str, store_id: str, category_id: str) -> int:
    """Taguje wszystkie transakcje sklepu, które są w kolejce. Zwraca ich liczbę.

    Tylko te z kolejki — przelewy, konta śledzone i transakcje z już nadanym
    tagiem zostają nietknięte, więc liczba z podglądu (`count_untagged`) to
    dokładnie to, co się zmieni.
    """
    _get_owned_or_404(db, Store, store_id, user_id, "Store")
    _get_owned_or_404(db, Category, category_id, user_id, "Category")
    ids = list(
        db.scalars(_queue_select([Transaction.id], user_id).where(Transaction.store_id == store_id))
    )
    if ids:
        db.execute(
            update(Transaction)
            .where(Transaction.id.in_(ids))
            .values(category_id=category_id)
            .execution_options(synchronize_session=False)
        )
        db.commit()
    return len(ids)
