"""Scalanie sklepów i jego cofanie.

Transakcje, paragony i aliasy przechodzą do sklepu docelowego, a źródło zostaje jako
ukryte przekierowanie (`merged_into_id`), bo import YNAB używa id payee jako id sklepu.
Łańcuchy są spłaszczane: każde przekierowanie wskazuje wprost sklep docelowy.

Cofnięcie działa przez 30 dni i tylko od końca: późniejsze scalenie tego samego sklepu
trzeba cofnąć najpierw, bo opiera się na stanie, który cofnięcie by zmieniło.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from fastapi import HTTPException, status
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..dependencies import _get_owned_or_404
from ..models import (
    Category,
    ProductAlias,
    ReceiptDraft,
    Store,
    StoreAlias,
    StoreMerge,
    Transaction,
    utcnow,
)
from .store_aliases import EXACT, resolve_names

UNDO_WINDOW = dt.timedelta(days=30)
_STORE_FIELDS = (
    "group_name",
    "automatic_category_id",
    "automatically_categorize_payee",
    "nip",
    "address",
    "is_deleted",
    "merged_into_id",
)


def resolve_merged_store(db: Session, store: Store) -> Store:
    """Sklep docelowy dla scalonego. Odporne na łańcuchy i cykle w starych danych."""
    seen = {store.id}
    current = store
    while current.merged_into_id and current.merged_into_id not in seen:
        following = db.get(Store, current.merged_into_id)
        if following is None:
            break
        seen.add(following.id)
        current = following
    return current


def _label(store: Store) -> str:
    return (store.group_name or "").strip() or store.name


def _load(
    db: Session, user_id: str, source_ids: Sequence[str], target_id: str
) -> tuple[Store, list[Store]]:
    unique_ids = list(dict.fromkeys(source_ids))
    if target_id in unique_ids:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "Sklep docelowy nie może być też scalany."
        )
    target = _get_owned_or_404(db, Store, target_id, user_id, "Store")
    return target, [_get_owned_or_404(db, Store, sid, user_id, "Store") for sid in unique_ids]


def _conflicts(db: Session, target: Store, sources: list[Store]) -> list[dict[str, object]]:
    conflicts: list[dict[str, object]] = [
        {
            "kind": "already_merged",
            "message": f"Sklep „{_label(store)}” został już scalony.",
            "blocking": True,
        }
        for store in [target, *sources]
        if store.merged_into_id
    ]

    ids = [target.id, *(store.id for store in sources)]
    duplicates = db.execute(
        select(Transaction.date, Transaction.receipt_number)
        .where(Transaction.store_id.in_(ids), Transaction.receipt_number.is_not(None))
        .group_by(Transaction.date, Transaction.receipt_number)
        .having(func.count(Transaction.id) > 1)
    ).all()
    conflicts.extend(
        {
            "kind": "receipt",
            "message": (
                f"Paragon nr {number} z {day:%d.%m.%Y} jest w obu sklepach. "
                "Usuń duplikat albo zmień jego numer, zanim scalisz."
            ),
            "blocking": True,
        }
        for day, number in duplicates
    )

    nips = {store.nip for store in [target, *sources] if store.nip}
    if len(nips) > 1:
        conflicts.append(
            {
                "kind": "nip",
                "message": (
                    f"Sklepy mają różne NIP-y ({', '.join(sorted(nips))}). Każdy dalej rozpozna "
                    "paragony tego sklepu, ale na stronie sklepu widać tylko NIP docelowego."
                ),
                "blocking": False,
            }
        )

    target_products = set(
        db.scalars(select(ProductAlias.alias).where(ProductAlias.store_id == target.id))
    )
    source_ids = [store.id for store in sources]
    repeated = sum(
        alias in target_products
        for alias in db.scalars(
            select(ProductAlias.alias).where(ProductAlias.store_id.in_(source_ids))
        )
    )
    if repeated:
        conflicts.append(
            {
                "kind": "product_alias",
                "message": (
                    f"Nazwy produktów ({repeated}) są już przypisane w sklepie docelowym; "
                    "zostaną jego ustawienia."
                ),
                "blocking": False,
            }
        )
    return conflicts


def preview_merge(
    db: Session, user_id: str, source_ids: Sequence[str], target_id: str
) -> dict[str, object]:
    target, sources = _load(db, user_id, source_ids, target_id)
    ids = [store.id for store in sources]
    transactions = (
        db.scalar(select(func.count(Transaction.id)).where(Transaction.store_id.in_(ids))) or 0
    )
    receipts = (
        db.scalar(
            select(func.count(Transaction.id)).where(
                Transaction.store_id.in_(ids), Transaction.receipt_number.is_not(None)
            )
        )
        or 0
    )
    drafts = (
        db.scalar(select(func.count(ReceiptDraft.id)).where(ReceiptDraft.matched_store_id.in_(ids)))
        or 0
    )
    aliases = db.scalar(select(func.count(StoreAlias.id)).where(StoreAlias.store_id.in_(ids))) or 0
    conflicts = _conflicts(db, target, sources)
    return {
        "transactions": transactions,
        "receipts": receipts + drafts,
        "aliases": aliases,
        "conflicts": conflicts,
        "can_merge": not any(item["blocking"] for item in conflicts),
    }


def _store_snapshot(store: Store) -> dict[str, object]:
    return {"id": store.id, **{field: getattr(store, field) for field in _STORE_FIELDS}}


def _alias_snapshot(alias: StoreAlias) -> dict[str, object]:
    return {
        "id": alias.id,
        "store_id": alias.store_id,
        "pattern": alias.pattern,
        "normalized": alias.normalized,
        "match_type": alias.match_type,
        "source": alias.source,
    }


def _commit(db: Session, message: str) -> None:
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, message) from exc


def merge_stores(
    db: Session,
    user_id: str,
    source_ids: Sequence[str],
    target_id: str,
    *,
    name: str | None = None,
    category_id: str | None = None,
) -> dict[str, object]:
    target, sources = _load(db, user_id, source_ids, target_id)
    blocking = [item for item in _conflicts(db, target, sources) if item["blocking"]]
    if blocking:
        # Okno scalania pokazuje to wcześniej z dry_run; to zabezpieczenie na wyścig.
        raise HTTPException(status.HTTP_409_CONFLICT, str(blocking[0]["message"]))

    ids = [store.id for store in sources]
    tx_ids = list(db.scalars(select(Transaction.id).where(Transaction.store_id.in_(ids))))
    draft_ids = list(
        db.scalars(select(ReceiptDraft.id).where(ReceiptDraft.matched_store_id.in_(ids)))
    )
    aliases = list(db.scalars(select(StoreAlias).where(StoreAlias.store_id.in_([*ids, target.id]))))
    tx_by_source: dict[str, list[str]] = {
        sid: list(db.scalars(select(Transaction.id).where(Transaction.store_id == sid)))
        for sid in ids
    }
    draft_by_source = {
        sid: list(db.scalars(select(ReceiptDraft.id).where(ReceiptDraft.matched_store_id == sid)))
        for sid in ids
    }
    target_products = set(
        db.scalars(select(ProductAlias.alias).where(ProductAlias.store_id == target.id))
    )
    moved_products: dict[str, str] = {}
    for alias in db.scalars(select(ProductAlias).where(ProductAlias.store_id.in_(ids))):
        # Powtórzona nazwa zostaje przy ukrytym źródle: docelowy ma już swoją.
        if alias.alias not in target_products:
            target_products.add(alias.alias)
            moved_products[alias.id] = alias.store_id
            alias.store_id = target.id
    flattened = {
        store.id: store.merged_into_id
        for store in db.scalars(select(Store).where(Store.merged_into_id.in_(ids)))
    }
    payload = {
        "target": _store_snapshot(target),
        "sources": [_store_snapshot(store) for store in sources],
        "source_names": [store.name for store in sources],
        "transactions": tx_by_source,
        "drafts": draft_by_source,
        "aliases": [_alias_snapshot(alias) for alias in aliases],
        "product_aliases": moved_products,
        "flattened": flattened,
    }

    db.execute(update(Transaction).where(Transaction.id.in_(tx_ids)).values(store_id=target.id))
    db.execute(
        update(ReceiptDraft)
        .where(ReceiptDraft.id.in_(draft_ids))
        .values(matched_store_id=target.id)
    )
    db.execute(update(Store).where(Store.merged_into_id.in_(ids)).values(merged_into_id=target.id))

    held = {(a.normalized, a.match_type) for a in aliases if a.store_id == target.id}
    names = {a.normalized for a in aliases if a.match_type == EXACT}
    for alias in aliases:
        if alias.store_id == target.id:
            continue
        key = (alias.normalized, alias.match_type)
        if key in held:
            db.delete(alias)
        else:
            held.add(key)
            alias.store_id = target.id

    # NIP przechodzi tylko, gdy jest jednoznaczny; inne zostają na źródłach i dalej
    # rozpoznają paragony. Najpierw czyścimy źródło, bo NIP jest unikalny.
    source_nips = {store.nip for store in sources if store.nip}
    moved_nip = source_nips.pop() if not target.nip and len(source_nips) == 1 else None
    for store in sources:
        if moved_nip and store.nip == moved_nip:
            store.nip = None
        store.is_deleted = True
        store.merged_into_id = target.id
    db.flush()
    if moved_nip:
        target.nip = moved_nip
    if not target.address:
        target.address = next((store.address for store in sources if store.address), None)
    if name:
        target.group_name = name.strip()
    if category_id:
        _get_owned_or_404(db, Category, category_id, user_id, "Category")
        target.automatic_category_id = category_id
        target.automatically_categorize_payee = True
    db.flush()
    resolve_names(db, user_id, names)

    merge = StoreMerge(user_id=user_id, target_store_id=target.id, payload=payload)
    db.add(merge)
    _commit(db, "Nie udało się scalić sklepów: dane zmieniły się w trakcie.")
    return {
        "merge_id": merge.id,
        "target_id": target.id,
        "transactions": len(tx_ids),
        "expires_at": merge.created_at + UNDO_WINDOW,
    }


def list_merges(db: Session, user_id: str, store_ids: Sequence[str]) -> list[dict[str, object]]:
    """Scalenia, które można jeszcze cofnąć, od najnowszego."""
    since = utcnow() - UNDO_WINDOW
    merges = db.scalars(
        select(StoreMerge)
        .where(
            StoreMerge.user_id == user_id,
            StoreMerge.target_store_id.in_(store_ids),
            StoreMerge.undone_at.is_(None),
            StoreMerge.created_at >= since,
        )
        .order_by(StoreMerge.created_at.desc())
    )
    return [
        {
            "id": merge.id,
            "target_store_id": merge.target_store_id,
            "source_names": merge.payload["source_names"],
            "transactions": sum(len(ids) for ids in merge.payload["transactions"].values()),
            "created_at": merge.created_at,
            "expires_at": merge.created_at + UNDO_WINDOW,
        }
        for merge in merges
    ]


def _restore_store(store: Store, snapshot: dict[str, object], *, nip: bool) -> None:
    for field in _STORE_FIELDS:
        if (field == "nip") == nip:
            setattr(store, field, snapshot[field])


def undo_merge(db: Session, user_id: str, merge_id: str) -> dict[str, int]:
    merge = _get_owned_or_404(db, StoreMerge, merge_id, user_id, "Store merge")
    if merge.undone_at is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "To scalenie zostało już cofnięte.")
    if utcnow() - merge.created_at > UNDO_WINDOW:
        raise HTTPException(status.HTTP_409_CONFLICT, "Minęło 30 dni, scalenia nie można cofnąć.")
    target = db.get(Store, merge.target_store_id)
    if target is None or target.merged_into_id:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Sklep docelowy został później scalony. Najpierw cofnij tamto scalenie.",
        )
    later = db.scalar(
        select(StoreMerge.id).where(
            StoreMerge.target_store_id == target.id,
            StoreMerge.undone_at.is_(None),
            StoreMerge.created_at > merge.created_at,
        )
    )
    if later:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Ten sklep scalono później ponownie. Najpierw cofnij tamto."
        )
    payload = merge.payload
    sources = {item["id"]: db.get(Store, item["id"]) for item in payload["sources"]}
    if not all(sources.values()):
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Jeden ze scalonych sklepów już nie istnieje."
        )

    restored = kept = 0
    for source_id, tx_ids in payload["transactions"].items():
        # Tylko te, które nadal są w sklepie docelowym: inne ktoś świadomie przeniósł.
        moved = db.execute(
            update(Transaction)
            .where(Transaction.id.in_(tx_ids), Transaction.store_id == target.id)
            .values(store_id=source_id)
        ).rowcount
        restored += moved
        kept += len(tx_ids) - moved
    for source_id, draft_ids in payload["drafts"].items():
        db.execute(
            update(ReceiptDraft)
            .where(ReceiptDraft.id.in_(draft_ids), ReceiptDraft.matched_store_id == target.id)
            .values(matched_store_id=source_id)
        )
    for alias_id, source_id in payload["product_aliases"].items():
        alias = db.get(ProductAlias, alias_id)
        if alias is not None and alias.store_id == target.id:
            alias.store_id = source_id

    # NIP jest unikalny: najpierw zwalniamy go w sklepie docelowym.
    _restore_store(target, payload["target"], nip=True)
    db.flush()
    for item in payload["sources"]:
        _restore_store(sources[item["id"]], item, nip=True)
        _restore_store(sources[item["id"]], item, nip=False)
    _restore_store(target, payload["target"], nip=False)
    for store_id, previous in payload["flattened"].items():
        chained = db.get(Store, store_id)
        if chained is not None and chained.merged_into_id == target.id:
            chained.merged_into_id = previous
    db.flush()

    names = set()
    for item in payload["aliases"]:
        alias = db.get(StoreAlias, item["id"])
        exact = item["match_type"] == EXACT
        if exact:
            names.add(item["normalized"])
        if alias is None:
            # Prefiks zajęty po scaleniu przez inny alias zostaje przy tamtym.
            taken = not exact and db.scalar(
                select(StoreAlias.id).where(
                    StoreAlias.user_id == user_id,
                    StoreAlias.normalized == item["normalized"],
                    StoreAlias.match_type == item["match_type"],
                )
            )
            if taken:
                continue
            alias = StoreAlias(
                id=item["id"],
                user_id=user_id,
                is_ambiguous=exact,
                **{
                    field: item[field]
                    for field in ("store_id", "pattern", "normalized", "match_type", "source")
                },
            )
            db.add(alias)
        else:
            alias.store_id = item["store_id"]
            alias.source = item["source"]
            if exact:
                alias.is_ambiguous = True
    db.flush()
    resolve_names(db, user_id, names)

    merge.undone_at = utcnow()
    _commit(db, "Nie udało się cofnąć scalenia: dane sklepu zmieniły się po scaleniu.")
    return {"restored_transactions": restored, "kept_transactions": kept}
