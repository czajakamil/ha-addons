"""Import wyciągu CSV: rozpoznanie pliku, podgląd z deduplikacją, zapis, cofnięcie.

Bezstanowo: plik przychodzi przy każdym kroku, a zapis parsuje go i przepuszcza
przez deduplikację od nowa, zamiast ufać statusom z podglądu. Projekt:
docs/design/import-csv.md §5–7.

Trzy sita, pierwsze trafienie wygrywa:
1. `already_imported` — ten sam `external_id` na tym koncie (ponowny import,
   nakładające się wyciągi),
2. `match` — ta sama transakcja z innego źródła (YNAB, paragon, ręczna):
   domyślnie scalamy, czyli dopisujemy jej tożsamość banku zamiast tworzyć nową,
3. `transfer` — druga połówka przelewu na innym własnym koncie.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter
from dataclasses import dataclass, field
from typing import Literal, cast

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from ..dependencies import _get_owned_or_404
from ..models import (
    Account,
    BankImportBatch,
    BankImportProfile,
    Transaction,
    TransactionAttachment,
    TransactionItem,
    utcnow,
)
from ..schemas import (
    BankCommitRequest,
    BankImportBatchRead,
    BankImportColumn,
    BankImportProfileRead,
    BankImportProfileUpdate,
    BankImportSpec,
    BankImportUndoResult,
    BankInspectResponse,
    BankMapping,
    BankMatchInfo,
    BankParseOptions,
    BankPreviewResponse,
    BankPreviewRow,
    BankPreviousImport,
    BankRowAction,
    BankRowDecision,
    BankRowStatus,
)
from . import bank_csv
from . import transaction_matching as matching
from .store_aliases import StoreMatcher, load_matcher

SAMPLE_COUNT = 5
_IN_CHUNK = 500

STATUS_NEW = "new"
STATUS_ALREADY = "already_imported"
STATUS_MATCH = "match"
STATUS_TRANSFER = "transfer"
STATUS_OTHER_CURRENCY = "other_currency"
STATUS_SKIPPED = "skipped"
STATUS_INVALID = "invalid"

# Statusy, których żadna decyzja nie zamieni w zapis.
_NEVER_IMPORTED = frozenset({STATUS_ALREADY, STATUS_OTHER_CURRENCY, STATUS_SKIPPED, STATUS_INVALID})
_DEFAULT_ACTION: dict[str, BankRowAction] = {
    STATUS_NEW: "create",
    STATUS_MATCH: "merge",
    STATUS_TRANSFER: "transfer",
}


def _bad_request(detail: object) -> HTTPException:
    return HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=detail)


# --- Plik → tabela --------------------------------------------------------------


def _read_table(content: bytes, options: BankParseOptions) -> bank_csv.CsvTable:
    try:
        return bank_csv.read_table(content, options.model_dump(exclude_none=True))
    except bank_csv.BankCsvError as exc:
        raise _bad_request(str(exc)) from exc


def _previous_import(db: Session, user_id: str, sha256: str) -> BankPreviousImport | None:
    batch = db.scalars(
        select(BankImportBatch)
        .options(selectinload(BankImportBatch.account))
        .where(BankImportBatch.user_id == user_id, BankImportBatch.file_sha256 == sha256)
        .order_by(BankImportBatch.created_at.desc())
    ).first()
    if not batch:
        return None
    return BankPreviousImport(
        batch_id=batch.id,
        created_at=batch.created_at,
        account_id=batch.account_id,
        account_name=batch.account.name,
    )


def _match_profile(
    db: Session, user_id: str, header: list[str]
) -> tuple[BankImportProfile | None, str | None, list[int]]:
    """Profil dla nagłówka: pełne dopasowanie po odcisku albo częściowe po nazwach.

    Częściowe = co najmniej połowa zmapowanych kolumn ma tę samą nazwę pod tym
    samym indeksem. Wtedy zwracamy też kolumny, które się nie zgadzają, żeby UI
    kazało sprawdzić mapowanie, zamiast po cichu przyjąć złe kolumny.
    """
    profiles = list(
        db.scalars(
            select(BankImportProfile)
            .where(BankImportProfile.user_id == user_id)
            .order_by(BankImportProfile.last_used_at.desc().nulls_last())
        )
    )
    signature = bank_csv.header_signature(header)
    for profile in profiles:
        if profile.header_signature == signature:
            return profile, "full", []

    normalized = [bank_csv.normalize_header_name(h) for h in header]
    best: tuple[float, BankImportProfile | None, list[int]] = (0.0, None, [])
    for profile in profiles:
        names = (profile.mapping or {}).get("header_names") or {}
        if not names:
            continue
        mismatched = [
            int(i)
            for i, name in names.items()
            if int(i) >= len(normalized)
            or normalized[int(i)] != bank_csv.normalize_header_name(name)
        ]
        score = 1 - len(mismatched) / len(names)
        if score >= 0.5 and score > best[0]:
            best = (score, profile, sorted(mismatched))
    if best[1] is not None:
        return best[1], "partial", best[2]
    return None, None, []


def serialize_profile(profile: BankImportProfile) -> BankImportProfileRead:
    return BankImportProfileRead(
        id=profile.id,
        name=profile.name,
        account_id=profile.account_id,
        parse_options=BankParseOptions(**(profile.parse_options or {})),
        mapping=BankMapping(**(profile.mapping or {})),
        last_used_at=profile.last_used_at,
    )


def inspect(
    db: Session, user_id: str, content: bytes, options: BankParseOptions
) -> BankInspectResponse:
    """Kolumny pliku z próbkami, wykryte ustawienia i pasujący profil.

    Bez narzuconych ustawień, gdy profil pasuje, czytamy plik jeszcze raz jego
    ustawieniami — profil mógł wymusić np. kodowanie, którego wykrywanie nie
    wybrałoby samo.
    """
    table = _read_table(content, options)
    profile, match, mismatched = _match_profile(db, user_id, table.header)
    if profile and match == "full" and not options.model_dump(exclude_none=True):
        stored = BankParseOptions(**(profile.parse_options or {}))
        table = _read_table(
            content,
            BankParseOptions(
                encoding=stored.encoding,
                delimiter=stored.delimiter,
                header_row=stored.header_row,
            ),
        )

    columns = []
    for index, name in enumerate(table.header):
        values = [v for v in table.column_values(index) if v]
        columns.append(BankImportColumn(index=index, name=name, samples=values[:SAMPLE_COUNT]))

    resolved = table.options
    if profile:
        stored = BankParseOptions(**(profile.parse_options or {}))
        resolved = bank_csv.ParseOptions(
            encoding=resolved.encoding,
            delimiter=resolved.delimiter,
            header_row=resolved.header_row,
            date_format=options.date_format or stored.date_format,
            decimal_separator=options.decimal_separator or stored.decimal_separator,
        )

    return BankInspectResponse(
        options=BankParseOptions(**resolved.as_dict()),
        columns=columns,
        row_count=len(table.rows),
        file_sha256=table.sha256,
        profile=serialize_profile(profile) if profile else None,
        profile_match=cast(Literal["full", "partial"] | None, match),
        profile_mismatched_columns=mismatched,
        previous_import=_previous_import(db, user_id, table.sha256),
    )


# --- Analiza (wspólna dla podglądu i zapisu) ------------------------------------


def _day_and_cents(parsed: bank_csv.ParsedRow) -> tuple[dt.date, int]:
    """Data i kwota wiersza, który parser uznał za poprawny (obie są wtedy pewne)."""
    if parsed.date is None or parsed.amount_cents is None:
        raise ValueError(f"Wiersz {parsed.line} bez daty lub kwoty dotarł do zapisu.")
    return parsed.date, parsed.amount_cents


@dataclass
class AnalyzedRow:
    parsed: bank_csv.ParsedRow
    status: str
    reason: str | None = None
    match: Transaction | None = None


@dataclass
class Analysis:
    account: Account
    table: bank_csv.CsvTable
    options: bank_csv.ParseOptions
    mapping: bank_csv.Mapping
    rows: list[AnalyzedRow] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return dict(Counter(r.status for r in self.rows))


def _existing_external_ids(db: Session, account_id: str, ids: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    for start in range(0, len(ids), _IN_CHUNK):
        chunk = ids[start : start + _IN_CHUNK]
        rows = db.execute(
            select(Transaction.id, Transaction.external_id).where(
                Transaction.account_id == account_id, Transaction.external_id.in_(chunk)
            )
        )
        found.update({ext: tx_id for tx_id, ext in rows})
    return found


def analyze(db: Session, user_id: str, content: bytes, spec: BankImportSpec) -> Analysis:
    account = _get_owned_or_404(db, Account, spec.account_id, user_id, "Account")
    table = _read_table(content, spec.options)
    mapping = bank_csv.Mapping.from_dict(spec.mapping.model_dump())
    errors = mapping.errors(table.width)
    if errors:
        raise _bad_request(errors)
    options = bank_csv.resolve_value_formats(table, mapping)
    analysis = Analysis(account=account, table=table, options=options, mapping=mapping)
    analysis.rows = [
        AnalyzedRow(parsed=parsed, status=parsed.status, reason=parsed.reason)
        for parsed in bank_csv.apply_mapping(table, mapping, options)
    ]
    sieve(db, user_id, account, analysis.rows)
    return analysis


def sieve(db: Session, user_id: str, account: Account, rows: list[AnalyzedRow]) -> None:
    """Nadaje status wierszom, które parser uznał za poprawne — trzy sita z nagłówka.

    Wspólne dla wyciągu CSV i synchronizacji z banku (services/bank_sync.py).
    """
    seen_ids: set[str] = set()
    pending: list[AnalyzedRow] = []
    for row in rows:
        parsed = row.parsed
        if row.status != bank_csv.ROW_OK:
            continue
        if parsed.currency and parsed.currency != account.currency:
            row.status = STATUS_OTHER_CURRENCY
            row.reason = f"Waluta {parsed.currency}, a konto jest w {account.currency}."
            continue
        if parsed.external_id in seen_ids:
            # Ten sam numer transakcji banku dwa razy w jednym pliku — drugi
            # wiersz wywróciłby unikat (account_id, external_id).
            row.status, row.reason = STATUS_SKIPPED, "Powtórzony numer transakcji w pliku."
            continue
        seen_ids.add(parsed.external_id or "")
        pending.append(row)

    # Sito 1: ten sam wiersz banku już jest na koncie.
    existing = _existing_external_ids(
        db, account.id, [r.parsed.external_id for r in pending if r.parsed.external_id]
    )
    already_ids = set(existing.values())
    already = {tx.id: tx for tx in _load(db, list(already_ids))}
    remaining: list[AnalyzedRow] = []
    for row in pending:
        tx_id = existing.get(row.parsed.external_id or "")
        if tx_id:
            row.status, row.match = STATUS_ALREADY, already.get(tx_id)
        else:
            remaining.append(row)

    by_index = {r.parsed.index: r for r in remaining}
    keys = [
        matching.RowKey(r.parsed.index, *_day_and_cents(r.parsed), r.parsed.description)
        for r in remaining
    ]

    # Sito 2: ta sama transakcja z innego źródła na tym koncie.
    same = matching.same_source_candidates(db, user_id, account.id, [k.date for k in keys])
    same_by_id = {tx.id: tx for tx in same}
    for index, cand in matching.assign(keys, matching.candidates_from(same)).items():
        row = by_index[index]
        row.status, row.match = STATUS_MATCH, same_by_id[cand.id]

    # Sito 3: druga połówka przelewu na innym własnym koncie.
    keys = [k for k in keys if by_index[k.index].status == bank_csv.ROW_OK]
    others = matching.transfer_candidates(db, user_id, account, [k.date for k in keys])
    others_by_id = {tx.id: tx for tx in others}
    for index, cand in matching.assign(
        keys, matching.candidates_from(others), opposite=True
    ).items():
        row = by_index[index]
        row.status, row.match = STATUS_TRANSFER, others_by_id[cand.id]

    for row in remaining:
        if row.status == bank_csv.ROW_OK:
            row.status = STATUS_NEW


def _load(db: Session, ids: list[str]) -> list[Transaction]:
    if not ids:
        return []
    return list(
        db.scalars(
            select(Transaction)
            .options(selectinload(Transaction.store), selectinload(Transaction.account))
            .where(Transaction.id.in_(ids))
        )
    )


def _source(db: Session, tx: Transaction) -> Literal["receipt", "bank", "other"]:
    if tx.external_id:
        return "bank"
    has_items = db.scalar(
        select(TransactionItem.id).where(TransactionItem.transaction_id == tx.id).limit(1)
    )
    return "receipt" if has_items else "other"


def match_info(db: Session, tx: Transaction) -> BankMatchInfo:
    label = " · ".join(p for p in (tx.store.name if tx.store else "", tx.comment or "") if p)
    return BankMatchInfo(
        transaction_id=tx.id,
        account_id=tx.account_id,
        account_name=tx.account.name,
        date=tx.date,
        amount_cents=tx.total_cents,
        label=label or "(bez opisu)",
        source=_source(db, tx),
    )


def preview(db: Session, user_id: str, content: bytes, spec: BankImportSpec) -> BankPreviewResponse:
    analysis = analyze(db, user_id, content, spec)
    rows = []
    for row in analysis.rows:
        parsed = row.parsed
        rows.append(
            BankPreviewRow(
                index=parsed.index,
                line=parsed.line,
                status=cast(BankRowStatus, row.status),
                reason=row.reason,
                date=parsed.date,
                amount_cents=parsed.amount_cents,
                currency=parsed.currency,
                payee=parsed.payee,
                memo=parsed.memo,
                suggested_action=_DEFAULT_ACTION.get(row.status, "skip"),
                match=match_info(db, row.match) if row.match else None,
            )
        )
    return BankPreviewResponse(
        options=BankParseOptions(**analysis.options.as_dict()),
        file_sha256=analysis.table.sha256,
        rows=rows,
        counts=analysis.counts(),
        previous_import=_previous_import(db, user_id, analysis.table.sha256),
    )


# --- Zapis -------------------------------------------------------------------------


def _recorded_at(parsed: bank_csv.ParsedRow) -> dt.datetime:
    # Jak w imporcie YNAB-a i w paragonach: czas lokalny traktujemy jako UTC.
    day, _ = _day_and_cents(parsed)
    return dt.datetime.combine(day, parsed.time or dt.time.min, tzinfo=dt.UTC)


def _new_transaction(
    user_id: str,
    account: Account,
    parsed: bank_csv.ParsedRow,
    batch: BankImportBatch,
    stores: StoreMatcher,
) -> Transaction:
    description = parsed.description or None
    store = stores.match(parsed.payee)
    category_id = None
    if store and store.automatically_categorize_payee and store.automatic_category_id:
        category_id = store.automatic_category_id
    day, cents = _day_and_cents(parsed)
    return Transaction(
        user_id=user_id,
        account_id=account.id,
        store_id=store.id if store else None,
        category_id=category_id,
        date=day,
        recorded_at=_recorded_at(parsed),
        total_cents=cents,
        currency=account.currency,
        comment=description,
        bank_description=description,
        cleared=True,
        external_id=parsed.external_id,
        import_batch_id=batch.id,
    )


def _save_profile(
    db: Session,
    user_id: str,
    request: BankCommitRequest,
    analysis: Analysis,
) -> str | None:
    if request.save_profile is None:
        if request.profile_id:
            profile = _get_owned_or_404(
                db, BankImportProfile, request.profile_id, user_id, "Profile"
            )
            profile.last_used_at = utcnow()
            return profile.id
        return None

    header = analysis.table.header
    mapping = request.mapping.model_dump()
    mapping["header_names"] = {
        str(i): header[int(i)] for i in mapping.get("columns", {}) if int(i) < len(header)
    }
    profile = db.scalars(
        select(BankImportProfile).where(
            BankImportProfile.user_id == user_id,
            BankImportProfile.name == request.save_profile.name.strip(),
        )
    ).first()
    if profile is None:
        profile = BankImportProfile(user_id=user_id, name=request.save_profile.name.strip())
        db.add(profile)
    profile.header_signature = bank_csv.header_signature(header)
    profile.parse_options = analysis.options.as_dict()
    profile.mapping = mapping
    if request.save_profile.bind_account:
        profile.account_id = analysis.account.id
    profile.last_used_at = utcnow()
    db.flush()
    return profile.id


def commit(
    db: Session, user_id: str, content: bytes, file_name: str, request: BankCommitRequest
) -> BankImportBatchRead:
    analysis = analyze(db, user_id, content, request)
    if analysis.table.sha256 != request.file_sha256:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Plik różni się od tego z podglądu — odśwież podgląd.",
        )
    account = analysis.account
    rows = {r.parsed.index: r for r in analysis.rows}
    decisions = {d.row: d for d in request.decisions}
    unknown = sorted(set(decisions) - set(rows))
    if unknown:
        raise _bad_request(f"Nieznane wiersze: {unknown[:10]}")

    batch = BankImportBatch(
        user_id=user_id,
        account_id=account.id,
        file_name=request.file_name or file_name,
        file_sha256=analysis.table.sha256,
        merged_transaction_ids=[],
    )
    db.add(batch)
    db.flush()

    created, merged, skipped, merged_ids, dates = write_rows(
        db, user_id, account, list(rows.values()), decisions, batch
    )

    batch.profile_id = _save_profile(db, user_id, request, analysis)
    fill_batch(batch, created, merged, skipped, merged_ids, dates)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Część wierszy została w międzyczasie zaimportowana — odśwież podgląd.",
        ) from exc
    db.refresh(batch)
    return serialize_batch(batch)


def default_decisions(rows: list[AnalyzedRow]) -> dict[int, BankRowDecision]:
    """Decyzje, jakie podgląd proponuje: nowe dodaj, dopasowane scal, przelew sparuj."""
    return {
        r.parsed.index: BankRowDecision(row=r.parsed.index, action=_DEFAULT_ACTION[r.status])
        for r in rows
        if r.status in _DEFAULT_ACTION
    }


def write_rows(
    db: Session,
    user_id: str,
    account: Account,
    rows: list[AnalyzedRow],
    decisions: dict[int, BankRowDecision],
    batch: BankImportBatch,
) -> tuple[int, int, int, list[str], list[dt.date]]:
    """Zapis wierszy po sitach według decyzji.

    Zwraca (dodane, scalone, pominięte, id scalonych, daty zapisanych wierszy).
    """
    stores = load_matcher(db, user_id)
    created = merged = skipped = 0
    merged_ids: list[str] = []
    dates: list[dt.date] = []

    for row in rows:
        decision = decisions.get(row.parsed.index)
        if decision is None or decision.action == "skip" or row.status in _NEVER_IMPORTED:
            if row.status not in (STATUS_SKIPPED, STATUS_INVALID):
                skipped += 1
            continue
        parsed = row.parsed
        dates.append(_day_and_cents(parsed)[0])

        if decision.action == "merge":
            target = row.match
            if row.status != STATUS_MATCH or target is None:
                raise _bad_request(f"Wiersz {parsed.line}: nie ma czego scalić.")
            if decision.target_transaction_id not in (None, target.id):
                raise _bad_request(f"Wiersz {parsed.line}: scalenie z niezaproponowaną transakcją.")
            target.external_id = parsed.external_id
            target.bank_description = parsed.description or None
            target.cleared = True
            merged_ids.append(target.id)
            merged += 1
            continue

        tx = _new_transaction(user_id, account, parsed, batch, stores)
        if decision.action == "transfer":
            _apply_transfer(db, user_id, account, row, decision.transfer_account_id, tx, batch)
        db.add(tx)
        db.flush()
        if tx.transfer_transaction_id:
            partner = db.get(Transaction, tx.transfer_transaction_id)
            if partner is not None:
                partner.transfer_transaction_id = tx.id
        created += 1

    return created, merged, skipped, merged_ids, dates


def fill_batch(
    batch: BankImportBatch,
    created: int,
    merged: int,
    skipped: int,
    merged_ids: list[str],
    dates: list[dt.date],
) -> None:
    batch.created_count = created
    batch.merged_count = merged
    batch.skipped_count = skipped
    batch.merged_transaction_ids = merged_ids
    batch.date_from = min(dates) if dates else None
    batch.date_to = max(dates) if dates else None


def _apply_transfer(
    db: Session,
    user_id: str,
    account: Account,
    row: AnalyzedRow,
    transfer_account_id: str | None,
    tx: Transaction,
    batch: BankImportBatch,
) -> None:
    """Spina nowy wiersz z drugą połówką przelewu — istniejącą albo nową.

    Nowa połówka powstaje bez `external_id`, więc import wyciągu z drugiego
    konta złapie ją sitem 2 i scali, zamiast zdublować. Należy do batcha, więc
    cofnięcie importu zabiera ją razem z tym wierszem.
    """
    parsed = row.parsed
    suggested = row.match if row.status == STATUS_TRANSFER else None
    target_account_id = transfer_account_id or (suggested.account_id if suggested else None)
    if not target_account_id:
        raise _bad_request(f"Wiersz {parsed.line}: wskaż konto drugiej strony przelewu.")
    other = _get_owned_or_404(db, Account, target_account_id, user_id, "Account")
    if other.id == account.id or other.currency != account.currency:
        raise _bad_request(
            f"Wiersz {parsed.line}: przelew tylko na inne konto w tej samej walucie."
        )
    tx.category_id = None
    tx.store_id = None
    tx.transfer_account_id = other.id

    if suggested is not None and suggested.account_id == other.id:
        suggested.transfer_account_id = account.id
        db.flush()
        tx.transfer_transaction_id = suggested.id
        return

    day, cents = _day_and_cents(parsed)
    partner = Transaction(
        user_id=user_id,
        account_id=other.id,
        date=day,
        recorded_at=_recorded_at(parsed),
        total_cents=-cents,
        currency=other.currency,
        comment=parsed.description or None,
        cleared=False,
        transfer_account_id=account.id,
        import_batch_id=batch.id,
    )
    db.add(partner)
    db.flush()
    tx.transfer_transaction_id = partner.id


# --- Profile i historia ----------------------------------------------------------


def list_profiles(db: Session, user_id: str) -> list[BankImportProfileRead]:
    profiles = db.scalars(
        select(BankImportProfile)
        .where(BankImportProfile.user_id == user_id)
        .order_by(BankImportProfile.name)
    )
    return [serialize_profile(p) for p in profiles]


def update_profile(
    db: Session, user_id: str, profile_id: str, payload: BankImportProfileUpdate
) -> BankImportProfileRead:
    profile = _get_owned_or_404(db, BankImportProfile, profile_id, user_id, "Profile")
    data = payload.model_dump(exclude_unset=True)
    if data.get("account_id"):
        _get_owned_or_404(db, Account, data["account_id"], user_id, "Account")
    if data.get("name"):
        profile.name = data["name"].strip()
    if "account_id" in data:
        profile.account_id = data["account_id"]
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Profil o tej nazwie już istnieje."
        ) from exc
    db.refresh(profile)
    return serialize_profile(profile)


def delete_profile(db: Session, user_id: str, profile_id: str) -> None:
    profile = _get_owned_or_404(db, BankImportProfile, profile_id, user_id, "Profile")
    db.delete(profile)
    db.commit()


def serialize_batch(batch: BankImportBatch) -> BankImportBatchRead:
    return BankImportBatchRead(
        id=batch.id,
        account_id=batch.account_id,
        account_name=batch.account.name,
        profile_id=batch.profile_id,
        file_name=batch.file_name,
        date_from=batch.date_from,
        date_to=batch.date_to,
        created_count=batch.created_count,
        merged_count=batch.merged_count,
        skipped_count=batch.skipped_count,
        created_at=batch.created_at,
    )


def list_batches(
    db: Session, user_id: str, account_id: str | None = None
) -> list[BankImportBatchRead]:
    stmt = (
        select(BankImportBatch)
        .options(selectinload(BankImportBatch.account))
        .where(BankImportBatch.user_id == user_id)
        .order_by(BankImportBatch.created_at.desc())
    )
    if account_id:
        stmt = stmt.where(BankImportBatch.account_id == account_id)
    return [serialize_batch(b) for b in db.scalars(stmt)]


def undo_batch(db: Session, user_id: str, batch_id: str) -> BankImportUndoResult:
    """Cofnięcie importu: usuwa utworzone, odpina scalone (§7.3).

    Odmawia, gdy do utworzonej transakcji podpięto już paragon — usunięcie
    zabrałoby jego pozycje i obraz, a tego cofnięcie importu robić nie może.
    """
    batch = _get_owned_or_404(db, BankImportBatch, batch_id, user_id, "Import")
    created = list(db.scalars(select(Transaction).where(Transaction.import_batch_id == batch.id)))
    created_ids = {tx.id for tx in created}
    if created_ids:
        enriched = db.scalar(
            select(TransactionItem.id)
            .where(TransactionItem.transaction_id.in_(created_ids))
            .union(
                select(TransactionAttachment.id).where(
                    TransactionAttachment.transaction_id.in_(created_ids)
                )
            )
            .limit(1)
        )
        if enriched:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Do części transakcji z tego importu podpięto paragony — "
                "cofnięcie usunęłoby je. Odepnij paragony albo usuń transakcje ręcznie.",
            )

        # Istniejące połówki przelewów, które ten import sparował — odpinamy.
        for partner in db.scalars(
            select(Transaction).where(
                Transaction.transfer_transaction_id.in_(created_ids),
                Transaction.id.not_in(created_ids),
            )
        ):
            partner.transfer_account_id = None
            partner.transfer_transaction_id = None

    unlinked = 0
    for tx_id in batch.merged_transaction_ids or []:
        tx = db.get(Transaction, tx_id)
        if tx is not None and tx.user_id == user_id:
            tx.external_id = None
            tx.bank_description = None
            unlinked += 1

    for tx in created:
        db.delete(tx)
    db.delete(batch)
    db.commit()
    return BankImportUndoResult(deleted=len(created), unlinked=unlinked)
