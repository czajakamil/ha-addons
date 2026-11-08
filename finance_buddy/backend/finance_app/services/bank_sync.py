"""Synchronizacja z bankiem przez Enable Banking: klucz, zgody, rachunki, pobieranie.

Pobrane transakcje idą tymi samymi trzema sitami co wyciąg CSV
(services/bank_import.py) z domyślnymi decyzjami podglądu: nowe dodajemy,
dopasowane scalamy, przelewy parujemy. Każde pobranie z wynikiem to osobny
import w historii, więc da się je cofnąć jak wyciąg.

Projekt i ograniczenia PSD2: docs/design/enable-banking.md.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import ipaddress
import json
import logging
import secrets
from collections.abc import Callable

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from ..dependencies import _get_owned_or_404
from ..models import (
    Account,
    BankConnection,
    BankConnectionAccount,
    BankImportBatch,
    Transaction,
    utcnow,
)
from ..schemas import (
    BankAspspRead,
    BankConnectionAccountRead,
    BankConnectionAccountUpdate,
    BankConnectionRead,
    BankConnectionStart,
    BankConnectionStartResponse,
    BankSyncAccountResult,
    BankSyncResult,
    BankSyncSettingsRead,
    BankSyncSettingsUpdate,
)
from . import bank_csv
from . import bank_descriptions as descriptions
from . import bank_import as importer
from . import enable_banking as eb
from .store_aliases import load_matcher

try:  # Ten sam pęk kluczy co klucz YNAB (/data/.keyring w dodatku HA).
    import keyring
    from keyring.errors import KeyringError, PasswordDeleteError
except Exception:  # pragma: no cover - opcjonalna zależność
    keyring = None  # ty: ignore[invalid-assignment]
    KeyringError = Exception  # ty: ignore[invalid-assignment]
    PasswordDeleteError = Exception

log = logging.getLogger("finance_app.bank_sync")

KEYCHAIN_SERVICE = "Finance-Buddy-EnableBanking"
# Pierwsze pobranie: tyle dni wstecz (o ile `sync_from` nie mówi inaczej).
INITIAL_DAYS = 90
# Kolejne pobrania zaczynają tyle dni przed ostatnią zaksięgowaną datą —
# bank potrafi zaksięgować operację z datą wsteczną.
OVERLAP_DAYS = 7
# Saldo w tle najwyżej raz na tyle godzin: każde zapytanie zjada limit 4/dobę.
BALANCE_REFRESH_HOURS = 20
# Synchronizacja w tle pomija połączenie pobrane niedawno (np. po restarcie).
BACKGROUND_MIN_GAP_HOURS = 6
PENDING_TTL = dt.timedelta(hours=1)


def _http(code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=code, detail=detail)


def _bank_error(exc: eb.EnableBankingError) -> HTTPException:
    return _http(status.HTTP_502_BAD_GATEWAY, str(exc))


# --- Klucz aplikacji Enable Banking --------------------------------------------------


def _keychain_user(user_id: str) -> str:
    return f"user:{user_id}"


def _load_credentials(user_id: str) -> dict | None:
    if not keyring:
        return None
    try:
        raw = keyring.get_password(KEYCHAIN_SERVICE, _keychain_user(user_id))
    except KeyringError as exc:
        raise _http(500, f"Nie udało się odczytać klucza: {exc}") from exc
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    return data if isinstance(data, dict) and data.get("app_id") and data.get("key_pem") else None


def _save_credentials(user_id: str, data: dict) -> None:
    if not keyring:
        raise _http(500, "Pęk kluczy jest niedostępny — nie ma gdzie zapisać klucza.")
    try:
        keyring.set_password(KEYCHAIN_SERVICE, _keychain_user(user_id), json.dumps(data))
    except KeyringError as exc:
        raise _http(500, f"Nie udało się zapisać klucza: {exc}") from exc


def _delete_credentials(user_id: str) -> None:
    if not keyring:
        return
    try:
        keyring.delete_password(KEYCHAIN_SERVICE, _keychain_user(user_id))
    except PasswordDeleteError:
        return
    except KeyringError as exc:
        raise _http(500, f"Nie udało się usunąć klucza: {exc}") from exc


def _credentials_or_400(user_id: str) -> dict:
    creds = _load_credentials(user_id)
    if not creds:
        raise _http(
            status.HTTP_400_BAD_REQUEST,
            "Najpierw dodaj Application ID i klucz z panelu Enable Banking.",
        )
    return creds


def client_for(user_id: str, psu_headers: dict[str, str] | None = None) -> eb.Client:
    creds = _credentials_or_400(user_id)
    return eb.Client(
        app_id=creds["app_id"],
        key=eb.load_private_key(creds["key_pem"]),
        psu_headers=psu_headers or {},
    )


def get_settings(user_id: str) -> BankSyncSettingsRead:
    creds = _load_credentials(user_id)
    if not creds:
        return BankSyncSettingsRead()
    return BankSyncSettingsRead(
        configured=True,
        app_id=creds["app_id"],
        app_name=creds.get("app_name"),
        redirect_url=creds.get("redirect_url"),
    )


def update_settings(user_id: str, payload: BankSyncSettingsUpdate) -> BankSyncSettingsRead:
    """Sprawdza klucz w Enable Banking (`GET /application`) i dopiero wtedy zapisuje."""
    app_id = payload.app_id.strip()
    pem = payload.private_key_pem.strip() + "\n"
    try:
        key = eb.load_private_key(pem)
    except eb.EnableBankingError as exc:
        raise _http(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    try:
        app = eb.Client(app_id=app_id, key=key).application()
    except eb.EnableBankingError as exc:
        if exc.status in (400, 401, 403, 404):
            raise _http(
                status.HTTP_400_BAD_REQUEST,
                "Enable Banking nie przyjął klucza — sprawdź, czy Application ID "
                "pasuje do pliku .pem.",
            ) from exc
        raise _bank_error(exc) from exc
    redirect_urls = app.get("redirect_urls") or []
    if not redirect_urls:
        raise _http(
            status.HTTP_400_BAD_REQUEST,
            "Aplikacja w Enable Banking nie ma adresu powrotu (Redirect URL). Dodaj go "
            "w panelu Enable Banking i spróbuj ponownie.",
        )
    _save_credentials(
        user_id,
        {
            "app_id": app_id,
            "key_pem": pem,
            "app_name": app.get("name"),
            "redirect_url": redirect_urls[0],
        },
    )
    return get_settings(user_id)


def delete_settings(user_id: str) -> None:
    _delete_credentials(user_id)


def list_banks(user_id: str, country: str) -> list[BankAspspRead]:
    try:
        aspsps = client_for(user_id).aspsps(country.upper())
    except eb.EnableBankingError as exc:
        raise _bank_error(exc) from exc
    banks = [
        BankAspspRead(
            name=a["name"],
            country=a.get("country") or country.upper(),
            logo=a.get("logo"),
            max_consent_days=eb.consent_days(a),
            psu_types=a.get("psu_types") or [],
        )
        for a in aspsps
        if a.get("name")
    ]
    return sorted(banks, key=lambda b: b.name.casefold())


# --- PSU (obecność użytkownika) ------------------------------------------------------


def psu_headers(
    forwarded_for: str | None, client_host: str | None, user_agent: str | None
) -> dict[str, str]:
    """Nagłówki PSU tylko z publicznym IP — prywatnych banki nie chcą (lekcja 5).

    Za Ingressem HA zwykle widzimy adres prywatny; wtedy pobranie liczy się do
    limitu 4/dobę jak synchronizacja w tle.
    """
    candidates = [p.strip() for p in (forwarded_for or "").split(",") if p.strip()]
    if client_host:
        candidates.append(client_host)
    for candidate in candidates:
        try:
            ip = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if ip.is_global:
            return {
                "Psu-Ip-Address": str(ip),
                "Psu-User-Agent": (user_agent or "Finance Buddy")[:255],
            }
    return {}


# --- Połączenia ----------------------------------------------------------------------


def _connection_or_404(db: Session, user_id: str, connection_id: str) -> BankConnection:
    return _get_owned_or_404(db, BankConnection, connection_id, user_id, "Connection")


def _balances(db: Session, user_id: str, account_ids: list[str]) -> dict[str, int]:
    if not account_ids:
        return {}
    rows = db.execute(
        select(Transaction.account_id, func.coalesce(func.sum(Transaction.total_cents), 0))
        .where(Transaction.user_id == user_id, Transaction.account_id.in_(account_ids))
        .group_by(Transaction.account_id)
    )
    return {account_id: int(total) for account_id, total in rows}


def serialize(db: Session, connection: BankConnection) -> BankConnectionRead:
    linked = [link.account_id for link in connection.accounts if link.account_id]
    balances = _balances(db, connection.user_id, linked)
    return BankConnectionRead(
        id=connection.id,
        aspsp_name=connection.aspsp_name,
        aspsp_country=connection.aspsp_country,
        status=connection.status,  # ty: ignore[invalid-argument-type]
        valid_until=connection.valid_until,
        last_synced_at=connection.last_synced_at,
        last_error=connection.last_error,
        created_at=connection.created_at,
        accounts=[
            BankConnectionAccountRead(
                id=link.id,
                uid=link.uid,
                name=link.name,
                iban_masked=link.iban_masked,
                currency=link.currency,
                account_id=link.account_id,
                account_name=link.account.name if link.account else None,
                account_balance_cents=balances.get(link.account_id, 0) if link.account_id else None,
                sync_from=link.sync_from,
                last_synced_at=link.last_synced_at,
                last_booked_date=link.last_booked_date,
                bank_balance_cents=link.bank_balance_cents,
                bank_balance_at=link.bank_balance_at,
            )
            for link in connection.accounts
        ],
    )


def _expire_if_due(connection: BankConnection, now: dt.datetime) -> None:
    if (
        connection.status == "active"
        and connection.valid_until is not None
        and connection.valid_until <= now
    ):
        connection.status = "expired"
        connection.last_error = "Zgoda banku wygasła — połącz ponownie."


def list_connections(db: Session, user_id: str) -> list[BankConnectionRead]:
    now = utcnow()
    connections = list(
        db.scalars(
            select(BankConnection)
            .options(
                selectinload(BankConnection.accounts).selectinload(BankConnectionAccount.account)
            )
            .where(BankConnection.user_id == user_id)
            .order_by(BankConnection.created_at)
        )
    )
    visible = []
    for connection in connections:
        if connection.status == "pending":
            # Porzucona zgoda: użytkownik poszedł do banku i nie wrócił.
            if connection.created_at < now - PENDING_TTL:
                db.delete(connection)
            continue
        _expire_if_due(connection, now)
        visible.append(connection)
    db.commit()
    return [serialize(db, c) for c in visible]


def start_connection(
    db: Session,
    user_id: str,
    payload: BankConnectionStart,
    headers: dict[str, str],
) -> BankConnectionStartResponse:
    creds = _credentials_or_400(user_id)
    client = client_for(user_id, headers)
    country = payload.country.upper()
    try:
        aspsps = client.aspsps(country)
        aspsp = next(
            (a for a in aspsps if a.get("name", "").casefold() == payload.aspsp_name.casefold()),
            None,
        )
        if aspsp is None:
            raise _http(status.HTTP_404_NOT_FOUND, f"Nie ma banku {payload.aspsp_name}.")
        valid_until = utcnow() + dt.timedelta(days=eb.consent_days(aspsp))
        state = secrets.token_urlsafe(24)
        url = client.start_auth(
            aspsp_name=aspsp["name"],
            country=country,
            valid_until=valid_until,
            state=state,
            redirect_url=creds["redirect_url"],
            psu_type=payload.psu_type,
        )
    except eb.EnableBankingError as exc:
        raise _bank_error(exc) from exc

    for stale in db.scalars(
        select(BankConnection).where(
            BankConnection.user_id == user_id, BankConnection.status == "pending"
        )
    ):
        db.delete(stale)
    connection = BankConnection(
        user_id=user_id,
        aspsp_name=aspsp["name"],
        aspsp_country=country,
        psu_type=payload.psu_type,
        status="pending",
        auth_state=state,
        valid_until=valid_until,
    )
    db.add(connection)
    db.commit()
    return BankConnectionStartResponse(
        connection_id=connection.id, url=url, valid_until=valid_until
    )


def _parse_datetime(raw: object) -> dt.datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        value = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=dt.UTC)


def _usable_accounts(client: eb.Client, session_id: str | None, accounts: object) -> list[dict]:
    """Rachunki z `uid`; gdy odpowiedź `POST /sessions` ich nie ma, dopytuje sesję.

    Część banków (np. Revolut) oddaje przy autoryzacji pustą listę albo rachunki
    bez `uid`, a pełna lista jest dopiero w `GET /sessions/{id}` (same `uid`)
    — szczegóły rachunku dociągamy wtedy z `/accounts/{uid}/details`.
    """
    given = [a for a in accounts if isinstance(a, dict)] if isinstance(accounts, list) else []
    usable = [a for a in given if a.get("uid")]
    if usable or not session_id:
        return usable
    try:
        data = client.get_session(session_id)
    except eb.EnableBankingError as exc:
        if exc.session_invalid:
            raise
        log.warning("Could not read Enable Banking session accounts: %s", exc)
        return []
    uids = [u for u in data.get("accounts") or [] if isinstance(u, str) and u]
    for extra in data.get("accounts_data") or []:
        uid = extra.get("uid") if isinstance(extra, dict) else None
        if uid and uid not in uids:
            uids.append(uid)
    log.info(
        "Enable Banking session gave %d account(s) at authorization (%d without uid), "
        "%d in GET /sessions.",
        len(given),
        len(given) - len(usable),
        len(uids),
    )
    found: list[dict] = []
    for uid in uids:
        try:
            details = client.account_details(uid) or {}
        except eb.EnableBankingError as exc:
            if exc.session_invalid:
                raise
            log.info("Account details unavailable for %s: %s", uid, exc)
            details = {}
        found.append({**details, "uid": uid})
    return found


def _add_accounts(
    db: Session, user_id: str, connection: BankConnection, accounts: list[dict]
) -> None:
    """Zakłada rachunki połączenia; powiązania ze starszych zgód przechodzą po IBAN-ie."""
    previous = {
        link.iban_hash: link
        for link in db.scalars(
            select(BankConnectionAccount)
            .join(BankConnection)
            .where(
                BankConnectionAccount.user_id == user_id,
                BankConnectionAccount.connection_id != connection.id,
                BankConnectionAccount.iban_hash.is_not(None),
            )
            .order_by(BankConnection.created_at)
        )
    }
    replaced: set[str] = set()
    for account in accounts:
        iban = eb.iban_of(account)
        digest = eb.iban_hash(iban)
        link = BankConnectionAccount(
            user_id=user_id,
            uid=account["uid"],
            name=account.get("name") or account.get("product"),
            iban_masked=eb.mask_iban(iban),
            iban_hash=digest,
            currency=account.get("currency"),
        )
        old = previous.get(digest) if digest else None
        if old is not None and old.account_id:
            link.account_id = old.account_id
            link.sync_from = old.sync_from
            link.last_booked_date = old.last_booked_date
            old.account_id = None
            replaced.add(old.connection_id)
        connection.accounts.append(link)
    db.flush()

    # Stare, wygasłe połączenia, z których wszystko przeszło do nowego, nie są już potrzebne.
    for old_id in replaced:
        old_connection = db.get(BankConnection, old_id)
        if (
            old_connection is not None
            and old_connection.status == "expired"
            and not any(link.account_id for link in old_connection.accounts)
        ):
            db.delete(old_connection)


def complete_connection(
    db: Session,
    user_id: str,
    connection_id: str,
    redirect: str,
    headers: dict[str, str],
) -> BankConnectionRead:
    """Kończy zgodę wklejonym adresem powrotu i zakłada rachunki z sesji.

    Rachunek, który już był powiązany w starszym połączeniu (ten sam IBAN),
    przejmuje powiązanie i punkt startu — po ponownej zgodzie bank nadaje
    nowe `uid`, a historia ma się ciągnąć dalej bez dubli.
    """
    connection = _connection_or_404(db, user_id, connection_id)
    if connection.status != "pending":
        raise _http(status.HTTP_409_CONFLICT, "To połączenie jest już zakończone.")
    try:
        code, state = eb.parse_redirect(redirect)
    except eb.EnableBankingError as exc:
        raise _http(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    if state is not None and not secrets.compare_digest(state, connection.auth_state or ""):
        raise _http(
            status.HTTP_400_BAD_REQUEST,
            "Ten adres jest z innej zgody. Zacznij łączenie z bankiem od nowa.",
        )
    client = client_for(user_id, headers)
    try:
        session = client.create_session(code)
    except eb.EnableBankingError as exc:
        raise _bank_error(exc) from exc

    connection.status = "active"
    connection.session_id = session.get("session_id")
    connection.auth_state = None
    connection.last_error = None
    access = session.get("access") or {}
    connection.valid_until = _parse_datetime(access.get("valid_until")) or connection.valid_until

    try:
        accounts = _usable_accounts(client, connection.session_id, session.get("accounts"))
    except eb.EnableBankingError as exc:
        raise _bank_error(exc) from exc
    _add_accounts(db, user_id, connection, accounts)
    db.commit()
    db.refresh(connection)
    return serialize(db, connection)


def delete_connection(db: Session, user_id: str, connection_id: str) -> None:
    """Usuwa połączenie i cofa zgodę w Enable Banking (jeśli się da)."""
    connection = _connection_or_404(db, user_id, connection_id)
    if connection.session_id and connection.status == "active":
        try:
            client_for(user_id).delete_session(connection.session_id)
        except (eb.EnableBankingError, HTTPException) as exc:
            # Zgoda i tak wygaśnie sama; lokalnie usuwamy mimo błędu.
            log.warning("Could not revoke Enable Banking session: %s", exc)
    db.delete(connection)
    db.commit()


def _default_sync_from(db: Session, account_id: str) -> dt.date | None:
    """Dzień po ostatniej transakcji z wyciągu CSV na koncie — od niego bank, bez dubli."""
    last_csv = db.scalar(
        select(func.max(Transaction.date)).where(
            Transaction.account_id == account_id,
            Transaction.external_id.is_not(None),
            Transaction.external_id.not_like("eb:%"),
            Transaction.external_id.not_like("eb-fp:%"),
        )
    )
    return last_csv + dt.timedelta(days=1) if last_csv else None


def update_account_link(
    db: Session, user_id: str, link_id: str, payload: BankConnectionAccountUpdate
) -> BankConnectionRead:
    link = _get_owned_or_404(db, BankConnectionAccount, link_id, user_id, "Bank account")
    data = payload.model_dump(exclude_unset=True)
    if "account_id" in data:
        account_id = data["account_id"]
        if account_id:
            account = _get_owned_or_404(db, Account, account_id, user_id, "Account")
            if link.currency and account.currency != link.currency:
                raise _http(
                    status.HTTP_400_BAD_REQUEST,
                    f"Rachunek jest w {link.currency}, "
                    f"a konto {account.name} w {account.currency}.",
                )
            taken = db.scalar(
                select(BankConnectionAccount.id).where(
                    BankConnectionAccount.user_id == user_id,
                    BankConnectionAccount.account_id == account_id,
                    BankConnectionAccount.id != link.id,
                )
            )
            if taken:
                raise _http(
                    status.HTTP_409_CONFLICT,
                    f"Konto {account.name} jest już powiązane z innym rachunkiem w banku.",
                )
            if account_id != link.account_id:
                link.last_booked_date = None
                if "sync_from" not in data:
                    link.sync_from = _default_sync_from(db, account_id)
        link.account_id = account_id
    if "sync_from" in data:
        link.sync_from = data["sync_from"]
    db.commit()
    return serialize(db, link.connection)


# --- Pobieranie ----------------------------------------------------------------------


def _rows(transactions: list[eb.ApiTransaction]) -> list[importer.AnalyzedRow]:
    return [
        importer.AnalyzedRow(
            parsed=bank_csv.ParsedRow(
                index=i,
                line=i + 1,
                status=bank_csv.ROW_OK,
                date=tx.date,
                amount_cents=tx.amount_cents,
                currency=tx.currency,
                payee=tx.payee,
                memo=tx.memo,
                external_id=tx.external_id,
            ),
            status=bank_csv.ROW_OK,
        )
        for i, tx in enumerate(transactions)
    ]


def _outdated(description: str) -> list[str] | None:
    """Pola starego opisu („NOWY TARGKAUFLANDPL · CARD-PAYMENT”), gdy da się go poprawić."""
    parts = description.split(descriptions.SEPARATOR)
    code = parts[-1]
    if len(parts) < 2 or code not in descriptions.OPERATION_LABELS:
        return None
    return None if descriptions.OPERATION_LABELS[code] == code else parts


def refresh_descriptions(db: Session, user_id: str, account_ids: list[str]) -> int:
    """Poprawia opisy transakcji pobranych przed rozcinaniem miejsca płatności kartą.

    Komentarz zmieniamy tylko, gdy nikt go nie ruszał (równy opisowi z banku), a sklep
    dobieramy tylko transakcjom bez sklepu. Nowe opisy nie kończą się kodem operacji,
    więc kolejne wywołania nic nie robią.
    """
    if not account_ids:
        return 0
    rows = db.scalars(
        select(Transaction).where(
            Transaction.user_id == user_id,
            Transaction.account_id.in_(account_ids),
            Transaction.bank_description.is_not(None),
            Transaction.external_id.like("eb%"),
        )
    ).all()
    outdated = [(tx, parts) for tx in rows if (parts := _outdated(tx.bank_description or ""))]
    if not outdated:
        return 0
    cities = descriptions.learned_cities(
        parts[0] for _, parts in outdated if parts[-1] == descriptions.CARD_PAYMENT
    )
    stores = load_matcher(db, user_id)
    for tx, parts in outdated:
        if parts[-1] == descriptions.CARD_PAYMENT:
            described = descriptions.describe(None, parts, cities)
        else:
            described = descriptions.describe(parts[0], parts[1:])
        text = descriptions.SEPARATOR.join(p for p in (described.payee, described.memo) if p)
        if tx.comment == tx.bank_description:
            tx.comment = text or None
        tx.bank_description = text or None
        if tx.store_id is None and tx.transfer_account_id is None:
            store = stores.match(described.payee)
            if store is not None:
                tx.store_id = store.id
                if (
                    tx.category_id is None
                    and store.automatically_categorize_payee
                    and store.automatic_category_id
                ):
                    tx.category_id = store.automatic_category_id
    return len(outdated)


def _window(link: BankConnectionAccount, today: dt.date) -> dt.date:
    start = today - dt.timedelta(days=INITIAL_DAYS)
    if link.last_booked_date:
        start = link.last_booked_date - dt.timedelta(days=OVERLAP_DAYS)
    if link.sync_from and link.sync_from > start:
        start = link.sync_from
    return start


def sync_account(
    db: Session,
    client: eb.Client,
    connection: BankConnection,
    link: BankConnectionAccount,
    *,
    with_balance: bool,
    now: dt.datetime | None = None,
) -> BankSyncAccountResult:
    """Jedno pobranie dla rachunku. Wyjątek `session_invalid` przepuszcza wyżej."""
    now = now or utcnow()
    today = now.date()
    account = link.account
    result = BankSyncAccountResult(
        connection_account_id=link.id, account_name=account.name if account else None
    )
    if account is None:
        return result
    start = _window(link, today)
    if start <= today:
        raw = client.transactions(link.uid, start, today)
        transactions = [
            tx for tx in eb.normalize(raw) if link.sync_from is None or tx.date >= link.sync_from
        ]
        rows = _rows(transactions)
        importer.sieve(db, link.user_id, account, rows)
        decisions = importer.default_decisions(rows)
        if decisions:
            ids = sorted(r.parsed.external_id or "" for r in rows if r.parsed.index in decisions)
            batch = BankImportBatch(
                user_id=link.user_id,
                account_id=account.id,
                file_name=f"{connection.aspsp_name} · pobrane z banku",
                file_sha256=hashlib.sha256("\n".join(ids).encode()).hexdigest(),
                merged_transaction_ids=[],
            )
            db.add(batch)
            db.flush()
            created, merged, skipped, merged_ids, dates = importer.write_rows(
                db, link.user_id, account, rows, decisions, batch
            )
            importer.fill_batch(batch, created, merged, skipped, merged_ids, dates)
            result.created, result.merged, result.batch_id = created, merged, batch.id
        if transactions:
            newest = max(tx.date for tx in transactions)
            if link.last_booked_date is None or newest > link.last_booked_date:
                link.last_booked_date = newest

    balance_due = link.bank_balance_at is None or link.bank_balance_at < now - dt.timedelta(
        hours=BALANCE_REFRESH_HOURS
    )
    if with_balance or balance_due:
        try:
            cents = eb.booked_balance_cents(client.balances(link.uid))
        except eb.EnableBankingError as exc:
            if exc.session_invalid:
                raise
            # Saldo bywa odrzucane — transakcje i tak mamy (lekcja 4).
            log.info("Balance unavailable for %s: %s", link.id, exc)
        else:
            if cents is not None:
                link.bank_balance_cents, link.bank_balance_at = cents, now
    link.last_synced_at = now
    return result


def sync_connection(
    db: Session,
    user_id: str,
    connection: BankConnection,
    headers: dict[str, str] | None = None,
    *,
    manual: bool = True,
) -> BankSyncResult:
    now = utcnow()
    _expire_if_due(connection, now)
    if connection.status != "active":
        db.commit()
        raise _http(status.HTTP_409_CONFLICT, "Zgoda banku wygasła — połącz ponownie.")
    client = client_for(user_id, headers)
    results: list[BankSyncAccountResult] = []
    errors: list[str] = []
    if not connection.accounts and connection.session_id:
        # Zgoda bez rachunków (np. Revolut, który nie oddał ich przy autoryzacji)
        # — dopytujemy sesję, zanim każemy łączyć się od nowa.
        try:
            _add_accounts(
                db, user_id, connection, _usable_accounts(client, connection.session_id, [])
            )
        except eb.EnableBankingError as exc:
            if not exc.session_invalid:
                raise _bank_error(exc) from exc
            connection.status = "expired"
            errors.append("Bank odrzucił zgodę — połącz ponownie.")
    refresh_descriptions(db, user_id, [a.account_id for a in connection.accounts if a.account_id])
    for link in connection.accounts if not errors else []:
        if not link.account_id:
            continue
        savepoint = db.begin_nested()
        try:
            results.append(sync_account(db, client, connection, link, with_balance=manual, now=now))
            savepoint.commit()
        except (eb.EnableBankingError, HTTPException) as exc:
            savepoint.rollback()
            if isinstance(exc, HTTPException):
                message = str(exc.detail)
            elif exc.session_invalid:
                connection.status = "expired"
                errors.append("Bank odrzucił zgodę — połącz ponownie.")
                break
            elif exc.status == 429:
                message = "Bank ograniczył liczbę pobrań na dziś — spróbuj jutro."
            else:
                message = str(exc)
            errors.append(message)
            results.append(
                BankSyncAccountResult(
                    connection_account_id=link.id,
                    account_name=link.account.name if link.account else None,
                    error=message,
                )
            )
    connection.last_synced_at = now
    connection.last_error = errors[0] if errors else None
    db.commit()
    db.refresh(connection)
    return BankSyncResult(connection=serialize(db, connection), accounts=results)


def sync_connection_by_id(
    db: Session, user_id: str, connection_id: str, headers: dict[str, str]
) -> BankSyncResult:
    return sync_connection(db, user_id, _connection_or_404(db, user_id, connection_id), headers)


def sync_all(db: Session, now: dt.datetime | None = None) -> int:
    """Synchronizacja w tle wszystkich aktywnych połączeń. Zwraca liczbę pobranych."""
    now = now or utcnow()
    done = 0
    connections = list(db.scalars(select(BankConnection).where(BankConnection.status == "active")))
    for connection in connections:
        if connection.last_synced_at and connection.last_synced_at > now - dt.timedelta(
            hours=BACKGROUND_MIN_GAP_HOURS
        ):
            continue
        if not any(link.account_id for link in connection.accounts):
            continue
        if not _load_credentials(connection.user_id):
            continue
        try:
            sync_connection(db, connection.user_id, connection, manual=False)
            done += 1
        except HTTPException as exc:
            log.info("Bank sync skipped for %s: %s", connection.id, exc.detail)
        except Exception:
            db.rollback()
            log.exception("Bank sync failed for %s", connection.id)
    return done


async def run_scheduler(interval: dt.timedelta, session_factory: Callable[[], Session]) -> None:
    """Pętla w tle: co `interval` pobranie ze wszystkich banków (w wątku)."""

    def _run() -> int:
        with session_factory() as db:
            return sync_all(db)

    while True:
        try:
            done = await asyncio.to_thread(_run)
            if done:
                log.info("Background bank sync: %d connection(s).", done)
        except Exception:
            log.exception("Background bank sync failed.")
        await asyncio.sleep(interval.total_seconds())
