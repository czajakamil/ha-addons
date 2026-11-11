from __future__ import annotations

import datetime as dt
from collections.abc import Iterator

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from backend.finance_app.models import (
    Account,
    BankConnection,
    BankConnectionAccount,
    BankImportBatch,
    Base,
    Transaction,
    User,
    utcnow,
)
from backend.finance_app.schemas import BankConnectionAccountUpdate
from backend.finance_app.services import bank_import, bank_sync
from backend.finance_app.services import enable_banking as eb

USER = "user-1"
NOW = dt.datetime(2026, 11, 8, 12, 0, tzinfo=dt.UTC)


def api_tx(
    ref: str | None,
    day: str,
    amount: str,
    indicator: str = "DBIT",
    *,
    name: str = "Żabka",
    status: str = "BOOK",
    remittance: list[str] | None = None,
) -> dict:
    party = "creditor" if indicator == "DBIT" else "debtor"
    tx = {
        "booking_date": day,
        "transaction_amount": {"amount": amount, "currency": "PLN"},
        "credit_debit_indicator": indicator,
        "status": status,
        party: {"name": name},
        "remittance_information": remittance or [],
    }
    if ref:
        tx["entry_reference"] = ref
    return tx


class FakeClient:
    def __init__(self, transactions: list[dict], balance: str | None = "1000.00"):
        self._transactions = transactions
        self._balance = balance
        self.windows: list[tuple[dt.date, dt.date]] = []
        self.balance_calls = 0

    def transactions(self, _uid: str, date_from: dt.date, date_to: dt.date) -> list[dict]:
        self.windows.append((date_from, date_to))
        return self._transactions

    def balances(self, _uid: str) -> list[dict]:
        self.balance_calls += 1
        if self._balance is None:
            raise eb.EnableBankingError("no balance", status=400)
        return [{"balance_type": "ITBD", "balance_amount": {"amount": self._balance}}]


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                User(id=USER, username="u"),
                Account(id="pko", user_id=USER, name="PKO", currency="PLN"),
                Account(id="cash", user_id=USER, name="Gotówka", currency="PLN"),
                Account(id="eur", user_id=USER, name="Euro", currency="EUR"),
            ]
        )
        session.flush()
        yield session


def connection(db: Session, *, account_id: str | None = "pko") -> BankConnectionAccount:
    conn = BankConnection(
        id="c1",
        user_id=USER,
        aspsp_name="PKO BP",
        aspsp_country="PL",
        psu_type="personal",
        status="active",
        session_id="s1",
        valid_until=NOW + dt.timedelta(days=90),
    )
    link = BankConnectionAccount(
        id="l1",
        user_id=USER,
        uid="uid-1",
        currency="PLN",
        account_id=account_id,
        iban_hash=eb.iban_hash("PL00123"),
    )
    conn.accounts.append(link)
    db.add(conn)
    db.flush()
    return link


# --- normalizacja --------------------------------------------------------------------


def test_normalize_signs_skips_pending_and_fingerprints_missing_ids() -> None:
    rows = eb.normalize(
        [
            api_tx("O;1", "2026-11-01", "12.50"),
            api_tx("O;2", "2026-11-02", "100.00", "CRDT", name="Jan"),
            api_tx(None, "2026-11-03", "5.00"),
            api_tx(None, "2026-11-03", "5.00"),
            api_tx("O;9", "2026-11-04", "1.00", status="PDNG"),
            {"status": "BOOK", "transaction_amount": {"amount": "1"}},  # bez daty
        ]
    )
    assert [r.amount_cents for r in rows] == [-1250, 10000, -500, -500]
    assert rows[0].external_id == "eb:O;1"
    assert rows[1].payee == "Jan"
    assert rows[2].external_id.startswith("eb-fp:")
    assert rows[3].external_id == rows[2].external_id + "-1"


def test_memo_drops_remittance_line_used_as_payee() -> None:
    [row] = eb.normalize(
        [
            {
                "entry_reference": "O;1",
                "booking_date": "2026-11-01",
                "transaction_amount": {"amount": "3"},
                "credit_debit_indicator": "DBIT",
                "remittance_information": ["BIEDRONKA 123", "Płatność kartą"],
            }
        ]
    )
    assert row.payee == "BIEDRONKA 123"
    assert row.memo == "Płatność kartą"


def test_parse_redirect_and_booked_balance() -> None:
    assert eb.parse_redirect("https://x/cb?code=abc&state=xyz") == ("abc", "xyz")
    assert eb.parse_redirect("abc") == ("abc", None)
    with pytest.raises(eb.EnableBankingError):
        eb.parse_redirect("https://x/cb?error=access_denied")
    assert (
        eb.booked_balance_cents(
            [
                {"balance_type": "ITAV", "balance_amount": {"amount": "5.00"}},
                {"balance_type": "ITBD", "balance_amount": {"amount": "-12.34"}},
            ]
        )
        == -1234
    )


def test_psu_headers_only_for_public_ip() -> None:
    assert bank_sync.psu_headers("192.168.1.5", "172.30.32.2", "UA") == {}
    headers = bank_sync.psu_headers("8.8.8.8, 10.0.0.1", "172.30.32.2", "UA")
    assert headers == {"Psu-Ip-Address": "8.8.8.8", "Psu-User-Agent": "UA"}


# --- pobieranie ----------------------------------------------------------------------


def test_sync_creates_transactions_once_and_records_batch(db: Session) -> None:
    link = connection(db)
    client = FakeClient([api_tx("O;1", "2026-11-01", "12.50"), api_tx("O;2", "2026-11-05", "3")])

    result = bank_sync.sync_account(db, client, link.connection, link, with_balance=True, now=NOW)
    assert (result.created, result.merged) == (2, 0)
    assert client.windows[0][0] == NOW.date() - dt.timedelta(days=bank_sync.INITIAL_DAYS)
    assert link.last_booked_date == dt.date(2026, 11, 5)
    assert link.bank_balance_cents == 100000
    batch = db.get(BankImportBatch, result.batch_id)
    assert batch is not None
    assert batch.created_count == 2

    again = bank_sync.sync_account(db, client, link.connection, link, with_balance=False, now=NOW)
    assert (again.created, again.batch_id) == (0, None)
    # Okno zaczyna się kilka dni przed ostatnią zaksięgowaną datą.
    assert client.windows[1][0] == dt.date(2026, 11, 5) - dt.timedelta(days=bank_sync.OVERLAP_DAYS)
    # Saldo pobrane niedawno — w tle nie pytamy o nie drugi raz.
    assert client.balance_calls == 1
    created = db.scalars(select(Transaction)).all()
    assert len(created) == 2
    # Nowe wiersze z banku czekają na kategorię i zatwierdzenie.
    assert not any(tx.cleared for tx in created)


def test_sync_merges_manual_transaction_and_undo_unlinks(db: Session) -> None:
    link = connection(db)
    manual = Transaction(
        user_id=USER,
        account_id="pko",
        date=dt.date(2026, 11, 2),
        recorded_at=utcnow(),
        total_cents=-1250,
        currency="PLN",
    )
    db.add(manual)
    db.flush()

    result = bank_sync.sync_account(
        db,
        FakeClient([api_tx("O;1", "2026-11-01", "12.50")]),
        link.connection,
        link,
        with_balance=False,
        now=NOW,
    )
    assert (result.created, result.merged) == (0, 1)
    assert manual.external_id == "eb:O;1"

    db.commit()
    bank_import.undo_batch(db, USER, result.batch_id or "")
    db.refresh(manual)
    assert manual.external_id is None


def test_sync_respects_sync_from(db: Session) -> None:
    link = connection(db)
    link.sync_from = dt.date(2026, 11, 3)
    client = FakeClient([api_tx("O;1", "2026-11-01", "1"), api_tx("O;2", "2026-11-04", "2")])
    result = bank_sync.sync_account(db, client, link.connection, link, with_balance=False, now=NOW)
    assert result.created == 1
    assert client.windows[0][0] == dt.date(2026, 11, 3)


def test_sync_connection_marks_expired_on_rejected_session(db: Session, monkeypatch) -> None:
    link = connection(db)

    class Rejecting(FakeClient):
        def transactions(self, *_args: object) -> list[dict]:
            raise eb.EnableBankingError("gone", status=401)

    monkeypatch.setattr(bank_sync, "client_for", lambda *_a, **_k: Rejecting([]))
    result = bank_sync.sync_connection(db, USER, link.connection)
    assert result.connection.status == "expired"
    assert "połącz ponownie" in (result.connection.last_error or "")


def test_sync_connection_refuses_expired_consent(db: Session) -> None:
    link = connection(db)
    link.connection.valid_until = utcnow() - dt.timedelta(minutes=1)
    with pytest.raises(HTTPException) as exc:
        bank_sync.sync_connection(db, USER, link.connection)
    assert exc.value.status_code == 409
    assert link.connection.status == "expired"


# --- powiązania ----------------------------------------------------------------------


def test_linking_defaults_sync_from_after_last_csv_row(db: Session) -> None:
    link = connection(db, account_id=None)
    db.add(
        Transaction(
            user_id=USER,
            account_id="pko",
            date=dt.date(2026, 10, 20),
            recorded_at=utcnow(),
            total_cents=-100,
            currency="PLN",
            external_id="csv:abc",
        )
    )
    db.flush()
    bank_sync.update_account_link(db, USER, link.id, BankConnectionAccountUpdate(account_id="pko"))
    assert link.account_id == "pko"
    assert link.sync_from == dt.date(2026, 10, 21)


def test_linking_rejects_other_currency_and_taken_account(db: Session) -> None:
    link = connection(db)
    with pytest.raises(HTTPException) as exc:
        bank_sync.update_account_link(
            db, USER, link.id, BankConnectionAccountUpdate(account_id="eur")
        )
    assert exc.value.status_code == 400

    other = BankConnectionAccount(
        user_id=USER, connection_id=link.connection_id, uid="uid-2", currency="PLN"
    )
    db.add(other)
    db.flush()
    with pytest.raises(HTTPException) as exc:
        bank_sync.update_account_link(
            db, USER, other.id, BankConnectionAccountUpdate(account_id="pko")
        )
    assert exc.value.status_code == 409


def test_reconnect_carries_link_over_by_iban(db: Session, monkeypatch) -> None:
    old = connection(db)
    old.connection.status = "expired"
    old.sync_from = dt.date(2026, 9, 1)
    old.last_booked_date = dt.date(2026, 11, 1)
    pending = BankConnection(
        id="c2",
        user_id=USER,
        aspsp_name="PKO BP",
        aspsp_country="PL",
        psu_type="personal",
        status="pending",
        auth_state="st",
    )
    db.add(pending)
    db.commit()

    class Sessions:
        def create_session(self, code: str) -> dict:
            assert code == "abc"
            return {
                "session_id": "s2",
                "access": {"valid_until": "2027-05-01T00:00:00+00:00"},
                "accounts": [
                    {"uid": "new-uid", "account_id": {"iban": "PL00123"}, "currency": "PLN"},
                    {"uid": "other", "account_id": {"iban": "PL99999"}, "currency": "PLN"},
                ],
            }

    monkeypatch.setattr(bank_sync, "client_for", lambda *_a, **_k: Sessions())
    result = bank_sync.complete_connection(db, USER, "c2", "https://x/cb?code=abc&state=st", {})

    assert result.status == "active"
    carried = next(a for a in result.accounts if a.uid == "new-uid")
    assert carried.account_id == "pko"
    assert carried.sync_from == dt.date(2026, 9, 1)
    assert carried.last_booked_date == dt.date(2026, 11, 1)
    assert carried.iban_masked == "…0123"
    # Stare, wygasłe połączenie bez powiązań znika.
    assert db.get(BankConnection, "c1") is None


def test_complete_rejects_state_from_other_consent(db: Session) -> None:
    db.add(
        BankConnection(
            id="c2",
            user_id=USER,
            aspsp_name="PKO BP",
            aspsp_country="PL",
            status="pending",
            auth_state="st",
        )
    )
    db.flush()
    with pytest.raises(HTTPException) as exc:
        bank_sync.complete_connection(db, USER, "c2", "https://x/cb?code=a&state=zzz", {})
    assert exc.value.status_code == 400


class SessionWithoutAccounts:
    """Jak Revolut: `POST /sessions` bez rachunków, lista dopiero w `GET /sessions`."""

    def __init__(self) -> None:
        self.details_calls: list[str] = []

    def create_session(self, _code: str) -> dict:
        return {"session_id": "s2", "access": {}, "accounts": []}

    def get_session(self, session_id: str) -> dict:
        assert session_id == "s2"
        return {
            "accounts": ["rev-pln"],
            "accounts_data": [{"uid": "rev-pln"}, {"uid": "rev-eur"}],
        }

    def account_details(self, uid: str) -> dict:
        self.details_calls.append(uid)
        if uid == "rev-eur":
            raise eb.EnableBankingError("no details", status=400)
        return {"uid": uid, "account_id": {"iban": "LT001234"}, "currency": "PLN", "name": "Main"}


def pending_connection(db: Session) -> None:
    db.add(
        BankConnection(
            id="c2",
            user_id=USER,
            aspsp_name="Revolut",
            aspsp_country="PL",
            status="pending",
            auth_state="st",
        )
    )
    db.commit()


def test_complete_falls_back_to_session_accounts(db: Session, monkeypatch) -> None:
    pending_connection(db)
    fake = SessionWithoutAccounts()
    monkeypatch.setattr(bank_sync, "client_for", lambda *_a, **_k: fake)
    result = bank_sync.complete_connection(db, USER, "c2", "https://x/cb?code=a&state=st", {})

    assert {a.uid for a in result.accounts} == {"rev-pln", "rev-eur"}
    main = next(a for a in result.accounts if a.uid == "rev-pln")
    assert main.name == "Main"
    assert main.currency == "PLN"
    assert main.iban_masked == "…1234"
    assert fake.details_calls == ["rev-pln", "rev-eur"]


def test_sync_fetches_accounts_for_connection_without_any(db: Session, monkeypatch) -> None:
    db.add(
        BankConnection(
            id="c2",
            user_id=USER,
            aspsp_name="Revolut",
            aspsp_country="PL",
            status="active",
            session_id="s2",
            valid_until=utcnow() + dt.timedelta(days=90),
        )
    )
    db.commit()
    monkeypatch.setattr(bank_sync, "client_for", lambda *_a, **_k: SessionWithoutAccounts())
    conn = db.get(BankConnection, "c2")
    result = bank_sync.sync_connection(db, USER, conn, {})

    assert {a.uid for a in result.connection.accounts} == {"rev-pln", "rev-eur"}
    assert result.accounts == []
    assert result.connection.last_error is None


# --- opisy płatności kartą -------------------------------------------------------------


def card_tx(ref: str, place: str) -> dict:
    return {
        "entry_reference": ref,
        "booking_date": "2026-11-01",
        "transaction_amount": {"amount": "10.00", "currency": "PLN"},
        "credit_debit_indicator": "DBIT",
        "remittance_information": [place, "CARD-PAYMENT"],
    }


def test_normalize_splits_card_place_into_store_and_city() -> None:
    rows = eb.normalize(
        [card_tx("O;1", "Nowy TargMcDonaldsPL"), card_tx("O;2", "NOWY TARGLIDL 2265PL")]
    )
    assert [(r.payee, r.memo) for r in rows] == [
        ("McDonalds", "Nowy Targ"),
        ("LIDL 2265", "Nowy Targ"),
    ]


def test_sync_matches_store_by_merchant_name(db: Session, monkeypatch) -> None:
    from backend.finance_app.models import Store
    from backend.finance_app.services.store_aliases import register_store_name

    kaufland = Store(user_id=USER, name="Kaufland")
    db.add(kaufland)
    db.flush()
    register_store_name(db, kaufland)
    link = connection(db)
    client = FakeClient([card_tx("O;1", "NOWY TARGKAUFLANDPL")])
    monkeypatch.setattr(bank_sync, "client_for", lambda *_a, **_k: client)

    bank_sync.sync_connection(db, USER, link.connection)

    tx = db.scalars(select(Transaction)).one()
    assert tx.store_id == kaufland.id
    assert tx.comment == "KAUFLAND · Nowy Targ"


def test_refresh_fixes_descriptions_imported_before_splitting(db: Session) -> None:
    from backend.finance_app.models import Store
    from backend.finance_app.services.store_aliases import register_store_name

    kaufland = Store(user_id=USER, name="Kaufland")
    db.add(kaufland)
    db.flush()
    register_store_name(db, kaufland)
    old = "NOWY TARGKAUFLANDPL · CARD-PAYMENT"
    untouched = Transaction(
        user_id=USER,
        account_id="pko",
        date=dt.date(2026, 10, 1),
        total_cents=-100,
        comment=old,
        bank_description=old,
        external_id="eb:1",
    )
    edited = Transaction(
        user_id=USER,
        account_id="pko",
        date=dt.date(2026, 10, 1),
        total_cents=-200,
        comment="Zakupy na weekend",
        bank_description="MARIA ANTOLEC · MARIA · TRANSFER",
        external_id="eb:2",
    )
    csv_row = Transaction(
        user_id=USER,
        account_id="pko",
        date=dt.date(2026, 10, 1),
        total_cents=-300,
        comment=old,
        bank_description=old,
        external_id="csv:1",
    )
    db.add_all([untouched, edited, csv_row])
    db.flush()

    assert bank_sync.refresh_descriptions(db, USER, ["pko"]) == 2
    assert (untouched.comment, untouched.bank_description) == (
        "KAUFLAND · Nowy Targ",
        "KAUFLAND · Nowy Targ",
    )
    assert untouched.store_id == kaufland.id
    assert edited.comment == "Zakupy na weekend"
    assert edited.bank_description == "MARIA ANTOLEC · MARIA"
    assert csv_row.bank_description == old
    assert bank_sync.refresh_descriptions(db, USER, ["pko"]) == 0


def test_normalize_prefers_transaction_date_over_booking_date() -> None:
    tx = card_tx("O;1", "NOWY TARGKAUFLANDPL")
    tx["booking_date"], tx["transaction_date"] = "2026-11-08", "2026-11-06"
    [row] = eb.normalize([tx])
    assert row.date == dt.date(2026, 11, 6)
