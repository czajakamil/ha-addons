from __future__ import annotations

import datetime as dt
import os
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from backend.finance_app.models import Account, Base, Store, StoreAlias, Transaction
from backend.finance_app.services.bank_csv import ParsedRow
from backend.finance_app.services.bank_import import _new_transaction
from backend.finance_app.services.store_aliases import (
    add_alias,
    exact_names_of,
    load_matcher,
    normalize_alias,
    parse_alias,
    preview_alias,
    register_store_name,
    remove_alias,
    resolve_names,
)

USER = "user-1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def db() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(Account(id="a", user_id=USER, name="Konto", currency="PLN"))
        session.flush()
        yield session


def store(db: Session, name: str, **extra: object) -> Store:
    item = Store(user_id=USER, name=name, **extra)
    db.add(item)
    register_store_name(db, item)
    return item


def add_tx(db: Session, store_id: str | None, description: str | None = None) -> None:
    moment = dt.datetime(2026, 10, 1, tzinfo=dt.UTC)
    db.add(
        Transaction(
            user_id=USER,
            account_id="a",
            store_id=store_id,
            date=moment.date(),
            recorded_at=moment,
            created_at=moment,
            total_cents=-100,
            currency="PLN",
            bank_description=description,
        )
    )
    db.flush()


def matched(db: Session, name: str) -> str | None:
    found = load_matcher(db, USER).match(name)
    return found.name if found else None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  BIEDRONKA   4521 ", "biedronka 4521"),
        ("Absolut Sp. z o.o.", "absolut"),
        ("Absolut Sp Z O.o.", "absolut"),
        ("Orlen S.A.", "orlen"),
        ("JMP S.A. BIEDRONKA 1123", "jmp biedronka 1123"),
        ("Żabka", "żabka"),
        ("Salsa", "salsa"),
        ("S.A.", "s.a."),
    ],
)
def test_normalize(raw: str, expected: str) -> None:
    assert normalize_alias(raw) == expected


def test_only_trailing_star_is_wildcard() -> None:
    assert parse_alias("BIEDRONKA*").match_type == "prefix"
    assert parse_alias("PayU*Allegro").match_type == "exact"
    with pytest.raises(HTTPException):
        parse_alias("abc*")
    with pytest.raises(HTTPException):
        parse_alias("*")


def test_exact_beats_prefix_and_longest_prefix_wins(db: Session) -> None:
    short, longer, exact = store(db, "Sklep A"), store(db, "Sklep B"), store(db, "Sklep C")
    add_alias(db, USER, short, "BIEDRONKA*")
    add_alias(db, USER, longer, "BIEDRONKA KRAKOW*")
    add_alias(db, USER, exact, "BIEDRONKA KRAKOW 12")

    assert matched(db, "biedronka  WARSZAWA") == "Sklep A"
    assert matched(db, "BIEDRONKA KRAKOW 99") == "Sklep B"
    assert matched(db, "Biedronka Krakow 12") == "Sklep C"
    assert matched(db, "LIDL") is None
    assert matched(db, "XBIEDRONKA") is None


def test_exact_alias_is_not_a_prefix(db: Session) -> None:
    store(db, "Lidl")
    assert matched(db, "Lidl") == "Lidl"
    assert matched(db, "Lidl 123") is None


def test_collision_is_ambiguous_and_unused_until_resolved(db: Session) -> None:
    first, second = store(db, "Absolut Sp. z o.o."), store(db, "ABSOLUT")

    assert matched(db, "Absolut") is None
    rows = db.scalars(select(StoreAlias)).all()
    assert [row.is_ambiguous for row in rows] == [True, True]

    remove_alias(db, next(row for row in rows if row.store_id == second.id))

    assert matched(db, "absolut") == first.name


def test_archiving_gives_name_to_the_only_active_store(db: Session) -> None:
    first, second = store(db, "Action"), store(db, "action")
    second.is_deleted = True
    resolve_names(db, USER, exact_names_of(db, second))
    assert load_matcher(db, USER).match("ACTION").id == first.id

    second.is_deleted = False
    resolve_names(db, USER, exact_names_of(db, second))
    assert load_matcher(db, USER).match("ACTION") is None


def test_archived_store_is_not_matched(db: Session) -> None:
    item = store(db, "Lidl")
    item.is_deleted = True
    resolve_names(db, USER, exact_names_of(db, item))
    assert matched(db, "Lidl") is None


def test_add_alias_conflict(db: Session) -> None:
    first, second = store(db, "Lidl"), store(db, "Inny")
    with pytest.raises(HTTPException) as exc:
        add_alias(db, USER, second, " LIDL ")
    assert exc.value.status_code == 409
    assert first.name in exc.value.detail


def test_preview_counts_all_and_other_stores(db: Session) -> None:
    own, other = store(db, "Biedronka"), store(db, "BIEDRONKA 4521")
    add_tx(db, own.id)
    add_tx(db, other.id)
    add_tx(db, other.id)
    add_tx(db, None, "BIEDRONKA 9 · tytuł")
    add_tx(db, None, "tytuł BIEDRONKA")

    assert preview_alias(db, USER, "BIEDRONKA*", [own.id]) == {
        "match_type": "prefix",
        "matches": 4,
        "matches_other_stores": 2,
        "conflict": None,
    }
    assert preview_alias(db, USER, "biedronka", [own.id])["matches"] == 1
    assert "Biedronka" in preview_alias(db, USER, "biedronka", [other.id])["conflict"]


def test_importer_uses_aliases_and_default_category(db: Session) -> None:
    from backend.finance_app.models import BankImportBatch

    item = store(db, "Biedronka", automatically_categorize_payee=False)
    add_alias(db, USER, item, "BIEDRONKA*")
    account = db.get(Account, "a")
    batch = BankImportBatch(id="b", user_id=USER, account_id="a", file_name="x", file_sha256="y")
    parsed = ParsedRow(
        index=0,
        line=1,
        status="new",
        date=dt.date(2026, 10, 1),
        amount_cents=-100,
        currency="PLN",
        payee="BIEDRONKA 4521 KRAKOW",
    )

    tx = _new_transaction(USER, account, parsed, batch, load_matcher(db, USER))
    assert tx.store_id == item.id
    parsed.payee = "NIEZNANY"
    assert _new_transaction(USER, account, parsed, batch, load_matcher(db, USER)).store_id is None


# --- Migracja na syntetycznej bazie, ze ścieżką przez kopię przed migracją -------


def _run(tmp_path: Path, code: str) -> None:
    env = {
        **os.environ,
        "FINANCE_DB_PATH": str(tmp_path / "finance.sqlite3"),
        "FINANCE_BACKUP_DIR": str(tmp_path / "backups"),
        "FINANCE_LOG_FILE": str(tmp_path / "app.log"),
    }
    subprocess.run([sys.executable, "-c", code], cwd=PROJECT_ROOT, env=env, check=True)  # noqa: S603


ALEMBIC = "from alembic import command; from backend.finance_app.db import _alembic_config as c; "


def test_migration_on_synthetic_database(tmp_path: Path) -> None:
    _run(tmp_path, ALEMBIC + "command.upgrade(c(), '56ce42c3205a')")
    db_file = tmp_path / "finance.sqlite3"
    stores = [
        ("1", "Lidl", 0),
        ("2", "Absolut Sp. z o.o.", 0),
        ("3", "ABSOLUT", 0),
        ("4", "Action", 0),
        ("5", "action", 1),
        ("6", "Stary", 1),
        ("7", "PayU*Allegro", 0),
    ]
    with sqlite3.connect(db_file) as conn:
        conn.execute(
            "INSERT INTO users (id, username, display_name, created_at, is_admin) "
            "VALUES ('u', 'u', 'u', '2026-01-01', 1)"
        )
        conn.executemany(
            "INSERT INTO stores (id, user_id, name, is_deleted, renaming_rules) "
            "VALUES (?, 'u', ?, ?, '[]')",
            stores,
        )

    _run(tmp_path, "from backend.finance_app.db import init_db; init_db()")

    assert list((tmp_path / "backups" / "pre-migration").glob("finance-pre-migration-*.sqlite3"))
    with sqlite3.connect(db_file) as conn:
        rows = dict(conn.execute("SELECT store_id, is_ambiguous FROM store_aliases"))
        assert conn.execute("SELECT count(*) FROM stores").fetchone() == (7,)
    assert rows == {"1": 0, "2": 1, "3": 1, "4": 0, "5": 1, "6": 0, "7": 0}

    _run(tmp_path, ALEMBIC + "command.downgrade(c(), '56ce42c3205a')")
    with sqlite3.connect(db_file) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
    assert "store_aliases" not in tables
    assert "stores" in tables
