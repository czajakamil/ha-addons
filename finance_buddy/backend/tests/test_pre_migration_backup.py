"""Kopia przed migracją. Każdy start to osobny proces, bo `settings` i silnik są globalne."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Scalenie gałęzi nie zmienia schematu, więc z bazy cofniętej do niego można ustawić
# obu rodziców i dostać bazę z dwiema głowami oraz prawdziwy przebieg `upgrade`.
MERGE = "56ce42c3205a"
PARENTS = {"b3d1c0e5f7a2", "b3d8e1f07a52"}


def start_app(tmp_path: Path, backup_dir: Path) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "FINANCE_DB_PATH": str(tmp_path / "finance.sqlite3"),
        "FINANCE_BACKUP_DIR": str(backup_dir),
        "FINANCE_LOG_FILE": str(tmp_path / "app.log"),
    }
    code = "from backend.finance_app.db import init_db; init_db()"
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def revisions(db: Path) -> set[str]:
    with sqlite3.connect(db) as conn:
        return {row[0] for row in conn.execute("SELECT version_num FROM alembic_version")}


def rewind(tmp_path: Path) -> None:
    code = (
        "from alembic import command; from backend.finance_app.db import _alembic_config as c; "
        f"command.downgrade(c(), '{MERGE}')"
    )
    env = {**os.environ, "FINANCE_DB_PATH": str(tmp_path / "finance.sqlite3")}
    env["FINANCE_LOG_FILE"] = str(tmp_path / "app.log")
    subprocess.run([sys.executable, "-c", code], cwd=PROJECT_ROOT, env=env, check=True)  # noqa: S603
    with sqlite3.connect(tmp_path / "finance.sqlite3") as conn:
        conn.execute("DELETE FROM alembic_version")
        conn.executemany("INSERT INTO alembic_version VALUES (?)", [(rev,) for rev in PARENTS])


@pytest.fixture
def migrated(tmp_path: Path) -> Path:
    backup_dir = tmp_path / "backups"
    assert start_app(tmp_path, backup_dir).returncode == 0
    # Nowa baza nie ma czego zabezpieczać.
    assert not (backup_dir / "pre-migration").exists()
    return tmp_path / "finance.sqlite3"


@pytest.mark.usefixtures("migrated")
def test_no_copy_when_already_at_head(tmp_path: Path) -> None:
    assert start_app(tmp_path, tmp_path / "backups").returncode == 0
    assert not (tmp_path / "backups" / "pre-migration").exists()


def test_copy_with_date_is_made_before_upgrade(tmp_path: Path, migrated: Path) -> None:
    rewind(tmp_path)
    assert start_app(tmp_path, tmp_path / "backups").returncode == 0

    copy_dir = tmp_path / "backups" / "pre-migration"
    copies = list(copy_dir.glob("finance-pre-migration-20*.sqlite3"))
    assert len(copies) == 1
    assert revisions(copies[0]) == PARENTS
    assert revisions(migrated).isdisjoint(PARENTS | {MERGE})


def test_failed_copy_stops_migration(tmp_path: Path, migrated: Path) -> None:
    rewind(tmp_path)
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("")

    result = start_app(tmp_path, blocker)

    assert result.returncode != 0
    assert "NotADirectoryError" in result.stderr or "FileExistsError" in result.stderr
    assert revisions(migrated) == PARENTS
