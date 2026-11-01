"""Kopie zapasowe bazy SQLite i załączników.

Baza pracuje w trybie WAL, więc skopiowanie samego pliku ``finance.sqlite3``
gubi zmiany siedzące jeszcze w ``-wal``, a kopiowanie trzech plików po kolei
nie jest atomowe. Kopię robi więc sam SQLite (backup API): czyta ją w jednej
transakcji odczytu, która w WAL nie blokuje zapisów aplikacji, i oddaje spójny
obraz bazy z jednej chwili. Wynik dostaje ``journal_mode=DELETE``, żeby był
jednym samodzielnym plikiem, i ``quick_check``, zanim trafi pod docelową nazwę.

Załączniki nie mieszczą się sensownie w każdej kopii (dziesiątki MB skanów na
każdy dzień), ale są niezmienne: nazwa to UUID, plik nigdy nie jest
nadpisywany. Dlatego katalog kopii trzyma jedno wspólne lustro
``attachments/``, do którego dogrywamy tylko nowe pliki. Z lustra usuwamy plik
dopiero wtedy, gdy nie ma go już ani w bieżących załącznikach, ani nie wskazuje
na niego żadna z zachowanych kopii bazy — więc każda zachowana kopia da się
odtworzyć razem z załącznikami.

Rotacja (jak ``restic forget``): z każdego z ``daily`` ostatnich dni, w których
powstała jakaś kopia, zostaje najnowsza; tak samo z ``weekly`` tygodni ISO i
``monthly`` miesięcy. Liczymy dni z kopią, nie dni kalendarzowe, więc po
dwóch tygodniach wyłączonej aplikacji nadal zostaje 7 dziennych kopii. Dni,
tygodnie i miesiące liczymy w UTC (tak samo jak znacznik w nazwie pliku).
Najnowsza kopia zostaje zawsze.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import logging
import re
import shutil
import sqlite3
import threading
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from ..config import PROJECT_ROOT, Settings, settings

log = logging.getLogger(__name__)

BACKUP_PREFIX = "finance-"
BACKUP_SUFFIX = ".sqlite3"
BACKUP_NAME_RE = re.compile(r"^finance-(\d{8}T\d{6}Z)(?:-(\d+))?\.sqlite3$")
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
ATTACHMENTS_MIRROR = "attachments"
TMP_SUFFIX = ".tmp"
STAGING_PREFIX = ".staging-"

# Po nieudanej próbie harmonogram nie ponawia jej od razu w pętli.
FAILURE_RETRY_SECONDS = 60 * 60
# Najdłuższa pojedyncza drzemka harmonogramu. Zegar asyncio jest monotoniczny
# i na macOS stoi w czasie uśpienia laptopa, więc zamiast spać do terminu
# budzimy się co godzinę i porównujemy z zegarem ściennym.
MAX_SLEEP_SECONDS = 60 * 60
MIN_SLEEP_SECONDS = 60


class BackupError(RuntimeError):
    """Kopia nie powstała albo nie przeszła kontroli spójności."""


@dataclass(frozen=True)
class Retention:
    daily: int = 7
    weekly: int = 4
    monthly: int = 6


@dataclass(frozen=True)
class BackupConfig:
    db_path: Path
    attachments_dir: Path
    backup_dir: Path
    retention: Retention = field(default_factory=Retention)
    include_attachments: bool = True

    @property
    def attachments_mirror(self) -> Path:
        return self.backup_dir / ATTACHMENTS_MIRROR

    @classmethod
    def from_settings(cls, source: Settings = settings) -> BackupConfig:
        return cls(
            db_path=_resolve(source.db_path),
            attachments_dir=_resolve(source.attachments_dir),
            backup_dir=_resolve(source.backup_dir),
            retention=Retention(
                daily=source.backup_keep_daily,
                weekly=source.backup_keep_weekly,
                monthly=source.backup_keep_monthly,
            ),
            include_attachments=source.backup_include_attachments,
        )


@dataclass(frozen=True)
class BackupInfo:
    name: str
    path: Path
    created_at: dt.datetime
    size_bytes: int


@dataclass(frozen=True)
class BackupResult:
    backup: BackupInfo
    removed: list[str]
    attachments_copied: int
    attachments_pruned: int


@dataclass
class BackupRunState:
    """Wynik ostatniej próby w tym procesie — na stronę ustawień."""

    last_error: str | None = None
    last_error_at: dt.datetime | None = None
    running: bool = False


run_state = BackupRunState()
_lock = threading.Lock()


def _resolve(raw: str) -> Path:
    path = Path(raw).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


# --- Nazwy i lista kopii ----------------------------------------------------


def backup_name(created_at: dt.datetime, counter: int = 0) -> str:
    stamp = created_at.astimezone(dt.UTC).strftime(TIMESTAMP_FORMAT)
    tail = f"-{counter}" if counter else ""
    return f"{BACKUP_PREFIX}{stamp}{tail}{BACKUP_SUFFIX}"


def parse_backup_name(name: str) -> dt.datetime | None:
    match = BACKUP_NAME_RE.match(name)
    if not match:
        return None
    return dt.datetime.strptime(match.group(1), TIMESTAMP_FORMAT).replace(tzinfo=dt.UTC)


def _newest_first(backups: Iterable[BackupInfo]) -> list[BackupInfo]:
    # Dwie kopie z tej samej sekundy różni licznik w nazwie („…Z-1"), nie czas.
    def key(backup: BackupInfo) -> tuple[dt.datetime, int]:
        match = BACKUP_NAME_RE.match(backup.name)
        counter = int(match.group(2)) if match and match.group(2) else 0
        return backup.created_at, counter

    return sorted(backups, key=key, reverse=True)


def list_backups(backup_dir: Path) -> list[BackupInfo]:
    """Kopie z katalogu, od najnowszej. Pliki o innych nazwach są pomijane."""
    if not backup_dir.is_dir():
        return []
    backups = []
    for entry in backup_dir.iterdir():
        created_at = parse_backup_name(entry.name)
        if created_at is None or not entry.is_file():
            continue
        backups.append(
            BackupInfo(
                name=entry.name,
                path=entry,
                created_at=created_at,
                size_bytes=entry.stat().st_size,
            )
        )
    return _newest_first(backups)


def resolve_backup(backup_dir: Path, name: str) -> Path | None:
    """Ścieżka kopii o tej nazwie albo None — nazwa z zewnątrz, więc tylko wzorzec."""
    if parse_backup_name(name) is None:
        return None
    path = backup_dir / name
    return path if path.is_file() else None


def _free_backup_path(backup_dir: Path, created_at: dt.datetime) -> Path:
    counter = 0
    while (path := backup_dir / backup_name(created_at, counter)).exists():
        counter += 1
    return path


# --- Spójna kopia bazy ------------------------------------------------------


def snapshot_database(source: Path, target: Path) -> None:
    """Spójna kopia działającej bazy (backup API), zapisana atomowo pod ``target``.

    Kopia powstaje najpierw obok bazy, na lokalnym dysku, a do katalogu kopii
    trafia już jako gotowy plik. Katalog kopii bywa zasobem SMB/NFS (NAS, Home
    Assistant), gdzie blokady plików SQLite działają różnie albo wcale — tam
    tylko kopiujemy bajty i podmieniamy nazwę.
    """
    if not source.is_file():
        raise BackupError(f"Baza {source} nie istnieje.")

    staging = source.with_name(f"{STAGING_PREFIX}{target.name}{TMP_SUFFIX}")
    partial = target.with_name(target.name + TMP_SUFFIX)
    staging.unlink(missing_ok=True)
    try:
        with (
            contextlib.closing(sqlite3.connect(source, timeout=30)) as src,
            contextlib.closing(sqlite3.connect(staging)) as dst,
        ):
            # pages=-1 (domyślne): całość w jednym kroku, czyli w jednej
            # transakcji odczytu. Kopiowanie porcjami zaczynałoby od nowa
            # po każdym zapisie aplikacji w trakcie.
            src.backup(dst)
            dst.execute("PRAGMA journal_mode=DELETE")
            (check,) = dst.execute("PRAGMA quick_check").fetchone()
        if check != "ok":
            raise BackupError(f"Kopia nie przeszła quick_check: {check}")
        if staging.parent == target.parent:
            staging.replace(target)
        else:
            shutil.copyfile(staging, partial)
            partial.replace(target)
    finally:
        staging.unlink(missing_ok=True)
        partial.unlink(missing_ok=True)


def verify_backup(path: Path) -> None:
    """Sprawdza, czy plik to nieuszkodzona baza aplikacji (przed odtworzeniem)."""
    try:
        with contextlib.closing(_open_readonly(path)) as conn:
            (check,) = conn.execute("PRAGMA integrity_check").fetchone()
            if check != "ok":
                raise BackupError(f"{path.name}: integrity_check zwrócił {check!r}")
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
    except sqlite3.DatabaseError as exc:
        raise BackupError(f"{path.name} nie jest poprawną bazą SQLite: {exc}") from exc
    if "alembic_version" not in tables or "transactions" not in tables:
        raise BackupError(f"{path.name} nie wygląda na bazę Finance Buddy.")


def _open_readonly(path: Path) -> sqlite3.Connection:
    # immutable=1: gotowa kopia już się nie zmienia, więc SQLite nie musi
    # zakładać blokad — ważne, gdy katalog kopii to udział sieciowy.
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro&immutable=1", uri=True)


# --- Rotacja ----------------------------------------------------------------


def _bucket_keys(retention: Retention) -> Iterable[tuple[int, Callable[[dt.datetime], object]]]:
    yield retention.daily, lambda ts: ts.date()
    yield retention.weekly, lambda ts: tuple(ts.isocalendar())[:2]
    yield retention.monthly, lambda ts: (ts.year, ts.month)


def select_backups_to_keep(backups: list[BackupInfo], retention: Retention) -> set[str]:
    """Nazwy kopii, które zostają (lista od najnowszej, jak z ``list_backups``)."""
    newest_first = _newest_first(backups)
    keep: set[str] = set()
    if newest_first:
        keep.add(newest_first[0].name)
    for limit, bucket in _bucket_keys(retention):
        seen: set[object] = set()
        for backup in newest_first:
            key = bucket(backup.created_at.astimezone(dt.UTC))
            if key in seen:
                continue
            if len(seen) >= limit:
                break
            seen.add(key)
            keep.add(backup.name)
    return keep


def rotate_backups(backup_dir: Path, retention: Retention) -> list[str]:
    backups = list_backups(backup_dir)
    keep = select_backups_to_keep(backups, retention)
    removed = []
    for backup in backups:
        if backup.name not in keep:
            backup.path.unlink(missing_ok=True)
            removed.append(backup.name)
    return removed


# --- Lustro załączników -----------------------------------------------------


def _files_under(root: Path) -> dict[str, Path]:
    if not root.is_dir():
        return {}
    return {
        path.relative_to(root).as_posix(): path
        for path in root.rglob("*")
        if path.is_file() and not path.name.endswith(TMP_SUFFIX)
    }


def mirror_attachments(source_dir: Path, mirror_dir: Path) -> int:
    """Dogrywa do lustra pliki, których tam jeszcze nie ma. Zwraca ich liczbę."""
    copied = 0
    for rel, path in _files_under(source_dir).items():
        target = mirror_dir / rel
        if target.is_file() and target.stat().st_size == path.stat().st_size:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + TMP_SUFFIX)
        shutil.copy2(path, tmp)
        tmp.replace(target)
        copied += 1
    return copied


def _is_safe_attachment_name(rel: str) -> bool:
    """Czy nazwę wolno dokleić do katalogu załączników.

    Nazwy nadaje aplikacja (UUID, ewentualnie pod prefiksem
    ``receipt_drafts/``), ale czytamy je z odtwarzanej kopii, więc są równie
    nieufne jak nazwa pliku podana z zewnątrz — por. ``resolve_backup``.
    ``verify_backup`` sprawdza tylko spójność bazy, a samo ``..`` w
    ``stored_name`` wyprowadziłoby ścieżkę poza katalog: przy odtwarzaniu
    źródłem byłby plik z lustra wskazany przez atakującego, a celem dowolne
    miejsce w systemie plików. Rozszerzenia nie zawężamy — bierze się z nazwy
    wgranego pliku.

    Składowe sprawdzamy na surowym napisie, nie przez ``PurePosixPath``: ta
    skraca ``.`` po drodze, więc ``receipt_drafts/./x`` przeszłoby jako nazwa,
    której aplikacja nigdy nie zapisała.
    """
    if not rel or rel.startswith("/"):
        return False
    return all(part and part not in {".", ".."} for part in rel.split("/"))


def referenced_attachments(db_file: Path) -> set[str]:
    """Pliki załączników (ścieżki względne), na które wskazuje dana kopia.

    Tylko dla gotowych kopii (``journal_mode=DELETE``), nie dla działającej bazy.
    Nazwy wychodzą stąd już sprawdzone, więc wolno je doklejać do katalogu.
    """
    names: set[str] = set()
    queries = (
        ("SELECT stored_name FROM transaction_attachments", ""),
        ("SELECT image_name FROM receipt_drafts WHERE image_name IS NOT NULL", "receipt_drafts/"),
    )
    with contextlib.closing(_open_readonly(db_file)) as conn:
        for sql, prefix in queries:
            try:
                rows = conn.execute(sql).fetchall()
            except sqlite3.OperationalError:
                # Kopia sprzed migracji, która dodała tabelę.
                continue
            for row in rows:
                if not row[0]:
                    continue
                rel = f"{prefix}{row[0]}"
                if not _is_safe_attachment_name(rel):
                    log.warning(
                        "Pomijam załącznik o niedozwolonej nazwie w %s: %r", db_file.name, rel
                    )
                    continue
                names.add(rel)
    return names


def prune_attachment_mirror(config: BackupConfig) -> int:
    """Usuwa z lustra pliki, których nie potrzebuje już nic, co da się odtworzyć."""
    needed = set(_files_under(config.attachments_dir))
    for backup in list_backups(config.backup_dir):
        try:
            needed |= referenced_attachments(backup.path)
        except sqlite3.DatabaseError:
            # Nie wiemy, czego ta kopia potrzebuje — lepiej nic nie kasować.
            log.warning("Nie da się odczytać %s; pomijam sprzątanie lustra.", backup.name)
            return 0
    pruned = 0
    for rel, path in _files_under(config.attachments_mirror).items():
        if rel not in needed:
            path.unlink(missing_ok=True)
            pruned += 1
    return pruned


# --- Całość -----------------------------------------------------------------


def create_backup(config: BackupConfig, now: dt.datetime | None = None) -> BackupResult:
    """Kopia bazy + dogranie załączników + rotacja. Bezpieczna z wielu wątków."""
    with _lock:
        run_state.running = True
        try:
            result = _create_backup_locked(config, now or utcnow())
        except Exception as exc:
            run_state.last_error = str(exc) or exc.__class__.__name__
            run_state.last_error_at = utcnow()
            raise
        else:
            run_state.last_error = None
            run_state.last_error_at = None
            return result
        finally:
            run_state.running = False


def _create_backup_locked(config: BackupConfig, now: dt.datetime) -> BackupResult:
    config.backup_dir.mkdir(parents=True, exist_ok=True)
    # Resztki po przerwanej kopii (np. zamknięte okno wersji desktopowej).
    for stale in (
        *config.backup_dir.glob(f"*{BACKUP_SUFFIX}{TMP_SUFFIX}"),
        *config.db_path.parent.glob(f"{STAGING_PREFIX}*{TMP_SUFFIX}"),
    ):
        stale.unlink(missing_ok=True)

    target = _free_backup_path(config.backup_dir, now.astimezone(dt.UTC))
    snapshot_database(config.db_path, target)

    copied = 0
    if config.include_attachments:
        # Po kopii bazy, nie przed: plik wgrany w międzyczasie ma już wiersz w
        # kopii albo trafi do lustra przy następnej okazji, a plik usunięty w
        # tej chwili i tak należał do transakcji usuniętej przez użytkownika.
        copied = mirror_attachments(config.attachments_dir, config.attachments_mirror)

    removed = rotate_backups(config.backup_dir, config.retention)
    pruned = prune_attachment_mirror(config) if config.include_attachments else 0

    info = BackupInfo(
        name=target.name,
        path=target,
        created_at=parse_backup_name(target.name) or now,
        size_bytes=target.stat().st_size,
    )
    log.info(
        "Backup %s (%d B); attachments +%d/-%d; rotated out: %s",
        info.name,
        info.size_bytes,
        copied,
        pruned,
        ", ".join(removed) or "none",
    )
    return BackupResult(
        backup=info, removed=removed, attachments_copied=copied, attachments_pruned=pruned
    )


def last_backup_at(backup_dir: Path) -> dt.datetime | None:
    backups = list_backups(backup_dir)
    return backups[0].created_at if backups else None


# --- Odtwarzanie ------------------------------------------------------------


@dataclass(frozen=True)
class RestoreResult:
    safety_copy: Path | None
    attachments_restored: int
    attachments_missing: list[str]


def restore_backup(
    backup_path: Path, config: BackupConfig, now: dt.datetime | None = None
) -> RestoreResult:
    """Zastępuje bazę zawartością kopii i dogrywa brakujące załączniki z lustra.

    Aplikacja powinna być wtedy zatrzymana. Bieżąca baza (o ile istnieje) jest
    najpierw zachowywana obok niej jako ``<nazwa>.przed-odtworzeniem-<czas>``.
    Kopia ze starszym schematem dostanie brakujące migracje przy najbliższym
    starcie aplikacji (Alembic w ``init_db``).
    """
    verify_backup(backup_path)
    stamp = (now or utcnow()).astimezone(dt.UTC).strftime(TIMESTAMP_FORMAT)
    db_path = config.db_path

    safety_copy = None
    if db_path.is_file():
        safety_copy = db_path.with_name(f"{db_path.name}.przed-odtworzeniem-{stamp}")
        snapshot_database(db_path, safety_copy)

    # Przez backup API, nie kopiowaniem pliku: SQLite sam zadba o -wal/-shm
    # bieżącej bazy, więc nie zostaje w nich nic, co nadpisałoby kopię.
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        contextlib.closing(_open_readonly(backup_path)) as src,
        contextlib.closing(sqlite3.connect(db_path, timeout=30)) as dst,
    ):
        src.backup(dst)
        dst.execute("PRAGMA journal_mode=WAL")

    restored = 0
    missing: list[str] = []
    # Z pliku kopii, nie z odtworzonej bazy: ta jest w WAL, a odczyt
    # „immutable" nie widziałby świeżo zapisanych stron z -wal.
    for rel in sorted(referenced_attachments(backup_path)):
        target = config.attachments_dir / rel
        if target.is_file():
            continue
        source = config.attachments_mirror / rel
        if not source.is_file():
            missing.append(rel)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        restored += 1
    return RestoreResult(
        safety_copy=safety_copy, attachments_restored=restored, attachments_missing=missing
    )


# --- Harmonogram ------------------------------------------------------------


def seconds_until_due(last: dt.datetime | None, now: dt.datetime, interval: dt.timedelta) -> float:
    """Ile zostało do następnej kopii; <= 0 znaczy „teraz"."""
    if last is None:
        return 0.0
    return (last + interval - now).total_seconds()


async def run_scheduler(
    config: BackupConfig,
    interval: dt.timedelta,
    *,
    clock: Callable[[], dt.datetime] = utcnow,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> None:
    """Pętla w tle: kopia, gdy od ostatniej minął ``interval``.

    Przy starcie oznacza to kopię od razu, jeśli ostatnia jest starsza niż
    ``interval``; potem co ``interval`` od ostatniej (ręczna też się liczy).

    Kopia i odczyt katalogu idą w wątku, żeby nie blokować pętli zdarzeń.
    """
    while True:
        last = await asyncio.to_thread(last_backup_at, config.backup_dir)
        now = clock()
        wait = seconds_until_due(last, now, interval)
        if wait <= 0:
            try:
                await asyncio.to_thread(create_backup, config, now)
            except Exception:
                log.exception("Automatic backup failed.")
                wait = FAILURE_RETRY_SECONDS
            else:
                wait = interval.total_seconds()
        await sleep(min(max(wait, MIN_SLEEP_SECONDS), MAX_SLEEP_SECONDS))
