"""Kopie zapasowe z wiersza poleceń: lista, kopia na żądanie, weryfikacja, odtworzenie.

    uv run python -m backend.scripts.backup list
    uv run python -m backend.scripts.backup create
    uv run python -m backend.scripts.backup verify latest
    uv run python -m backend.scripts.backup restore latest      # aplikacja zatrzymana!

Ścieżki (baza, załączniki, katalog kopii) biorą się z tych samych zmiennych
FINANCE_* co w aplikacji. Kopię wskazuje się nazwą z katalogu kopii, słowem
``latest`` albo ścieżką do pliku (np. pobranego ze strony Panelu).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ..finance_app.services import backups


def _human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _resolve(config: backups.BackupConfig, ref: str) -> Path:
    if ref == "latest":
        found = backups.list_backups(config.backup_dir)
        if not found:
            raise SystemExit(f"Brak kopii w {config.backup_dir}.")
        return found[0].path
    by_name = backups.resolve_backup(config.backup_dir, ref)
    if by_name is not None:
        return by_name
    path = Path(ref).expanduser()
    if path.is_file():
        return path.resolve()
    raise SystemExit(f"Nie ma kopii {ref!r} (ani w {config.backup_dir}, ani pod tą ścieżką).")


def cmd_list(config: backups.BackupConfig, _: argparse.Namespace) -> int:
    found = backups.list_backups(config.backup_dir)
    print(f"Katalog kopii: {config.backup_dir}")
    if not found:
        print("Brak kopii.")
        return 0
    for info in found:
        local = info.created_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {info.name}  {local}  {_human_size(info.size_bytes)}")
    return 0


def cmd_create(config: backups.BackupConfig, _: argparse.Namespace) -> int:
    result = backups.create_backup(config)
    print(f"Kopia: {result.backup.path} ({_human_size(result.backup.size_bytes)})")
    if config.include_attachments:
        print(
            f"Załączniki: +{result.attachments_copied} nowych, "
            f"-{result.attachments_pruned} niepotrzebnych"
        )
    if result.removed:
        print("Usunięte przez rotację: " + ", ".join(result.removed))
    return 0


def cmd_verify(config: backups.BackupConfig, args: argparse.Namespace) -> int:
    path = _resolve(config, args.backup)
    backups.verify_backup(path)
    referenced = backups.referenced_attachments(path)
    missing = [
        rel
        for rel in sorted(referenced)
        if not (config.attachments_mirror / rel).is_file()
        and not (config.attachments_dir / rel).is_file()
    ]
    print(f"{path.name}: baza spójna, załączników w bazie: {len(referenced)}")
    if missing:
        print(f"Brakuje {len(missing)} plików załączników: " + ", ".join(missing))
        return 1
    return 0


def cmd_restore(config: backups.BackupConfig, args: argparse.Namespace) -> int:
    path = _resolve(config, args.backup)
    print(f"Odtwarzam {path}\n  do bazy {config.db_path}\n  załączniki do {config.attachments_dir}")
    if not args.yes:
        print("Aplikacja musi być zatrzymana. Kontynuować? [t/N] ", end="", flush=True)
        if sys.stdin.readline().strip().lower() not in {"t", "tak", "y", "yes"}:
            print("Przerwano.")
            return 1
    result = backups.restore_backup(path, config)
    if result.safety_copy:
        print(f"Poprzednia baza zachowana jako {result.safety_copy}")
    print(f"Odtworzono. Dograne załączniki: {result.attachments_restored}")
    if result.attachments_missing:
        print(
            f"Uwaga: brak {len(result.attachments_missing)} plików załączników "
            "(ani w katalogu załączników, ani w lustrze kopii): "
            + ", ".join(result.attachments_missing)
        )
    print("Uruchom aplikację — ewentualne migracje schematu wykonają się przy starcie.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="backup", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="pokaż kopie")
    sub.add_parser("create", help="zrób kopię teraz")
    verify = sub.add_parser("verify", help="sprawdź kopię i jej załączniki")
    verify.add_argument("backup", help="nazwa, 'latest' albo ścieżka")
    restore = sub.add_parser("restore", help="odtwórz bazę z kopii (aplikacja zatrzymana)")
    restore.add_argument("backup", help="nazwa, 'latest' albo ścieżka")
    restore.add_argument("--yes", action="store_true", help="bez pytania o potwierdzenie")
    args = parser.parse_args(argv)

    config = backups.BackupConfig.from_settings()
    handlers = {
        "list": cmd_list,
        "create": cmd_create,
        "verify": cmd_verify,
        "restore": cmd_restore,
    }
    try:
        return handlers[args.command](config, args)
    except backups.BackupError as exc:
        print(f"Błąd: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
