"""System endpoints (health check, user preferences, backups)."""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from ..config import settings
from ..dependencies import _require_admin, get_current_user
from ..models import User
from ..preferences import get_table_column_widths, set_table_column_widths
from ..schemas import (
    BackupFileRead,
    BackupStatusRead,
    HealthResponse,
    TableColumnWidthsRead,
    TableColumnWidthsUpdate,
)
from ..services import backups

router = APIRouter()


@router.get("/health", tags=["health"], response_model=HealthResponse)
def healthcheck():
    return HealthResponse()


@router.get("/preferences/table-columns/{table_id}", response_model=TableColumnWidthsRead)
def read_table_column_widths(
    table_id: str,
    current_user: User = Depends(get_current_user),
):
    widths = get_table_column_widths(table_id, current_user.id)
    return TableColumnWidthsRead(table=table_id, widths=widths)


@router.put("/preferences/table-columns/{table_id}", response_model=TableColumnWidthsRead)
def write_table_column_widths(
    table_id: str,
    payload: TableColumnWidthsUpdate,
    current_user: User = Depends(get_current_user),
):
    widths = set_table_column_widths(table_id, payload.widths, current_user.id)
    return TableColumnWidthsRead(table=table_id, widths=widths)


# --- Kopie zapasowe ---------------------------------------------------------
# Tylko dla administratora: kopia to cała baza wszystkich użytkowników.
# Zwykłe `def` — FastAPI puszcza je w puli wątków, więc kopia i przeglądanie
# katalogu nie blokują pętli zdarzeń.


def _backup_file(info: backups.BackupInfo) -> BackupFileRead:
    return BackupFileRead(name=info.name, created_at=info.created_at, size_bytes=info.size_bytes)


@router.get("/system/backups", tags=["backups"], response_model=BackupStatusRead)
def read_backup_status(current_user: User = Depends(get_current_user)):
    _require_admin(current_user)
    config = backups.BackupConfig.from_settings()
    files = backups.list_backups(config.backup_dir)
    last = files[0].created_at if files else None
    interval = dt.timedelta(hours=settings.backup_interval_hours)
    next_at = None
    if settings.backup_enabled:
        next_at = max(last + interval, backups.utcnow()) if last else backups.utcnow()
    mirror = [p for p in config.attachments_mirror.rglob("*") if p.is_file()]
    state = backups.run_state
    return BackupStatusRead(
        enabled=settings.backup_enabled,
        directory=str(config.backup_dir),
        interval_hours=settings.backup_interval_hours,
        keep_daily=config.retention.daily,
        keep_weekly=config.retention.weekly,
        keep_monthly=config.retention.monthly,
        include_attachments=config.include_attachments,
        last_backup_at=last,
        next_backup_at=next_at,
        running=state.running,
        last_error=state.last_error,
        last_error_at=state.last_error_at,
        attachments_files=len(mirror),
        attachments_bytes=sum(p.stat().st_size for p in mirror),
        backups=[_backup_file(info) for info in files],
    )


@router.post(
    "/system/backups",
    tags=["backups"],
    response_model=BackupFileRead,
    status_code=status.HTTP_201_CREATED,
)
def create_backup_now(current_user: User = Depends(get_current_user)):
    _require_admin(current_user)
    try:
        result = backups.create_backup(backups.BackupConfig.from_settings())
    except (backups.BackupError, OSError) as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Nie udało się zrobić kopii: {exc}",
        ) from exc
    return _backup_file(result.backup)


@router.get("/system/backups/{name}", tags=["backups"])
def download_backup(name: str, current_user: User = Depends(get_current_user)):
    _require_admin(current_user)
    path = backups.resolve_backup(backups.BackupConfig.from_settings().backup_dir, name)
    if path is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backup not found.")
    return FileResponse(path, media_type="application/vnd.sqlite3", filename=name)
