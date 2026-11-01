"""Configuration helpers for the finance backend."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "finance.sqlite3"
DEFAULT_ATTACHMENTS_DIR = PROJECT_ROOT / "data" / "attachments"
DEFAULT_LOG_FILE = PROJECT_ROOT / "data" / "logs" / "finance.log"
DEFAULT_BACKUP_DIR = PROJECT_ROOT / "data" / "backups"


class Settings(BaseSettings):
    """Application-level configuration sourced from the environment/.env."""

    db_path: str = Field(str(DEFAULT_DB_PATH), alias="FINANCE_DB_PATH")
    attachments_dir: str = Field(str(DEFAULT_ATTACHMENTS_DIR), alias="FINANCE_ATTACHMENTS_DIR")
    log_level: str = Field("INFO", alias="FINANCE_LOG_LEVEL")
    log_file: str = Field(str(DEFAULT_LOG_FILE), alias="FINANCE_LOG_FILE")
    log_to_console: bool = Field(True, alias="FINANCE_LOG_TO_CONSOLE")
    allowed_origins: str = Field("http://localhost:5173", alias="FINANCE_ALLOWED_ORIGINS")

    # Silnik ekstrakcji paragonów: "rapidocr" (domyślny, PP-OCR przez ONNX —
    # zmierzony jako najlepszy dostępny na Linux/aarch64) albo "tesseract"
    # (zapasowy). Liczby: docs/design/ocr-quality-log.md, Runda 4.
    receipt_extractor: str = Field("rapidocr", alias="RECEIPT_EXTRACTOR")

    # Upper bound for a single uploaded file (receipt scan or attachment). A
    # multi-receipt PDF batch is the largest legitimate upload.
    max_upload_mb: int = Field(50, alias="FINANCE_MAX_UPLOAD_MB")

    # Automatyczne kopie zapasowe (services/backups.py). Katalog powinien leżeć
    # poza katalogiem bazy albo wręcz na innym dysku/NAS-ie — README opisuje,
    # jak go podpiąć w Dockerze. Rotacja: z każdego z N ostatnich dni / tygodni
    # / miesięcy, w których powstała kopia, zostaje najnowsza.
    backup_enabled: bool = Field(True, alias="FINANCE_BACKUP_ENABLED")
    backup_dir: str = Field(str(DEFAULT_BACKUP_DIR), alias="FINANCE_BACKUP_DIR")
    backup_interval_hours: float = Field(24, gt=0, alias="FINANCE_BACKUP_INTERVAL_HOURS")
    backup_keep_daily: int = Field(7, ge=0, alias="FINANCE_BACKUP_KEEP_DAILY")
    backup_keep_weekly: int = Field(4, ge=0, alias="FINANCE_BACKUP_KEEP_WEEKLY")
    backup_keep_monthly: int = Field(6, ge=0, alias="FINANCE_BACKUP_KEEP_MONTHLY")
    backup_include_attachments: bool = Field(True, alias="FINANCE_BACKUP_ATTACHMENTS")

    passkey_rp_id: str = Field("localhost", alias="PASSKEY_RP_ID")
    passkey_rp_name: str = Field("Finance Buddy", alias="PASSKEY_RP_NAME")
    passkey_origins: str = Field("http://localhost:5173", alias="PASSKEY_ORIGINS")
    passkey_user_verification: str = Field("preferred", alias="PASSKEY_USER_VERIFICATION")
    passkey_challenge_ttl_seconds: int = Field(300, alias="PASSKEY_CHALLENGE_TTL_SECONDS")
    passkey_invite_ttl_seconds: int = Field(60 * 60 * 24 * 7, alias="PASSKEY_INVITE_TTL_SECONDS")
    auth_session_ttl_seconds: int = Field(60 * 60 * 24 * 30, alias="AUTH_SESSION_TTL_SECONDS")

    # Developer-only password-free login. Disabled by default; never enable in
    # production, as it lets anyone obtain a session for the dev user.
    dev_login_enabled: bool = Field(False, alias="FINANCE_DEV_LOGIN")
    dev_login_username: str = Field("dev", alias="FINANCE_DEV_LOGIN_USERNAME")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
