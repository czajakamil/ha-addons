from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_CONFIGURED = False


class _UvicornNameFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith("uvicorn."):
            record.name = "uvicorn"
        return True


def _resolve_log_path(log_file: str | None, project_root: Path) -> Path | None:
    """Resolve and create parent directories for the log file if provided."""
    path_str = (log_file or "").strip()
    if not path_str:
        return None

    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = (project_root / path).resolve()

    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def setup_logging(
    *,
    log_level: str = "INFO",
    log_file: str | None = None,
    project_root: Path | None = None,
    log_to_console: bool = True,
):
    """Configure application-wide logging (console + rotating file)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved_root = project_root or Path.cwd()
    log_path = _resolve_log_path(log_file, resolved_root)
    level = (log_level or "INFO").upper()

    handlers: dict[str, dict] = {}
    handler_names: list[str] = []

    if log_to_console:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": level,
            "filters": ["uvicorn_name"],
        }
        handler_names.append("console")

    if log_path:
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "filename": str(log_path),
            "maxBytes": 2 * 1024 * 1024,  # 2 MB
            "backupCount": 3,
            "encoding": "utf-8",
            "level": level,
            "filters": ["uvicorn_name"],
        }
        handler_names.append("file")

    if not handler_names:
        handlers["null"] = {"class": "logging.NullHandler"}
        handler_names.append("null")

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": LOG_FORMAT,
                    "datefmt": LOG_DATE_FORMAT,
                }
            },
            "filters": {
                "uvicorn_name": {
                    "()": _UvicornNameFilter,
                }
            },
            "handlers": handlers,
            "root": {
                "handlers": handler_names,
                "level": level,
            },
            "loggers": {
                "uvicorn.error": {
                    "level": level,
                    "handlers": handler_names,
                    "propagate": False,
                },
                "uvicorn.access": {
                    "level": level,
                    "handlers": handler_names,
                    "propagate": False,
                },
            },
        }
    )

    logging.getLogger(__name__).info(
        "Logging initialized (level=%s, file=%s, console=%s)", level, log_path, log_to_console
    )
    _CONFIGURED = True
