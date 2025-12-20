from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from .db import execute_raw_sql


def parse_any_datetime(s: str) -> datetime:
    s = s.strip()

    # ISO z T
    try:
        if "T" in s:
            return datetime.fromisoformat(s)
        # "YYYY-MM-DD HH:MM:SS+02:00" -> zamiana pierwszej spacji na T
        if " " in s and ("+" in s or "-" in s):
            tmp = s.replace(" ", "T", 1)
            return datetime.fromisoformat(tmp)
    except ValueError:
        pass

    # "YYYY-MM-DD HH:MM:SS +0200" / bez strefy
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    # "YYYY-MM-DD"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass

    raise ValueError(f"Unsupported datetime format: {s}")


def normalize_to_utc_iso(s: str) -> str:
    dt = parse_any_datetime(s)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat()


def local_date_and_hour(date_str: str) -> tuple[str | None, int]:
    s = date_str.strip()

    # "YYYY-MM-DD HH:mm:ss +0200"
    m = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2}):(\d{2})", s)
    if m:
        date_part = m.group(1)
        hour = int(m.group(2))
        return date_part, hour

    # ISO "YYYY-MM-DDTHH:mm:ss+02:00"
    m2 = re.match(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}):", s)
    if m2:
        date_part = m2.group(1)
        hour = int(m2.group(2))
        return date_part, hour

    try:
        dt = parse_any_datetime(s)
        return dt.date().isoformat(), dt.hour
    except Exception:
        return None, 12


def prev_day_str(yyyy_mm_dd: str) -> str:
    d = datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").date()
    return (d - timedelta(days=1)).isoformat()


def ensure_partition_silver_heart_data(conn, date_str: str):
    base_date_str = date_str.split(" ")[0]
    year, month, day = map(int, base_date_str.split("-"))

    input_date = datetime(year, month, day, tzinfo=timezone.utc)
    input_year = input_date.year
    input_month_index = input_date.month - 1  # 0-11

    month_start = datetime(input_year, input_month_index + 1, 1, tzinfo=timezone.utc)
    if input_month_index == 11:
        next_month_start = datetime(input_year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month_start = datetime(input_year, input_month_index + 2, 1, tzinfo=timezone.utc)

    partition_name = f"heart_rate_detailed_{input_year}_{str(input_month_index + 1).zfill(2)}"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {partition_name}
    PARTITION OF silver_heart_data
    FOR VALUES FROM ('{month_start.date().isoformat()}') TO ('{next_month_start.date().isoformat()}');
    """
    execute_raw_sql(conn, sql)


def ensure_partition_heart_rate_detailed(conn, date_str: str):
    base_date_str = date_str.split("T")[0].split(" ")[0]
    year, month, _day = map(int, base_date_str.split("-"))

    input_year = year
    input_month_index = month - 1

    month_start = datetime(input_year, input_month_index + 1, 1, tzinfo=timezone.utc)
    if input_month_index == 11:
        next_month_start = datetime(input_year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month_start = datetime(input_year, input_month_index + 2, 1, tzinfo=timezone.utc)

    partition_name = f"heart_rate_detailed_{input_year}_{str(input_month_index + 1).zfill(2)}"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {partition_name}
    PARTITION OF heart_rate_detailed
    FOR VALUES FROM ('{month_start.date().isoformat()}') TO ('{next_month_start.date().isoformat()}');
    """
    execute_raw_sql(conn, sql)