from __future__ import annotations

from typing import Dict, List, Any, Optional

from .schemas import RootPayload, Metric
from .db import row_exists, execute_insert
from .utils import (
    normalize_to_utc_iso,
    parse_any_datetime,
    local_date_and_hour,
    prev_day_str,
    ensure_partition_silver_heart_data,
    ensure_partition_heart_rate_detailed,
)


def process_body_composition(metrics: List[Metric], conn):
    metric_field_map = {
        "weight_body_mass": "weight_kg",
        "body_mass_index": "bmi",
        "body_fat_percentage": "body_fat_percentage",
        "lean_body_mass": "lean_mass_kg",
    }

    merged: Dict[str, Dict[str, Any]] = {}

    for metric in metrics:
        field_name = metric_field_map.get(metric.name)
        if not field_name:
            continue

        for entry in metric.data:
            if not entry.date or not entry.source:
                continue

            key = f"{entry.date}__{entry.source}"
            if key not in merged:
                measured_at_iso = normalize_to_utc_iso(entry.date)
                merged[key] = {
                    "measured_at": measured_at_iso,
                    "source": entry.source,
                    "weight_kg": None,
                    "bmi": None,
                    "body_fat_percentage": None,
                    "lean_mass_kg": None,
                }

            merged[key][field_name] = entry.qty

    for rec in merged.values():
        exists_q = """
            SELECT id
            FROM public.silver_body_composition
            WHERE measured_at = %(measured_at)s
              AND source = %(source)s
            LIMIT 1;
        """
        if row_exists(conn, exists_q, rec):
            continue

        insert_q = """
            INSERT INTO public.silver_body_composition
                (measured_at, source, weight_kg, bmi, body_fat_percentage, lean_mass_kg)
            VALUES
                (%(measured_at)s, %(source)s, %(weight_kg)s, %(bmi)s,
                 %(body_fat_percentage)s, %(lean_mass_kg)s);
        """
        execute_insert(conn, insert_q, rec)


def process_sleep_analysis(metrics: List[Metric], conn):
    segs_raw: List[Dict[str, Any]] = []

    for metric in metrics:
        for r in metric.data:
            if r.startDate and r.endDate:
                segs_raw.append(
                    {
                        "startDate": r.startDate,
                        "endDate": r.endDate,
                        "qty": r.qty,
                        "value": r.value,
                        "source": r.source,
                    }
                )

    segs: List[Dict[str, Any]] = []
    for r in segs_raw:
        start_ms = int(parse_any_datetime(r["startDate"]).timestamp() * 1000)
        end_ms = int(parse_any_datetime(r["endDate"]).timestamp() * 1000)
        r["_start_ms"] = start_ms
        r["_end_ms"] = end_ms
        segs.append(r)

    segs.sort(key=lambda x: x["_start_ms"])

    SPLIT_GAP_MIN = 120
    sessions: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    for s in segs:
        if cur is None:
            cur = {"segs": [s], "start_ms": s["_start_ms"], "end_ms": s["_end_ms"]}
            continue

        gap_min = (s["_start_ms"] - cur["end_ms"]) / 60000.0
        if gap_min > SPLIT_GAP_MIN:
            sessions.append(cur)
            cur = {"segs": [s], "start_ms": s["_start_ms"], "end_ms": s["_end_ms"]}
        else:
            cur["segs"].append(s)
            if s["_end_ms"] > cur["end_ms"]:
                cur["end_ms"] = s["_end_ms"]

    if cur is not None:
        sessions.append(cur)

    for sess in sessions:
        session_start_str = sess["segs"][0]["startDate"]
        date_part, hour = local_date_and_hour(session_start_str)
        if date_part is None:
            dt = parse_any_datetime(session_start_str)
            date_part = dt.date().isoformat()
            hour = dt.hour

        sleep_date = prev_day_str(date_part) if hour < 12 else date_part

        for seg in sess["segs"]:
            rec = {
                "session_start": seg["startDate"],
                "session_end": seg["endDate"],
                "duration_hours": seg.get("qty"),
                "stage": seg.get("value"),
                "source": seg.get("source"),
                "sleep_date": sleep_date,
            }

            exists_q = """
                SELECT id
                FROM public.silver_sleep_sessions
                WHERE session_start = %(session_start)s
                  AND session_end = %(session_end)s
                  AND duration_hours = %(duration_hours)s
                  AND stage = %(stage)s
                  AND sleep_date = %(sleep_date)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, rec):
                continue

            insert_q = """
                INSERT INTO public.silver_sleep_sessions
                    (session_start, session_end, duration_hours, stage, source, sleep_date)
                VALUES
                    (%(session_start)s, %(session_end)s, %(duration_hours)s,
                     %(stage)s, %(source)s, %(sleep_date)s);
            """
            execute_insert(conn, insert_q, rec)


def process_vo2_max(metrics: List[Metric], conn):
    for metric in metrics:
        for entry in metric.data:
            if not entry.date:
                continue

            date_full = entry.date
            params = {
                "qty": entry.qty,
                "recorded_at": date_full,
                "date": date_full.split(" ")[0],
                "source": entry.source,
            }

            exists_q = """
                SELECT id
                FROM public.silver_heart_data
                WHERE qty = %(qty)s
                  AND context = 'Vo2_Max'
                  AND source = %(source)s
                  AND recorded_at = %(recorded_at)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, params):
                continue

            ensure_partition_silver_heart_data(conn, params["date"])

            insert_q = """
                INSERT INTO public.silver_heart_data
                    (qty, recorded_at, date, source, context)
                VALUES
                    (%(qty)s, %(recorded_at)s, %(date)s, %(source)s, 'Vo2_Max');
            """
            execute_insert(conn, insert_q, params)


def process_heart_rate(metrics: List[Metric], conn):
    for metric in metrics:
        for entry in metric.data:
            if not entry.date:
                continue

            date_full = entry.date
            params = {
                "recorded_at": date_full,
                "date": date_full.split(" ")[0],
                "qty": None,
                "avg_bpm": entry.Avg,
                "min_bpm": entry.Min,
                "max_bpm": entry.Max,
                "source": entry.source,
                "health_context": entry.context,
            }

            exists_q = """
                SELECT id
                FROM public.silver_heart_data
                WHERE avg_bpm = %(avg_bpm)s
                  AND context = 'heart_rate'
                  AND source = %(source)s
                  AND recorded_at = %(recorded_at)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, params):
                continue

            ensure_partition_heart_rate_detailed(conn, params["date"])

            insert_q = """
                INSERT INTO public.silver_heart_data
                    (qty, recorded_at, date, source, context,
                     min_bpm, max_bpm, health_context, avg_bpm)
                VALUES
                    (%(qty)s, %(recorded_at)s, %(date)s, %(source)s, 'heart_rate',
                     %(min_bpm)s, %(max_bpm)s, %(health_context)s, %(avg_bpm)s);
            """
            execute_insert(conn, insert_q, params)


def process_resting_heart_rate(metrics: List[Metric], conn):
    for metric in metrics:
        for entry in metric.data:
            if not entry.date:
                continue

            date_full = entry.date
            params = {
                "qty": entry.qty,
                "recorded_at": date_full,
                "date": date_full.split(" ")[0],
                "source": entry.source,
            }

            exists_q = """
                SELECT id
                FROM public.silver_heart_data
                WHERE qty = %(qty)s
                  AND context = 'resting_heart_rate'
                  AND source = %(source)s
                  AND recorded_at = %(recorded_at)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, params):
                continue

            ensure_partition_heart_rate_detailed(conn, params["date"])

            insert_q = """
                INSERT INTO public.silver_heart_data
                    (qty, recorded_at, date, source, context,
                     min_bpm, max_bpm, health_context, avg_bpm)
                VALUES
                    (%(qty)s, %(recorded_at)s, %(date)s, %(source)s, 'resting_heart_rate',
                     NULL, NULL, NULL, NULL);
            """
            execute_insert(conn, insert_q, params)


def process_respiratory_rate(metrics: List[Metric], conn):
    for metric in metrics:
        for entry in metric.data:
            if not entry.date:
                continue

            date_full = entry.date
            dt = parse_any_datetime(date_full)
            measured_at_ts = int(dt.timestamp() * 1000)

            params = {
                "qty": entry.qty,
                "measured_at": date_full,
                "source": entry.source,
                "measurement_type": "respiratory_rate",
                "measured_at_ts": measured_at_ts,
            }

            exists_q = """
                SELECT id
                FROM public.silver_misc_measurments
                WHERE measured_at = %(measured_at)s
                  AND qty = %(qty)s
                  AND measurement_type = 'respiratory_rate'
                  AND source = %(source)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, params):
                continue

            insert_q = """
                INSERT INTO public.silver_misc_measurments
                    (qty, source, measured_at, measurement_type, measured_at_ts)
                VALUES
                    (%(qty)s, %(source)s, %(measured_at)s, %(measurement_type)s,
                     %(measured_at_ts)s);
            """
            execute_insert(conn, insert_q, params)


def process_hrv(metrics: List[Metric], conn):
    for metric in metrics:
        for entry in metric.data:
            if not entry.date:
                continue

            date_full = entry.date
            params = {
                "qty": entry.qty,
                "recorded_at": date_full,
                "date": date_full.split(" ")[0],
                "source": entry.source,
            }

            exists_q = """
                SELECT id
                FROM public.silver_heart_data
                WHERE qty = %(qty)s
                  AND context = 'hrv'
                  AND source = %(source)s
                  AND recorded_at = %(recorded_at)s
                LIMIT 1;
            """
            if row_exists(conn, exists_q, params):
                continue

            ensure_partition_heart_rate_detailed(conn, params["date"])

            insert_q = """
                INSERT INTO public.silver_heart_data
                    (qty, recorded_at, date, source, context,
                     min_bpm, max_bpm, health_context, avg_bpm)
                VALUES
                    (%(qty)s, %(recorded_at)s, %(date)s, %(source)s, 'hrv',
                     NULL, NULL, NULL, NULL);
            """
            execute_insert(conn, insert_q, params)


def process_all_metrics(payload: RootPayload, conn):
    metrics_list = payload.data.metrics

    # body composition używa pełnej listy metrics
    process_body_composition(metrics_list, conn)

    grouped: Dict[str, List[Metric]] = {}
    for m in metrics_list:
        grouped.setdefault(m.name, []).append(m)

    if "sleep_analysis" in grouped:
        process_sleep_analysis(grouped["sleep_analysis"], conn)

    if "vo2_max" in grouped:
        process_vo2_max(grouped["vo2_max"], conn)

    if "heart_rate" in grouped:
        process_heart_rate(grouped["heart_rate"], conn)

    if "resting_heart_rate" in grouped:
        process_resting_heart_rate(grouped["resting_heart_rate"], conn)

    if "respiratory_rate" in grouped:
        process_respiratory_rate(grouped["respiratory_rate"], conn)

    if "heart_rate_variability" in grouped:
        process_hrv(grouped["heart_rate_variability"], conn)