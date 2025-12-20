from typing import Any, Dict
import psycopg2

from .config import settings


def get_db_connection():
    conn = psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.db,
        user=settings.user,
        password=settings.password,
    )
    conn.autocommit = False
    return conn


def row_exists(conn, query: str, params: Dict[str, Any]) -> bool:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone() is not None


def execute_insert(conn, query: str, params: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute(query, params)


def execute_raw_sql(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)