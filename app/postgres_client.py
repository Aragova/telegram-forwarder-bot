from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Iterator

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None
    dict_row = None


class PostgresClient:
    def __init__(self) -> None:
        self.host = os.getenv("APP_PG_HOST", "").strip()
        self.port = os.getenv("APP_PG_PORT", "5432").strip()
        self.dbname = os.getenv("APP_PG_DB", "").strip()
        self.user = os.getenv("APP_PG_USER", "").strip()
        self.password = os.getenv("APP_PG_PASSWORD", "").strip()

        self._local = threading.local()

    def is_configured(self) -> bool:
        return all([
            self.host,
            self.port,
            self.dbname,
            self.user,
        ])

    def get_dsn(self) -> str:
        if not self.is_configured():
            raise RuntimeError(
                "PostgreSQL не настроен: проверь APP_PG_HOST, APP_PG_PORT, APP_PG_DB, APP_PG_USER"
            )

        password_part = f" password={self.password}" if self.password else ""

        return (
            f"host={self.host}"
            f" port={self.port}"
            f" dbname={self.dbname}"
            f" user={self.user}"
            f"{password_part}"
        )

    def ensure_driver(self) -> None:
        if psycopg is None:
            raise RuntimeError(
                "Не установлен psycopg. Установи драйвер PostgreSQL перед использованием APP_DB_BACKEND=postgres"
            )

    def _get_thread_connection(self):
        conn = getattr(self._local, "conn", None)

        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS ok")
                    row = cur.fetchone()
                    if row and row["ok"] == 1:
                        return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                self._local.conn = None

        conn = psycopg.connect(
            self.get_dsn(),
            autocommit=False,
            row_factory=dict_row,
            connect_timeout=5,
        )

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 15000")

        self._local.conn = conn
        return conn

    @contextmanager
    def connect(self) -> Iterator:
        self.ensure_driver()
        conn = self._get_thread_connection()

        try:
            yield conn
            conn.commit()

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    def close_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return

        try:
            conn.close()
        except Exception:
            pass
        finally:
            self._local.conn = None

    def ping(self) -> bool:
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
                return bool(row and row["ok"] == 1)

    def execute_script(self, sql: str) -> None:
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def execute(self, sql: str, params: tuple | list | None = None) -> None:
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())

    def fetchone(self, sql: str, params: tuple | list | None = None):
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchone()

    def fetchall(self, sql: str, params: tuple | list | None = None):
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchall()

    def executemany(self, sql: str, seq_of_params) -> None:
        self.ensure_driver()

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, seq_of_params)
