from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Iterator
from weakref import WeakSet

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
        self._connections_lock = threading.Lock()
        self._connections = WeakSet()

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
            # libpq's local state check needs no SQL round trip. A connection
            # lost while idle can still fail on the actual operation; never
            # automatically replay a transaction with an unknown outcome.
            if not conn.closed and not conn.broken:
                return conn
            self.close_thread_connection()

        conn = psycopg.connect(
            self.get_dsn(),
            autocommit=False,
            row_factory=dict_row,
            connect_timeout=5,
            # Detect a black-holed TCP peer before the systemd stop budget.
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
            tcp_user_timeout=60000,
        )

        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 15000")
            conn.commit()
        except BaseException:
            conn.close()
            raise

        with self._connections_lock:
            self._connections.add(conn)
        self._local.conn = conn
        return conn

    @contextmanager
    def connect(self) -> Iterator:
        self.ensure_driver()
        conn = self._get_thread_connection()

        committed = False
        try:
            yield conn
            conn.commit()
            committed = True
        finally:
            # Roll back also on GeneratorExit/KeyboardInterrupt, not just Exception.
            # The thread-local connection remains available for subsequent operations.
            if not committed:
                try:
                    conn.rollback()
                except Exception:
                    # Do not reuse a connection whose transaction could not
                    # be rolled back (e.g. after a network disconnect).
                    self.close_thread_connection()

    def close_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return

        try:
            conn.close()
        except Exception:
            pass
        finally:
            with self._connections_lock:
                self._connections.discard(conn)
            self._local.conn = None

    def close_all_connections(self) -> None:
        """Call after the runtime executor has drained all database work."""
        with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()
        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass
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
