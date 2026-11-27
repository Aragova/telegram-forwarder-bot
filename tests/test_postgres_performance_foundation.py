from __future__ import annotations

import asyncio
import threading
from contextvars import ContextVar
from types import SimpleNamespace

import pytest

from app import postgres_client
from app.postgres_repository import PostgresRepository
from app.runtime_utils import (
    UiDatabaseMiddleware, create_background_task, run_db, shutdown_ui_db_executor,
)


def test_reused_postgres_connection_does_not_execute_probe(monkeypatch):
    client = postgres_client.PostgresClient()
    connection = SimpleNamespace(closed=False, broken=False)
    client._local.conn = connection
    assert client._get_thread_connection() is connection


def test_failed_rollback_discards_connection(monkeypatch):
    client = postgres_client.PostgresClient()
    class Connection:
        def rollback(self):
            raise OSError("connection lost")
    conn = Connection()
    discarded = []
    monkeypatch.setattr(client, "ensure_driver", lambda: None)
    monkeypatch.setattr(client, "_get_thread_connection", lambda: conn)
    monkeypatch.setattr(client, "close_thread_connection", lambda: discarded.append(True))
    with pytest.raises(ValueError):
        with client.connect():
            raise ValueError("original error")
    assert discarded == [True]


def test_ui_database_lane_preserves_context_and_background_isolation():
    marker = ContextVar("marker", default="missing")
    def inspect():
        return threading.current_thread().name, marker.get()
    async def handler(event, data):
        marker.set("request")
        foreground = await run_db(inspect)
        background = await create_background_task(run_db(inspect))
        assert foreground[0].startswith("vimi-ui-db")
        assert not background[0].startswith("vimi-ui-db")
        assert foreground[1] == background[1] == "request"
    async def main():
        await UiDatabaseMiddleware()(handler, SimpleNamespace(chat=SimpleNamespace(type="private")), {})
        assert not (await run_db(inspect))[0].startswith("vimi-ui-db")
    try:
        asyncio.run(main())
    finally:
        shutdown_ui_db_executor()


def test_queue_stats_uses_one_statement():
    from contextlib import nullcontext
    expected = dict(posts=8, deliveries=9, pending=3, processing=2, sent=3, faulty=1, rules=2, active_rules=1)
    calls = []
    cursor = SimpleNamespace(execute=lambda sql: calls.append(sql), fetchone=lambda: expected)
    repo = object.__new__(PostgresRepository)
    repo.connect = lambda: nullcontext(SimpleNamespace(cursor=lambda: nullcontext(cursor)))
    assert repo.get_queue_stats() == expected
    assert len(calls) == 1
    assert calls[0].count("FROM deliveries") == 1
    assert calls[0].count("FROM routing") == 1
