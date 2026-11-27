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


def test_reused_postgres_connection_does_not_execute_probe():
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


def test_ui_lane_progresses_while_background_executor_is_saturated():
    from concurrent.futures import ThreadPoolExecutor

    started = threading.Event()
    release = threading.Event()

    def slow():
        started.set()
        return release.wait(3)

    async def handler(event, data):
        return await run_db(lambda: "ui")

    async def main():
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
        background = asyncio.create_task(run_db(slow))
        try:
            async with asyncio.timeout(2):
                while not started.is_set():
                    await asyncio.sleep(0.01)
                result = await UiDatabaseMiddleware()(
                    handler, SimpleNamespace(chat=SimpleNamespace(type="private")), {},
                )
                assert result == "ui"
                assert not background.done()
        finally:
            release.set()
            await background

    try:
        asyncio.run(main())
    finally:
        shutdown_ui_db_executor()


def test_workers_started_from_private_handler_do_not_inherit_ui_lane():
    import logging
    from app.job_worker_lifecycle import JobWorkerLifecycle
    from app.runtime_roles import RuntimeOwnership

    async def main():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        observed = asyncio.get_running_loop().create_future()

        async def worker(stop):
            observed.set_result(await run_db(lambda: threading.current_thread().name))
            await stop.wait()

        async def handler(event, data):
            lifecycle.ensure_running({"test-worker": worker})
            assert (await run_db(lambda: threading.current_thread().name)).startswith("vimi-ui-db")
            assert not (await asyncio.wait_for(observed, 2)).startswith("vimi-ui-db")

        try:
            await UiDatabaseMiddleware()(handler, SimpleNamespace(chat=SimpleNamespace(type="private")), {})
        finally:
            await lifecycle.stop(timeout_sec=2)

    try:
        asyncio.run(main())
    finally:
        shutdown_ui_db_executor()


def test_abandoned_db_failure_logs_stack_once_without_exception_payload(caplog):
    from app.runtime_utils import run_ui_db

    started = threading.Event()
    release = threading.Event()

    def failing_db():
        started.set()
        release.wait(3)
        raise ValueError("private database payload")

    async def main():
        task = asyncio.create_task(run_ui_db(failing_db))
        try:
            async with asyncio.timeout(2):
                while not started.is_set():
                    await asyncio.sleep(0.01)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                release.set()
                while "UI_DB_TASK_FAILED_AFTER_CANCEL" not in caplog.text:
                    await asyncio.sleep(0.01)
        finally:
            release.set()

    try:
        asyncio.run(main())
    finally:
        shutdown_ui_db_executor()
    assert caplog.text.count("UI_DB_TASK_FAILED_AFTER_CANCEL") == 1
    assert "failing_db" in caplog.text
    assert "private database payload" not in caplog.text
