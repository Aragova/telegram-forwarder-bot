"""Regression tests for SIGINT-driven worker drain and ambiguous uploads."""
from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path

from app.worker_resource_policy import WorkerResourcePolicy
from app.worker_runtime import _worker_loop
from app.worker_shutdown import drain_job_workers


def test_draining_waits_for_active_telegram_send_and_keeps_result():
    async def scenario():
        stop = asyncio.Event()
        completed = []
        async def worker():
            await stop.wait()
            await asyncio.sleep(0.06)
            completed.append("saved_receipt")
        task = asyncio.create_task(worker())
        await asyncio.sleep(0)
        await drain_job_workers(
            tasks=[task], stop_event=stop, timeout_sec=0.5,
            logger=logging.getLogger("test.drain"),
        )
        assert completed == ["saved_receipt"]
        assert not task.cancelled()
        assert task.done()
    asyncio.run(scenario())


def test_draining_only_cancels_on_deadline():
    async def scenario():
        stop = asyncio.Event()
        cancelled = []
        async def worker():
            try:
                await stop.wait()
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
        task = asyncio.create_task(worker())
        await asyncio.sleep(0)
        await drain_job_workers(
            tasks=[task], stop_event=stop, timeout_sec=0.02,
            logger=logging.getLogger("test.drain"),
        )
        assert cancelled == [True]
        assert task.cancelled()
    asyncio.run(scenario())


def test_shutdown_during_db_lease_defers_unstarted_job_without_send():
    entered = threading.Event()
    release = threading.Event()
    class Repo:
        def __init__(self):
            self.deferred = []
            self.leases = 0

        def lease_jobs(self, queue, worker_id, limit, lease_seconds):
            self.leases += 1
            entered.set()
            release.wait(timeout=2)
            return [{"id": 77, "job_type": "repost_single", "queue": "light"}]

        def defer_job(self, job_id, error_text, delay_seconds):
            self.deferred.append((job_id, error_text))
            return True

    repo = Repo()
    async def scenario():
        stop = asyncio.Event()
        task = asyncio.create_task(_worker_loop(
            repo, sender_service=object(), queue="light",
            worker_id="test-light", policy=WorkerResourcePolicy(),
            stop_event=stop,
        ))
        ready = await asyncio.to_thread(entered.wait, 1)
        assert ready
        stop.set()
        release.set()
        await asyncio.wait_for(task, timeout=2)
        assert repo.deferred and repo.deferred[0][0] == 77
        assert repo.leases == 1

    asyncio.run(scenario())


def test_production_service_timeout_exceeds_worker_grace():
    policy = WorkerResourcePolicy()
    assert policy.graceful_shutdown_timeout_sec >= 600
    legacy = Path("deploy/systemd/forwarder_bot.service.d/20-graceful-drain.conf").read_text()
    split = Path("deploy/systemd/forwarder-bot-worker.service").read_text()
    for unit in (legacy, split):
        assert "KillSignal=SIGINT" in unit
        assert "TimeoutStopSec=720" in unit
        assert "GRACEFUL_SHUTDOWN_TIMEOUT_SEC=600" in unit


def test_no_instant_cancel_during_bot_shutdown():
    source = Path("bot.py").read_text(encoding="utf-8")
    start = source.index("async def stop_job_workers_runtime()")
    end = source.index("def _register_user_saas_handlers", start)
    body = source[start:end]
    assert "await drain_job_workers(" in body
    assert "task.cancel()" not in body
