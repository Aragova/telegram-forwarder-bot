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
    start = source.index("async def stop_job_workers_runtime(")
    end = source.index("def _register_user_saas_handlers", start)
    body = source[start:end]
    assert "await job_workers.stop(" in body
    lifecycle = Path("app/job_worker_lifecycle.py").read_text(encoding="utf-8")
    assert "await drain_job_workers(" in lifecycle
    assert "task.cancel()" not in body


def test_all_role_keeps_bot_session_open_until_after_worker_drain():
    """Polling must not close a session while workers may be sending media."""
    source = Path("bot.py").read_text(encoding="utf-8")
    start = source.index("async def _start_all_role()")
    end = source.index("async def start_job_workers_runtime(", start)
    all_role = source[start:end]
    assert "close_bot_session=False" in all_role

    shutdown_start = source.index("async def _shutdown_runtime(")
    shutdown_end = source.index("async def _run_scheduler_role_loop()", shutdown_start)
    shutdown = source[shutdown_start:shutdown_end]
    assert shutdown.index("lambda: stop_job_workers_runtime(wait_until_complete=True)") < shutdown.index("await bot.session.close()")



def test_shutdown_drains_publication_before_cancelling_retries():
    from pathlib import Path
    source = Path("bot.py").read_text(encoding="utf-8")
    shutdown = source[
        source.index("async def _shutdown_runtime("):
        source.index("async def _run_scheduler_role_loop()")
    ]
    assert shutdown.index("await asyncio.wait_for(flush_publication()") < shutdown.index("await stop_background_tasks()")
    assert shutdown.rindex("publication_evidence_unpersisted") > shutdown.index("await stop_background_tasks()")
    assert shutdown.rindex("publication_evidence_unpersisted") < shutdown.index("await raw_telethon.disconnect()")


def test_outer_cancellation_does_not_detach_child_cleanup():
    from app.worker_shutdown import drain_in_flight

    async def scenario():
        started = asyncio.Event()
        cleaning = asyncio.Event()
        release = asyncio.Event()

        async def child():
            started.set()
            try:
                await asyncio.sleep(60)
            finally:
                cleaning.set()
                await release.wait()

        task = asyncio.create_task(child())
        await started.wait()
        parent = asyncio.create_task(drain_in_flight(
            [task], timeout_sec=60, logger=logging.getLogger("test.drain"), queue="heavy",
        ))
        await asyncio.sleep(0)
        parent.cancel()
        await asyncio.wait_for(cleaning.wait(), 1)
        parent.cancel()
        await asyncio.sleep(0)
        assert not parent.done()
        assert not task.done()
        release.set()
        await asyncio.gather(parent, return_exceptions=True)
        assert parent.cancelled()
        assert task.done()

    asyncio.run(scenario())



def test_outer_drain_deadline_keeps_worker_visible_during_blocked_thread_cleanup(monkeypatch):
    from app import worker_shutdown

    monkeypatch.setattr(worker_shutdown, "_CANCEL_SETTLE_TIMEOUT_SEC", 0.02)
    release = threading.Event()

    async def scenario():
        cleaning = asyncio.Event()

        async def child():
            try:
                await asyncio.sleep(60)
            finally:
                cleaning.set()
                await asyncio.to_thread(release.wait, 2)

        task = asyncio.create_task(child())
        parent = asyncio.create_task(worker_shutdown.drain_in_flight(
            [task], timeout_sec=60, logger=logging.getLogger("test.drain"), queue="heavy",
        ))
        await asyncio.sleep(0)
        try:
            await worker_shutdown.drain_job_workers(
                tasks=[parent], stop_event=None, timeout_sec=0.01,
                logger=logging.getLogger("test.drain"),
            )
            assert cleaning.is_set()
            assert not parent.done() and not task.done()
        finally:
            release.set()
            await asyncio.gather(parent, return_exceptions=True)
        assert parent.cancelled() and task.done()

    asyncio.run(scenario())


def test_shutdown_stops_scheduler_and_rescan_before_draining_workers():
    source = Path("bot.py").read_text(encoding="utf-8")
    shutdown = source[source.index("async def _shutdown_runtime("):
                      source.index("async def _run_scheduler_role_loop()")]
    assert shutdown.index("await asyncio.gather(scheduler_runtime_task") < shutdown.index("lambda: stop_job_workers_runtime(wait_until_complete=True)")
    assert shutdown.index("await asyncio.gather(rescan_recovery_task") < shutdown.index("lambda: stop_job_workers_runtime(wait_until_complete=True)")
    assert shutdown.index("lambda: stop_job_workers_runtime(wait_until_complete=True)") < shutdown.index("await raw_telethon.disconnect()")


def test_shutdown_job_lifecycle_joins_pending_cleanup_without_recancelling(monkeypatch):
    from app import worker_shutdown
    from app.job_worker_lifecycle import JobWorkerLifecycle
    from app.runtime_roles import RuntimeOwnership

    async def scenario():
        owner = RuntimeOwnership()
        owner.bind("all")
        lifecycle = JobWorkerLifecycle(owner, logging.getLogger("test.shutdown"))
        cleaning, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def worker(stop):
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
                finished.set()

        lifecycle.ensure_running({"light": worker})
        await asyncio.sleep(0)
        owner.begin_shutdown()
        stop = asyncio.create_task(lifecycle.stop(timeout_sec=0.01, wait_until_complete=True))
        try:
            await asyncio.wait_for(cleaning.wait(), timeout=1)
            await asyncio.sleep(0.05)
            assert not stop.done() and lifecycle.live_count == 1
        finally:
            release.set()
            await asyncio.wait_for(stop, timeout=1)
        assert finished.is_set() and lifecycle.live_count == 0

    monkeypatch.setattr(worker_shutdown, "_CANCEL_SETTLE_TIMEOUT_SEC", 0.01)
    asyncio.run(scenario())


def test_publication_flush_runs_before_stalled_drain_finishes():
    import ast
    from types import SimpleNamespace
    from app.runtime_utils import wait_for_db_task

    source = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(n for n in source.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_drain_with_publication_flush")

    async def scenario():
        drained = asyncio.Event()
        flushed = asyncio.Event()

        async def flush():
            flushed.set()

        async def drain():
            await flushed.wait()
            assert not drained.is_set()
            drained.set()

        ns = {"asyncio": asyncio, "logger": logging.getLogger(__name__),
              "telethon_client": SimpleNamespace(flush_publication_inbox=flush),
              "wait_for_db_task": wait_for_db_task}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "bot.py", "exec"), ns)
        await asyncio.wait_for(ns["_drain_with_publication_flush"](drain), timeout=1)
        assert flushed.is_set() and drained.is_set()
        assert not any(t.get_name() == "publication-drain-flush" for t in asyncio.all_tasks())

    asyncio.run(scenario())
