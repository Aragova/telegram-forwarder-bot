"""Concurrency regression tests for Codex findings in worker PR #505."""
import asyncio
from threading import Event
from types import SimpleNamespace

import pytest

import app.worker_runtime as runtime


def test_successful_own_sql_commit_must_not_cancel_post_finalize_cleanup():
    """DB CAS completes in thread, heartbeat sees status done, SQL thread returns later."""
    class Repo:
        def __init__(self):
            self.status = "processing"
            self.started = Event()
            self.release = Event()

        def heartbeat_job_processing(self, job_id, owner):
            return self.status == "processing"

        def finalize_owned_job(self, job_id, owner, action, *args):
            assert action == "complete"
            self.status = "done"
            self.started.set()
            assert self.release.wait(2), "Test stalled while waiting for CAS response"
            return True

    async def check():
        repo = Repo()
        finishing = Event()
        actions = []

        async def process():
            assert await runtime._finish_owned_job(
                repo, "complete", 919, "w:919", finalization_started=finishing,
            )
            actions.extend(["cleanup", "usage"])

        task = asyncio.create_task(process())
        monitor = asyncio.create_task(runtime._heartbeat_active_job(
            repo, 919, "w:919", "video_send", task,
            interval_seconds=0.01, finalization_started=finishing,
        ))
        try:
            assert await asyncio.to_thread(repo.started.wait, 1)
            await asyncio.wait_for(monitor, timeout=1)
            assert not task.done(), "Handler should still await DB thread"
        finally:
            repo.release.set()
        await asyncio.wait_for(task, timeout=2)
        assert actions == ["cleanup", "usage"]
        assert finishing.is_set()

    asyncio.run(check())


def test_rejected_cas_must_abort_old_worker_before_restart_download_cleanup(monkeypatch):
    """Lost lease cannot delete media while another invocation may be using it."""
    class Repo:
        def mark_job_processing(self, job_id, owner):
            return True

        def heartbeat_job_processing(self, job_id, owner):
            return True

        def finalize_owned_job(self, job_id, owner, action, *args):
            return False

    class Sender:
        async def execute_video_process_from_job(self, **kwargs):
            return {"ok": False, "restart_download": True, "invalid_file_attempts": 1}

    class Limits:
        def __init__(self, *args):
            pass

        def can_execute_admitted_job(self, tenant_id):
            return True, None

        def can_process_video(self, tenant_id):
            return True, None

    calls = []
    monkeypatch.setattr(runtime, "LimitService", Limits)
    monkeypatch.setattr(runtime, "SubscriptionService", lambda repo: object())
    monkeypatch.setattr(runtime, "UsageService", lambda repo: object())
    monkeypatch.setattr(runtime, "_can_start_job", lambda *args: (True, None))
    monkeypatch.setattr(runtime, "enqueue_video_download", lambda *args, **kwargs: calls.append("enqueue"))
    monkeypatch.setattr(runtime, "cleanup_video_artifacts", lambda *args, **kwargs: calls.append("cleanup"))

    async def check():
        return await runtime._process_job(
            Repo(), Sender(), "old:owner", "heavy",
            {"id": 919, "job_type": "video_process",
             "payload_json": {"rule_id": 12, "delivery_id": 1445723}, "attempts": 0},
            policy=runtime.WorkerResourcePolicy(), state=runtime._InFlightState(),
        )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(check())
    assert "cleanup" not in calls


def test_rejected_terminal_cas_does_not_retry_or_continue():
    class Repo:
        def finalize_owned_job(self, job_id, owner, action, *args):
            return False

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runtime._finish_owned_job(
            Repo(), "complete", 4, "lost", finalization_started=Event(),
        ))
