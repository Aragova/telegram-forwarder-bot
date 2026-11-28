import asyncio
from unittest.mock import Mock

import pytest

from app.rescan_runtime import rescan_db
from app.rescan_safety import RescanBlocked, RescanSourceWriter
from app import scheduler_runtime


def test_rescan_source_writer_forwards_fence_and_never_replaces_deliveries():
    repo = Mock()
    writer = RescanSourceWriter(repo, 94, "owner")
    rows = [(1, "-1001", None, {}, None)]
    writer.save_post_batch(rows, preserve_existing_deliveries=True)
    repo.save_post_batch.assert_called_once_with(
        rows, preserve_existing_deliveries=True, rescan_owner=(94, "owner"),
    )
    with pytest.raises(RescanBlocked):
        writer.save_post_batch(rows)
    assert repo.save_post_batch.call_count == 1


def test_rescan_db_cancellation_waits_for_transaction():
    async def scenario():
        started, finish = asyncio.Event(), asyncio.Event()
        async def run_db(func):
            started.set()
            await finish.wait()
            return func()
        write = Mock()
        task = asyncio.create_task(rescan_db(run_db, write))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        write.assert_called_once()
    asyncio.run(scenario())


def test_scheduler_cancellation_joins_running_tick_without_starting_next_rule(monkeypatch):
    async def scenario():
        started, finished = asyncio.Event(), asyncio.Event()
        release = asyncio.Event()
        ticks = []
        async def tick(repo, *, enabled, stop_event):
            ticks.append(stop_event)
            started.set()
            await release.wait()
            assert stop_event.is_set()
            finished.set()
            return {"created": 1}
        monkeypatch.setattr(scheduler_runtime, "scheduler_tick", tick)
        task = asyncio.create_task(scheduler_runtime.run_scheduler_loop(object()))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and ticks[0].is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and len(ticks) == 1
    asyncio.run(scenario())


def test_recovery_stop_event_wakes_supervisor_without_waiting_for_period():
    import ast
    import logging
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    source = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(n for n in source.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_rescan_recovery_loop")

    async def scenario():
        entered = asyncio.Event()
        stop = asyncio.Event()
        resume_query = asyncio.Event()
        async def read(*args, **kwargs):
            entered.set()
            await resume_query.wait()
            return []
        ns = {"asyncio": asyncio, "runtime_ownership": SimpleNamespace(can_poll=True),
              "db": SimpleNamespace(list_expired_rule_rescans=Mock()),
              "run_db": AsyncMock(side_effect=read), "logger": logging.getLogger(__name__),
              "rescan_recovery_job": None}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "bot.py", "exec"), ns)
        supervisor = asyncio.create_task(ns["_rescan_recovery_loop"](stop))
        await asyncio.wait_for(entered.wait(), timeout=1)
        stop.set()
        resume_query.set()
        await asyncio.wait_for(supervisor, timeout=0.5)
        ns["run_db"].assert_awaited_once_with(ns["db"].list_expired_rule_rescans, phase="rebuilt")

    asyncio.run(scenario())


def test_rebuilt_recovery_runs_while_history_is_still_reading():
    import ast
    import logging
    from pathlib import Path
    from types import SimpleNamespace

    source = ast.parse(Path("bot.py").read_text(encoding="utf-8"))
    fn = next(n for n in source.body if isinstance(n, ast.AsyncFunctionDef)
              and n.name == "_rescan_recovery_loop")

    async def scenario():
        stop = asyncio.Event()
        scanning = asyncio.Event()
        released = asyncio.Event()
        cancelled = asyncio.Event()

        async def read(func, *, phase, **kwargs):
            if phase == "rebuilt":
                return [2] if scanning.is_set() else []
            return [1]

        async def job(*, rule_id, recovery, recovery_phase):
            assert recovery
            if recovery_phase == "scanning":
                assert rule_id == 1
                scanning.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            else:
                assert rule_id == 2 and scanning.is_set() and not cancelled.is_set()
                released.set()
                stop.set()

        async def wait_for(awaitable, *, timeout):
            awaitable.close()
            await asyncio.sleep(0)
            if not stop.is_set():
                raise TimeoutError

        ns = {"asyncio": SimpleNamespace(gather=asyncio.gather, wait_for=wait_for,
                                         Event=asyncio.Event),
              "runtime_ownership": SimpleNamespace(can_poll=True),
              "db": SimpleNamespace(list_expired_rule_rescans=Mock()),
              "run_db": read, "logger": logging.getLogger(__name__),
              "rescan_recovery_job": None, "_run_rescan_rule_job": job,
              "create_background_task": asyncio.create_task}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "bot.py", "exec"), ns)
        await asyncio.wait_for(ns["_rescan_recovery_loop"](stop), timeout=2)
        assert released.is_set() and cancelled.is_set()
        assert ns["rescan_recovery_job"] is None

    asyncio.run(scenario())


def test_heartbeat_exits_when_in_flight_renewal_fails_during_context_exit(monkeypatch):
    from app import rescan_runtime

    monkeypatch.setattr(rescan_runtime, "_HEARTBEAT_INTERVAL_SEC", 0.001)

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()
        leaving = asyncio.Event()
        calls = 0

        async def run_db(func, *args):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            raise RuntimeError("database connection lost")

        async def owner():
            async with rescan_runtime.maintain_rescan_lease(Mock(), run_db, 94, "owner"):
                await entered.wait()
                leaving.set()

        task = asyncio.create_task(owner())
        try:
            await asyncio.wait_for(leaving.wait(), timeout=1)
            assert not task.done()
            release.set()
            await asyncio.wait_for(asyncio.shield(task), timeout=1)
            assert calls == 1
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_heartbeat_retries_transient_failure_while_context_active(monkeypatch):
    from app import rescan_runtime

    monkeypatch.setattr(rescan_runtime, "_HEARTBEAT_INTERVAL_SEC", 0.001)

    async def scenario():
        recovered = asyncio.Event()
        calls = 0

        async def run_db(func, *args):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary error")
            recovered.set()
            return True

        async with rescan_runtime.maintain_rescan_lease(Mock(), run_db, 94, "owner"):
            await asyncio.wait_for(recovered.wait(), timeout=1)
        assert calls >= 2

    asyncio.run(scenario())
