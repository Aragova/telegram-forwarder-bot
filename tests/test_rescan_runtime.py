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
        async def read(*args):
            entered.set()
            return []
        ns = {"asyncio": asyncio, "runtime_ownership": SimpleNamespace(can_poll=True),
              "db": SimpleNamespace(list_expired_rule_rescans=Mock()),
              "run_db": AsyncMock(side_effect=read), "logger": logging.getLogger(__name__),
              "rescan_recovery_job": None}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "bot.py", "exec"), ns)
        supervisor = asyncio.create_task(ns["_rescan_recovery_loop"](stop))
        await entered.wait()
        stop.set()
        await asyncio.wait_for(supervisor, timeout=0.5)
        ns["run_db"].assert_awaited_once()

    asyncio.run(scenario())
