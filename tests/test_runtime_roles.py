from __future__ import annotations

import asyncio
import ast
import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.runtime_roles import ForwardingControlUnavailable, RuntimeOwnership, normalize_runtime_role, run_role
from app.job_worker_lifecycle import JobWorkerLifecycle
from app.runtime_utils import create_background_task


def test_bot_role_runs_only_bot_callable():
    calls: list[str] = []

    async def run_bot():
        calls.append("bot")

    async def run_scheduler():
        calls.append("scheduler")

    async def run_worker():
        calls.append("worker")

    async def run_all():
        calls.append("all")

    asyncio.run(
        run_role(
            "bot",
            run_bot=run_bot,
            run_scheduler=run_scheduler,
            run_worker=run_worker,
            run_all=run_all,
        )
    )

    assert calls == ["bot"]


def test_worker_role_does_not_run_polling_or_scheduler():
    calls: list[str] = []

    async def run_bot():
        calls.append("bot")

    async def run_scheduler():
        calls.append("scheduler")

    async def run_worker():
        calls.append("worker")

    async def run_all():
        calls.append("all")

    asyncio.run(
        run_role(
            "worker",
            run_bot=run_bot,
            run_scheduler=run_scheduler,
            run_worker=run_worker,
            run_all=run_all,
        )
    )

    assert calls == ["worker"]


def test_scheduler_role_does_not_run_polling_or_worker():
    calls: list[str] = []

    async def run_bot():
        calls.append("bot")

    async def run_scheduler():
        calls.append("scheduler")

    async def run_worker():
        calls.append("worker")

    async def run_all():
        calls.append("all")

    asyncio.run(
        run_role(
            "scheduler",
            run_bot=run_bot,
            run_scheduler=run_scheduler,
            run_worker=run_worker,
            run_all=run_all,
        )
    )

    assert calls == ["scheduler"]


def test_all_role_uses_legacy_all_path_for_compatibility():
    calls: list[str] = []

    async def run_bot():
        calls.append("bot")

    async def run_scheduler():
        calls.append("scheduler")

    async def run_worker():
        calls.append("worker")

    async def run_all():
        calls.append("all")

    asyncio.run(
        run_role(
            "all",
            run_bot=run_bot,
            run_scheduler=run_scheduler,
            run_worker=run_worker,
            run_all=run_all,
        )
    )

    assert calls == ["all"]


def test_normalize_runtime_role_defaults_to_all_for_invalid_value():
    assert normalize_runtime_role("bad-role") == "all"
    assert normalize_runtime_role(None) == "all"


# Execute the actual startup functions without importing bot.py, which configures
# production clients at module import. The sender wiring test covers that part.
def _load_runtime_functions(namespace):
    namespace.setdefault("create_background_task", create_background_task)
    tree = ast.parse((Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8"))
    names = {
        "_start_bot_role", "_start_scheduler_role", "_start_worker_role",
        "_start_all_role", "_prepare_polling_runtime", "ensure_rule_workers",
        "start_job_workers_runtime", "_supervise_job_workers", "stop_job_workers_runtime",
        "start_forwarding", "stop_forwarding", "stop_all_workers",
    }
    names.add("_require_forwarding_control")
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "bot.py", "exec"), namespace)


@pytest.mark.parametrize(
    ("role", "owns_polling", "owns_workers"),
    [("bot", True, False), ("ui", True, False), ("scheduler", False, False),
     ("worker", False, True), ("all", True, True)],
)
def test_runtime_role_owns_polling_and_job_workers(role, owns_polling, owns_workers):
    async def scenario():
        async def worker(*args, stop_event, **kwargs):
            await stop_event.wait()

        async def try_start_workers(*args, **kwargs):
            await ns["ensure_rule_workers"]()
            await ns["ensure_rule_workers"]()

        async def polling(*args, **kwargs):
            raw_bot.delete_webhook.assert_awaited_once_with(
                drop_pending_updates=False, request_timeout=90,
            )
            await try_start_workers()

        def discard_background(coro, **kwargs):
            coro.close()

        raw_bot = SimpleNamespace(delete_webhook=AsyncMock(), set_my_commands=AsyncMock())
        dispatcher = SimpleNamespace(start_polling=AsyncMock(side_effect=polling))
        light, heavy = AsyncMock(side_effect=worker), AsyncMock(side_effect=worker)
        ownership = RuntimeOwnership()
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        ns = {
            "asyncio": asyncio, "logger": logging.getLogger(__name__),
            "bot": raw_bot, "dp": dispatcher, "db": object(),
            "sender_service": object(), "posting_active": False,
            "runtime_ownership": ownership, "job_workers": lifecycle,
            "WORKER_RESOURCE_POLICY": SimpleNamespace(graceful_shutdown_timeout_sec=0),
            "scheduler_runtime_task": None, "job_watchdog_task": None,
            "reaction_worker_task": None,
            "DEFAULT_TELEGRAM_COMMANDS": [], "BotCommandScopeDefault": object,
            "_init_db_runtime": AsyncMock(), "_init_sender_runtime": AsyncMock(),
            "_run_worker_role_loop": try_start_workers,
            "run_scheduler_loop": try_start_workers,
            "run_watchdog_loop": AsyncMock(), "heartbeat_loop": AsyncMock(),
            "watchdog_loop": AsyncMock(), "start_background_task": discard_background,
            "_start_repost_campaign_runtime_tasks": lambda **kwargs: None,
            "_start_channel_title_sync_runtime": lambda: None,
            "_start_rescan_recovery_runtime": lambda: None,
            "ReactionWorker": lambda **kwargs: SimpleNamespace(run_forever=AsyncMock()),
            "run_light_worker": light, "run_heavy_worker": heavy,
        }
        _load_runtime_functions(ns)
        try:
            # No worker may be created until a runtime role explicitly enables it.
            await try_start_workers()
            assert lifecycle.live_count == 0
            with pytest.raises(RuntimeError, match="polling"):
                await ns["_prepare_polling_runtime"]()
            await run_role(
                role, run_bot=ns["_start_bot_role"],
                run_scheduler=ns["_start_scheduler_role"],
                run_worker=ns["_start_worker_role"], run_all=ns["_start_all_role"],
            )
            assert ownership.can_start_workers is owns_workers
            assert ownership.can_poll is owns_polling
            assert dispatcher.start_polling.await_count == int(owns_polling)
            assert raw_bot.delete_webhook.await_count == int(owns_polling)
            assert raw_bot.set_my_commands.await_count == int(owns_polling)
            assert light.call_count == heavy.call_count == int(owns_workers)
            assert lifecycle.live_count == (2 if owns_workers else 0)
            if not owns_polling:
                with pytest.raises(RuntimeError, match="polling"):
                    await ns["_prepare_polling_runtime"]()
            if owns_workers:
                await try_start_workers()
                assert light.call_count == heavy.call_count == 1
        finally:
            ownership.begin_shutdown()
            await lifecycle.stop(timeout_sec=1)
            tasks = [
                ns[key] for key in ("scheduler_runtime_task", "job_watchdog_task", "reaction_worker_task")
                if ns[key] is not None
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_runtime_ownership_cannot_change_role_or_reopen_after_shutdown():
    ownership = RuntimeOwnership()
    ownership.bind("ui")
    ownership.bind("bot")
    with pytest.raises(RuntimeError):
        ownership.bind("worker")
    ownership.begin_shutdown()
    assert not ownership.can_poll and not ownership.can_start_workers
    with pytest.raises(RuntimeError):
        ownership.bind("bot")


def test_worker_lifecycle_recovers_only_failed_worker_and_respects_pause():
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        failed = asyncio.Event()
        attempts = {"light": 0, "heavy": 0}

        async def light(stop):
            attempts["light"] += 1
            if attempts["light"] == 1:
                failed.set()
                raise RuntimeError("temporary failure")
            await stop.wait()

        async def heavy(stop):
            attempts["heavy"] += 1
            await stop.wait()

        factories = {"light": light, "heavy": heavy}
        lifecycle.ensure_running(factories)
        await failed.wait()
        lifecycle.ensure_running(factories)
        await asyncio.sleep(0)
        assert attempts == {"light": 2, "heavy": 1}
        assert lifecycle.live_count == 2
        await lifecycle.stop(timeout_sec=1)
        lifecycle.ensure_running(factories)
        assert lifecycle.live_count == 0
        lifecycle.ensure_running(factories, resume=True)
        assert lifecycle.live_count == 2
        ownership.begin_shutdown()
        await lifecycle.stop(timeout_sec=1)
        lifecycle.ensure_running(factories, resume=True)
        assert lifecycle.live_count == 0

    asyncio.run(scenario())


def test_worker_lifecycle_never_restarts_while_draining(monkeypatch):
    import app.job_worker_lifecycle as module

    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("worker")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        draining = asyncio.Event()
        release = asyncio.Event()

        async def worker(stop):
            await stop.wait()
            await release.wait()

        async def incomplete_drain(*, tasks, stop_event, **kwargs):
            stop_event.set()
            draining.set()
            await asyncio.sleep(0)

        monkeypatch.setattr(module, "drain_job_workers", incomplete_drain)
        factories = {"light": worker, "heavy": worker}
        lifecycle.ensure_running(factories)
        stop = asyncio.create_task(lifecycle.stop(timeout_sec=1))
        await draining.wait()
        with pytest.raises(RuntimeError, match="завершаются"):
            lifecycle.ensure_running(factories, resume=True)
        with pytest.raises(RuntimeError, match="Не завершены"):
            await stop
        lifecycle.ensure_running(factories)
        with pytest.raises(RuntimeError, match="завершаются"):
            lifecycle.ensure_running(factories, resume=True)
        assert lifecycle.live_count == 2
        release.set()
        await asyncio.sleep(0)
        await lifecycle.stop(timeout_sec=1)
        assert lifecycle.live_count == 0

    asyncio.run(scenario())


def test_worker_lifecycle_serializes_overlapping_stop_requests(monkeypatch):
    import app.job_worker_lifecycle as module

    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        entered = asyncio.Event()
        release = asyncio.Event()

        async def worker(stop):
            await stop.wait()

        async def drain(*, tasks, stop_event, **kwargs):
            entered.set()
            await release.wait()
            if stop_event is not None:
                stop_event.set()
            await asyncio.gather(*tasks)

        monkeypatch.setattr(module, "drain_job_workers", drain)
        factories = {"light": worker, "heavy": worker}
        lifecycle.ensure_running(factories)
        first = asyncio.create_task(lifecycle.stop(timeout_sec=1))
        await entered.wait()
        second = asyncio.create_task(lifecycle.stop(timeout_sec=1))
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="завершаются"):
            lifecycle.ensure_running(factories, resume=True)
        release.set()
        await asyncio.gather(first, second)
        assert lifecycle.live_count == 0
        lifecycle.ensure_running(factories, resume=True)
        assert lifecycle.live_count == 2
        await lifecycle.stop(timeout_sec=1)

    asyncio.run(scenario())


def test_polling_retries_and_only_continues_if_webhook_is_inactive(monkeypatch):
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("bot")
        raw_bot = SimpleNamespace(
            delete_webhook=AsyncMock(side_effect=TimeoutError),
            get_webhook_info=AsyncMock(return_value=SimpleNamespace(url="")),
            set_my_commands=AsyncMock(),
        )
        ns = {
            "runtime_ownership": ownership, "bot": raw_bot,
            "asyncio": SimpleNamespace(sleep=AsyncMock()),
            "logger": logging.getLogger(__name__),
            "DEFAULT_TELEGRAM_COMMANDS": [], "BotCommandScopeDefault": object,
        }
        _load_runtime_functions(ns)
        await ns["_prepare_polling_runtime"]()
        assert raw_bot.delete_webhook.await_count == 3
        assert all(call.kwargs["drop_pending_updates"] is False for call in raw_bot.delete_webhook.await_args_list)
        raw_bot.set_my_commands.assert_awaited_once()
        raw_bot.get_webhook_info.return_value = SimpleNamespace(url="https://example.invalid")
        with pytest.raises(RuntimeError, match="webhook"):
            await ns["_prepare_polling_runtime"]()
        assert raw_bot.set_my_commands.await_count == 1

    asyncio.run(scenario())


def test_supervisor_delegates_to_lifecycle():
    async def scenario():
        start = AsyncMock()
        lifecycle = SimpleNamespace(supervise=AsyncMock())
        ns = {"job_workers": lifecycle}
        _load_runtime_functions(ns)
        ns["start_job_workers_runtime"] = start
        await ns["_supervise_job_workers"]()
        lifecycle.supervise.assert_awaited_once_with(start)

    asyncio.run(scenario())


@pytest.mark.parametrize("role", ["bot", "scheduler", "worker"])
def test_split_forwarding_controls_never_report_local_success(role):
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind(role)
        ns = {
            "runtime_ownership": ownership, "posting_active": False,
            "ForwardingControlUnavailable": ForwardingControlUnavailable,
        }
        _load_runtime_functions(ns)
        for name in ("start_forwarding", "stop_forwarding"):
            with pytest.raises(ForwardingControlUnavailable):
                await ns[name]()
        assert ns["posting_active"] is False

    asyncio.run(scenario())


def test_failed_stop_can_be_retried_even_with_scheduler_paused():
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = SimpleNamespace(live_count=2)
        async def drain():
            if lifecycle.live_count == 2:
                lifecycle.live_count = 1
                raise RuntimeError("incomplete drain")
            lifecycle.live_count = 0
        ns = {
            "runtime_ownership": ownership, "posting_active": True,
            "job_workers": lifecycle, "ForwardingControlUnavailable": ForwardingControlUnavailable,
        }
        _load_runtime_functions(ns)
        ns["stop_all_workers"] = drain
        with pytest.raises(RuntimeError, match="incomplete"):
            await ns["stop_forwarding"]()
        assert ns["posting_active"] is False
        assert await ns["stop_forwarding"]() is True
        assert await ns["stop_forwarding"]() is False

    asyncio.run(scenario())


def test_live_count_snapshot_during_worker_start():
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        reading = threading.Event()
        release = threading.Event()
        loop_thread = threading.get_ident()

        async def worker(stop):
            await stop.wait()

        class BlockingTask(asyncio.Task):
            def done(self):
                if threading.get_ident() != loop_thread:
                    reading.set()
                    if not release.wait(5):
                        raise RuntimeError("snapshot reader timed out")
                return super().done()

        loop = asyncio.get_running_loop()
        original_factory = loop.get_task_factory()
        loop.set_task_factory(lambda loop, coro, **kwargs: BlockingTask(coro, loop=loop, **kwargs))
        try:
            lifecycle.ensure_running({"light": worker})
        finally:
            loop.set_task_factory(original_factory)
        reader = asyncio.create_task(asyncio.to_thread(lambda: lifecycle.live_count))
        try:
            assert await asyncio.to_thread(reading.wait, 5)
            lifecycle.ensure_running({"heavy": worker})
            release.set()
            assert await reader == 1
            assert lifecycle.live_count == 2
        finally:
            release.set()
            await asyncio.gather(reader, return_exceptions=True)
            await lifecycle.stop(timeout_sec=1)
        assert lifecycle.live_count == 0

    asyncio.run(scenario())


def test_forwarding_commands_from_previous_runtime_are_rejected():
    ownership = RuntimeOwnership()
    before_start = datetime.now(timezone.utc) - timedelta(seconds=2)
    ownership.bind("all")
    with pytest.raises(ForwardingControlUnavailable, match="устарела"):
        ownership.validate_forwarding_command(before_start)
    ownership.validate_forwarding_command(datetime.now(timezone.utc).replace(microsecond=0))
    with pytest.raises(ForwardingControlUnavailable, match="устарела"):
        ownership.validate_forwarding_command(datetime.now())
    ownership.begin_shutdown()
    with pytest.raises(ForwardingControlUnavailable):
        ownership.validate_forwarding_command(datetime.now(timezone.utc))
