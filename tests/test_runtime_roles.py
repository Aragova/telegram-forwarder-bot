from __future__ import annotations

import asyncio
import ast
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.runtime_roles import normalize_runtime_role, run_role


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
    tree = ast.parse((Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8"))
    names = {
        "_start_bot_role", "_start_scheduler_role", "_start_worker_role",
        "_start_all_role", "_prepare_polling_runtime", "ensure_rule_workers",
        "start_job_workers_runtime",
    }
    nodes = [node for node in tree.body if (
        isinstance(node, ast.AsyncFunctionDef) and node.name in names
    ) or (
        isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "workers_runtime_enabled"
            for target in node.targets
        )
    )]
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
        ns = {
            "asyncio": asyncio, "logger": logging.getLogger(__name__),
            "bot": raw_bot, "dp": dispatcher, "db": object(),
            "sender_service": object(), "posting_active": False,
            "job_worker_tasks": [], "job_workers_stop_event": None,
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
            "ReactionWorker": lambda **kwargs: SimpleNamespace(run_forever=AsyncMock()),
            "run_light_worker": light, "run_heavy_worker": heavy,
        }
        _load_runtime_functions(ns)
        try:
            # No worker may be created until a runtime role explicitly enables it.
            await try_start_workers()
            assert ns["job_worker_tasks"] == []
            assert ns["job_workers_stop_event"] is None
            await run_role(
                role, run_bot=ns["_start_bot_role"],
                run_scheduler=ns["_start_scheduler_role"],
                run_worker=ns["_start_worker_role"], run_all=ns["_start_all_role"],
            )
            assert ns["workers_runtime_enabled"] is owns_workers
            assert dispatcher.start_polling.await_count == int(owns_polling)
            assert raw_bot.delete_webhook.await_count == int(owns_polling)
            assert raw_bot.set_my_commands.await_count == int(owns_polling)
            assert light.call_count == heavy.call_count == int(owns_workers)
            assert len(ns["job_worker_tasks"]) == (2 if owns_workers else 0)
            if owns_workers:
                await try_start_workers()
                assert light.call_count == heavy.call_count == 1
            else:
                assert ns["job_workers_stop_event"] is None
        finally:
            if ns["job_workers_stop_event"] is not None:
                ns["job_workers_stop_event"].set()
            tasks = ns["job_worker_tasks"] + [
                ns[key] for key in ("scheduler_runtime_task", "job_watchdog_task", "reaction_worker_task")
                if ns[key] is not None
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())
