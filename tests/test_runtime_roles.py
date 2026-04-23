from __future__ import annotations

import asyncio

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
