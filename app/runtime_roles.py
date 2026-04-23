from __future__ import annotations

from typing import Awaitable, Callable

RuntimeCallable = Callable[[], Awaitable[None]]

_ALLOWED_ROLES = {"bot", "scheduler", "worker", "all"}


def normalize_runtime_role(role: str | None) -> str:
    normalized = (role or "all").strip().lower()
    if normalized not in _ALLOWED_ROLES:
        return "all"
    return normalized


async def run_role(
    role: str,
    *,
    run_bot: RuntimeCallable,
    run_scheduler: RuntimeCallable,
    run_worker: RuntimeCallable,
    run_all: RuntimeCallable,
) -> None:
    normalized = normalize_runtime_role(role)

    if normalized == "bot":
        await run_bot()
        return

    if normalized == "scheduler":
        await run_scheduler()
        return

    if normalized == "worker":
        await run_worker()
        return

    await run_all()
