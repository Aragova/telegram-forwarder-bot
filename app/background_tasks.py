from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from typing import Any

LOGGER = logging.getLogger("forwarder.background_tasks")

# A task needs a strong reference until it finishes.
_RUNNING_TASKS: set[asyncio.Task[Any]] = set()


def start_background_task(awaitable: Awaitable[Any], *, name: str) -> asyncio.Task[Any]:
    """Track a fire-and-forget task and log unexpected failures."""
    task = asyncio.create_task(awaitable, name=name)
    _RUNNING_TASKS.add(task)

    def _finished(done: asyncio.Task[Any]) -> None:
        _RUNNING_TASKS.discard(done)
        if done.cancelled():
            return
        try:
            done.result()
        except Exception:
            LOGGER.exception("Фоновая задача %s завершилась с ошибкой", name)

    task.add_done_callback(_finished)
    return task

async def shutdown_background_tasks() -> None:
    tasks = [task for task in _RUNNING_TASKS if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
