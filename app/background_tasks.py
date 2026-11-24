from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from typing import Any
from weakref import WeakSet

LOGGER = logging.getLogger("forwarder.background_tasks")

# A task needs a strong reference until it finishes.
_RUNNING_TASKS: set[asyncio.Task[Any]] = set()
_STOPPING_LOOPS = WeakSet()


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
    if task.get_loop() in _STOPPING_LOOPS:
        task.cancel()
    return task


async def stop_background_tasks() -> None:
    """Drain this loop's tracked tasks, including tasks spawned during cancellation."""
    loop = asyncio.get_running_loop()
    _STOPPING_LOOPS.add(loop)
    current = asyncio.current_task()
    while True:
        tasks = [task for task in _RUNNING_TASKS
                 if task.get_loop() is loop and task is not current and not task.done()]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
