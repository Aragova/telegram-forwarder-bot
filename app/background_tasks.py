from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable
from typing import Any
from weakref import WeakSet

LOGGER = logging.getLogger("forwarder.background_tasks")

# A task needs a strong reference until it finishes.
_RUNNING_TASKS: set[asyncio.Task[Any]] = set()
_STOPPING_LOOPS = WeakSet()
_DRAIN_ONLY_TASKS: set[asyncio.Task[Any]] = set()
_TASK_LOCK = threading.Lock()


def start_background_task(awaitable: Awaitable[Any], *, name: str, drain_only: bool = False) -> asyncio.Task[Any]:
    """Track a fire-and-forget task and log unexpected failures."""
    task = asyncio.create_task(awaitable, name=name)
    with _TASK_LOCK:
        _RUNNING_TASKS.add(task)
        if drain_only:
            _DRAIN_ONLY_TASKS.add(task)
        stopping = task.get_loop() in _STOPPING_LOOPS

    def _finished(done: asyncio.Task[Any]) -> None:
        with _TASK_LOCK:
            _RUNNING_TASKS.discard(done)
            _DRAIN_ONLY_TASKS.discard(done)
        if done.cancelled():
            return
        try:
            done.result()
        except Exception:
            LOGGER.exception("Фоновая задача %s завершилась с ошибкой", name)

    task.add_done_callback(_finished)
    if stopping and not drain_only:
        task.cancel()
    return task


async def stop_background_tasks() -> None:
    """Drain this loop's tracked tasks, including tasks spawned during cancellation."""
    loop = asyncio.get_running_loop()
    with _TASK_LOCK:
        _STOPPING_LOOPS.add(loop)
    current = asyncio.current_task()
    while True:
        with _TASK_LOCK:
            tasks = [task for task in _RUNNING_TASKS
                     if task.get_loop() is loop and task is not current and not task.done()]
            to_cancel = [task for task in tasks if task not in _DRAIN_ONLY_TASKS]
        if not tasks:
            return
        for task in to_cancel:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
