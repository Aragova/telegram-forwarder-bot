"""Process-local ownership and lifecycle of the two job-worker loops."""
from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import Any

from app.runtime_roles import RuntimeOwnership
from app.worker_shutdown import drain_job_workers

WorkerFactory = Callable[[asyncio.Event], Coroutine[Any, Any, None]]


class JobWorkerLifecycle:
    def __init__(self, ownership: RuntimeOwnership, logger: logging.Logger) -> None:
        self._ownership = ownership
        self._logger = logger
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._tasks_lock = threading.Lock()
        self._stop_event: asyncio.Event | None = None
        self._stop_lock = asyncio.Lock()
        self._paused = False
        self._stop_requests = 0
        self._changed = asyncio.Event()

    def _task_snapshot(self) -> tuple[asyncio.Task[None], ...]:
        # The dashboard reads from a DB executor thread. Never hold this
        # lock across task creation, task inspection or an await.
        with self._tasks_lock:
            return tuple(self._tasks.values())

    @property
    def live_count(self) -> int:
        return sum(not task.done() for task in self._task_snapshot())

    def ensure_running(self, factories: Mapping[str, WorkerFactory], *, resume: bool = False) -> None:
        # This method contains no await: checking and creating tasks is atomic
        # within the owning event loop. A stopped generation is never reused.
        if not self._ownership.can_start_workers:
            return
        if self._stop_requests or (self._stop_event is not None and self._stop_event.is_set() and self.live_count):
            if resume:
                raise RuntimeError("Предыдущие воркеры ещё завершаются")
            return
        if resume:
            self._paused = False
            self._changed.set()
        if self._paused:
            return
        if self._stop_event is None or self._stop_event.is_set():
            self._stop_event = asyncio.Event()
        for name, factory in factories.items():
            with self._tasks_lock:
                current = self._tasks.get(name)
            if current is not None and not current.done():
                continue
            task = asyncio.create_task(factory(self._stop_event), name=name)
            with self._tasks_lock:
                self._tasks[name] = task
            task.add_done_callback(self._report_exit)

    def _report_exit(self, task: asyncio.Task[None]) -> None:
        self._changed.set()
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                self._logger.error("JOB_WORKER_EXIT | name=%s", task.get_name(), exc_info=error)

    async def supervise(
        self, start: Callable[[], Awaitable[None]], *, retry_delay_sec: float = 10.0,
    ) -> None:
        """Wait for lifecycle changes; rate-limit recovery attempts after exits/errors."""
        loop = asyncio.get_running_loop()
        retry_at = 0.0
        while self._ownership.can_start_workers:
            self._changed.clear()
            if self._paused or self._stop_requests:
                await self._changed.wait()
                continue
            remaining = retry_at - loop.time()
            if remaining > 0:
                try:
                    await asyncio.wait_for(self._changed.wait(), timeout=remaining)
                except TimeoutError:
                    pass
                continue
            try:
                await start()
            except Exception:
                self._logger.exception("JOB_SUPERVISOR_ERROR")
                retry_at = loop.time() + retry_delay_sec
                continue
            retry_at = loop.time() + retry_delay_sec
            if self._ownership.can_start_workers:
                await self._changed.wait()

    async def stop(self, *, timeout_sec: float) -> None:
        self._paused = True
        self._stop_requests += 1
        self._changed.set()
        try:
            async with self._stop_lock:
                await drain_job_workers(
                    tasks=self._task_snapshot(), stop_event=self._stop_event,
                    timeout_sec=timeout_sec, logger=self._logger,
                )
                if self.live_count:
                    raise RuntimeError("Не завершены активные job workers")
                with self._tasks_lock:
                    self._tasks.clear()
                self._stop_event = None
        finally:
            self._stop_requests -= 1
            self._changed.set()
