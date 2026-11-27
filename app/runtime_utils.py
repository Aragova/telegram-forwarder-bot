from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from functools import partial
from threading import Lock
from weakref import WeakKeyDictionary


_UI_WORKERS = 4
_ui_scope = ContextVar("ui_db_scope", default=False)
_executor_lock = Lock()
_ui_executor: ThreadPoolExecutor | None = None
_admission = WeakKeyDictionary()


def _get_ui_executor() -> ThreadPoolExecutor:
    global _ui_executor
    with _executor_lock:
        if _ui_executor is None:
            _ui_executor = ThreadPoolExecutor(max_workers=_UI_WORKERS, thread_name_prefix="vimi-ui-db")
        return _ui_executor


async def run_ui_db(callable_obj, *args, **kwargs):
    """Reserve UI capacity independently of the default background executor."""
    loop = asyncio.get_running_loop()
    slots = _admission.get(loop)
    if slots is None:
        slots = asyncio.Semaphore(_UI_WORKERS)
        _admission[loop] = slots
    await slots.acquire()
    try:
        context = copy_context()
        future = _get_ui_executor().submit(context.run, partial(callable_obj, *args, **kwargs))
        wrapped = asyncio.wrap_future(future)
    except BaseException:
        slots.release()
        raise

    def finished(result):
        slots.release()
        if not result.cancelled():
            result.exception()

    wrapped.add_done_callback(finished)
    try:
        return await asyncio.shield(wrapped)
    except asyncio.CancelledError:
        # A queued call may be cancelled; a running call owns its slot until
        # it actually returns. Never replay a database write.
        future.cancel()
        raise


async def run_db(callable_obj, *args, **kwargs):
    """Use the dedicated UI lane only in a foreground private-chat handler."""
    if _ui_scope.get():
        return await run_ui_db(callable_obj, *args, **kwargs)
    return await asyncio.to_thread(callable_obj, *args, **kwargs)


class UiDatabaseMiddleware:
    """Apply the UI lane to user callbacks/messages, not channel ingestion."""

    async def __call__(self, handler, event, data):
        message = getattr(event, "message", event)
        chat = getattr(message, "chat", None)
        token = _ui_scope.set(getattr(chat, "type", None) == "private")
        try:
            return await handler(event, data)
        finally:
            _ui_scope.reset(token)


def create_background_task(coro, *, name=None):
    """Detached operations must not inherit foreground executor capacity."""
    context = copy_context()
    context.run(_ui_scope.set, False)
    return asyncio.create_task(coro, name=name, context=context)


def shutdown_ui_db_executor() -> None:
    """Called after asyncio.run; drain UI work before closing DB connections."""
    global _ui_executor
    with _executor_lock:
        executor, _ui_executor = _ui_executor, None
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=False)
    _admission.clear()


async def wait_for_db_task(task):
    """Finish an already-started DB write before propagating caller cancellation."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            if not task.done():
                # A second cancellation must not abandon the underlying write.
                await wait_for_db_task(task)
            elif not task.cancelled():
                task.exception()  # Retrieve a concurrent failure before propagating cancellation.
        finally:
            raise
