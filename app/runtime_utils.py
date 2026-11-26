from __future__ import annotations

import asyncio


async def run_db(callable_obj, *args, **kwargs):
    """
    Уводит sync DB-работу из event loop в thread pool.
    """
    return await asyncio.to_thread(callable_obj, *args, **kwargs)


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
