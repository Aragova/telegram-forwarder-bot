from __future__ import annotations

import asyncio


async def run_db(callable_obj, *args, **kwargs):
    """
    Уводит sync DB-работу из event loop в thread pool.
    """
    return await asyncio.to_thread(callable_obj, *args, **kwargs)
