"""Cancellation-safe subprocess communication for ffprobe and read-only video probes.

Do not release a worker execution slot with an orphaned child process. A
cancelled caller must not cancel stdout/stderr draining while we stop the child.
"""
from __future__ import annotations

import asyncio
from typing import Any


async def _terminate_and_reap(process: Any, *, grace_seconds: float = 5.0) -> None:
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(process.wait(), timeout=grace_seconds)
    except asyncio.TimeoutError:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()


async def _finish_cleanup_even_if_cancelled(cleanup_coro) -> None:
    cleanup_task = asyncio.create_task(cleanup_coro)
    cancelled_again = False
    while True:
        try:
            await asyncio.shield(cleanup_task)
            break
        except asyncio.CancelledError:
            cancelled_again = True
            if cleanup_task.done():
                break
    cleanup_task.result()
    if cancelled_again:
        raise asyncio.CancelledError


async def communicate_safely(process: Any, *, timeout_seconds: float = 180.0):
    """Read stdout/stderr while guaranteeing kill/reap under repeated cancellation."""
    reading = asyncio.create_task(process.communicate())
    try:
        return await asyncio.wait_for(
            asyncio.shield(reading), timeout=max(1.0, float(timeout_seconds))
        )
    except BaseException:
        async def cleanup():
            if process.returncode is None:
                await _terminate_and_reap(process)
            # Keep draining pipes while the OS child exits; critical for ffprobe
            # that prints large stderr before responding to SIGTERM.
            await reading

        await _finish_cleanup_even_if_cancelled(cleanup())
        raise
