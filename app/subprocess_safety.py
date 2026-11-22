"""Cancellation-safe subprocess communication for ffprobe and read-only video probes.

Do not release a worker execution slot with an orphaned child process. A
cancelled caller must not cancel stdout/stderr draining while we stop the child.
"""
from __future__ import annotations

import asyncio
from typing import Any


async def terminate_and_reap(process: Any, *, grace_seconds: float = 5.0) -> None:
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


async def await_critical_cleanup(cleanup_coro) -> None:
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
                await terminate_and_reap(process)
            # Keep draining pipes while the OS child exits; critical for ffprobe
            # that prints large stderr before responding to SIGTERM.
            await reading

        await await_critical_cleanup(cleanup())
        raise



async def iter_stream_lines(
    stream: Any, *, chunk_size: int = 16 * 1024, max_segment_bytes: int = 8 * 1024,
):
    """Drain subprocess streams in bounded chunks, including lines > StreamReader.limit.

    FFmpeg may emit a multi-megabyte stderr line without a newline when killed.
    StreamReader.readline() raises LimitOverrunError and stops draining that PIPE,
    which can deadlock process.wait() even after SIGKILL. This reader never calls
    readline() and yields bounded fragments for abnormally long lines.
    """
    pending = bytearray()
    while True:
        chunk = await stream.read(chunk_size)
        if not chunk:
            if pending:
                yield bytes(pending)
            return
        pending.extend(chunk)
        while True:
            pos = pending.find(b"\n")
            if pos < 0:
                break
            line = bytes(pending[:pos])
            del pending[:pos + 1]
            for start in range(0, len(line), max_segment_bytes):
                yield line[start:start + max_segment_bytes]
            if not line:
                yield b""
        while len(pending) >= max_segment_bytes:
            yield bytes(pending[:max_segment_bytes])
            del pending[:max_segment_bytes]
