"""Drain worker loops before closing Telegram clients.

A cancelled Telegram write is ambiguous, not a verified failure.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from app.runtime_utils import wait_for_db_task

_CANCEL_SETTLE_TIMEOUT_SEC = 10.0


async def drain_job_workers(
    *, tasks: Iterable[asyncio.Task], stop_event: asyncio.Event | None,
    timeout_sec: float, logger: logging.Logger,
) -> None:
    active = [task for task in tasks if not task.done()]
    if stop_event is not None:
        stop_event.set()
    logger.info("WORKER_DRAIN_START | active=%s | wait_sec=%s", len(active), timeout_sec)
    if not active:
        return
    done, pending = await asyncio.wait(active, timeout=max(0.01, float(timeout_sec)))
    if done:
        await asyncio.gather(*done, return_exceptions=True)
    if not pending:
        logger.info("WORKER_DRAIN_COMPLETE | workers=%s", len(active))
        return
    logger.error("WORKER_DRAIN_TIMEOUT | pending=%s | action=cancel_and_reconcile", len(pending))
    for task in pending:
        task.cancel()
    done_after, still_pending = await asyncio.wait(pending, timeout=_CANCEL_SETTLE_TIMEOUT_SEC)
    if done_after:
        await asyncio.gather(*done_after, return_exceptions=True)
    if still_pending:
        logger.error("WORKER_DRAIN_CANCEL_INCOMPLETE | workers=%s", len(still_pending))

async def drain_in_flight(
    tasks: Iterable[asyncio.Task], *, timeout_sec: float, logger: logging.Logger, queue: str,
) -> None:
    """A loop cannot report completion while its child sends still own resources.

    If the outer drain cancels the loop, cancel the children and keep the loop
    alive until they settle. Do not release a processing job or infer a failed
    Telegram write from cancellation. The durable watchdog/ledger own recovery.
    """
    children = tuple(tasks)
    if not children:
        return
    joined = asyncio.gather(*children, return_exceptions=True)
    cancellation_requested = False
    try:
        _, pending = await asyncio.wait(children, timeout=max(0.01, float(timeout_sec)))
        if pending:
            cancellation_requested = True
            logger.warning("WORKER_CHILD_DRAIN_CANCEL | queue=%s | unfinished=%s", queue, len(pending))
            for task in pending:
                task.cancel()
        await asyncio.shield(joined)
    except asyncio.CancelledError:
        if not cancellation_requested:
            for task in children:
                if not task.done():
                    task.cancel()
        # Reuse the existing cancellation-safe join. The outer lifecycle
        # has its own bounded settle timeout and will report incomplete drain
        # while this worker still owns children, rather than close resources.
        try:
            await wait_for_db_task(joined)
        finally:
            raise
