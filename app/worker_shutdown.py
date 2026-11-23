"""Drain worker loops before closing Telegram clients.

A cancelled Telegram write is ambiguous, not a verified failure.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable


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
    done_after, still_pending = await asyncio.wait(pending, timeout=10)
    if done_after:
        await asyncio.gather(*done_after, return_exceptions=True)
    if still_pending:
        logger.error("WORKER_DRAIN_CANCEL_INCOMPLETE | workers=%s", len(still_pending))
