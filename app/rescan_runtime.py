"""Lease maintenance for durable rescans; the repository is the fencing authority."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from app.runtime_utils import wait_for_db_task

logger = logging.getLogger("forwarder")
_HEARTBEAT_INTERVAL_SEC = 20


async def rescan_db(run_db, func, *args, **kwargs):
    """Wait for in-flight transactions even if the rescan task is cancelled."""
    return await wait_for_db_task(asyncio.create_task(run_db(func, *args, **kwargs)))


@asynccontextmanager
async def maintain_rescan_lease(repo, run_db, rule_id: int, owner_token: str):
    owner_task = asyncio.current_task()
    stopping = asyncio.Event()

    async def heartbeat():
        while not stopping.is_set():
            try:
                await asyncio.wait_for(stopping.wait(), timeout=_HEARTBEAT_INTERVAL_SEC)
                return
            except TimeoutError:
                if stopping.is_set():
                    return
            try:
                renewed = await rescan_db(run_db, repo.renew_rule_rescan, rule_id, owner_token)
            except Exception:
                if stopping.is_set():
                    return
                logger.warning("RESCAN_LEASE_RENEW_FAILED | rule_id=%s", rule_id, exc_info=True)
                # A later renewal must not revive an expired token.
                continue
            if stopping.is_set():
                return
            if not renewed:
                logger.warning("RESCAN_LEASE_LOST | rule_id=%s", rule_id)
                if owner_task is not None:
                    owner_task.cancel()
                return

    task = asyncio.create_task(heartbeat())
    try:
        yield
    finally:
        stopping.set()
        task.cancel()
        await wait_for_db_task(asyncio.gather(task, return_exceptions=True))
