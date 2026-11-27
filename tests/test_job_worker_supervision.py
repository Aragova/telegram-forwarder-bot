from __future__ import annotations

import asyncio
import logging

from app.job_worker_lifecycle import JobWorkerLifecycle
from app.runtime_roles import RuntimeOwnership


def test_supervisor_retries_failed_start_then_waits_without_polling():
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        recovered = asyncio.Event()
        calls = 0

        async def start():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary failure")
            recovered.set()

        supervisor = asyncio.create_task(lifecycle.supervise(start, retry_delay_sec=0.01))
        try:
            await asyncio.wait_for(recovered.wait(), timeout=2)
            await asyncio.sleep(0.05)
            assert calls == 2
        finally:
            ownership.begin_shutdown()
            await lifecycle.stop(timeout_sec=1)
            await asyncio.wait_for(supervisor, timeout=2)

    asyncio.run(scenario())


def test_supervisor_recovers_exited_worker_and_respects_pause():
    async def scenario():
        ownership = RuntimeOwnership()
        ownership.bind("all")
        lifecycle = JobWorkerLifecycle(ownership, logging.getLogger(__name__))
        fail = asyncio.Event()
        recovered = asyncio.Event()
        attempts = {"light": 0, "heavy": 0}

        async def light(stop):
            attempts["light"] += 1
            if attempts["light"] == 1:
                await fail.wait()
                return
            recovered.set()
            await stop.wait()

        async def heavy(stop):
            attempts["heavy"] += 1
            await stop.wait()

        factories = {"light": light, "heavy": heavy}

        async def start():
            lifecycle.ensure_running(factories)

        supervisor = asyncio.create_task(lifecycle.supervise(start, retry_delay_sec=0.01))
        try:
            fail.set()
            await asyncio.wait_for(recovered.wait(), timeout=2)
            assert attempts == {"light": 2, "heavy": 1}
            await lifecycle.stop(timeout_sec=1)
            await asyncio.sleep(0.05)
            assert lifecycle.live_count == 0
            assert attempts == {"light": 2, "heavy": 1}
            lifecycle.ensure_running(factories, resume=True)
            await asyncio.sleep(0)
            assert lifecycle.live_count == 2
            assert attempts == {"light": 3, "heavy": 2}
        finally:
            ownership.begin_shutdown()
            await lifecycle.stop(timeout_sec=1)
            await asyncio.wait_for(supervisor, timeout=2)

    asyncio.run(scenario())
