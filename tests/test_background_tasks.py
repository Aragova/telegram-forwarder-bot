from __future__ import annotations

import asyncio
import logging

from app.background_tasks import _RUNNING_TASKS, start_background_task


def test_background_task_keeps_reference_until_done() -> None:
    async def scenario() -> None:
        event = asyncio.Event()
        async def work() -> int:
            await event.wait()
            return 42
        task = start_background_task(work(), name="test-pending")
        assert task in _RUNNING_TASKS
        event.set()
        assert await task == 42
        await asyncio.sleep(0)
        assert task not in _RUNNING_TASKS
    asyncio.run(scenario())


def test_background_task_failure_and_cancellation(caplog) -> None:
    async def scenario() -> None:
        async def broken() -> None:
            raise RuntimeError("test-failure")
        with caplog.at_level(logging.ERROR, logger="forwarder.background_tasks"):
            task = start_background_task(broken(), name="test-failure")
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert task.done()
            assert task not in _RUNNING_TASKS
            assert "test-failure" in caplog.text
        event = asyncio.Event()
        task = start_background_task(event.wait(), name="test-cancel")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        assert task not in _RUNNING_TASKS
    asyncio.run(scenario())


def test_shutdown_cancels_tasks_spawned_during_cancellation():
    from app.background_tasks import stop_background_tasks

    async def scenario():
        spawned = []

        async def original():
            try:
                await asyncio.Event().wait()
            finally:
                spawned.append(start_background_task(asyncio.Event().wait(), name="late-task"))

        task = start_background_task(original(), name="original")
        await asyncio.sleep(0)
        await stop_background_tasks()
        assert task.cancelled()
        assert len(spawned) == 1 and spawned[0].cancelled()

    asyncio.run(scenario())


def test_shutdown_drains_thread_work_instead_of_discarding_result():
    from app.background_tasks import stop_background_tasks

    async def scenario():
        task = start_background_task(asyncio.to_thread(lambda: 42), name="thread", drain_only=True)
        await stop_background_tasks()
        assert task.result() == 42

    asyncio.run(scenario())
