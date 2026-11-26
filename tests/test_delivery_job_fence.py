import asyncio
from unittest.mock import AsyncMock

import pytest

from app.delivery_job_fence import (
    DeliveryJobRevoked, cancel_local_rule_delivery_jobs, enter_delivery_job,
    leave_delivery_job,
)
from app.send_intent import observe_publication


def test_local_cancellation_targets_only_the_selected_rule():
    async def scenario():
        ready = asyncio.Event()
        running = asyncio.Event()

        async def worker(rule_id, job_id):
            token = enter_delivery_job(object(), "repost_single", rule_id, job_id, "lease")
            try:
                ready.set()
                await running.wait()
            finally:
                leave_delivery_job(token)

        first = asyncio.create_task(worker(94, 1))
        await ready.wait()
        ready.clear()
        second = asyncio.create_task(worker(95, 2))
        await ready.wait()
        assert cancel_local_rule_delivery_jobs(94) == 1
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not second.done()
        running.set()
        await second
        assert cancel_local_rule_delivery_jobs(94) == 0

    asyncio.run(scenario())


def test_revoked_job_does_not_enter_publication_rpc():
    class Repo:
        def assert_current_delivery_job(self):
            raise DeliveryJobRevoked("rescan cancelled")

    async def scenario():
        call = AsyncMock()
        token = enter_delivery_job(Repo(), "repost_single", 94, 1, "lease")
        try:
            with pytest.raises(DeliveryJobRevoked):
                await observe_publication(call)
            call.assert_not_awaited()
        finally:
            leave_delivery_job(token)

    asyncio.run(scenario())
