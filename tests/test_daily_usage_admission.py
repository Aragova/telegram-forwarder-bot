import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app import scheduler_runtime


def test_scheduler_restart_does_not_reset_usage(monkeypatch):
    repo = Mock()
    tick = AsyncMock()
    monkeypatch.setattr(scheduler_runtime, "scheduler_tick", tick)

    async def stop(*args, **kwargs):
        raise asyncio.CancelledError
    monkeypatch.setattr(scheduler_runtime.asyncio, "sleep", stop)
    for _ in range(2):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(scheduler_runtime.run_scheduler_loop(repo, is_enabled=lambda: False))
    assert tick.await_count == 2
    assert repo.mock_calls == []


def test_atomic_scheduler_does_not_charge_a_second_time():
    repo = Mock()
    repo.get_all_rules.return_value = [SimpleNamespace(id=1, is_active=True, mode="repost", target_id="-1001", target_thread_id=None)]
    repo.mark_expired_campaign_top_time_pauses_completed.return_value = 0
    repo.get_due_delivery.return_value = {"delivery_id": 1, "tenant_id": 7}
    repo.get_active_campaign_top_time_pause_for_target.return_value = None
    repo.get_active_subscription.return_value = {"status": "active", "plan_name": "FREE", "max_jobs_per_day": 5}
    repo.get_usage_for_date.return_value = {"jobs_count": 4}
    repo.take_due_delivery_and_create_job.return_value = {"status": "created", "job_id": 1, "delivery_id": 1, "tenant_id": 7}
    assert asyncio.run(scheduler_runtime.scheduler_tick(repo))["created"] == 1
    repo.bump_usage.assert_not_called()
    repo.take_due_delivery_and_create_job.return_value = {"status": "limit_blocked", "reason": "limit"}
    assert asyncio.run(scheduler_runtime.scheduler_tick(repo))["created"] == 0
    repo.bump_usage.assert_not_called()
