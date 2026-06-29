from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.repost_single_active_canary import RepostSingleActiveCanaryResult, RepostSingleActiveCanaryStatus
from app.sender import SenderService


class DummyRepo:
    def log_delivery_event(self, *args, **kwargs):
        return None

    def mark_delivery_sent(self, *args, **kwargs):
        return None

    def touch_rule_after_send(self, *args, **kwargs):
        return None


class FakeProbe:
    def probe(self, **kwargs):
        return SimpleNamespace(to_log_context=lambda: {"status": "ready"})


class FakeRunner:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def try_run(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class SenderForTest(SenderService):
    def __init__(self, runner_result):
        self.copy_calls = []
        super().__init__(bot=SimpleNamespace(), telethon_client=None, reaction_clients=[], db=DummyRepo(), repost_single_rollout_probe=FakeProbe(), repost_single_active_canary_runner=FakeRunner(runner_result))

    async def _deliver_single_video(self, *args, **kwargs):
        raise AssertionError("video path must not be called")

    def _get_post_id_by_delivery_sync(self, delivery_id):
        return 3

    def _resolve_repost_caption_delivery_strategy_sync(self, **kwargs):
        return {"configured_mode": "copy", "requires_builder": False, "use_copy_first": True}

    def _get_post_row_for_rule_message_sync(self, *args, **kwargs):
        return {"content": {}}

    async def _log_delivery_pipeline_step(self, *args, **kwargs):
        return None

    async def _copy_single_via_bot(self, *args, **kwargs):
        self.copy_calls.append((args, kwargs))
        return {"raw_result": None, "sent_ids": [101], "attempted": True}

    async def _mark_delivery_sent_with_attempt_sync(self, *args, **kwargs):
        return None

    async def _verify_sent_messages_with_retry(self, *args, **kwargs):
        return None

    async def _log_delivery_final_success(self, *args, **kwargs):
        return None


def run(coro):
    return asyncio.run(coro)


def rule():
    return SimpleNamespace(id=12, mode="repost")


def test_attempted_pipeline_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is True
    assert service.copy_calls == []


def test_legacy_continues_when_runner_allows_fallback():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.NOT_READY, should_continue_legacy=True))

    run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert len(service.copy_calls) == 1


def test_failed_attempted_pipeline_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.FAILED, attempted_pipeline=True, should_continue_legacy=False))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is False
    assert service.copy_calls == []
