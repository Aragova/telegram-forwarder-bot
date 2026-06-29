from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.repost_single_active_canary import RepostSingleActiveCanaryResult, RepostSingleActiveCanaryStatus
from app.sender import SenderService


class DummyRepo:
    def __init__(self):
        self.sent_calls = []
        self.faulty_calls = []
        self.events = []

    def log_delivery_event(self, *args, **kwargs):
        self.events.append((args, kwargs))
        return None

    def mark_delivery_sent(self, *args, **kwargs):
        self.sent_calls.append((args, kwargs))
        return None

    def mark_delivery_sent_with_target_message(self, *args, **kwargs):
        self.sent_calls.append((args, kwargs))
        return None

    def mark_delivery_faulty(self, *args, **kwargs):
        self.faulty_calls.append((args, kwargs))
        return None

    def get_post_id_by_delivery(self, delivery_id):
        return 3

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
        self.reaction_calls = []
        self.final_success_calls = []
        self.repo = DummyRepo()
        super().__init__(bot=SimpleNamespace(), telethon_client=None, reaction_clients=[SimpleNamespace()], db=self.repo, repost_single_rollout_probe=FakeProbe(), repost_single_active_canary_runner=FakeRunner(runner_result))

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
        self.final_success_calls.append((args, kwargs))
        return None

    async def _add_reaction_for_rule_if_possible(self, **kwargs):
        self.reaction_calls.append(kwargs)
        if getattr(self, "fail_reaction", False):
            raise RuntimeError("reaction boom")
        return None


def run(coro):
    return asyncio.run(coro)


def rule():
    return SimpleNamespace(id=12, mode="repost", tenant_id=2, interval=0)


def test_active_success_runs_reaction_and_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, sent_message_ids=(101,), pipeline_status="finalized"))
    test_rule = rule()

    result = run(service._deliver_single(test_rule, 1, 10, -100, -200, None))

    assert result is True
    assert service.copy_calls == []
    assert service.repo.sent_calls
    assert service.repo.faulty_calls == []
    assert len(service.reaction_calls) == 1
    assert service.reaction_calls[0] == {
        "rule": test_rule,
        "target_id": -200,
        "sent_message_id": 101,
        "source_channel": "-100",
        "source_message_ids": [10],
        "delivery_id": 1,
    }
    assert service.repost_single_active_canary_runner.calls[0]["unsupported_features"] == ()


def test_reaction_failure_after_active_success_still_marks_sent_without_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, sent_message_ids=(101,), pipeline_status="finalized"))
    service.fail_reaction = True

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is True
    assert service.copy_calls == []
    assert service.repo.sent_calls
    assert service.repo.faulty_calls == []
    assert len(service.reaction_calls) == 1
    assert service.final_success_calls[-1][1]["final_method"] == "repost_single_active_pipeline"
    assert service.final_success_calls[-1][1]["extra"]["post_send_warnings"] == ["reaction_failed_after_active_pipeline"]


def test_legacy_continues_when_runner_allows_fallback():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.NOT_READY, should_continue_legacy=True))

    run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert len(service.copy_calls) == 1


def test_failed_attempted_pipeline_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.FAILED, attempted_pipeline=True, should_continue_legacy=False))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is False
    assert service.copy_calls == []
    assert service.repo.sent_calls == []
    assert service.repo.faulty_calls
    assert service.reaction_calls == []


def test_handled_without_sent_ids_marks_faulty_and_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, pipeline_status="finalized"))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is False
    assert service.copy_calls == []
    assert service.repo.sent_calls == []
    assert service.repo.faulty_calls
    assert service.reaction_calls == []
