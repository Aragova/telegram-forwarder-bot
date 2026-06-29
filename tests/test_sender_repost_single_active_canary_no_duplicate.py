from __future__ import annotations

import asyncio
from types import SimpleNamespace

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
    def __init__(self):
        self.calls = []

    def probe(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(to_log_context=lambda: {"status": "ready"})


class FakeRunner:
    def __init__(self):
        self.calls = []

    async def try_run(self, **kwargs):
        self.calls.append(kwargs)
        raise AssertionError("active canary runner must not be called")


class SenderForTest(SenderService):
    def __init__(self):
        self.copy_calls = []
        self.reaction_calls = []
        self.final_success_calls = []
        self.fake_probe = FakeProbe()
        self.fake_runner = FakeRunner()
        self.repo = DummyRepo()
        super().__init__(
            bot=SimpleNamespace(),
            telethon_client=None,
            reaction_clients=[SimpleNamespace()],
            db=self.repo,
            repost_single_rollout_probe=self.fake_probe,
            repost_single_active_canary_runner=self.fake_runner,
        )

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


def test_deprecated_probe_and_runner_are_ignored_and_legacy_copy_runs_once():
    service = SenderForTest()
    test_rule = rule()

    result = run(service._deliver_single(test_rule, 1, 10, -100, -200, None))

    assert result is True
    assert len(service.copy_calls) == 1
    assert service.fake_probe.calls == []
    assert service.fake_runner.calls == []
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


def test_reaction_failure_in_legacy_path_still_does_not_call_active_runner():
    service = SenderForTest()
    service.fail_reaction = True

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is True
    assert len(service.copy_calls) == 1
    assert service.fake_runner.calls == []
    assert service.repo.sent_calls
    assert service.repo.faulty_calls == []
    assert len(service.reaction_calls) == 1
