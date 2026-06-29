from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.repost_single_active_canary import RepostSingleActiveCanaryResult, RepostSingleActiveCanaryStatus
from app.sender import SenderService


class DummyRepo:
    def __init__(self, reaction_settings=None):
        self.sent_calls = []
        self.faulty_calls = []
        self.events = []
        self.reaction_settings = reaction_settings

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

    def get_rule_reaction_settings_for_tenant(self, tenant_id, rule_id):
        return self.reaction_settings


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
    def __init__(self, runner_result, *, reaction_clients=None, reaction_settings=None):
        self.copy_calls = []
        self.repo = DummyRepo(reaction_settings=reaction_settings)
        super().__init__(bot=SimpleNamespace(), telethon_client=None, reaction_clients=reaction_clients or [], db=self.repo, repost_single_rollout_probe=FakeProbe(), repost_single_active_canary_runner=FakeRunner(runner_result))

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


def rule(**kwargs):
    data = {"id": 12, "mode": "repost"}
    data.update(kwargs)
    return SimpleNamespace(**data)


def test_attempted_pipeline_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, sent_message_ids=(101,), pipeline_status="finalized"))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is True
    assert service.copy_calls == []
    assert service.repo.sent_calls
    assert service.repo.faulty_calls == []


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


def test_handled_without_sent_ids_marks_faulty_and_stops_legacy_copy():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, pipeline_status="finalized"))

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is False
    assert service.copy_calls == []
    assert service.repo.sent_calls == []
    assert service.repo.faulty_calls


def test_global_reaction_clients_do_not_block_rule_without_reaction_requirement():
    service = SenderForTest(
        RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, sent_message_ids=(101,), pipeline_status="finalized"),
        reaction_clients=[object()],
    )

    result = run(service._deliver_single(rule(), 1, 10, -100, -200, None))

    assert result is True
    assert service.repost_single_active_canary_runner.calls[0]["unsupported_features"] == ()
    assert service.copy_calls == []


def test_explicit_rule_reaction_requirement_blocks_active_canary_and_falls_back():
    service = SenderForTest(RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.NOT_READY, should_continue_legacy=True, reason="unsupported_feature:reactions"))

    run(service._deliver_single(rule(reactions_enabled=True), 1, 10, -100, -200, None))

    assert service.repost_single_active_canary_runner.calls[0]["unsupported_features"] == ("reactions",)
    assert len(service.copy_calls) == 1


def test_tenant_reaction_settings_block_only_enabled_rule_and_fallback_continues():
    service = SenderForTest(
        RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.NOT_READY, should_continue_legacy=True, reason="unsupported_feature:reactions"),
        reaction_settings={"enabled": True},
    )

    run(service._deliver_single(rule(tenant_id=89), 1, 10, -100, -200, None))

    assert service.repost_single_active_canary_runner.calls[0]["unsupported_features"] == ("reactions",)
    assert len(service.copy_calls) == 1


def test_tenant_without_enabled_reaction_settings_can_reach_active_pipeline():
    service = SenderForTest(
        RepostSingleActiveCanaryResult(status=RepostSingleActiveCanaryStatus.HANDLED, attempted_pipeline=True, should_continue_legacy=False, sent_message_ids=(101,), pipeline_status="finalized"),
        reaction_settings={"enabled": False},
    )

    result = run(service._deliver_single(rule(tenant_id=89), 1, 10, -100, -200, None))

    assert result is True
    assert service.repost_single_active_canary_runner.calls[0]["unsupported_features"] == ()
    assert service.copy_calls == []
