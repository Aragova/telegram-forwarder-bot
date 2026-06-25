from __future__ import annotations

import asyncio
from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationStatus, DeliveryOutcome, DeliveryFinalizer
from app.post_send_steps import PostSendStepsResult
from app.repost_campaign_pipeline import (
    RepostCampaignInput,
    RepostCampaignPipeline,
    normalize_campaign_message_ids,
    normalize_cleanup_message_ids,
)
from app.telegram_send_result import telegram_send_success
from app.transport_policy import TransportRateLimited


class FakeGateway:
    def __init__(self, sent_ids=None, *, fail_at=None, rate_limit_at=None, events=None):
        self.sent_ids = list(sent_ids or [])
        self.fail_at = fail_at
        self.rate_limit_at = rate_limit_at
        self.calls = []
        self.events = events if events is not None else []

    @property
    def bot(self):
        raise AssertionError("raw bot access is forbidden")

    @property
    def raw(self):
        raise AssertionError("raw gateway access is forbidden")

    @property
    def telethon_client(self):
        raise AssertionError("raw telethon access is forbidden")

    async def copy_message(self, **kwargs):
        self.calls.append(kwargs)
        self.events.append(("copy", kwargs["message_id"]))
        call_no = len(self.calls)
        if self.rate_limit_at == call_no:
            raise TransportRateLimited(retry_after_seconds=33, backend="bot", op_name="copy_message", key="k")
        if self.fail_at == call_no:
            raise RuntimeError("copy failed PRIVATE CAPTION")
        return telegram_send_success(method="copy_message", sent_message_ids=[self.sent_ids[call_no - 1]])


class FakeCleanup:
    def __init__(self, *, result=True, fail=False, events=None):
        self.result = result
        self.fail = fail
        self.calls = []
        self.events = events if events is not None else []

    async def cleanup_campaign_messages(self, **kwargs):
        self.calls.append(kwargs)
        self.events.append(("cleanup", tuple(kwargs["message_ids"])))
        if self.fail:
            raise RuntimeError("cleanup failed 123:SECRET_TOKEN")
        return self.result


class FakePostSend:
    def __init__(self, result=None, *, fail=False, events=None):
        self.result = result
        self.fail = fail
        self.calls = []
        self.events = events if events is not None else []

    async def run_after_send(self, **kwargs):
        self.calls.append(kwargs)
        self.events.append(("post_send", tuple(kwargs["sent_message_ids"])))
        if self.fail:
            raise RuntimeError("post failed content_json")
        if self.result is not None:
            return self.result
        return PostSendStepsResult.completed(
            context=kwargs["context"],
            sent_message_ids=tuple(kwargs["sent_message_ids"]),
            idempotency_key=kwargs["idempotency_key"],
            target_id=kwargs["target_id"],
            telegram_method=kwargs["telegram_method"],
            completed_steps=("normalize_sent_ids",),
        )


def ctx() -> DeliveryContext:
    return DeliveryContext(delivery_id=1, rule_id=2, post_id=3, target_id="target")


def input_data(**overrides) -> RepostCampaignInput:
    base = dict(
        context=ctx(),
        campaign_id="campaign-1",
        campaign_label="PRIVATE CAMPAIGN TEXT",
        source_chat_id="source",
        source_message_ids=[10, 11],
        target_chat_id="target",
        idempotency_key="idem-1",
    )
    base.update(overrides)
    return RepostCampaignInput(**base)


def run(coro):
    return asyncio.run(coro)


def test_normalize_campaign_and_cleanup_message_ids():
    cases = [
        (None, ()),
        (5, (5,)),
        ([1, 2, 2], (1, 2, 2)),
        ((3, 4), (3, 4)),
        ((x for x in [7, 7, 8]), (7, 7, 8)),
        ([], ()),
    ]
    for value, expected in cases:
        assert normalize_campaign_message_ids(value) == expected
    assert normalize_cleanup_message_ids(None) == ()
    assert normalize_cleanup_message_ids(5) == (5,)
    assert normalize_cleanup_message_ids([1, 2, 2]) == (1, 2, 2)
    assert normalize_cleanup_message_ids((x for x in [7, 7, 8])) == (7, 7, 8)


def test_input_safe_logs_do_not_include_raw_label_or_payload_like_values():
    data = input_data(cleanup_after_copy=True, cleanup_chat_id="source", cleanup_message_ids=[10], cleanup_reason="PRIVATE CAPTION")
    log_context = data.to_log_context()
    assert log_context["campaign_id"] == "campaign-1"
    assert log_context["has_campaign_label"] is True
    assert log_context["source_message_count"] == 2
    assert log_context["cleanup_after_copy"] is True
    assert log_context["cleanup_message_count"] == 1
    assert log_context["has_cleanup_reason"] is True
    rendered = str(log_context)
    assert "PRIVATE CAMPAIGN TEXT" not in rendered
    assert "PRIVATE CAPTION" not in rendered
    assert "content_json" not in rendered
    assert "123:SECRET_TOKEN" not in rendered


def test_pipeline_stores_injected_dependencies_and_default_finalizer():
    gateway = FakeGateway([101])
    cleanup = FakeCleanup()
    post = FakePostSend()
    finalizer = DeliveryFinalizer()
    pipeline = RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup, post_send_steps=post, finalizer=finalizer)
    assert pipeline.send_gateway is gateway
    assert pipeline.cleanup_service is cleanup
    assert pipeline.post_send_steps is post
    assert pipeline.finalizer is finalizer
    assert isinstance(RepostCampaignPipeline(send_gateway=gateway).finalizer, DeliveryFinalizer)


def test_successful_campaign_copy_without_post_send_or_cleanup():
    gateway = FakeGateway([101, 102])
    result = run(RepostCampaignPipeline(send_gateway=gateway).run(input_data()))
    assert len(gateway.calls) == 2
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101, 102)
    assert result.pipeline_status == "sent"


def test_successful_campaign_copy_with_post_send():
    gateway = FakeGateway([101, 102])
    post = FakePostSend()
    result = run(RepostCampaignPipeline(send_gateway=gateway, post_send_steps=post).run(input_data()))
    assert len(post.calls) == 1
    call = post.calls[0]
    assert call["send_result"] is None
    assert call["sent_message_ids"] == (101, 102)
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.post_send_status == "completed"


def test_successful_campaign_copy_with_cleanup_before_post_send():
    events = []
    gateway = FakeGateway([101, 102], events=events)
    cleanup = FakeCleanup(events=events)
    post = FakePostSend(events=events)
    data = input_data(cleanup_after_copy=True, cleanup_chat_id="source", cleanup_message_ids=[10, 11])
    result = run(RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup, post_send_steps=post).run(data))
    assert events == [("copy", 10), ("copy", 11), ("cleanup", (10, 11)), ("post_send", (101, 102))]
    assert cleanup.calls[0]["reason"] == "repost_campaign_cleanup"
    assert result.status == DeliveryFinalizationStatus.FINALIZED


def test_copy_kwargs_order_and_none_values_omitted():
    gateway = FakeGateway([101, 102])
    data = input_data(
        target_thread_id=99,
        disable_notification=True,
        protect_content=False,
        allow_sending_without_reply=True,
    )
    run(RepostCampaignPipeline(send_gateway=gateway).run(data))
    assert gateway.calls == [
        {
            "chat_id": "target",
            "from_chat_id": "source",
            "message_id": 10,
            "message_thread_id": 99,
            "disable_notification": True,
            "protect_content": False,
            "allow_sending_without_reply": True,
        },
        {
            "chat_id": "target",
            "from_chat_id": "source",
            "message_id": 11,
            "message_thread_id": 99,
            "disable_notification": True,
            "protect_content": False,
            "allow_sending_without_reply": True,
        },
    ]
    gateway2 = FakeGateway([101])
    run(RepostCampaignPipeline(send_gateway=gateway2).run(input_data(source_message_ids=[10])))
    assert set(gateway2.calls[0]) == {"chat_id", "from_chat_id", "message_id"}


def test_context_operation_in_post_send_and_final_result():
    gateway = FakeGateway([101])
    post = FakePostSend()
    result = run(RepostCampaignPipeline(send_gateway=gateway, post_send_steps=post).run(input_data(source_message_ids=[10])))
    assert post.calls[0]["context"].operation == "repost_campaign"
    assert result.context.operation == "repost_campaign"


def test_missing_required_inputs_do_not_call_dependencies():
    cases = [
        (dict(context=None), "missing_context"),
        (dict(source_chat_id=None), "missing_source_chat_id"),
        (dict(source_message_ids=None), "missing_source_message_ids"),
        (dict(source_message_ids=[]), "missing_source_message_ids"),
        (dict(target_chat_id=None), "missing_target_chat_id"),
    ]
    for overrides, reason in cases:
        gateway = FakeGateway([101])
        cleanup = FakeCleanup()
        post = FakePostSend()
        result = run(RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup, post_send_steps=post).run(input_data(**overrides)))
        assert result.status == DeliveryFinalizationStatus.FAILED
        assert result.outcome == DeliveryOutcome.FAULTY
        assert result.reason == reason
        assert gateway.calls == []
        assert cleanup.calls == []
        assert post.calls == []


def test_cleanup_validation_happens_before_copy():
    cases = [
        ({}, None, "campaign_cleanup_not_configured"),
        ({"cleanup_chat_id": None, "cleanup_message_ids": [10]}, FakeCleanup(), "missing_campaign_cleanup_chat_id"),
        ({"cleanup_chat_id": "source", "cleanup_message_ids": []}, FakeCleanup(), "missing_campaign_cleanup_message_ids"),
    ]
    for overrides, cleanup, reason in cases:
        gateway = FakeGateway([101])
        data = input_data(cleanup_after_copy=True, **overrides)
        result = run(RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup).run(data))
        assert result.reason == reason
        assert result.status == DeliveryFinalizationStatus.FAILED
        assert gateway.calls == []


def test_copy_exception_stops_loop_and_preserves_partial_sent_ids():
    gateway = FakeGateway([101, 102], fail_at=2)
    cleanup = FakeCleanup()
    post = FakePostSend()
    result = run(RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup, post_send_steps=post).run(input_data()))
    assert len(gateway.calls) == 2
    assert cleanup.calls == []
    assert post.calls == []
    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.reason == "repost_campaign_copy_failed"
    assert result.sent_message_ids == (101,)
    assert "RuntimeError" == result.error_type
    assert not isinstance(result.error_text, RuntimeError)


def test_transport_rate_limited_stops_loop_and_preserves_retry_after_and_partial_ids():
    gateway = FakeGateway([101, 102], rate_limit_at=2)
    result = run(RepostCampaignPipeline(send_gateway=gateway, post_send_steps=FakePostSend(), cleanup_service=FakeCleanup()).run(input_data()))
    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.reason == "repost_campaign_rate_limited"
    assert result.retry_after == 33
    assert result.sent_message_ids == (101,)


def test_cleanup_exception_fails_safely_without_post_send():
    gateway = FakeGateway([101])
    cleanup = FakeCleanup(fail=True)
    post = FakePostSend()
    data = input_data(source_message_ids=[10], cleanup_after_copy=True, cleanup_chat_id="source", cleanup_message_ids=[10])
    result = run(RepostCampaignPipeline(send_gateway=gateway, cleanup_service=cleanup, post_send_steps=post).run(data))
    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.reason == "campaign_cleanup_failed"
    assert result.sent_message_ids == (101,)
    assert post.calls == []
    assert result.error_type == "RuntimeError"


def test_cleanup_false_fails_but_none_succeeds():
    false_result = run(
        RepostCampaignPipeline(send_gateway=FakeGateway([101]), cleanup_service=FakeCleanup(result=False)).run(
            input_data(source_message_ids=[10], cleanup_after_copy=True, cleanup_chat_id="source", cleanup_message_ids=[10])
        )
    )
    assert false_result.status == DeliveryFinalizationStatus.FAILED
    assert false_result.reason == "campaign_cleanup_failed"
    none_result = run(
        RepostCampaignPipeline(send_gateway=FakeGateway([101]), cleanup_service=FakeCleanup(result=None)).run(
            input_data(source_message_ids=[10], cleanup_after_copy=True, cleanup_chat_id="source", cleanup_message_ids=[10])
        )
    )
    assert none_result.status == DeliveryFinalizationStatus.FINALIZED


def test_post_send_rate_limited_and_failed_results_are_finalized():
    rate_limited = PostSendStepsResult.rate_limited(context=ctx(), sent_message_ids=(101,), retry_after=44, reason="verify_rate_limited")
    failed = PostSendStepsResult.failed(context=ctx(), sent_message_ids=(101,), reason="ledger_failed")
    result1 = run(RepostCampaignPipeline(send_gateway=FakeGateway([101]), post_send_steps=FakePostSend(rate_limited)).run(input_data(source_message_ids=[10])))
    result2 = run(RepostCampaignPipeline(send_gateway=FakeGateway([101]), post_send_steps=FakePostSend(failed)).run(input_data(source_message_ids=[10])))
    assert result1.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result1.outcome == DeliveryOutcome.DEFERRED
    assert result2.status == DeliveryFinalizationStatus.FAILED
    assert result2.outcome == DeliveryOutcome.FAULTY


def test_post_send_exception_fails_safely():
    result = run(RepostCampaignPipeline(send_gateway=FakeGateway([101]), post_send_steps=FakePostSend(fail=True)).run(input_data(source_message_ids=[10])))
    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.reason == "post_send_steps_failed"
    assert result.error_type == "RuntimeError"


def test_no_raw_gateway_access():
    gateway = FakeGateway([101])
    result = run(RepostCampaignPipeline(send_gateway=gateway).run(input_data(source_message_ids=[10])))
    assert result.ok
    assert len(gateway.calls) == 1


def test_safe_result_logs_do_not_include_sensitive_values():
    gateway = FakeGateway([101], fail_at=1)
    result = run(RepostCampaignPipeline(send_gateway=gateway).run(input_data(source_message_ids=[10], idempotency_key="safe-id")))
    rendered = str(result.to_log_context()) + result.log_label()
    assert "PRIVATE CAMPAIGN TEXT" not in rendered
    assert "PRIVATE CAPTION" not in rendered
    assert "123:SECRET_TOKEN" not in rendered
    assert "content_json" not in rendered


def test_input_is_frozen_slots_dataclass_with_expected_fields():
    assert RepostCampaignInput.__dataclass_params__.frozen is True
    assert {field.name for field in fields(RepostCampaignInput)} == {
        "context",
        "campaign_id",
        "campaign_label",
        "source_chat_id",
        "source_message_ids",
        "target_chat_id",
        "target_thread_id",
        "idempotency_key",
        "telegram_method",
        "disable_notification",
        "protect_content",
        "allow_sending_without_reply",
        "cleanup_after_copy",
        "cleanup_chat_id",
        "cleanup_message_ids",
        "cleanup_reason",
    }
