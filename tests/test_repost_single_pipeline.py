from __future__ import annotations

import asyncio

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationStatus, DeliveryOutcome, DeliveryFinalizer
from app.post_send_steps import PostSendStepsResult
from app.repost_single_pipeline import RepostSingleInput, RepostSinglePipeline
from app.telegram_send_result import telegram_send_success
from app.transport_policy import TransportRateLimited


class FakeGateway:
    def __init__(self, result=None, error=None):
        self.result = result or telegram_send_success(method="copy_message", sent_message_ids=[101])
        self.error = error
        self.calls = []

    async def copy_message(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result


class GuardedGateway(FakeGateway):
    @property
    def bot(self):
        raise AssertionError("pipeline must not read gateway.bot")

    @property
    def raw(self):
        raise AssertionError("pipeline must not read gateway.raw")

    @property
    def telethon_client(self):
        raise AssertionError("pipeline must not read gateway.telethon_client")


class FakePostSendSteps:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    async def run_after_send(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        if self.result is not None:
            return self.result
        return PostSendStepsResult.completed(
            context=kwargs.get("context"),
            sent_message_ids=tuple(kwargs.get("sent_message_ids") or ()),
            idempotency_key=kwargs.get("idempotency_key"),
            target_id=kwargs.get("target_id"),
            telegram_method=kwargs.get("telegram_method"),
            completed_steps=("fake_post_send",),
        )


def context(**kwargs) -> DeliveryContext:
    values = dict(delivery_id=1, rule_id=2, post_id=3, source_id=-100, target_id=-200, message_id=10, mode="repost")
    values.update(kwargs)
    return DeliveryContext(**values)


def input_data(**kwargs) -> RepostSingleInput:
    values = dict(
        context=context(operation="initial", target_id=-200),
        source_chat_id=-100,
        source_message_id=10,
        target_chat_id=-200,
        target_thread_id=55,
        idempotency_key="idem-1",
        telegram_method=None,
        disable_notification=True,
        protect_content=False,
    )
    values.update(kwargs)
    return RepostSingleInput(**values)


def run(coro):
    return asyncio.run(coro)


def test_input_to_log_context_is_safe():
    data = input_data(caption="PRIVATE TEXT 123:SECRET_TOKEN")

    log_context = data.to_log_context()
    text = str(log_context)

    assert log_context["source_chat_id"] == -100
    assert log_context["source_message_id"] == 10
    assert log_context["target_chat_id"] == -200
    assert log_context["target_thread_id"] == 55
    assert log_context["idempotency_key"] == "idem-1"
    assert "context" in log_context
    assert "caption" not in log_context
    assert "PRIVATE TEXT" not in text
    assert "SECRET_TOKEN" not in text


def test_pipeline_stores_injected_dependencies():
    gateway = FakeGateway()
    post_send_steps = FakePostSendSteps()
    finalizer = DeliveryFinalizer()

    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps, finalizer=finalizer)
    default_pipeline = RepostSinglePipeline(send_gateway=gateway)

    assert pipeline.send_gateway is gateway
    assert pipeline.post_send_steps is post_send_steps
    assert pipeline.finalizer is finalizer
    assert isinstance(default_pipeline.finalizer, DeliveryFinalizer)


def test_successful_copy_without_post_send_steps_finalizes_pipeline_result():
    gateway = FakeGateway()
    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=None)

    result = run(pipeline.run(input_data()))

    assert len(gateway.calls) == 1
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101,)
    assert result.pipeline_status == "sent"


def test_successful_copy_with_post_send_steps():
    gateway = FakeGateway()
    post_send_steps = FakePostSendSteps()
    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert len(gateway.calls) == 1
    assert len(post_send_steps.calls) == 1
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.post_send_status == "completed"


def test_copy_message_kwargs_forwarded_correctly_and_skips_none_values():
    gateway = FakeGateway()
    pipeline = RepostSinglePipeline(send_gateway=gateway)
    data = input_data(target_thread_id=55, disable_notification=True, protect_content=False, parse_mode=None)

    run(pipeline.run(data))

    assert gateway.calls == [
        {
            "chat_id": -200,
            "from_chat_id": -100,
            "message_id": 10,
            "message_thread_id": 55,
            "disable_notification": True,
            "protect_content": False,
        }
    ]


def test_context_operation_updated_to_copy_message():
    gateway = FakeGateway()
    post_send_steps = FakePostSendSteps()
    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert post_send_steps.calls[0]["context"].operation == "copy_message"
    assert result.context.operation == "copy_message"


def test_send_exception_maps_to_failed_finalization():
    gateway = FakeGateway(error=RuntimeError("boom"))
    post_send_steps = FakePostSendSteps()
    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "copy_message_failed"
    assert post_send_steps.calls == []
    assert not isinstance(result.error_text, RuntimeError)


def test_transport_rate_limited_maps_to_deferred_finalization():
    error = TransportRateLimited(retry_after_seconds=60, backend="bot", op_name="copy_message", key="sender.bot.copy_message")
    gateway = FakeGateway(error=error)
    post_send_steps = FakePostSendSteps()
    pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.should_defer is True
    assert result.retry_after == 60
    assert result.reason == "copy_message_rate_limited"
    assert post_send_steps.calls == []


def test_post_send_rate_limited_propagates_to_finalization():
    post_send_steps = FakePostSendSteps(result=PostSendStepsResult.rate_limited(retry_after=60))
    pipeline = RepostSinglePipeline(send_gateway=FakeGateway(), post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.retry_after == 60


def test_post_send_failed_maps_to_failed_finalization():
    post_send_steps = FakePostSendSteps(result=PostSendStepsResult.failed(reason="ledger_failed"))
    pipeline = RepostSinglePipeline(send_gateway=FakeGateway(), post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "ledger_failed"


def test_post_send_exception_maps_to_failed_finalization():
    post_send_steps = FakePostSendSteps(error=RuntimeError("post boom"))
    pipeline = RepostSinglePipeline(send_gateway=FakeGateway(), post_send_steps=post_send_steps)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "post_send_steps_failed"
    assert not isinstance(result.error_text, RuntimeError)


def test_missing_required_input_fields_fail_safely():
    cases = [
        ("context", None, "missing_context"),
        ("source_chat_id", None, "missing_source_chat_id"),
        ("source_message_id", None, "missing_source_message_id"),
        ("target_chat_id", None, "missing_target_chat_id"),
    ]
    for field, value, reason in cases:
        gateway = FakeGateway()
        post_send_steps = FakePostSendSteps()
        pipeline = RepostSinglePipeline(send_gateway=gateway, post_send_steps=post_send_steps)

        result = run(pipeline.run(input_data(**{field: value})))

        assert result.status == DeliveryFinalizationStatus.FAILED
        assert result.outcome == DeliveryOutcome.FAULTY
        assert result.reason == reason
        assert gateway.calls == []
        assert post_send_steps.calls == []


def test_pipeline_does_not_access_raw_bot_or_telethon_directly():
    gateway = GuardedGateway()
    pipeline = RepostSinglePipeline(send_gateway=gateway)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert len(gateway.calls) == 1


def test_no_sensitive_values_in_result_log_context_or_label():
    data = input_data(
        context=context(operation="PRIVATE TEXT", mode="repost"),
        caption="PRIVATE CAPTION 123:SECRET_TOKEN",
    )
    pipeline = RepostSinglePipeline(send_gateway=FakeGateway())

    result = run(pipeline.run(data))
    log_text = str(result.to_log_context())
    label = result.log_label()

    assert "PRIVATE CAPTION" not in log_text
    assert "SECRET_TOKEN" not in log_text
    assert "PRIVATE CAPTION" not in label
    assert "SECRET_TOKEN" not in label
