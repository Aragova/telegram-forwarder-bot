from __future__ import annotations

import asyncio
from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationResult, DeliveryFinalizationStatus, DeliveryOutcome, DeliveryFinalizer
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.legacy_video_delivery_pipeline import LegacyVideoDeliveryInput, LegacyVideoDeliveryPipeline
from app.telegram_send_result import telegram_send_success
from app.transport_policy import TransportRateLimited


def run(coro):
    return asyncio.run(coro)


def context(**kwargs) -> DeliveryContext:
    values = dict(
        delivery_id=1,
        rule_id=2,
        post_id=3,
        source_id=-100,
        target_id=-200,
        message_id=10,
        mode="video",
        operation="initial",
    )
    values.update(kwargs)
    return DeliveryContext(**values)


def input_data(**kwargs) -> LegacyVideoDeliveryInput:
    values = dict(
        context=context(),
        source_chat_id=-100,
        source_message_id=10,
        target_chat_id=-200,
        target_thread_id=55,
        idempotency_key="idem-1",
        telegram_method="legacy_video_delivery",
        legacy_payload={"safe": "transient"},
        legacy_label="legacy-video",
    )
    values.update(kwargs)
    return LegacyVideoDeliveryInput(**values)


class FakeLegacyDelivery:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    def __call__(self, input_data):
        self.calls.append(input_data)
        if self.error is not None:
            raise self.error
        return self.result


class GuardedLegacyDelivery(FakeLegacyDelivery):
    @property
    def bot(self):
        raise AssertionError("pipeline must not read legacy_video_delivery.bot")

    @property
    def raw(self):
        raise AssertionError("pipeline must not read legacy_video_delivery.raw")

    @property
    def telethon_client(self):
        raise AssertionError("pipeline must not read legacy_video_delivery.telethon_client")

    @property
    def video_processor(self):
        raise AssertionError("pipeline must not read legacy_video_delivery.video_processor")


def assert_failed_faulty(result, reason):
    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == reason


def test_input_to_log_context_is_safe():
    data = input_data(
        legacy_payload={
            "content_json": {"caption": "PRIVATE CAPTION"},
            "caption": "PRIVATE CAPTION",
            "video_path": "/tmp/PRIVATE_VIDEO_PATH_SECRET_TOKEN.mp4",
            "token": "123:SECRET_TOKEN",
        }
    )

    log_context = data.to_log_context()
    rendered = str(log_context)

    assert log_context["source_chat_id"] == -100
    assert log_context["source_message_id"] == 10
    assert log_context["target_chat_id"] == -200
    assert log_context["target_thread_id"] == 55
    assert log_context["idempotency_key"] == "idem-1"
    assert log_context["telegram_method"] == "legacy_video_delivery"
    assert log_context["legacy_label"] == "legacy-video"
    assert log_context["has_legacy_payload"] is True
    assert log_context["context"] == data.context.to_log_context()
    assert "legacy_payload" not in log_context
    assert "PRIVATE CAPTION" not in rendered
    assert "PRIVATE_VIDEO_PATH" not in rendered
    assert "SECRET_TOKEN" not in rendered


def test_pipeline_stores_injected_dependency():
    fake_callable = FakeLegacyDelivery()
    finalizer = DeliveryFinalizer()

    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable, finalizer=finalizer)
    default_pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    assert pipeline.legacy_video_delivery is fake_callable
    assert pipeline.finalizer is finalizer
    assert isinstance(default_pipeline.finalizer, DeliveryFinalizer)


def test_missing_legacy_dependency_fails_safely():
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=None)

    result = run(pipeline.run(input_data()))

    assert_failed_faulty(result, "legacy_video_delivery_not_configured")


def test_missing_context_fails_safely_and_does_not_call_legacy():
    fake_callable = FakeLegacyDelivery(result=DeliveryPipelineResult.sent(context=context(), sent_message_ids=[101]))
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data(context=None)))

    assert_failed_faulty(result, "missing_context")
    assert fake_callable.calls == []


def test_missing_target_chat_id_fails_safely_and_does_not_call_legacy():
    fake_callable = FakeLegacyDelivery(result=DeliveryPipelineResult.sent(context=context(), sent_message_ids=[101]))
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data(target_chat_id=None)))

    assert_failed_faulty(result, "missing_target_chat_id")
    assert fake_callable.calls == []


def test_async_legacy_callable_returns_delivery_finalization_result():
    expected = DeliveryFinalizationResult.finalized(
        context=context(operation="legacy_video_delivery"),
        sent_message_ids=[101],
        target_id=-200,
        idempotency_key="idem-1",
    )
    calls = []

    async def fake_callable(data):
        calls.append(data)
        return expected

    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result is expected
    assert len(calls) == 1
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT


def test_sync_legacy_callable_returns_delivery_finalization_result():
    expected = DeliveryFinalizationResult.finalized(
        context=context(operation="legacy_video_delivery"),
        sent_message_ids=[101],
        target_id=-200,
        idempotency_key="idem-1",
    )
    fake_callable = FakeLegacyDelivery(result=expected)
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result is expected
    assert len(fake_callable.calls) == 1


def test_legacy_callable_returns_delivery_pipeline_result_sent():
    fake_callable = FakeLegacyDelivery(
        result=DeliveryPipelineResult.sent(context=context(operation="legacy_video_delivery"), sent_message_ids=[101])
    )
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101,)
    assert result.pipeline_status == "sent"


def test_legacy_callable_returns_delivery_pipeline_result_failed():
    fake_callable = FakeLegacyDelivery(
        result=DeliveryPipelineResult.failed(
            context=context(operation="legacy_video_delivery"),
            error_type="LegacyError",
            error_text="safe failure",
            reason="legacy_failed",
        )
    )
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert_failed_faulty(result, "legacy_failed")
    assert result.error_type == "LegacyError"
    assert result.error_text == "safe failure"


def test_legacy_callable_returns_telegram_send_result():
    send_result = telegram_send_success(method="legacy_video_delivery", sent_message_ids=[101])
    fake_callable = FakeLegacyDelivery(result=send_result)
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101,)
    assert "telegram_send_result" not in result.to_log_context()


def test_legacy_callable_returns_none():
    fake_callable = FakeLegacyDelivery(result=None)
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert_failed_faulty(result, "legacy_video_delivery_returned_none")


def test_legacy_callable_returns_unsupported_object():
    unsupported = object()
    fake_callable = FakeLegacyDelivery(result=unsupported)
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert_failed_faulty(result, "unsupported_legacy_video_delivery_result")
    assert str(id(unsupported)) not in str(result.to_log_context())


def test_transport_rate_limited_maps_to_deferred_finalization():
    fake_callable = FakeLegacyDelivery(
        error=TransportRateLimited(retry_after_seconds=33, backend="bot", op_name="send_video", key="k")
    )
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.should_defer is True
    assert result.retry_after == 33
    assert result.reason == "legacy_video_delivery_rate_limited"
    assert len(fake_callable.calls) == 1


def test_generic_exception_maps_to_failed_finalization():
    fake_callable = FakeLegacyDelivery(error=RuntimeError("boom"))
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert_failed_faulty(result, "legacy_video_delivery_failed")
    assert result.error_type == "RuntimeError"
    assert "boom" in result.error_text
    assert "exception" not in result.to_log_context()


def test_context_operation_updated_to_legacy_video_delivery():
    fake_callable = FakeLegacyDelivery(
        result=DeliveryPipelineResult.sent(context=context(operation="legacy_video_delivery"), sent_message_ids=[101])
    )
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert fake_callable.calls[0].context.operation == "legacy_video_delivery"
    assert result.context.operation == "legacy_video_delivery"


def test_pipeline_does_not_access_raw_bot_telethon_or_video_processor_directly():
    fake_callable = GuardedLegacyDelivery(result=DeliveryPipelineResult.sent(context=context(), sent_message_ids=[101]))
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert len(fake_callable.calls) == 1


def test_no_sensitive_values_in_result_log_context_or_label():
    data = input_data(
        legacy_payload={
            "caption": "PRIVATE CAPTION",
            "text": "PRIVATE TEXT",
            "video_path": "PRIVATE_VIDEO_PATH_SECRET_TOKEN",
            "token": "123:SECRET_TOKEN",
        }
    )
    fake_callable = FakeLegacyDelivery(result=None)
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(data))
    rendered = str(result.to_log_context())
    label = result.log_label()

    assert "PRIVATE CAPTION" not in rendered
    assert "PRIVATE TEXT" not in rendered
    assert "PRIVATE_VIDEO_PATH_SECRET_TOKEN" not in rendered
    assert "123:SECRET_TOKEN" not in rendered
    assert "PRIVATE CAPTION" not in label
    assert "PRIVATE TEXT" not in label
    assert "PRIVATE_VIDEO_PATH_SECRET_TOKEN" not in label
    assert "123:SECRET_TOKEN" not in label


def test_raw_result_objects_are_not_stored():
    fake_callable = FakeLegacyDelivery(result=telegram_send_success(method="legacy_video_delivery", sent_message_ids=[101]))
    pipeline = LegacyVideoDeliveryPipeline(legacy_video_delivery=fake_callable)

    result = run(pipeline.run(input_data()))
    field_names = {field.name for field in fields(result)}

    assert "legacy_result" not in field_names
    assert "telegram_send_result" not in field_names
    assert "pipeline_result" not in field_names
    assert "error" not in field_names
    assert "exception" not in field_names
    assert "legacy_payload" not in field_names
