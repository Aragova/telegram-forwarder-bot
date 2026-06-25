from __future__ import annotations

import asyncio

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import DeliveryFinalizationStatus, DeliveryOutcome, DeliveryFinalizer
from app.post_send_steps import PostSendStepsResult
from app.telegram_send_result import telegram_send_success
from app.transport_policy import TransportRateLimited
from app.video_send_pipeline import VideoSendInput, VideoSendMethod, VideoSendPipeline


class FakeGateway:
    def __init__(self, result=None, error=None):
        self.result = result or telegram_send_success(method="send_video", sent_message_ids=[101])
        self.error = error
        self.send_video_calls = []
        self.send_document_calls = []
        self.telethon_send_file_calls = []

    async def send_video(self, **kwargs):
        self.send_video_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result

    async def send_document(self, **kwargs):
        self.send_document_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result

    async def telethon_send_file(self, **kwargs):
        self.telethon_send_file_calls.append(kwargs)
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


def run(coro):
    return asyncio.run(coro)


def context(**kwargs) -> DeliveryContext:
    values = dict(delivery_id=1, rule_id=2, post_id=3, source_id=-100, target_id=-200, message_id=10, mode="video")
    values.update(kwargs)
    return DeliveryContext(**values)


def input_data(**kwargs) -> VideoSendInput:
    values = dict(
        context=context(operation="initial", target_id=-200),
        target_chat_id=-200,
        target_thread_id=55,
        video="prepared-video",
        method=VideoSendMethod.BOT_SEND_VIDEO,
        idempotency_key="idem-1",
        telegram_method=None,
        disable_notification=True,
        protect_content=False,
    )
    values.update(kwargs)
    return VideoSendInput(**values)


def test_input_to_log_context_is_safe():
    data = input_data(
        video="PRIVATE_VIDEO_PATH_SECRET_TOKEN",
        thumbnail="PRIVATE_THUMB_PATH",
        caption="PRIVATE TEXT SECRET_TOKEN",
        caption_entities={"secret": "SECRET_TOKEN"},
    )

    log_context = data.to_log_context()
    rendered = str(log_context)

    assert log_context["target_chat_id"] == -200
    assert log_context["target_thread_id"] == 55
    assert log_context["method"] == "bot_send_video"
    assert log_context["idempotency_key"] == "idem-1"
    assert log_context["has_video"] is True
    assert log_context["has_caption"] is True
    assert log_context["has_caption_entities"] is True
    assert log_context["has_thumbnail"] is True
    assert log_context["context"] == data.context.to_log_context()
    assert "PRIVATE_VIDEO_PATH_SECRET_TOKEN" not in rendered
    assert "PRIVATE_THUMB_PATH" not in rendered
    assert "PRIVATE TEXT" not in rendered
    assert "SECRET_TOKEN" not in rendered


def test_pipeline_stores_injected_dependencies():
    gateway = FakeGateway()
    post_send = FakePostSendSteps()
    finalizer = DeliveryFinalizer()

    pipeline = VideoSendPipeline(send_gateway=gateway, post_send_steps=post_send, finalizer=finalizer)
    default_pipeline = VideoSendPipeline(send_gateway=gateway)

    assert pipeline.send_gateway is gateway
    assert pipeline.post_send_steps is post_send
    assert pipeline.finalizer is finalizer
    assert isinstance(default_pipeline.finalizer, DeliveryFinalizer)


def test_bot_send_video_success_without_post_send_steps():
    gateway = FakeGateway(result=telegram_send_success(method="send_video", sent_message_ids=[101]))
    pipeline = VideoSendPipeline(send_gateway=gateway, post_send_steps=None)

    result = run(pipeline.run(input_data()))

    assert len(gateway.send_video_calls) == 1
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101,)
    assert result.pipeline_status == "sent"


def test_bot_send_video_success_with_post_send_steps():
    gateway = FakeGateway(result=telegram_send_success(method="send_video", sent_message_ids=[101]))
    post_send = FakePostSendSteps()
    pipeline = VideoSendPipeline(send_gateway=gateway, post_send_steps=post_send)

    result = run(pipeline.run(input_data()))

    assert len(gateway.send_video_calls) == 1
    assert len(post_send.calls) == 1
    assert post_send.calls[0]["send_result"] is gateway.result
    assert post_send.calls[0]["sent_message_ids"] == [101]
    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.post_send_status == "completed"


def test_bot_send_document_method_uses_send_document():
    gateway = FakeGateway(result=telegram_send_success(method="send_document", sent_message_ids=[202]))
    pipeline = VideoSendPipeline(send_gateway=gateway)

    result = run(pipeline.run(input_data(method=VideoSendMethod.BOT_SEND_DOCUMENT)))

    assert len(gateway.send_document_calls) == 1
    assert not gateway.send_video_calls
    assert not gateway.telethon_send_file_calls
    assert result.context.operation == "send_document"


def test_telethon_send_file_method_uses_telethon_send_file():
    gateway = FakeGateway(result=telegram_send_success(method="telethon_send_file", sent_message_ids=[303]))
    pipeline = VideoSendPipeline(send_gateway=gateway)

    result = run(pipeline.run(input_data(method=VideoSendMethod.TELETHON_SEND_FILE)))

    assert len(gateway.telethon_send_file_calls) == 1
    assert not gateway.send_video_calls
    assert not gateway.send_document_calls
    assert result.context.operation == "telethon_send_file"


def test_kwargs_forwarding_for_send_video_and_none_values_are_omitted():
    gateway = FakeGateway()
    pipeline = VideoSendPipeline(send_gateway=gateway)
    entities = object()
    markup = object()
    thumb = object()
    data = input_data(
        video="video-object",
        caption="caption",
        caption_entities=entities,
        parse_mode="HTML",
        thumbnail=thumb,
        duration=12,
        width=640,
        height=360,
        supports_streaming=True,
        reply_markup=markup,
        allow_sending_without_reply=False,
        telegram_method=None,
    )

    run(pipeline.run(data))

    assert gateway.send_video_calls[0] == {
        "chat_id": -200,
        "video": "video-object",
        "message_thread_id": 55,
        "caption": "caption",
        "caption_entities": entities,
        "parse_mode": "HTML",
        "thumbnail": thumb,
        "duration": 12,
        "width": 640,
        "height": 360,
        "supports_streaming": True,
        "disable_notification": True,
        "protect_content": False,
        "reply_markup": markup,
        "allow_sending_without_reply": False,
    }

    gateway = FakeGateway()
    pipeline = VideoSendPipeline(send_gateway=gateway)
    run(pipeline.run(input_data(target_thread_id=None, caption=None, thumbnail=None)))
    assert "message_thread_id" not in gateway.send_video_calls[0]
    assert "caption" not in gateway.send_video_calls[0]
    assert "thumbnail" not in gateway.send_video_calls[0]


def test_kwargs_forwarding_for_send_document_without_thumbnail():
    gateway = FakeGateway(result=telegram_send_success(method="send_document", sent_message_ids=[202]))
    pipeline = VideoSendPipeline(send_gateway=gateway)
    entities = object()
    markup = object()
    data = input_data(
        method=VideoSendMethod.BOT_SEND_DOCUMENT,
        video="video-document",
        caption="caption",
        caption_entities=entities,
        parse_mode="HTML",
        thumbnail="not-forwarded-for-document",
        reply_markup=markup,
        allow_sending_without_reply=False,
    )

    run(pipeline.run(data))

    assert gateway.send_document_calls[0] == {
        "chat_id": -200,
        "document": "video-document",
        "message_thread_id": 55,
        "caption": "caption",
        "caption_entities": entities,
        "parse_mode": "HTML",
        "disable_notification": True,
        "protect_content": False,
        "reply_markup": markup,
        "allow_sending_without_reply": False,
    }
    assert "thumbnail" not in gateway.send_document_calls[0]


def test_kwargs_forwarding_for_telethon_send_file():
    gateway = FakeGateway(result=telegram_send_success(method="telethon_send_file", sent_message_ids=[303]))
    pipeline = VideoSendPipeline(send_gateway=gateway)

    run(pipeline.run(input_data(method=VideoSendMethod.TELETHON_SEND_FILE, video="video-file", caption="caption")))

    assert gateway.telethon_send_file_calls[0] == {"entity": -200, "file": "video-file", "caption": "caption"}


def test_context_operation_updated_by_method():
    expected = {
        VideoSendMethod.BOT_SEND_VIDEO: "send_video",
        VideoSendMethod.BOT_SEND_DOCUMENT: "send_document",
        VideoSendMethod.TELETHON_SEND_FILE: "telethon_send_file",
    }
    for method, operation in expected.items():
        gateway = FakeGateway(result=telegram_send_success(method=operation, sent_message_ids=[101]))
        result = run(VideoSendPipeline(send_gateway=gateway).run(input_data(method=method)))
        assert result.context.operation == operation


def test_send_exception_maps_to_failed_finalization():
    gateway = FakeGateway(error=RuntimeError("boom"))
    post_send = FakePostSendSteps()
    pipeline = VideoSendPipeline(send_gateway=gateway, post_send_steps=post_send)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "video_send_failed"
    assert not post_send.calls
    assert not isinstance(result.error_text, RuntimeError)


def test_transport_rate_limited_maps_to_deferred_finalization():
    error = TransportRateLimited(retry_after_seconds=60, backend="bot", op_name="send_video", key="sender.bot.send_video")
    gateway = FakeGateway(error=error)
    post_send = FakePostSendSteps()
    pipeline = VideoSendPipeline(send_gateway=gateway, post_send_steps=post_send)

    result = run(pipeline.run(input_data()))

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.should_defer is True
    assert result.retry_after == 60
    assert result.reason == "video_send_rate_limited"
    assert not post_send.calls


def test_post_send_rate_limited_propagates_to_finalization():
    post_result = PostSendStepsResult.rate_limited(retry_after=60, reason="post_send_rate_limited")
    result = run(VideoSendPipeline(send_gateway=FakeGateway(), post_send_steps=FakePostSendSteps(post_result)).run(input_data()))

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.retry_after == 60


def test_post_send_failed_maps_to_failed_finalization():
    post_result = PostSendStepsResult.failed(reason="ledger_failed")
    result = run(VideoSendPipeline(send_gateway=FakeGateway(), post_send_steps=FakePostSendSteps(post_result)).run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "ledger_failed"


def test_post_send_exception_maps_to_failed_finalization():
    post_send = FakePostSendSteps(error=RuntimeError("post boom"))
    result = run(VideoSendPipeline(send_gateway=FakeGateway(), post_send_steps=post_send).run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.reason == "post_send_steps_failed"
    assert not isinstance(result.error_text, RuntimeError)


def test_missing_required_input_fields_fail_safely():
    cases = [
        (dict(context=None), "missing_context"),
        (dict(target_chat_id=None), "missing_target_chat_id"),
        (dict(video=None), "missing_video"),
        (dict(method=None), "missing_video_send_method"),
    ]
    for overrides, reason in cases:
        gateway = FakeGateway()
        post_send = FakePostSendSteps()
        result = run(VideoSendPipeline(send_gateway=gateway, post_send_steps=post_send).run(input_data(**overrides)))
        assert result.status == DeliveryFinalizationStatus.FAILED
        assert result.outcome == DeliveryOutcome.FAULTY
        assert result.reason == reason
        assert not gateway.send_video_calls
        assert not gateway.send_document_calls
        assert not gateway.telethon_send_file_calls
        assert not post_send.calls


def test_pipeline_does_not_access_raw_bot_or_telethon_directly():
    gateway = GuardedGateway()
    result = run(VideoSendPipeline(send_gateway=gateway).run(input_data()))

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert len(gateway.send_video_calls) == 1


def test_no_sensitive_values_in_result_log_context_or_label():
    result = run(
        VideoSendPipeline(send_gateway=FakeGateway()).run(
            input_data(
                video="PRIVATE_VIDEO_PATH_SECRET_TOKEN",
                thumbnail="PRIVATE_THUMB_PATH",
                caption="PRIVATE CAPTION 123:SECRET_TOKEN",
            )
        )
    )

    rendered = str(result.to_log_context())
    label = result.log_label()
    for secret in ("PRIVATE_VIDEO_PATH_SECRET_TOKEN", "PRIVATE_THUMB_PATH", "PRIVATE CAPTION", "SECRET_TOKEN"):
        assert secret not in rendered
        assert secret not in label
