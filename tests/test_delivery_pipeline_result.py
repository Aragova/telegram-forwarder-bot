from __future__ import annotations

from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.delivery_pipeline_result import DeliveryPipelineResult, DeliveryPipelineStatus


def _ctx() -> DeliveryContext:
    return DeliveryContext(
        delivery_id=123,
        rule_id=45,
        post_id=67,
        target_id=-100123,
        mode="repost",
        operation="copy_message",
    )


def test_sent_result_construction():
    ctx = _ctx()

    result = DeliveryPipelineResult.sent(context=ctx, sent_message_ids=[101, 102])

    assert result.status == DeliveryPipelineStatus.SENT
    assert result.sent_message_ids == (101, 102)
    assert result.is_success is True
    assert result.is_failure is False
    assert result.should_defer is False


def test_sent_ids_normalization():
    assert DeliveryPipelineResult.sent(sent_message_ids=None).sent_message_ids == ()
    assert DeliveryPipelineResult.sent(sent_message_ids=101).sent_message_ids == (101,)
    assert DeliveryPipelineResult.sent(sent_message_ids=[101, 102]).sent_message_ids == (101, 102)
    assert DeliveryPipelineResult.sent(sent_message_ids=(201, 202)).sent_message_ids == (201, 202)


def test_failed_result_from_exception_does_not_store_raw_exception():
    error = RuntimeError("boom")

    result = DeliveryPipelineResult.failed(context=_ctx(), error=error)

    assert result.status == DeliveryPipelineStatus.FAILED
    assert result.error_type == "RuntimeError"
    assert result.error_text is not None
    assert "boom" in result.error_text
    assert result.is_failure is True
    assert "error" not in {field.name for field in fields(result)}


def test_rate_limited_result():
    result = DeliveryPipelineResult.rate_limited(context=_ctx(), retry_after=60)

    assert result.status == DeliveryPipelineStatus.RATE_LIMITED
    assert result.retry_after == 60
    assert result.should_defer is True
    assert result.is_failure is False


def test_skipped_and_noop_are_skipped():
    assert DeliveryPipelineResult.skipped(reason="already_sent").is_skipped is True
    assert DeliveryPipelineResult.noop(reason="empty").is_skipped is True


def test_to_log_context_is_safe_and_includes_context():
    result = DeliveryPipelineResult.sent(context=_ctx(), sent_message_ids=[101, 102])

    log_context = result.to_log_context()

    assert log_context["status"] == "sent"
    assert log_context["sent_message_count"] == 2
    assert log_context["context"]["delivery_id"] == 123
    assert log_context["context"]["rule_id"] == 45
    assert log_context["context"]["post_id"] == 67


def test_log_label_is_compact_and_safe():
    result = DeliveryPipelineResult.sent(context=_ctx(), sent_message_ids=[101])

    label = result.log_label()

    assert "status=sent" in label
    assert "delivery=123" in label
    assert "rule=45" in label
    assert "post=67" in label
    assert "sent_count=1" in label


def test_sensitive_payload_does_not_leak_to_logs_or_label():
    ctx = DeliveryContext.from_job_payload(
        {
            "delivery_id": 123,
            "rule_id": 45,
            "post_id": 67,
            "content_json": {"text": "PRIVATE TEXT"},
            "caption": "PRIVATE CAPTION",
            "bot_token": "123:SECRET_TOKEN",
        }
    )
    result = DeliveryPipelineResult.sent(context=ctx, sent_message_ids=[101])

    log_context_text = str(result.to_log_context())
    label = result.log_label()

    assert "PRIVATE TEXT" not in log_context_text
    assert "PRIVATE CAPTION" not in log_context_text
    assert "SECRET_TOKEN" not in log_context_text
    assert "PRIVATE TEXT" not in label
    assert "PRIVATE CAPTION" not in label
    assert "SECRET_TOKEN" not in label


def test_with_context_returns_new_result_without_mutating_original():
    result = DeliveryPipelineResult.sent(sent_message_ids=[1])

    result2 = result.with_context(_ctx())

    assert result.context is None
    assert result2.context is not None
    assert result2.context.delivery_id == 123
    assert result2 is not result
