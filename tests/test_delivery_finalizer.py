from __future__ import annotations

from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.delivery_finalizer import (
    DeliveryFinalizationResult,
    DeliveryFinalizationSource,
    DeliveryFinalizationStatus,
    DeliveryFinalizer,
    DeliveryOutcome,
)
from app.delivery_pipeline_result import DeliveryPipelineResult
from app.post_send_steps import PostSendStepsResult


def _ctx() -> DeliveryContext:
    return DeliveryContext(
        delivery_id=123,
        rule_id=45,
        post_id=67,
        target_id=-100123,
        mode="repost",
        operation="copy_message",
    )


def test_result_finalized_construction() -> None:
    result = DeliveryFinalizationResult.finalized(
        context=_ctx(),
        sent_message_ids=[101, 102],
        source=DeliveryFinalizationSource.POST_SEND,
    )

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.ok is True
    assert result.sent_message_ids == (101, 102)


def test_result_failed_construction() -> None:
    result = DeliveryFinalizationResult.failed(
        context=_ctx(),
        reason="pipeline_failed",
        error_type="RuntimeError",
        error_text="boom",
    )

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.is_failure is True
    assert result.error_type == "RuntimeError"
    assert result.error_text == "boom"


def test_result_rate_limited_construction() -> None:
    result = DeliveryFinalizationResult.rate_limited(
        retry_after=60,
        reason="telegram_rate_limited",
    )

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.should_defer is True
    assert result.retry_after == 60


def test_result_skipped_and_noop_construction() -> None:
    assert DeliveryFinalizationResult.skipped(reason="already_sent").is_skipped is True
    assert DeliveryFinalizationResult.noop(reason="missing_finalization_input").is_noop is True


def test_finalize_pipeline_result_maps_sent() -> None:
    pipeline_result = DeliveryPipelineResult.sent(context=_ctx(), sent_message_ids=[101, 102])

    result = DeliveryFinalizer().finalize_pipeline_result(pipeline_result=pipeline_result)

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.sent_message_ids == (101, 102)
    assert result.pipeline_status == "sent"


def test_finalize_pipeline_result_maps_failed() -> None:
    pipeline_result = DeliveryPipelineResult.failed(error=RuntimeError("boom"), reason="pipeline_failed")

    result = DeliveryFinalizer().finalize_pipeline_result(pipeline_result=pipeline_result)

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.error_type == "RuntimeError"
    assert result.error_text is not None
    assert "boom" in result.error_text
    assert "pipeline_result" not in {field.name for field in fields(result)}


def test_finalize_pipeline_result_maps_rate_limited() -> None:
    pipeline_result = DeliveryPipelineResult.rate_limited(retry_after=60)

    result = DeliveryFinalizer().finalize_pipeline_result(pipeline_result=pipeline_result)

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.retry_after == 60


def test_finalize_pipeline_result_maps_skipped_and_noop() -> None:
    finalizer = DeliveryFinalizer()

    skipped = finalizer.finalize_pipeline_result(pipeline_result=DeliveryPipelineResult.skipped(reason="already_sent"))
    noop = finalizer.finalize_pipeline_result(pipeline_result=DeliveryPipelineResult.noop(reason="nothing_to_send"))

    assert skipped.status == DeliveryFinalizationStatus.SKIPPED
    assert skipped.outcome == DeliveryOutcome.SKIPPED
    assert noop.status == DeliveryFinalizationStatus.NOOP
    assert noop.outcome == DeliveryOutcome.NOOP


def test_finalize_post_send_result_maps_completed() -> None:
    post_send_result = PostSendStepsResult.completed(sent_message_ids=(101, 102))

    result = DeliveryFinalizer().finalize_post_send_result(post_send_result=post_send_result)

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.post_send_status == "completed"
    assert result.sent_message_ids == (101, 102)


def test_finalize_post_send_result_maps_failed() -> None:
    post_send_result = PostSendStepsResult.failed(
        reason="verification_failed",
        error_type="RuntimeError",
        error_text="boom",
    )

    result = DeliveryFinalizer().finalize_post_send_result(post_send_result=post_send_result)

    assert result.status == DeliveryFinalizationStatus.FAILED
    assert result.outcome == DeliveryOutcome.FAULTY
    assert result.post_send_status == "failed"
    assert result.reason == "verification_failed"
    assert result.error_type == "RuntimeError"
    assert result.error_text == "boom"


def test_finalize_post_send_result_maps_rate_limited() -> None:
    post_send_result = PostSendStepsResult.rate_limited(retry_after=60)

    result = DeliveryFinalizer().finalize_post_send_result(post_send_result=post_send_result)

    assert result.status == DeliveryFinalizationStatus.RATE_LIMITED
    assert result.outcome == DeliveryOutcome.DEFERRED
    assert result.retry_after == 60


def test_finalize_post_send_result_maps_skipped() -> None:
    post_send_result = PostSendStepsResult.skipped(reason="disabled")

    result = DeliveryFinalizer().finalize_post_send_result(post_send_result=post_send_result)

    assert result.status == DeliveryFinalizationStatus.SKIPPED
    assert result.outcome == DeliveryOutcome.SKIPPED


def test_finalize_prioritizes_post_send_result_over_pipeline_result() -> None:
    pipeline_result = DeliveryPipelineResult.failed(error=RuntimeError("boom"))
    post_send_result = PostSendStepsResult.completed(sent_message_ids=(301,))

    result = DeliveryFinalizer().finalize(
        pipeline_result=pipeline_result,
        post_send_result=post_send_result,
    )

    assert result.status == DeliveryFinalizationStatus.FINALIZED
    assert result.outcome == DeliveryOutcome.SENT
    assert result.pipeline_status == "failed"
    assert result.post_send_status == "completed"


def test_finalize_without_inputs_returns_noop() -> None:
    result = DeliveryFinalizer().finalize()

    assert result.status == DeliveryFinalizationStatus.NOOP
    assert result.outcome == DeliveryOutcome.NOOP
    assert result.reason == "missing_finalization_input"


def test_safe_log_context_and_label_do_not_include_sensitive_values() -> None:
    ctx = DeliveryContext.from_job_payload(
        {
            "delivery_id": 123,
            "rule_id": 45,
            "post_id": 67,
            "target_id": -100123,
            "mode": "repost",
            "content_json": {"text": "PRIVATE TEXT"},
            "caption": "PRIVATE CAPTION",
            "bot_token": "123:SECRET_TOKEN",
        }
    )
    result = DeliveryFinalizationResult.finalized(
        context=ctx,
        sent_message_ids=[101, 102],
        source=DeliveryFinalizationSource.POST_SEND,
        pipeline_status="sent",
        post_send_status="completed",
    )

    log_context = result.to_log_context()
    label = result.log_label()
    combined = f"{log_context} {label}"

    assert log_context["status"] == "finalized"
    assert log_context["outcome"] == "sent"
    assert log_context["source"] == "post_send"
    assert log_context["sent_message_count"] == 2
    assert log_context["pipeline_status"] == "sent"
    assert log_context["post_send_status"] == "completed"
    assert "status=finalized" in label
    assert "outcome=sent" in label
    assert "source=post_send" in label
    assert "ids=2" in label
    assert "pipeline=sent" in label
    assert "post_send=completed" in label
    assert "PRIVATE CAPTION" not in combined
    assert "PRIVATE TEXT" not in combined
    assert "123:SECRET_TOKEN" not in combined


def test_raw_result_objects_are_not_stored() -> None:
    result = DeliveryFinalizationResult.noop()
    field_names = {field.name for field in fields(result)}

    assert "pipeline_result" not in field_names
    assert "post_send_result" not in field_names
    assert "telegram_send_result" not in field_names
    assert "ledger_result" not in field_names
    assert "verification_result" not in field_names
    assert "error" not in field_names
    assert "exception" not in field_names


def test_error_text_truncation() -> None:
    long_text = "x" * 600

    result = DeliveryFinalizationResult.failed(error_text=long_text)

    assert result.error_text is not None
    assert len(result.error_text) == 500
    assert result.error_text.endswith("...")
