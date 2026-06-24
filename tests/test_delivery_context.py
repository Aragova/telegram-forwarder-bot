from __future__ import annotations

from app.delivery_context import DeliveryContext


def test_basic_construction_with_minimal_fields():
    ctx = DeliveryContext(delivery_id=123, rule_id=45, target_id=-100123, mode="repost")

    assert ctx.delivery_id == 123
    assert ctx.rule_id == 45
    assert ctx.target_id == -100123
    assert ctx.mode == "repost"
    assert ctx.is_album is False


def test_to_log_context_returns_safe_technical_fields():
    ctx = DeliveryContext(
        delivery_id=123,
        rule_id=45,
        post_id=67,
        target_id=-100123,
        mode="repost",
        operation="copy_message",
    )

    log_context = ctx.to_log_context()

    assert isinstance(log_context, dict)
    assert log_context["delivery_id"] == 123
    assert log_context["rule_id"] == 45
    assert log_context["post_id"] == 67
    assert log_context["target_id"] == -100123
    assert log_context["mode"] == "repost"
    assert log_context["operation"] == "copy_message"
    assert set(log_context) == {
        "delivery_id",
        "rule_id",
        "post_id",
        "source_id",
        "source_thread_id",
        "target_id",
        "target_thread_id",
        "message_id",
        "media_group_id",
        "mode",
        "operation",
        "is_album",
    }


def test_to_log_context_does_not_include_sensitive_payload_from_factory():
    payload = {
        "delivery_id": 123,
        "rule_id": 45,
        "post_id": 67,
        "target_id": -100123,
        "mode": "repost",
        "content_json": {"text": "PRIVATE TEXT"},
        "caption": "PRIVATE CAPTION",
        "bot_token": "123:SECRET_TOKEN",
    }

    ctx = DeliveryContext.from_job_payload(payload)
    log_context_text = str(ctx.to_log_context())

    assert "PRIVATE TEXT" not in log_context_text
    assert "PRIVATE CAPTION" not in log_context_text
    assert "SECRET_TOKEN" not in log_context_text


def test_log_label_is_compact_and_safe():
    payload = {
        "delivery_id": 123,
        "rule_id": 45,
        "post_id": 67,
        "target_id": -100123,
        "mode": "repost",
        "caption": "PRIVATE CAPTION",
        "content_json": {"text": "PRIVATE TEXT"},
        "bot_token": "123:SECRET_TOKEN",
    }

    label = DeliveryContext.from_job_payload(payload).log_label()

    assert "delivery=123" in label
    assert "rule=45" in label
    assert "post=67" in label
    assert "mode=repost" in label
    assert "PRIVATE CAPTION" not in label
    assert "SECRET_TOKEN" not in label
    assert "content_json" not in label


def test_with_operation_returns_new_context_without_mutating_original():
    ctx = DeliveryContext(delivery_id=123)

    ctx2 = ctx.with_operation("copy_message")

    assert ctx.operation is None
    assert ctx2.operation == "copy_message"
    assert ctx2 is not ctx


def test_factory_ignores_unknown_keys_and_does_not_store_raw_payload():
    ctx = DeliveryContext.from_job_payload(
        {
            "delivery_id": 123,
            "unknown_payload": {"secret": "PRIVATE VALUE"},
            "raw_rule": {"caption": "PRIVATE CAPTION"},
        }
    )

    assert ctx.delivery_id == 123
    assert not hasattr(ctx, "unknown_payload")
    assert "PRIVATE VALUE" not in str(ctx.to_log_context())
    assert "PRIVATE CAPTION" not in ctx.log_label()


def test_factory_handles_missing_keys():
    ctx = DeliveryContext.from_job_payload({})

    assert ctx.delivery_id is None
    assert ctx.rule_id is None
    assert ctx.post_id is None
    assert ctx.source_id is None
    assert ctx.source_thread_id is None
    assert ctx.target_id is None
    assert ctx.target_thread_id is None
    assert ctx.message_id is None
    assert ctx.media_group_id is None
    assert ctx.mode is None
    assert ctx.operation is None
    assert ctx.is_album is False
