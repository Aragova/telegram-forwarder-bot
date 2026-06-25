import asyncio
from dataclasses import fields

from app.delivery_context import DeliveryContext
from app.reaction_post_send_service import (
    ReactionPostSendInput,
    ReactionPostSendResult,
    ReactionPostSendService,
    ReactionPostSendStatus,
    normalize_reaction_message_ids,
    normalize_reactions,
)
from app.transport_policy import TransportRateLimited


class FakeReactionSender:
    def __init__(self, results=None, exceptions=None):
        self.calls = []
        self.results = list(results or [])
        self.exceptions = dict(exceptions or {})

    async def apply_reaction(self, **kwargs):
        self.calls.append(kwargs)
        message_id = kwargs["message_id"]
        if message_id in self.exceptions:
            raise self.exceptions[message_id]
        if self.results:
            return self.results.pop(0)
        return None


class CallableReactionSender:
    def __init__(self):
        self.calls = []

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return None


class GuardedRawSender(FakeReactionSender):
    @property
    def bot(self):
        raise AssertionError("raw bot must not be accessed")

    @property
    def raw(self):
        raise AssertionError("raw client must not be accessed")

    @property
    def telethon_client(self):
        raise AssertionError("telethon client must not be accessed")


def run(coro):
    return asyncio.run(coro)


def make_input(**overrides):
    data = {
        "context": DeliveryContext(delivery_id=1, operation="delivery"),
        "target_chat_id": -100,
        "target_message_ids": (10,),
        "reactions": ("👍",),
        "idempotency_key": "idem-1",
    }
    data.update(overrides)
    return ReactionPostSendInput(**data)


def test_normalize_reaction_message_ids():
    assert normalize_reaction_message_ids(None) == ()
    assert normalize_reaction_message_ids(1) == (1,)
    assert normalize_reaction_message_ids([1, 2]) == (1, 2)
    assert normalize_reaction_message_ids((3, 4)) == (3, 4)
    assert normalize_reaction_message_ids(i for i in [5, 5, 6]) == (5, 5, 6)
    assert normalize_reaction_message_ids([]) == ()


def test_normalize_reactions_ignores_empty_strings_and_preserves_order_duplicates():
    assert normalize_reactions(None) == ()
    assert normalize_reactions("👍") == ("👍",)
    assert normalize_reactions("") == ()
    assert normalize_reactions(["👍", "", "👍", "🔥"]) == ("👍", "👍", "🔥")
    assert normalize_reactions(("a", "b")) == ("a", "b")
    assert normalize_reactions(reaction for reaction in ["x", "", "x"]) == ("x", "x")
    assert normalize_reactions([]) == ()


def test_input_safe_log_context_excludes_raw_reactions_and_label():
    input_data = make_input(
        target_message_ids=[1, 2],
        reactions=("PRIVATE_REACTION_SECRET_TOKEN", "🔥"),
        reaction_label="PRIVATE_LABEL_SECRET_TOKEN",
    )

    log_context = input_data.to_log_context()
    log_text = str(log_context)

    assert log_context["target_message_ids"] == (1, 2)
    assert log_context["target_message_count"] == 2
    assert log_context["enabled"] is True
    assert log_context["idempotency_key"] == "idem-1"
    assert log_context["reaction_count"] == 2
    assert log_context["has_reactions"] is True
    assert log_context["has_reaction_label"] is True
    assert log_context["continue_on_error"] is False
    assert "PRIVATE_REACTION_SECRET_TOKEN" not in log_text
    assert "PRIVATE_LABEL_SECRET_TOKEN" not in log_text
    assert "SECRET_TOKEN" not in log_text


def test_result_constructors_properties_and_logs():
    base = {
        "context": DeliveryContext(delivery_id=1),
        "target_chat_id": -100,
        "target_message_ids": (1, 2),
        "applied_message_ids": (1,),
        "failed_message_ids": (2,),
        "reaction_count": 1,
        "idempotency_key": "idem",
    }
    applied = ReactionPostSendResult.applied(**{**base, "applied_message_ids": (1, 2), "failed_message_ids": ()})
    partial = ReactionPostSendResult.partial(**base, reason="reaction_apply_failed")
    skipped = ReactionPostSendResult.skipped(**base, reason="reactions_disabled")
    failed = ReactionPostSendResult.failed(**base, reason="reaction_apply_failed")
    limited = ReactionPostSendResult.rate_limited(**base, retry_after=60, reason="reaction_rate_limited")

    assert applied.ok is True
    assert applied.is_applied is True
    assert partial.ok is False
    assert partial.is_partial is True
    assert partial.is_failure is False
    assert skipped.is_skipped is True
    assert failed.is_failure is True
    assert limited.should_defer is True
    assert partial.to_log_context()["status"] == "partial"
    assert partial.to_log_context()["failed_message_count"] == 1
    assert "reaction_post_send status=partial target=-100 messages=2 applied=1 failed=1" == partial.log_label()


def test_service_stores_injected_dependency():
    fake_sender = FakeReactionSender()
    assert ReactionPostSendService(reaction_sender=fake_sender).reaction_sender is fake_sender


def test_disabled_skips_without_calling_sender():
    fake_sender = FakeReactionSender()
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(enabled=False)))
    assert result.status == ReactionPostSendStatus.SKIPPED
    assert result.reason == "reactions_disabled"
    assert fake_sender.calls == []


def test_missing_ids_and_reactions_skip():
    service = ReactionPostSendService(reaction_sender=FakeReactionSender())
    missing_ids = run(service.run(make_input(target_message_ids=None)))
    missing_reactions = run(service.run(make_input(reactions=[])))
    assert missing_ids.status == ReactionPostSendStatus.SKIPPED
    assert missing_ids.reason == "missing_target_message_ids"
    assert missing_reactions.status == ReactionPostSendStatus.SKIPPED
    assert missing_reactions.reason == "missing_reactions"


def test_missing_target_chat_id_and_sender_fail():
    missing_chat = run(ReactionPostSendService(reaction_sender=FakeReactionSender()).run(make_input(target_chat_id=None)))
    missing_sender = run(ReactionPostSendService(reaction_sender=None).run(make_input()))
    assert missing_chat.status == ReactionPostSendStatus.FAILED
    assert missing_chat.reason == "missing_target_chat_id"
    assert missing_sender.status == ReactionPostSendStatus.FAILED
    assert missing_sender.reason == "reaction_sender_not_configured"


def test_success_one_message():
    fake_sender = FakeReactionSender()
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=10)))
    assert len(fake_sender.calls) == 1
    assert result.status == ReactionPostSendStatus.APPLIED
    assert result.applied_message_ids == (10,)
    assert result.failed_message_ids == ()


def test_success_multiple_messages_in_order_with_duplicates():
    fake_sender = FakeReactionSender()
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1, 2, 2, 3])))
    assert [call["message_id"] for call in fake_sender.calls] == [1, 2, 2, 3]
    assert result.status == ReactionPostSendStatus.APPLIED
    assert result.applied_message_ids == (1, 2, 2, 3)


def test_apply_reaction_kwargs_shape_and_context_operation():
    fake_sender = FakeReactionSender()
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_chat_id="chat", target_message_ids=[7], reactions=["", "👍"])))
    call = fake_sender.calls[0]
    assert result.status == ReactionPostSendStatus.APPLIED
    assert call["chat_id"] == "chat"
    assert call["message_id"] == 7
    assert call["reactions"] == ("👍",)
    assert call["context"].operation == "reaction_post_send"


def test_callable_fallback():
    callable_sender = CallableReactionSender()
    result = run(ReactionPostSendService(reaction_sender=callable_sender).run(make_input(target_message_ids=[11])))
    assert result.status == ReactionPostSendStatus.APPLIED
    assert callable_sender.calls[0]["message_id"] == 11


def test_false_result_failure_stops_when_continue_on_error_false():
    fake_sender = FakeReactionSender(results=[None, False, None])
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1, 2, 3])))
    assert [call["message_id"] for call in fake_sender.calls] == [1, 2]
    assert result.status == ReactionPostSendStatus.FAILED
    assert result.reason == "reaction_apply_failed"
    assert result.applied_message_ids == (1,)
    assert result.failed_message_ids == (2,)


def test_continue_on_error_partial():
    fake_sender = FakeReactionSender(results=[None, False, None])
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1, 2, 3], continue_on_error=True)))
    assert result.status == ReactionPostSendStatus.PARTIAL
    assert result.applied_message_ids == (1, 3)
    assert result.failed_message_ids == (2,)


def test_all_failed_with_continue_on_error_returns_failed():
    fake_sender = FakeReactionSender(results=[False, False])
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1, 2], continue_on_error=True)))
    assert result.status == ReactionPostSendStatus.FAILED
    assert result.applied_message_ids == ()
    assert result.failed_message_ids == (1, 2)


def test_generic_exception_failure_stores_safe_error_type_text_only():
    fake_sender = FakeReactionSender(exceptions={1: RuntimeError("boom")})
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1])))
    assert result.status == ReactionPostSendStatus.FAILED
    assert result.reason == "reaction_apply_failed"
    assert result.error_type == "RuntimeError"
    assert result.error_text == "boom"
    assert not hasattr(result, "exception")


def test_transport_rate_limited_returns_defer_result_without_retry():
    error = TransportRateLimited(retry_after_seconds=33, backend="bot", op_name="set_reaction", key="k")
    fake_sender = FakeReactionSender(exceptions={2: error})
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input(target_message_ids=[1, 2, 3])))
    assert [call["message_id"] for call in fake_sender.calls] == [1, 2]
    assert result.status == ReactionPostSendStatus.RATE_LIMITED
    assert result.reason == "reaction_rate_limited"
    assert result.should_defer is True
    assert result.retry_after == 33
    assert result.applied_message_ids == (1,)
    assert result.failed_message_ids == (2,)


def test_no_raw_client_access():
    fake_sender = GuardedRawSender()
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(make_input()))
    assert result.status == ReactionPostSendStatus.APPLIED


def test_no_sensitive_result_logs():
    fake_sender = FakeReactionSender()
    input_data = make_input(
        reaction_label="PRIVATE_LABEL_SECRET_TOKEN",
        reactions=("🔥", "PRIVATE_REACTION_SECRET_TOKEN"),
    )
    result = run(ReactionPostSendService(reaction_sender=fake_sender).run(input_data))
    log_text = str(result.to_log_context())
    assert "PRIVATE_LABEL_SECRET_TOKEN" not in log_text
    assert "PRIVATE_REACTION_SECRET_TOKEN" not in log_text
    assert "PRIVATE_LABEL_SECRET_TOKEN" not in result.log_label()
    assert "PRIVATE_REACTION_SECRET_TOKEN" not in result.log_label()


def test_result_has_no_raw_fields():
    result_field_names = {field.name for field in fields(ReactionPostSendResult)}
    assert not {"reaction_sender", "raw_result", "raw_reaction_result", "exception", "error", "payload"} & result_field_names


def test_context_none_allowed():
    result = run(ReactionPostSendService(reaction_sender=FakeReactionSender()).run(make_input(context=None)))
    assert result.status == ReactionPostSendStatus.APPLIED
    assert result.context is None
    assert result.reason is None
