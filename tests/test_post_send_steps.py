from __future__ import annotations

import asyncio

from app.attempt_ledger_service import AttemptLedgerAction, AttemptLedgerOperationResult, DeliveryAttemptStatus
from app.delivery_context import DeliveryContext
from app.post_send_steps import PostSendSteps, PostSendStepsResult, PostSendStepsStatus
from app.target_verifier import TargetVerificationResult
from app.telegram_send_result import TelegramSendResult


def run(coro):
    return asyncio.run(coro)

class FakeLedger:
    def __init__(self, result=None, *, raises: BaseException | None = None, events: list[str] | None = None):
        self.result = result or AttemptLedgerOperationResult(
            ok=True,
            action=AttemptLedgerAction.MARK_ACCEPTED,
            idempotency_key="key-1",
            attempt_id=42,
            status=DeliveryAttemptStatus.ACCEPTED,
        )
        self.raises = raises
        self.calls = []
        self.events = events

    def mark_accepted(self, **kwargs):
        if self.events is not None:
            self.events.append("ledger")
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return self.result


class FakeVerifier:
    def __init__(self, result=None, *, raises: BaseException | None = None, events: list[str] | None = None):
        self.result = result or TargetVerificationResult.verified(target_id=-100, message_ids=(10,))
        self.raises = raises
        self.message_calls = []
        self.send_result_calls = []
        self.events = events

    async def verify_message_ids(self, **kwargs):
        if self.events is not None:
            self.events.append("verifier")
        self.message_calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return self.result

    async def verify_send_result(self, **kwargs):
        if self.events is not None:
            self.events.append("verifier")
        self.send_result_calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return self.result


def send_result(ids=(10, 11)):
    return TelegramSendResult(ok=True, method="copy_message", sent_message_ids=list(ids), sent_message_id=ids[0] if ids else None)


def test_service_stores_injected_dependencies():
    ledger = FakeLedger()
    verifier = FakeVerifier()
    service = PostSendSteps(attempt_ledger=ledger, target_verifier=verifier)

    assert service.attempt_ledger is ledger
    assert service.target_verifier is verifier


def test_normalize_sent_message_ids():
    service = PostSendSteps()

    assert service.normalize_sent_message_ids() == ()
    assert service.normalize_sent_message_ids(10) == (10,)
    assert service.normalize_sent_message_ids([10, 11]) == (10, 11)
    assert service.normalize_sent_message_ids((12, 13)) == (12, 13)
    assert service.normalize_sent_message_ids((value for value in [14, 15])) == (14, 15)
    assert service.normalize_sent_message_ids(send_result=send_result((20, 21))) == (20, 21)
    assert service.normalize_sent_message_ids([30], send_result=send_result((20, 21))) == (30,)


def test_missing_sent_ids_fails_safely_without_calling_dependencies():
    ledger = FakeLedger()
    verifier = FakeVerifier()
    result = run(PostSendSteps(attempt_ledger=ledger, target_verifier=verifier).run_after_send(send_result=None, sent_message_ids=None))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.ok is False
    assert result.reason == "missing_sent_message_ids"
    assert ledger.calls == []
    assert verifier.message_calls == []
    assert verifier.send_result_calls == []


def test_mark_accepted_only_completes_without_verifier():
    ledger = FakeLedger()
    verifier = FakeVerifier()

    result = run(PostSendSteps(attempt_ledger=ledger, target_verifier=verifier).run_after_send(
        sent_message_ids=(10, 11), idempotency_key="key-1", mark_attempt_accepted=True, verify_target=False
    ))

    assert len(ledger.calls) == 1
    assert ledger.calls[0]["sent_message_ids"] == (10, 11)
    assert verifier.message_calls == []
    assert verifier.send_result_calls == []
    assert result.status == PostSendStepsStatus.COMPLETED
    assert result.ledger_ok is True
    assert result.sent_message_ids == (10, 11)


def test_verify_target_only_completes_without_ledger():
    ledger = FakeLedger()
    verifier = FakeVerifier(TargetVerificationResult.verified(target_id=-100, message_ids=(10,), found_message_ids=(10,)))

    result = run(PostSendSteps(attempt_ledger=ledger, target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), target_id=-100, mark_attempt_accepted=False, verify_target=True
    ))

    assert ledger.calls == []
    assert len(verifier.message_calls) == 1
    assert result.status == PostSendStepsStatus.COMPLETED
    assert result.verification_status == "verified"


def test_both_steps_enabled_success_calls_ledger_before_verifier():
    events = []
    ledger = FakeLedger(events=events)
    verifier = FakeVerifier(TargetVerificationResult.verified(target_id=-100, message_ids=(10,)), events=events)

    result = run(PostSendSteps(attempt_ledger=ledger, target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), idempotency_key="key-1", target_id=-100
    ))

    assert events == ["ledger", "verifier"]
    assert result.status == PostSendStepsStatus.COMPLETED
    assert result.ledger_ok is True
    assert result.verification_status == "verified"


def test_missing_ledger_when_mark_enabled_fails_before_verifier():
    verifier = FakeVerifier()
    result = run(PostSendSteps(attempt_ledger=None, target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), idempotency_key="key-1", mark_attempt_accepted=True, verify_target=False
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.reason == "attempt_ledger_not_configured"
    assert verifier.message_calls == []


def test_missing_idempotency_key_when_mark_enabled_fails_before_ledger():
    ledger = FakeLedger()
    result = run(PostSendSteps(attempt_ledger=ledger).run_after_send(
        sent_message_ids=(10,), mark_attempt_accepted=True, verify_target=False
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.reason == "missing_idempotency_key"
    assert ledger.calls == []


def test_ledger_failure_stops_verifier():
    ledger = FakeLedger(AttemptLedgerOperationResult(ok=False, action=AttemptLedgerAction.MARK_ACCEPTED, idempotency_key="key-1", status="accepted"))
    verifier = FakeVerifier()

    result = run(PostSendSteps(attempt_ledger=ledger, target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), idempotency_key="key-1", target_id=-100
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.reason == "ledger_mark_accepted_failed"
    assert verifier.message_calls == []
    assert verifier.send_result_calls == []


def test_missing_verifier_when_verify_enabled_fails():
    result = run(PostSendSteps(target_verifier=None).run_after_send(
        sent_message_ids=(10,), mark_attempt_accepted=False, verify_target=True, target_id=-100
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.reason == "target_verifier_not_configured"


def test_missing_target_id_when_verify_enabled_fails_before_verifier():
    verifier = FakeVerifier()
    result = run(PostSendSteps(target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), mark_attempt_accepted=False, verify_target=True, target_id=None
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.reason == "missing_target_id"
    assert verifier.message_calls == []


def test_verifier_rate_limited_maps_to_rate_limited():
    verifier = FakeVerifier(TargetVerificationResult.rate_limited(target_id=-100, message_ids=(10,), retry_after=12.5))

    result = run(PostSendSteps(target_verifier=verifier).run_after_send(
        sent_message_ids=(10,), target_id=-100, mark_attempt_accepted=False, verify_target=True
    ))

    assert result.status == PostSendStepsStatus.RATE_LIMITED
    assert result.should_defer is True
    assert result.retry_after == 12.5


def test_verifier_not_found_maps_to_failed_with_missing_ids():
    verifier = FakeVerifier(TargetVerificationResult.not_found(target_id=-100, message_ids=(10, 11), found_message_ids=(10,), missing_message_ids=(11,)))

    result = run(PostSendSteps(target_verifier=verifier).run_after_send(
        sent_message_ids=(10, 11), target_id=-100, mark_attempt_accepted=False, verify_target=True
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.verification_status == "not_found"
    assert result.missing_message_ids == (11,)


def test_both_steps_disabled_returns_skipped():
    result = run(PostSendSteps().run_after_send(sent_message_ids=(10,), mark_attempt_accepted=False, verify_target=False))

    assert result.status == PostSendStepsStatus.SKIPPED
    assert result.reason == "post_send_steps_disabled"


def test_dependency_exceptions_are_safe_failures_without_raw_exception():
    result = run(PostSendSteps(attempt_ledger=FakeLedger(raises=RuntimeError("boom"))).run_after_send(
        sent_message_ids=(10,), idempotency_key="key-1", mark_attempt_accepted=True, verify_target=False
    ))

    assert result.status == PostSendStepsStatus.FAILED
    assert result.error_type == "RuntimeError"
    assert "boom" in (result.error_text or "")
    assert not any(isinstance(value, RuntimeError) for value in result.to_log_context().values())


def test_safe_log_context_and_label_do_not_include_sensitive_values():
    context = DeliveryContext.from_job_payload(
        {
            "delivery_id": 1,
            "rule_id": 2,
            "target_id": -100,
            "caption": "PRIVATE CAPTION",
            "text": "PRIVATE TEXT",
            "bot_token": "123:SECRET_TOKEN",
        }
    )
    result = PostSendStepsResult.completed(
        context=context,
        sent_message_ids=(10, 11),
        idempotency_key="key-1",
        target_id=-100,
        ledger_ok=True,
        ledger_action="mark_accepted",
        ledger_attempt_id=42,
        ledger_status="accepted",
        verification_status="verified",
        found_message_ids=(10, 11),
    )

    log_context = result.to_log_context()
    label = result.log_label()
    serialized = f"{log_context} {label}"

    assert log_context["status"] == "completed"
    assert log_context["sent_message_ids"] == (10, 11)
    assert log_context["sent_message_count"] == 2
    assert log_context["ledger_ok"] is True
    assert log_context["verification_status"] == "verified"
    assert "status=completed" in label
    assert "ids=2" in label
    assert "ledger=True" in label
    assert "verification=verified" in label
    assert "PRIVATE CAPTION" not in serialized
    assert "PRIVATE TEXT" not in serialized
    assert "123:SECRET_TOKEN" not in serialized
