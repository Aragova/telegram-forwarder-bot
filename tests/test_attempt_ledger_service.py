from dataclasses import fields

from app.attempt_ledger_service import (
    AttemptLedgerAction,
    AttemptLedgerOperationResult,
    AttemptLedgerService,
    DeliveryAttemptStatus,
    _normalize_message_ids,
)
from app.delivery_context import DeliveryContext
from app.telegram_send_result import telegram_send_success


class FakeRepository:
    def __init__(self):
        self.calls = []
        self.created_id = 10
        self.sending_result = True
        self.accepted_result = True
        self.failed_result = True
        self.attempt_row = {"id": 10, "status": "created"}

    def get_delivery_attempt_by_idempotency_key(self, idempotency_key):
        self.calls.append(("get_delivery_attempt_by_idempotency_key", idempotency_key))
        return self.attempt_row

    def create_delivery_attempt(self, **kwargs):
        self.calls.append(("create_delivery_attempt", kwargs))
        return self.created_id

    def mark_delivery_attempt_sending(self, idempotency_key, **kwargs):
        self.calls.append(("mark_delivery_attempt_sending", idempotency_key, kwargs))
        return self.sending_result

    def mark_delivery_attempt_accepted(self, idempotency_key, *, sent_message_ids, telegram_method=None):
        self.calls.append(("mark_delivery_attempt_accepted", idempotency_key, sent_message_ids, telegram_method))
        return self.accepted_result

    def mark_delivery_attempt_failed(self, idempotency_key, **kwargs):
        self.calls.append(("mark_delivery_attempt_failed", idempotency_key, kwargs))
        return self.failed_result


def test_service_stores_injected_repository():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)

    assert service.repository is repository


def test_get_attempt_delegates_to_repository():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)

    row = service.get_attempt(idempotency_key="key")

    assert row == {"id": 10, "status": "created"}
    assert repository.calls == [("get_delivery_attempt_by_idempotency_key", "key")]


def test_ensure_created_delegates_with_expected_fields():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)
    context = DeliveryContext(delivery_id=100, rule_id=20, target_id=-100123, message_id=777)

    result = service.ensure_created(
        context=context,
        tenant_id=1,
        job_id=5,
        idempotency_key="abc",
        operation_kind="copy_message",
        telegram_method="copy_message",
        source_message_ids=[777],
    )

    assert result.ok is True
    assert result.action == AttemptLedgerAction.ENSURE_CREATED
    assert result.attempt_id == 10
    assert repository.calls[0][0] == "create_delivery_attempt"
    kwargs = repository.calls[0][1]
    assert kwargs == {
        "delivery_id": 100,
        "rule_id": 20,
        "tenant_id": 1,
        "job_id": 5,
        "idempotency_key": "abc",
        "operation_kind": "copy_message",
        "status": "created",
        "telegram_method": "copy_message",
        "target_id": "-100123",
        "source_message_ids": [777],
        "sent_message_ids": None,
        "error_text": None,
    }


def test_ensure_created_handles_missing_delivery_or_rule_context():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)
    context = DeliveryContext(delivery_id=None, rule_id=20)

    result = service.ensure_created(context=context, tenant_id=1, idempotency_key="abc", operation_kind="copy_message")

    assert result.ok is False
    assert result.reason == "missing_delivery_context"
    assert repository.calls == []


def test_mark_sending_delegates_and_reflects_repository_result():
    repository = FakeRepository()
    repository.sending_result = False
    service = AttemptLedgerService(repository=repository)

    result = service.mark_sending(idempotency_key="abc", job_id=5, telegram_method="copy_message")

    assert result.ok is False
    assert result.status == DeliveryAttemptStatus.SENDING
    assert repository.calls == [("mark_delivery_attempt_sending", "abc", {"job_id": 5, "telegram_method": "copy_message"})]


def test_mark_accepted_uses_explicit_sent_ids():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)

    result = service.mark_accepted(idempotency_key="abc", sent_message_ids=[101, 102], telegram_method="copy_message")

    assert result.ok is True
    assert result.status == DeliveryAttemptStatus.ACCEPTED
    assert repository.calls == [("mark_delivery_attempt_accepted", "abc", [101, 102], "copy_message")]


def test_mark_accepted_uses_telegram_send_result_ids_without_storing_raw_result():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)
    send_result = telegram_send_success(method="copy_message", sent_message_ids=[201, 202])

    result = service.mark_accepted(idempotency_key="abc", send_result=send_result)

    assert repository.calls == [("mark_delivery_attempt_accepted", "abc", [201, 202], None)]
    assert all(getattr(result, field.name) is not send_result for field in fields(result))


def test_mark_accepted_rejects_empty_ids_without_repository_call():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)

    result = service.mark_accepted(idempotency_key="abc", sent_message_ids=[])

    assert result.ok is False
    assert result.reason == "missing_sent_message_ids"
    assert repository.calls == []


def test_mark_failed_before_send_delegates_and_sanitizes_error():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)
    error = RuntimeError("boom")

    result = service.mark_failed_before_send(idempotency_key="abc", error=error)

    assert result.ok is True
    assert result.status == DeliveryAttemptStatus.FAILED_BEFORE_SEND
    assert result.error_type == "RuntimeError"
    assert "boom" in result.error_text
    assert all(getattr(result, field.name) is not error for field in fields(result))
    assert repository.calls == [("mark_delivery_attempt_failed", "abc", {"status": "failed_before_send", "error_text": "boom"})]


def test_mark_failed_after_send_delegates_and_sanitizes_error():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)

    result = service.mark_failed_after_send(idempotency_key="abc", error_text="after boom")

    assert result.ok is True
    assert result.status == DeliveryAttemptStatus.FAILED_AFTER_SEND
    assert result.error_type is None
    assert result.error_text == "after boom"
    assert repository.calls == [("mark_delivery_attempt_failed", "abc", {"status": "failed_after_send", "error_text": "after boom"})]


def test_error_text_truncation_for_result_and_repository():
    repository = FakeRepository()
    service = AttemptLedgerService(repository=repository)
    long_error = "x" * 1500

    result = service.mark_failed_before_send(idempotency_key="abc", error_text=long_error)

    assert len(result.error_text) <= 1000
    assert len(repository.calls[0][2]["error_text"]) <= 1000


def test_ids_normalization():
    assert _normalize_message_ids(7) == [7]
    assert _normalize_message_ids((1, 2)) == [1, 2]
    assert _normalize_message_ids([3, 4]) == [3, 4]
    assert _normalize_message_ids(item for item in [5, 6]) == [5, 6]
    assert _normalize_message_ids(None) is None
    assert _normalize_message_ids([]) is None


def test_safe_log_context_and_label_do_not_include_sensitive_values():
    result = AttemptLedgerOperationResult(
        ok=True,
        action=AttemptLedgerAction.MARK_ACCEPTED,
        idempotency_key="abc",
        attempt_id=123,
        status=DeliveryAttemptStatus.ACCEPTED,
        delivery_id=10,
        rule_id=5,
        job_id=3,
        telegram_method="copy_message",
    )

    context = result.to_log_context()
    label = result.log_label()
    combined = f"{context} {label}"

    assert context["ok"] is True
    assert context["action"] == "mark_accepted"
    assert context["idempotency_key"] == "abc"
    assert context["attempt_id"] == 123
    assert context["status"] == "accepted"
    assert "action=mark_accepted" in label
    assert "ok=True" in label
    assert "attempt=123" in label
    assert "status=accepted" in label
    assert "PRIVATE CAPTION" not in combined
    assert "PRIVATE TEXT" not in combined
    assert "123:SECRET_TOKEN" not in combined
