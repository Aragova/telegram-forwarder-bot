from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.delivery_context import DeliveryContext
from app.telegram_send_result import TelegramSendResult


_ERROR_TEXT_LIMIT = 1000


class DeliveryAttemptStatus(str, Enum):
    CREATED = "created"
    SENDING = "sending"
    ACCEPTED = "accepted"
    FAILED_BEFORE_SEND = "failed_before_send"
    FAILED_AFTER_SEND = "failed_after_send"
    VERIFIED = "verified"


class AttemptLedgerAction(str, Enum):
    GET = "get"
    ENSURE_CREATED = "ensure_created"
    MARK_SENDING = "mark_sending"
    MARK_ACCEPTED = "mark_accepted"
    MARK_FAILED_BEFORE_SEND = "mark_failed_before_send"
    MARK_FAILED_AFTER_SEND = "mark_failed_after_send"


@dataclass(frozen=True, slots=True)
class AttemptLedgerOperationResult:
    ok: bool
    action: AttemptLedgerAction
    idempotency_key: str
    attempt_id: int | None = None
    status: DeliveryAttemptStatus | str | None = None
    error_type: str | None = None
    error_text: str | None = None
    reason: str | None = None
    delivery_id: int | None = None
    rule_id: int | None = None
    job_id: int | None = None
    telegram_method: str | None = None

    def to_log_context(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "action": self.action.value,
            "idempotency_key": self.idempotency_key,
            "attempt_id": self.attempt_id,
            "status": _enum_value(self.status),
            "delivery_id": self.delivery_id,
            "rule_id": self.rule_id,
            "job_id": self.job_id,
            "telegram_method": self.telegram_method,
            "error_type": self.error_type,
            "error_text": self.error_text,
            "reason": self.reason,
        }

    def log_label(self) -> str:
        return " ".join(
            [
                f"action={self.action.value}",
                f"ok={self.ok}",
                f"attempt={self.attempt_id}",
                f"delivery={self.delivery_id}",
                f"rule={self.rule_id}",
                f"status={_enum_value(self.status)}",
            ]
        )


class AttemptLedgerService:
    def __init__(self, *, repository: Any) -> None:
        self.repository = repository

    def get_attempt(self, *, idempotency_key: str) -> Mapping[str, Any] | None:
        return self.repository.get_delivery_attempt_by_idempotency_key(idempotency_key)

    def ensure_created(
        self,
        *,
        context: DeliveryContext,
        tenant_id: int,
        idempotency_key: str,
        operation_kind: str,
        job_id: int | None = None,
        telegram_method: str | None = None,
        target_id: int | str | None = None,
        source_message_ids: Iterable[int] | int | None = None,
        sent_message_ids: Iterable[int] | int | None = None,
        error_text: str | None = None,
    ) -> AttemptLedgerOperationResult:
        if context.delivery_id is None or context.rule_id is None:
            return AttemptLedgerOperationResult(
                ok=False,
                action=AttemptLedgerAction.ENSURE_CREATED,
                idempotency_key=idempotency_key,
                status=DeliveryAttemptStatus.CREATED,
                reason="missing_delivery_context",
                delivery_id=context.delivery_id,
                rule_id=context.rule_id,
                job_id=job_id,
                telegram_method=telegram_method,
            )

        effective_target_id = target_id if target_id is not None else context.target_id
        safe_error_text = _safe_error_text(error_text)
        attempt_id = self.repository.create_delivery_attempt(
            delivery_id=int(context.delivery_id),
            rule_id=int(context.rule_id),
            tenant_id=tenant_id,
            job_id=job_id,
            idempotency_key=idempotency_key,
            operation_kind=operation_kind,
            status=DeliveryAttemptStatus.CREATED.value,
            telegram_method=telegram_method,
            target_id=str(effective_target_id) if effective_target_id is not None else None,
            source_message_ids=_normalize_message_ids(source_message_ids),
            sent_message_ids=_normalize_message_ids(sent_message_ids),
            error_text=safe_error_text,
        )
        return AttemptLedgerOperationResult(
            ok=attempt_id is not None,
            action=AttemptLedgerAction.ENSURE_CREATED,
            idempotency_key=idempotency_key,
            attempt_id=attempt_id,
            status=DeliveryAttemptStatus.CREATED,
            error_text=safe_error_text,
            delivery_id=int(context.delivery_id),
            rule_id=int(context.rule_id),
            job_id=job_id,
            telegram_method=telegram_method,
        )

    def mark_sending(
        self,
        *,
        idempotency_key: str,
        job_id: int | None = None,
        telegram_method: str | None = None,
    ) -> AttemptLedgerOperationResult:
        ok = bool(self.repository.mark_delivery_attempt_sending(idempotency_key, job_id=job_id, telegram_method=telegram_method))
        return AttemptLedgerOperationResult(
            ok=ok,
            action=AttemptLedgerAction.MARK_SENDING,
            idempotency_key=idempotency_key,
            status=DeliveryAttemptStatus.SENDING,
            job_id=job_id,
            telegram_method=telegram_method,
        )

    def mark_accepted(
        self,
        *,
        idempotency_key: str,
        sent_message_ids: Iterable[int] | int | None = None,
        send_result: TelegramSendResult | None = None,
        telegram_method: str | None = None,
    ) -> AttemptLedgerOperationResult:
        effective_ids = sent_message_ids if sent_message_ids is not None else (send_result.sent_message_ids if send_result is not None else None)
        normalized_ids = _normalize_message_ids(effective_ids)
        if not normalized_ids:
            return AttemptLedgerOperationResult(
                ok=False,
                action=AttemptLedgerAction.MARK_ACCEPTED,
                idempotency_key=idempotency_key,
                status=DeliveryAttemptStatus.ACCEPTED,
                reason="missing_sent_message_ids",
                telegram_method=telegram_method,
            )
        ok = bool(
            self.repository.mark_delivery_attempt_accepted(
                idempotency_key,
                sent_message_ids=normalized_ids,
                telegram_method=telegram_method,
            )
        )
        return AttemptLedgerOperationResult(
            ok=ok,
            action=AttemptLedgerAction.MARK_ACCEPTED,
            idempotency_key=idempotency_key,
            status=DeliveryAttemptStatus.ACCEPTED,
            telegram_method=telegram_method,
        )

    def mark_failed_before_send(
        self,
        *,
        idempotency_key: str,
        error: BaseException | None = None,
        error_text: str | None = None,
    ) -> AttemptLedgerOperationResult:
        return self._mark_failed(
            action=AttemptLedgerAction.MARK_FAILED_BEFORE_SEND,
            status=DeliveryAttemptStatus.FAILED_BEFORE_SEND,
            idempotency_key=idempotency_key,
            error=error,
            error_text=error_text,
        )

    def mark_failed_after_send(
        self,
        *,
        idempotency_key: str,
        error: BaseException | None = None,
        error_text: str | None = None,
    ) -> AttemptLedgerOperationResult:
        return self._mark_failed(
            action=AttemptLedgerAction.MARK_FAILED_AFTER_SEND,
            status=DeliveryAttemptStatus.FAILED_AFTER_SEND,
            idempotency_key=idempotency_key,
            error=error,
            error_text=error_text,
        )

    def _mark_failed(
        self,
        *,
        action: AttemptLedgerAction,
        status: DeliveryAttemptStatus,
        idempotency_key: str,
        error: BaseException | None,
        error_text: str | None,
    ) -> AttemptLedgerOperationResult:
        effective_error_text = _safe_error_text(str(error) if error is not None else error_text)
        ok = bool(
            self.repository.mark_delivery_attempt_failed(
                idempotency_key,
                status=status.value,
                error_text=effective_error_text or "",
            )
        )
        return AttemptLedgerOperationResult(
            ok=ok,
            action=action,
            idempotency_key=idempotency_key,
            status=status,
            error_type=error.__class__.__name__ if error is not None else None,
            error_text=effective_error_text,
        )


def _normalize_message_ids(value: Iterable[int] | int | None) -> list[int] | None:
    if value is None:
        return None
    if isinstance(value, int):
        return [int(value)]
    normalized = [int(item) for item in value if item is not None]
    return normalized or None


def _safe_error_text(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)[:_ERROR_TEXT_LIMIT]


def _enum_value(value: Enum | str | None) -> str | None:
    if isinstance(value, Enum):
        return str(value.value)
    return value
