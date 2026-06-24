from __future__ import annotations

from collections.abc import Awaitable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.attempt_ledger_service import AttemptLedgerOperationResult
from app.delivery_context import DeliveryContext
from app.target_verifier import TargetVerificationResult, TargetVerificationStatus
from app.telegram_send_result import TelegramSendResult

_ERROR_TEXT_LIMIT = 500


class PostSendStepsStatus(str, Enum):
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"


class PostSendStepName(str, Enum):
    NORMALIZE_SENT_IDS = "normalize_sent_ids"
    MARK_ATTEMPT_ACCEPTED = "mark_attempt_accepted"
    VERIFY_TARGET = "verify_target"


@dataclass(frozen=True, slots=True)
class PostSendStepsResult:
    status: PostSendStepsStatus
    context: DeliveryContext | None = None
    sent_message_ids: tuple[int, ...] = ()
    idempotency_key: str | None = None
    target_id: int | str | None = None
    ledger_ok: bool | None = None
    ledger_action: str | None = None
    ledger_attempt_id: int | None = None
    ledger_status: str | None = None
    verification_status: str | None = None
    found_message_ids: tuple[int, ...] = ()
    missing_message_ids: tuple[int, ...] = ()
    retry_after: float | int | None = None
    error_type: str | None = None
    error_text: str | None = None
    reason: str | None = None
    telegram_method: str | None = None
    completed_steps: tuple[str, ...] = ()
    skipped_steps: tuple[str, ...] = ()

    @classmethod
    def completed(cls, **kwargs: Any) -> PostSendStepsResult:
        return cls(status=PostSendStepsStatus.COMPLETED, **kwargs)

    @classmethod
    def skipped(cls, **kwargs: Any) -> PostSendStepsResult:
        return cls(status=PostSendStepsStatus.SKIPPED, **kwargs)

    @classmethod
    def failed(cls, **kwargs: Any) -> PostSendStepsResult:
        return cls(status=PostSendStepsStatus.FAILED, **kwargs)

    @classmethod
    def rate_limited(cls, **kwargs: Any) -> PostSendStepsResult:
        return cls(status=PostSendStepsStatus.RATE_LIMITED, **kwargs)

    @property
    def ok(self) -> bool:
        return self.status == PostSendStepsStatus.COMPLETED

    @property
    def should_defer(self) -> bool:
        return self.status == PostSendStepsStatus.RATE_LIMITED

    @property
    def is_failure(self) -> bool:
        return self.status == PostSendStepsStatus.FAILED

    @property
    def is_skipped(self) -> bool:
        return self.status == PostSendStepsStatus.SKIPPED

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "status": self.status.value,
            "sent_message_ids": self.sent_message_ids,
            "sent_message_count": len(self.sent_message_ids),
            "idempotency_key": self.idempotency_key,
            "target_id": self.target_id,
            "ledger_ok": self.ledger_ok,
            "ledger_action": self.ledger_action,
            "ledger_attempt_id": self.ledger_attempt_id,
            "ledger_status": self.ledger_status,
            "verification_status": self.verification_status,
            "found_message_ids": self.found_message_ids,
            "missing_message_ids": self.missing_message_ids,
            "found_count": len(self.found_message_ids),
            "missing_count": len(self.missing_message_ids),
            "retry_after": self.retry_after,
            "error_type": self.error_type,
            "error_text": self.error_text,
            "reason": self.reason,
            "telegram_method": self.telegram_method,
            "completed_steps": self.completed_steps,
            "skipped_steps": self.skipped_steps,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result

    def log_label(self) -> str:
        return " ".join(
            [
                f"status={self.status.value}",
                f"ids={len(self.sent_message_ids)}",
                f"ledger={self.ledger_ok}",
                f"verification={self.verification_status}",
                f"target={self.target_id}",
            ]
        )


class PostSendSteps:
    def __init__(self, *, attempt_ledger: Any | None = None, target_verifier: Any | None = None) -> None:
        self.attempt_ledger = attempt_ledger
        self.target_verifier = target_verifier

    def normalize_sent_message_ids(
        self,
        sent_message_ids: Iterable[int] | int | None = None,
        *,
        send_result: TelegramSendResult | None = None,
    ) -> tuple[int, ...]:
        if sent_message_ids is not None:
            return _normalize_message_ids(sent_message_ids)
        if send_result is not None:
            return _normalize_message_ids(send_result.sent_message_ids)
        return ()

    async def run_after_send(
        self,
        *,
        context: DeliveryContext | None = None,
        send_result: TelegramSendResult | None = None,
        sent_message_ids: Iterable[int] | int | None = None,
        idempotency_key: str | None = None,
        target_id: int | str | None = None,
        telegram_method: str | None = None,
        mark_attempt_accepted: bool = True,
        verify_target: bool = True,
    ) -> PostSendStepsResult:
        normalized_ids = self.normalize_sent_message_ids(sent_message_ids, send_result=send_result)
        base = dict(
            context=context,
            sent_message_ids=normalized_ids,
            idempotency_key=idempotency_key,
            target_id=target_id,
            telegram_method=telegram_method,
        )
        if not normalized_ids:
            return PostSendStepsResult.failed(**base, reason="missing_sent_message_ids")
        if not mark_attempt_accepted and not verify_target:
            return PostSendStepsResult.skipped(
                **base,
                reason="post_send_steps_disabled",
                skipped_steps=(PostSendStepName.MARK_ATTEMPT_ACCEPTED.value, PostSendStepName.VERIFY_TARGET.value),
            )

        completed_steps = [PostSendStepName.NORMALIZE_SENT_IDS.value]
        ledger_fields: dict[str, Any] = {}
        try:
            if mark_attempt_accepted:
                if self.attempt_ledger is None:
                    return PostSendStepsResult.failed(**base, reason="attempt_ledger_not_configured")
                if not idempotency_key:
                    return PostSendStepsResult.failed(**base, reason="missing_idempotency_key")
                ledger_result = self.attempt_ledger.mark_accepted(
                    idempotency_key=idempotency_key,
                    sent_message_ids=normalized_ids,
                    send_result=send_result,
                    telegram_method=telegram_method,
                )
                ledger_result = await _maybe_await(ledger_result)
                ledger_fields = _ledger_fields(ledger_result)
                if not bool(getattr(ledger_result, "ok", False)):
                    return PostSendStepsResult.failed(
                        **base,
                        **ledger_fields,
                        reason=getattr(ledger_result, "reason", None) or "ledger_mark_accepted_failed",
                        error_type=getattr(ledger_result, "error_type", None),
                        error_text=_safe_error_text(getattr(ledger_result, "error_text", None)),
                        completed_steps=tuple(completed_steps),
                    )
                completed_steps.append(PostSendStepName.MARK_ATTEMPT_ACCEPTED.value)

            if verify_target:
                if self.target_verifier is None:
                    return PostSendStepsResult.failed(**base, **ledger_fields, reason="target_verifier_not_configured", completed_steps=tuple(completed_steps))
                if target_id is None:
                    return PostSendStepsResult.failed(**base, **ledger_fields, reason="missing_target_id", completed_steps=tuple(completed_steps))
                if send_result is not None:
                    verification_result = self.target_verifier.verify_send_result(target_id=target_id, send_result=send_result, context=context)
                else:
                    verification_result = self.target_verifier.verify_message_ids(target_id=target_id, message_ids=normalized_ids, context=context)
                verification_result = await _maybe_await(verification_result)
                verification_fields = _verification_fields(verification_result)
                if bool(getattr(verification_result, "should_defer", False)):
                    return PostSendStepsResult.rate_limited(**base, **ledger_fields, **verification_fields, completed_steps=tuple(completed_steps))
                if bool(getattr(verification_result, "is_failure", False)):
                    failure_fields = dict(verification_fields)
                    failure_reason = failure_fields.pop("reason", None) or "verification_failed"
                    return PostSendStepsResult.failed(
                        **base,
                        **ledger_fields,
                        **failure_fields,
                        reason=failure_reason,
                        completed_steps=tuple(completed_steps),
                    )
                if bool(getattr(verification_result, "is_skipped", False)):
                    skipped_fields = dict(verification_fields)
                    skipped_fields.pop("reason", None)
                    return PostSendStepsResult.failed(
                        **base,
                        **ledger_fields,
                        **skipped_fields,
                        reason="verification_skipped",
                        completed_steps=tuple(completed_steps),
                    )
                completed_steps.append(PostSendStepName.VERIFY_TARGET.value)
                return PostSendStepsResult.completed(**base, **ledger_fields, **verification_fields, completed_steps=tuple(completed_steps))

            return PostSendStepsResult.completed(**base, **ledger_fields, completed_steps=tuple(completed_steps))
        except Exception as error:
            return PostSendStepsResult.failed(
                **base,
                **ledger_fields,
                reason="post_send_step_exception",
                error_type=error.__class__.__name__,
                error_text=_safe_error_text(error),
                completed_steps=tuple(completed_steps),
            )


def _normalize_message_ids(value: Iterable[int] | int | None) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, int):
        return (int(value),)
    return tuple(int(item) for item in value if item is not None)


def _enum_value(value: Enum | str | None) -> str | None:
    if isinstance(value, Enum):
        return str(value.value)
    return value


def _ledger_fields(result: AttemptLedgerOperationResult | Any) -> dict[str, Any]:
    return {
        "ledger_ok": getattr(result, "ok", None),
        "ledger_action": _enum_value(getattr(result, "action", None)),
        "ledger_attempt_id": getattr(result, "attempt_id", None),
        "ledger_status": _enum_value(getattr(result, "status", None)),
    }


def _verification_fields(result: TargetVerificationResult | Any) -> dict[str, Any]:
    status = _enum_value(getattr(result, "status", None))
    reason = getattr(result, "reason", None)
    if status == TargetVerificationStatus.SKIPPED.value and not reason:
        reason = "verification_skipped"
    return {
        "verification_status": status,
        "found_message_ids": _normalize_message_ids(getattr(result, "found_message_ids", None)),
        "missing_message_ids": _normalize_message_ids(getattr(result, "missing_message_ids", None)),
        "retry_after": getattr(result, "retry_after", None),
        "error_type": getattr(result, "error_type", None),
        "error_text": _safe_error_text(getattr(result, "error_text", None)),
        "reason": reason,
    }


async def _maybe_await(value: Any) -> Any:
    if isinstance(value, Awaitable):
        return await value
    return value


def _safe_error_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return text[:_ERROR_TEXT_LIMIT]
