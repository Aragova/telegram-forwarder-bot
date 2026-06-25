from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.delivery_context import DeliveryContext
from app.delivery_pipeline_result import DeliveryPipelineResult, DeliveryPipelineStatus
from app.post_send_steps import PostSendStepsResult, PostSendStepsStatus

_ERROR_TEXT_LIMIT = 500


class DeliveryFinalizationStatus(str, Enum):
    FINALIZED = "finalized"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    SKIPPED = "skipped"
    NOOP = "noop"


class DeliveryOutcome(str, Enum):
    SENT = "sent"
    FAULTY = "faulty"
    DEFERRED = "deferred"
    SKIPPED = "skipped"
    NOOP = "noop"


class DeliveryFinalizationSource(str, Enum):
    PIPELINE = "pipeline"
    POST_SEND = "post_send"
    VERIFICATION = "verification"
    LEDGER = "ledger"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class DeliveryFinalizationResult:
    status: DeliveryFinalizationStatus
    outcome: DeliveryOutcome
    source: DeliveryFinalizationSource
    context: DeliveryContext | None = None
    sent_message_ids: tuple[int, ...] = ()
    target_id: int | str | None = None
    idempotency_key: str | None = None
    pipeline_status: str | None = None
    post_send_status: str | None = None
    ledger_status: str | None = None
    verification_status: str | None = None
    retry_after: float | int | None = None
    error_type: str | None = None
    error_text: str | None = None
    reason: str | None = None
    delivery_id: int | None = None
    rule_id: int | None = None
    post_id: int | None = None
    telegram_method: str | None = None
    completed_steps: tuple[str, ...] = ()
    skipped_steps: tuple[str, ...] = ()

    @classmethod
    def finalized(cls, **kwargs: Any) -> DeliveryFinalizationResult:
        return cls(
            status=DeliveryFinalizationStatus.FINALIZED,
            outcome=DeliveryOutcome.SENT,
            source=kwargs.pop("source", DeliveryFinalizationSource.MANUAL),
            **_normalized_kwargs(kwargs),
        )

    @classmethod
    def failed(cls, **kwargs: Any) -> DeliveryFinalizationResult:
        error = kwargs.pop("error", None)
        if error is not None:
            kwargs.setdefault("error_type", error.__class__.__name__)
            kwargs.setdefault("error_text", str(error))
        return cls(
            status=DeliveryFinalizationStatus.FAILED,
            outcome=DeliveryOutcome.FAULTY,
            source=kwargs.pop("source", DeliveryFinalizationSource.MANUAL),
            **_normalized_kwargs(kwargs),
        )

    @classmethod
    def rate_limited(cls, **kwargs: Any) -> DeliveryFinalizationResult:
        error = kwargs.pop("error", None)
        if error is not None:
            kwargs.setdefault("error_type", error.__class__.__name__)
            kwargs.setdefault("error_text", str(error))
        return cls(
            status=DeliveryFinalizationStatus.RATE_LIMITED,
            outcome=DeliveryOutcome.DEFERRED,
            source=kwargs.pop("source", DeliveryFinalizationSource.MANUAL),
            **_normalized_kwargs(kwargs),
        )

    @classmethod
    def skipped(cls, **kwargs: Any) -> DeliveryFinalizationResult:
        return cls(
            status=DeliveryFinalizationStatus.SKIPPED,
            outcome=DeliveryOutcome.SKIPPED,
            source=kwargs.pop("source", DeliveryFinalizationSource.MANUAL),
            **_normalized_kwargs(kwargs),
        )

    @classmethod
    def noop(cls, **kwargs: Any) -> DeliveryFinalizationResult:
        return cls(
            status=DeliveryFinalizationStatus.NOOP,
            outcome=DeliveryOutcome.NOOP,
            source=kwargs.pop("source", DeliveryFinalizationSource.MANUAL),
            **_normalized_kwargs(kwargs),
        )

    @property
    def ok(self) -> bool:
        return self.status == DeliveryFinalizationStatus.FINALIZED and self.outcome == DeliveryOutcome.SENT

    @property
    def should_defer(self) -> bool:
        return self.status == DeliveryFinalizationStatus.RATE_LIMITED or self.outcome == DeliveryOutcome.DEFERRED

    @property
    def is_failure(self) -> bool:
        return self.status == DeliveryFinalizationStatus.FAILED or self.outcome == DeliveryOutcome.FAULTY

    @property
    def is_skipped(self) -> bool:
        return self.status == DeliveryFinalizationStatus.SKIPPED or self.outcome == DeliveryOutcome.SKIPPED

    @property
    def is_noop(self) -> bool:
        return self.status == DeliveryFinalizationStatus.NOOP or self.outcome == DeliveryOutcome.NOOP

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "status": self.status.value,
            "outcome": self.outcome.value,
            "source": self.source.value,
            "sent_message_ids": self.sent_message_ids,
            "sent_message_count": len(self.sent_message_ids),
            "target_id": self.target_id,
            "idempotency_key": self.idempotency_key,
            "pipeline_status": self.pipeline_status,
            "post_send_status": self.post_send_status,
            "ledger_status": self.ledger_status,
            "verification_status": self.verification_status,
            "retry_after": self.retry_after,
            "error_type": self.error_type,
            "error_text": self.error_text,
            "reason": self.reason,
            "delivery_id": self.delivery_id,
            "rule_id": self.rule_id,
            "post_id": self.post_id,
            "telegram_method": self.telegram_method,
            "completed_steps": self.completed_steps,
            "skipped_steps": self.skipped_steps,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result

    def log_label(self) -> str:
        parts = [
            f"status={self.status.value}",
            f"outcome={self.outcome.value}",
            f"source={self.source.value}",
            f"ids={len(self.sent_message_ids)}",
            f"pipeline={self.pipeline_status}",
            f"post_send={self.post_send_status}",
        ]
        if self.reason is not None:
            parts.append(f"reason={self.reason}")
        return " ".join(parts)


class DeliveryFinalizer:
    def finalize_pipeline_result(
        self,
        *,
        pipeline_result: DeliveryPipelineResult,
        context: DeliveryContext | None = None,
        idempotency_key: str | None = None,
        target_id: int | str | None = None,
    ) -> DeliveryFinalizationResult:
        base = _base_fields(
            context=context or pipeline_result.context,
            sent_message_ids=pipeline_result.sent_message_ids,
            idempotency_key=idempotency_key,
            target_id=target_id,
            pipeline_status=_enum_value(pipeline_result.status),
            retry_after=pipeline_result.retry_after,
            error_type=pipeline_result.error_type,
            error_text=pipeline_result.error_text,
            reason=pipeline_result.reason,
        )
        if pipeline_result.status == DeliveryPipelineStatus.SENT:
            return DeliveryFinalizationResult.finalized(source=DeliveryFinalizationSource.PIPELINE, **base)
        if pipeline_result.status == DeliveryPipelineStatus.FAILED:
            return DeliveryFinalizationResult.failed(source=DeliveryFinalizationSource.PIPELINE, **base)
        if pipeline_result.status == DeliveryPipelineStatus.RATE_LIMITED:
            return DeliveryFinalizationResult.rate_limited(source=DeliveryFinalizationSource.PIPELINE, **base)
        if pipeline_result.status == DeliveryPipelineStatus.SKIPPED:
            return DeliveryFinalizationResult.skipped(source=DeliveryFinalizationSource.PIPELINE, **base)
        return DeliveryFinalizationResult.noop(source=DeliveryFinalizationSource.PIPELINE, **base)

    def finalize_post_send_result(
        self,
        *,
        post_send_result: PostSendStepsResult,
        pipeline_result: DeliveryPipelineResult | None = None,
        context: DeliveryContext | None = None,
        idempotency_key: str | None = None,
        target_id: int | str | None = None,
    ) -> DeliveryFinalizationResult:
        base = _base_fields(
            context=context or post_send_result.context,
            sent_message_ids=post_send_result.sent_message_ids,
            idempotency_key=idempotency_key or post_send_result.idempotency_key,
            target_id=target_id if target_id is not None else post_send_result.target_id,
            pipeline_status=_enum_value(pipeline_result.status) if pipeline_result is not None else None,
            post_send_status=_enum_value(post_send_result.status),
            ledger_status=post_send_result.ledger_status,
            verification_status=post_send_result.verification_status,
            retry_after=post_send_result.retry_after,
            error_type=post_send_result.error_type,
            error_text=post_send_result.error_text,
            reason=post_send_result.reason,
            telegram_method=post_send_result.telegram_method,
            completed_steps=post_send_result.completed_steps,
            skipped_steps=post_send_result.skipped_steps,
        )
        if post_send_result.status == PostSendStepsStatus.COMPLETED:
            return DeliveryFinalizationResult.finalized(source=DeliveryFinalizationSource.POST_SEND, **base)
        if post_send_result.status == PostSendStepsStatus.FAILED:
            return DeliveryFinalizationResult.failed(source=DeliveryFinalizationSource.POST_SEND, **base)
        if post_send_result.status == PostSendStepsStatus.RATE_LIMITED:
            return DeliveryFinalizationResult.rate_limited(source=DeliveryFinalizationSource.POST_SEND, **base)
        return DeliveryFinalizationResult.skipped(source=DeliveryFinalizationSource.POST_SEND, **base)

    def finalize(
        self,
        *,
        pipeline_result: DeliveryPipelineResult | None = None,
        post_send_result: PostSendStepsResult | None = None,
        context: DeliveryContext | None = None,
        idempotency_key: str | None = None,
        target_id: int | str | None = None,
    ) -> DeliveryFinalizationResult:
        if post_send_result is not None:
            return self.finalize_post_send_result(
                post_send_result=post_send_result,
                pipeline_result=pipeline_result,
                context=context,
                idempotency_key=idempotency_key,
                target_id=target_id,
            )
        if pipeline_result is not None:
            return self.finalize_pipeline_result(
                pipeline_result=pipeline_result,
                context=context,
                idempotency_key=idempotency_key,
                target_id=target_id,
            )
        return DeliveryFinalizationResult.noop(
            context=context,
            idempotency_key=idempotency_key,
            target_id=target_id,
            reason="missing_finalization_input",
        )


def _base_fields(
    *,
    context: DeliveryContext | None = None,
    sent_message_ids: Iterable[int] | int | None = None,
    target_id: int | str | None = None,
    idempotency_key: str | None = None,
    pipeline_status: str | None = None,
    post_send_status: str | None = None,
    ledger_status: str | None = None,
    verification_status: str | None = None,
    retry_after: float | int | None = None,
    error_type: str | None = None,
    error_text: str | None = None,
    reason: str | None = None,
    telegram_method: str | None = None,
    completed_steps: Iterable[str] | str | None = None,
    skipped_steps: Iterable[str] | str | None = None,
) -> dict[str, Any]:
    return {
        "context": context,
        "sent_message_ids": _normalize_sent_message_ids(sent_message_ids),
        "target_id": target_id if target_id is not None else (context.target_id if context is not None else None),
        "idempotency_key": idempotency_key,
        "pipeline_status": pipeline_status,
        "post_send_status": post_send_status,
        "ledger_status": ledger_status,
        "verification_status": verification_status,
        "retry_after": retry_after,
        "error_type": error_type,
        "error_text": _safe_error_text(error_text),
        "reason": reason,
        "delivery_id": context.delivery_id if context is not None else None,
        "rule_id": context.rule_id if context is not None else None,
        "post_id": context.post_id if context is not None else None,
        "telegram_method": telegram_method,
        "completed_steps": _normalize_string_tuple(completed_steps),
        "skipped_steps": _normalize_string_tuple(skipped_steps),
    }


def _normalized_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    kwargs = dict(kwargs)
    kwargs["sent_message_ids"] = _normalize_sent_message_ids(kwargs.get("sent_message_ids"))
    kwargs["error_text"] = _safe_error_text(kwargs.get("error_text"))
    kwargs["completed_steps"] = _normalize_string_tuple(kwargs.get("completed_steps"))
    kwargs["skipped_steps"] = _normalize_string_tuple(kwargs.get("skipped_steps"))
    context = kwargs.get("context")
    if isinstance(context, DeliveryContext):
        kwargs.setdefault("target_id", context.target_id)
        kwargs.setdefault("delivery_id", context.delivery_id)
        kwargs.setdefault("rule_id", context.rule_id)
        kwargs.setdefault("post_id", context.post_id)
    return kwargs


def _normalize_sent_message_ids(sent_message_ids: Iterable[int] | int | None) -> tuple[int, ...]:
    if sent_message_ids is None:
        return ()
    if isinstance(sent_message_ids, int):
        return (sent_message_ids,)
    return tuple(int(message_id) for message_id in sent_message_ids)


def _normalize_string_tuple(values: Iterable[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        return (values,)
    return tuple(str(value) for value in values)


def _safe_error_text(error_text: str | None) -> str | None:
    if error_text is None:
        return None
    text = str(error_text).replace("\n", " ").replace("\r", " ").strip()
    if len(text) <= _ERROR_TEXT_LIMIT:
        return text
    return f"{text[: _ERROR_TEXT_LIMIT - 3]}..."


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "value", str(value))


__all__ = [
    "DeliveryFinalizationResult",
    "DeliveryFinalizationSource",
    "DeliveryFinalizationStatus",
    "DeliveryFinalizer",
    "DeliveryOutcome",
]
