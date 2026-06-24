from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Iterable

from app.delivery_context import DeliveryContext


_MAX_ERROR_TEXT_LENGTH = 500


class DeliveryPipelineStatus(str, Enum):
    SENT = "sent"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    SKIPPED = "skipped"
    NOOP = "noop"


@dataclass(frozen=True, slots=True)
class DeliveryPipelineResult:
    """Compact, safe result object for future delivery pipeline stages.

    This foundation object intentionally stores only technical result metadata.
    It must not keep raw payloads, captions, Telegram objects, clients, DB
    connections, raw exceptions, or raw transport/result objects.
    """

    status: DeliveryPipelineStatus
    context: DeliveryContext | None = None
    sent_message_ids: tuple[int, ...] = ()
    error_type: str | None = None
    error_text: str | None = None
    retry_after: float | int | None = None
    reason: str | None = None

    @classmethod
    def sent(
        cls,
        *,
        context: DeliveryContext | None = None,
        sent_message_ids: Iterable[int] | int | None = None,
        reason: str | None = None,
    ) -> DeliveryPipelineResult:
        return cls(
            status=DeliveryPipelineStatus.SENT,
            context=context,
            sent_message_ids=_normalize_sent_message_ids(sent_message_ids),
            reason=reason,
        )

    @classmethod
    def failed(
        cls,
        *,
        context: DeliveryContext | None = None,
        error: BaseException | None = None,
        error_type: str | None = None,
        error_text: str | None = None,
        reason: str | None = None,
    ) -> DeliveryPipelineResult:
        safe_error_type, safe_error_text = _extract_error_fields(
            error=error,
            error_type=error_type,
            error_text=error_text,
        )
        return cls(
            status=DeliveryPipelineStatus.FAILED,
            context=context,
            error_type=safe_error_type,
            error_text=safe_error_text,
            reason=reason,
        )

    @classmethod
    def rate_limited(
        cls,
        *,
        context: DeliveryContext | None = None,
        retry_after: float | int | None = None,
        error: BaseException | None = None,
        reason: str | None = None,
    ) -> DeliveryPipelineResult:
        error_type, error_text = _extract_error_fields(error=error)
        return cls(
            status=DeliveryPipelineStatus.RATE_LIMITED,
            context=context,
            error_type=error_type,
            error_text=error_text,
            retry_after=retry_after,
            reason=reason,
        )

    @classmethod
    def skipped(
        cls,
        *,
        context: DeliveryContext | None = None,
        reason: str | None = None,
    ) -> DeliveryPipelineResult:
        return cls(
            status=DeliveryPipelineStatus.SKIPPED,
            context=context,
            reason=reason,
        )

    @classmethod
    def noop(
        cls,
        *,
        context: DeliveryContext | None = None,
        reason: str | None = None,
    ) -> DeliveryPipelineResult:
        return cls(
            status=DeliveryPipelineStatus.NOOP,
            context=context,
            reason=reason,
        )

    @property
    def is_success(self) -> bool:
        return self.status == DeliveryPipelineStatus.SENT

    @property
    def is_failure(self) -> bool:
        return self.status == DeliveryPipelineStatus.FAILED

    @property
    def should_defer(self) -> bool:
        return self.status == DeliveryPipelineStatus.RATE_LIMITED

    @property
    def is_skipped(self) -> bool:
        return self.status in (DeliveryPipelineStatus.SKIPPED, DeliveryPipelineStatus.NOOP)

    def to_log_context(self) -> dict[str, object]:
        log_context: dict[str, object] = {
            "status": self.status.value,
            "sent_message_ids": self.sent_message_ids,
            "sent_message_count": len(self.sent_message_ids),
            "error_type": self.error_type,
            "error_text": self.error_text,
            "retry_after": self.retry_after,
            "reason": self.reason,
        }
        if self.context is not None:
            log_context["context"] = self.context.to_log_context()
        return log_context

    def log_label(self) -> str:
        parts = [
            f"status={self.status.value}",
            f"sent_count={len(self.sent_message_ids)}",
        ]
        if self.context is not None:
            parts.extend(
                [
                    f"delivery={self.context.delivery_id}",
                    f"rule={self.context.rule_id}",
                    f"post={self.context.post_id}",
                ]
            )
        if self.reason is not None:
            parts.append(f"reason={self.reason}")
        return " ".join(parts)

    def with_context(self, context: DeliveryContext) -> DeliveryPipelineResult:
        return replace(self, context=context)


def _normalize_sent_message_ids(
    sent_message_ids: Iterable[int] | int | None,
) -> tuple[int, ...]:
    if sent_message_ids is None:
        return ()
    if isinstance(sent_message_ids, int):
        return (sent_message_ids,)
    return tuple(sent_message_ids)


def _extract_error_fields(
    *,
    error: BaseException | None = None,
    error_type: str | None = None,
    error_text: str | None = None,
) -> tuple[str | None, str | None]:
    if error is not None:
        return error.__class__.__name__, _safe_error_text(str(error))
    return error_type, _safe_error_text(error_text)


def _safe_error_text(error_text: str | None) -> str | None:
    if error_text is None:
        return None
    if len(error_text) <= _MAX_ERROR_TEXT_LENGTH:
        return error_text
    return f"{error_text[:_MAX_ERROR_TEXT_LENGTH]}..."


__all__ = ["DeliveryPipelineResult", "DeliveryPipelineStatus"]
