from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.delivery_context import DeliveryContext
from app.telegram_send_result import TelegramSendResult
from app.transport_policy import TransportRateLimited

_ERROR_TEXT_MAX_LEN = 400


class TargetVerificationStatus(str, Enum):
    VERIFIED = "verified"
    NOT_FOUND = "not_found"
    RATE_LIMITED = "rate_limited"
    FAILED = "failed"
    SKIPPED = "skipped"


def _normalize_message_ids(message_ids: Iterable[int] | int | None) -> tuple[int, ...]:
    if message_ids is None:
        return ()
    if isinstance(message_ids, int):
        return (message_ids,)
    return tuple(int(message_id) for message_id in message_ids)


def _safe_error_text(error: BaseException, *, max_len: int = _ERROR_TEXT_MAX_LEN) -> str:
    text = str(error).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return f"{text[: max_len - 3]}..."
    return text


def _retry_after_from_error(error: TransportRateLimited) -> int | float | None:
    return getattr(error, "retry_after_seconds", None) or getattr(error, "retry_after", None)


@dataclass(frozen=True, slots=True)
class TargetVerificationResult:
    status: TargetVerificationStatus
    context: DeliveryContext | None = None
    target_id: int | str | None = None
    message_ids: tuple[int, ...] = ()
    found_message_ids: tuple[int, ...] = ()
    missing_message_ids: tuple[int, ...] = ()
    error_type: str | None = None
    error_text: str | None = None
    retry_after: float | int | None = None
    reason: str | None = None

    @classmethod
    def verified(
        cls,
        *,
        target_id: int | str | None = None,
        message_ids: Iterable[int] | int | None = None,
        found_message_ids: Iterable[int] | int | None = None,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        normalized_ids = _normalize_message_ids(message_ids)
        normalized_found = _normalize_message_ids(found_message_ids)
        return cls(
            status=TargetVerificationStatus.VERIFIED,
            context=context,
            target_id=target_id,
            message_ids=normalized_ids,
            found_message_ids=normalized_found or normalized_ids,
            missing_message_ids=(),
        )

    @classmethod
    def not_found(
        cls,
        *,
        target_id: int | str | None = None,
        message_ids: Iterable[int] | int | None = None,
        found_message_ids: Iterable[int] | int | None = None,
        missing_message_ids: Iterable[int] | int | None = None,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        return cls(
            status=TargetVerificationStatus.NOT_FOUND,
            context=context,
            target_id=target_id,
            message_ids=_normalize_message_ids(message_ids),
            found_message_ids=_normalize_message_ids(found_message_ids),
            missing_message_ids=_normalize_message_ids(missing_message_ids),
        )

    @classmethod
    def rate_limited(
        cls,
        *,
        target_id: int | str | None = None,
        message_ids: Iterable[int] | int | None = None,
        retry_after: float | int | None = None,
        error: TransportRateLimited | None = None,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        effective_retry_after = retry_after if retry_after is not None else (_retry_after_from_error(error) if error else None)
        return cls(
            status=TargetVerificationStatus.RATE_LIMITED,
            context=context,
            target_id=target_id,
            message_ids=_normalize_message_ids(message_ids),
            retry_after=effective_retry_after,
            error_type=error.__class__.__name__ if error else None,
            error_text=_safe_error_text(error) if error else None,
        )

    @classmethod
    def failed(
        cls,
        *,
        target_id: int | str | None = None,
        message_ids: Iterable[int] | int | None = None,
        error: BaseException,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        return cls(
            status=TargetVerificationStatus.FAILED,
            context=context,
            target_id=target_id,
            message_ids=_normalize_message_ids(message_ids),
            error_type=error.__class__.__name__,
            error_text=_safe_error_text(error),
        )

    @classmethod
    def skipped(
        cls,
        *,
        target_id: int | str | None = None,
        message_ids: Iterable[int] | int | None = None,
        reason: str,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        return cls(
            status=TargetVerificationStatus.SKIPPED,
            context=context,
            target_id=target_id,
            message_ids=_normalize_message_ids(message_ids),
            reason=reason,
        )

    @property
    def ok(self) -> bool:
        return self.status == TargetVerificationStatus.VERIFIED

    @property
    def should_defer(self) -> bool:
        return self.status == TargetVerificationStatus.RATE_LIMITED

    @property
    def is_failure(self) -> bool:
        return self.status in (TargetVerificationStatus.NOT_FOUND, TargetVerificationStatus.FAILED)

    @property
    def is_skipped(self) -> bool:
        return self.status == TargetVerificationStatus.SKIPPED

    def to_log_context(self) -> dict[str, object]:
        result: dict[str, object] = {
            "status": self.status.value,
            "target_id": self.target_id,
            "message_ids": self.message_ids,
            "found_message_ids": self.found_message_ids,
            "missing_message_ids": self.missing_message_ids,
            "message_count": len(self.message_ids),
            "found_count": len(self.found_message_ids),
            "missing_count": len(self.missing_message_ids),
            "error_type": self.error_type,
            "error_text": self.error_text,
            "retry_after": self.retry_after,
            "reason": self.reason,
        }
        if self.context is not None:
            result["context"] = self.context.to_log_context()
        return result

    def log_label(self) -> str:
        parts = [
            f"status={self.status.value}",
            f"target={self.target_id}",
            f"messages={len(self.message_ids)}",
            f"found={len(self.found_message_ids)}",
            f"missing={len(self.missing_message_ids)}",
        ]
        if self.context is not None:
            parts.append(f"delivery={self.context.delivery_id}")
            parts.append(f"rule={self.context.rule_id}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return " ".join(parts)


class TargetVerifier:
    def __init__(self, *, telethon_client: Any) -> None:
        self.telethon_client = telethon_client

    async def verify_message_exists(
        self,
        *,
        target_id: int | str,
        message_id: int | None,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        if message_id is None:
            return TargetVerificationResult.skipped(target_id=target_id, reason="missing_message_id", context=context)
        try:
            raw_messages = await self.telethon_client.get_messages(target_id, ids=message_id)
        except TransportRateLimited as error:
            return TargetVerificationResult.rate_limited(
                target_id=target_id,
                message_ids=(message_id,),
                error=error,
                context=context,
            )
        except Exception as error:
            return TargetVerificationResult.failed(target_id=target_id, message_ids=(message_id,), error=error, context=context)

        found_ids = _found_message_ids(raw_messages, (message_id,))
        if found_ids:
            return TargetVerificationResult.verified(
                target_id=target_id,
                message_ids=(message_id,),
                found_message_ids=found_ids,
                context=context,
            )
        return TargetVerificationResult.not_found(
            target_id=target_id,
            message_ids=(message_id,),
            missing_message_ids=(message_id,),
            context=context,
        )

    async def verify_message_ids(
        self,
        *,
        target_id: int | str,
        message_ids: Iterable[int] | int | None,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        normalized_ids = _normalize_message_ids(message_ids)
        if not normalized_ids:
            return TargetVerificationResult.skipped(target_id=target_id, reason="missing_message_ids", context=context)
        try:
            raw_messages = await self.telethon_client.get_messages(target_id, ids=list(normalized_ids))
        except TransportRateLimited as error:
            return TargetVerificationResult.rate_limited(
                target_id=target_id,
                message_ids=normalized_ids,
                error=error,
                context=context,
            )
        except Exception as error:
            return TargetVerificationResult.failed(target_id=target_id, message_ids=normalized_ids, error=error, context=context)

        found_ids = _found_message_ids(raw_messages, normalized_ids)
        missing_ids = tuple(message_id for message_id in normalized_ids if message_id not in set(found_ids))
        if not missing_ids:
            return TargetVerificationResult.verified(
                target_id=target_id,
                message_ids=normalized_ids,
                found_message_ids=found_ids,
                context=context,
            )
        return TargetVerificationResult.not_found(
            target_id=target_id,
            message_ids=normalized_ids,
            found_message_ids=found_ids,
            missing_message_ids=missing_ids,
            context=context,
        )

    async def verify_send_result(
        self,
        *,
        target_id: int | str,
        send_result: TelegramSendResult,
        context: DeliveryContext | None = None,
    ) -> TargetVerificationResult:
        return await self.verify_message_ids(
            target_id=target_id,
            message_ids=send_result.sent_message_ids,
            context=context,
        )


def _message_id_from_raw(message: Any) -> int | None:
    if message is None:
        return None
    value = None
    if isinstance(message, dict):
        value = message.get("message_id") or message.get("id")
    else:
        value = getattr(message, "message_id", None) or getattr(message, "id", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _found_message_ids(raw_messages: Any, requested_ids: tuple[int, ...]) -> tuple[int, ...]:
    if raw_messages is None:
        return ()
    raw_items = raw_messages if isinstance(raw_messages, (list, tuple)) else (raw_messages,)
    extracted = {_message_id_from_raw(message) for message in raw_items}
    return tuple(message_id for message_id in requested_ids if message_id in extracted)
