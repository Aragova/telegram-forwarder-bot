from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .delivery_idempotency import normalize_valid_sent_message_ids


def normalize_telegram_sent_ids(values: Any) -> list[int]:
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        return normalize_valid_sent_message_ids(list(values))
    return normalize_valid_sent_message_ids([values])


@dataclass(slots=True)
class TelegramSendResult:
    ok: bool
    method: str
    sent_message_ids: list[int] = field(default_factory=list)
    sent_message_id: int | None = None
    raw_result_type: str | None = None
    raw_result_repr: str | None = None
    error_text: str | None = None
    retryable: bool = True
    attempted: bool = True
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def has_valid_sent_ids(self) -> bool:
        return bool(self.sent_message_ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "method": self.method,
            "sent_message_ids": self.sent_message_ids[:],
            "sent_message_id": self.sent_message_id,
            "raw_result_type": self.raw_result_type,
            "raw_result_repr": self.raw_result_repr,
            "error_text": self.error_text,
            "retryable": self.retryable,
            "attempted": self.attempted,
            "extra": dict(self.extra or {}),
        }


def telegram_send_result_from_raw(raw_result: Any, *, method: str, fallback_sent_ids: list[int] | None = None, error_text: str | None = None, retryable: bool = True, attempted: bool = True, extra: dict[str, Any] | None = None) -> TelegramSendResult:
    raw_type = type(raw_result).__name__ if raw_result is not None else "NoneType"
    sent_ids: list[int] = []
    result_ok = None
    effective_error_text = error_text
    effective_retryable = retryable

    if isinstance(raw_result, dict):
        sent_ids = normalize_telegram_sent_ids(
            raw_result.get("sent_message_ids")
            or raw_result.get("sent_ids")
            or raw_result.get("message_ids")
            or raw_result.get("sent_message_id")
            or raw_result.get("message_id")
            or raw_result.get("id")
        )
        if "ok" in raw_result:
            result_ok = bool(raw_result.get("ok"))
        if raw_result.get("error_text") and not effective_error_text:
            effective_error_text = str(raw_result.get("error_text"))
        if "retryable" in raw_result:
            effective_retryable = bool(raw_result.get("retryable"))
    elif isinstance(raw_result, (list, tuple)):
        extracted: list[Any] = []
        for item in raw_result:
            extracted.append(getattr(item, "message_id", None) or getattr(item, "id", None))
        sent_ids = normalize_telegram_sent_ids(extracted)
    else:
        sent_ids = normalize_telegram_sent_ids(getattr(raw_result, "message_id", None) or getattr(raw_result, "id", None))

    if not sent_ids and fallback_sent_ids:
        sent_ids = normalize_telegram_sent_ids(fallback_sent_ids)

    ok = bool(sent_ids) and (result_ok is not False)
    if not ok and not effective_error_text:
        effective_error_text = "telegram_send_result_has_no_valid_message_ids"
    sent_message_id = sent_ids[0] if sent_ids else None
    return TelegramSendResult(
        ok=ok,
        method=method,
        sent_message_ids=sent_ids,
        sent_message_id=sent_message_id,
        raw_result_type=raw_type,
        raw_result_repr=repr(raw_result)[:300] if raw_result is not None else None,
        error_text=effective_error_text,
        retryable=effective_retryable,
        attempted=attempted,
        extra=extra or {},
    )


def telegram_send_success(*, method: str, sent_message_ids: list[int], raw_result: Any = None, extra: dict[str, Any] | None = None) -> TelegramSendResult:
    return telegram_send_result_from_raw(raw_result, method=method, fallback_sent_ids=sent_message_ids, extra=extra, retryable=True, attempted=True)


def telegram_send_failure(*, method: str, error_text: str, retryable: bool = True, attempted: bool = True, raw_result: Any = None, extra: dict[str, Any] | None = None) -> TelegramSendResult:
    return telegram_send_result_from_raw(raw_result, method=method, error_text=error_text, retryable=retryable, attempted=attempted, extra=extra)
