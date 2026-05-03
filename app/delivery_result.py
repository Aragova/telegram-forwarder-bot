from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .delivery_idempotency import normalize_valid_sent_message_ids


@dataclass(slots=True)
class DeliveryResult:
    ok: bool
    retryable: bool = True
    accepted: bool = False
    cache_hit: bool = False
    fallback_to_legacy: bool = False
    final_invalid_source_file: bool = False
    restart_download: bool = False

    sent_message_ids: list[int] = field(default_factory=list)
    sent_message_id: int | None = None

    error_text: str | None = None
    warning_text: str | None = None
    warnings: list[str] = field(default_factory=list)

    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": bool(self.ok),
            "retryable": bool(self.retryable),
            "accepted": bool(self.accepted),
            "cache_hit": bool(self.cache_hit),
            "fallback_to_legacy": bool(self.fallback_to_legacy),
            "final_invalid_source_file": bool(self.final_invalid_source_file),
            "restart_download": bool(self.restart_download),
            "sent_message_ids": list(self.sent_message_ids),
            "sent_message_id": self.sent_message_id,
            "error_text": self.error_text,
            "warning_text": self.warning_text,
            "warnings": list(self.warnings),
        }
        result.update(dict(self.extra or {}))
        return result


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return bool(value)


def _to_optional_int(value: Any) -> int | None:
    try:
        int_value = int(value)
    except Exception:
        return None
    if int_value <= 0:
        return None
    return int_value


def delivery_result_from_dict(value: dict[str, Any]) -> DeliveryResult:
    known_fields = {
        "ok",
        "retryable",
        "accepted",
        "cache_hit",
        "fallback_to_legacy",
        "final_invalid_source_file",
        "restart_download",
        "sent_message_ids",
        "sent_message_id",
        "error_text",
        "warning_text",
        "warnings",
    }
    sent_message_ids = normalize_valid_sent_message_ids(value.get("sent_message_ids") or [])
    sent_message_id = _to_optional_int(value.get("sent_message_id"))

    if sent_message_ids:
        sent_message_id = sent_message_ids[0]
    elif sent_message_id is not None:
        sent_message_ids = [sent_message_id]

    warnings_value = value.get("warnings")
    warnings: list[str] = []
    if isinstance(warnings_value, list):
        warnings = [str(item) for item in warnings_value if str(item).strip()]

    extra = {key: val for key, val in value.items() if key not in known_fields}

    return DeliveryResult(
        ok=_to_bool(value.get("ok"), False),
        retryable=_to_bool(value.get("retryable"), True),
        accepted=_to_bool(value.get("accepted"), False),
        cache_hit=_to_bool(value.get("cache_hit"), False),
        fallback_to_legacy=_to_bool(value.get("fallback_to_legacy"), False),
        final_invalid_source_file=_to_bool(value.get("final_invalid_source_file"), False),
        restart_download=_to_bool(value.get("restart_download"), False),
        sent_message_ids=sent_message_ids,
        sent_message_id=sent_message_id,
        error_text=str(value.get("error_text")) if value.get("error_text") is not None else None,
        warning_text=str(value.get("warning_text")) if value.get("warning_text") is not None else None,
        warnings=warnings,
        extra=extra,
    )


def normalize_delivery_result(value: Any) -> DeliveryResult:
    if isinstance(value, DeliveryResult):
        return value
    if isinstance(value, bool):
        return DeliveryResult(ok=value)
    if isinstance(value, dict):
        return delivery_result_from_dict(value)
    if value is None:
        return DeliveryResult(ok=False, retryable=True, error_text="empty_result")
    return DeliveryResult(ok=bool(value))


def delivery_result_to_bool(value: Any) -> bool:
    return normalize_delivery_result(value).ok
