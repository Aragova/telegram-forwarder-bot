from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping


_SAFE_LOG_FIELDS = (
    "delivery_id",
    "rule_id",
    "post_id",
    "source_id",
    "source_thread_id",
    "target_id",
    "target_thread_id",
    "message_id",
    "media_group_id",
    "mode",
    "operation",
    "is_album",
)


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Safe technical context for one delivery operation.

    This object intentionally stores only identifiers and routing metadata. It
    must not keep raw payloads, captions, Telegram objects, clients, DB
    connections, or temporary media paths.
    """

    delivery_id: int | None = None
    rule_id: int | None = None
    post_id: int | None = None

    source_id: int | str | None = None
    source_thread_id: int | None = None
    target_id: int | str | None = None
    target_thread_id: int | None = None

    message_id: int | None = None
    media_group_id: str | None = None

    mode: str | None = None
    operation: str | None = None
    is_album: bool = False

    @classmethod
    def from_job_payload(cls, payload: Mapping[str, Any]) -> DeliveryContext:
        """Build a context from known safe payload keys only.

        Unknown keys are ignored instead of being stored, which keeps sensitive
        payload data out of the context and its log helpers.
        """

        return cls(
            delivery_id=payload.get("delivery_id"),
            rule_id=payload.get("rule_id"),
            post_id=payload.get("post_id"),
            source_id=payload.get("source_id"),
            source_thread_id=payload.get("source_thread_id"),
            target_id=payload.get("target_id"),
            target_thread_id=payload.get("target_thread_id"),
            message_id=payload.get("message_id"),
            media_group_id=payload.get("media_group_id"),
            mode=payload.get("mode"),
            operation=payload.get("operation"),
            is_album=bool(payload.get("is_album", False)),
        )

    def to_log_context(self) -> dict[str, object]:
        """Return safe technical fields for structured logs.

        None values are kept deliberately so callers get a stable set of keys.
        """

        return {field: getattr(self, field) for field in _SAFE_LOG_FIELDS}

    def log_label(self) -> str:
        """Return a compact, stable, payload-free label for logs/debug."""

        parts = [
            f"delivery={self.delivery_id}",
            f"rule={self.rule_id}",
            f"post={self.post_id}",
            f"mode={self.mode}",
            f"operation={self.operation}",
            f"target={self.target_id}",
            f"album={self.is_album}",
        ]
        return " ".join(parts)

    def with_operation(self, operation: str) -> DeliveryContext:
        """Return a new context with updated operation, preserving immutability."""

        return replace(self, operation=operation)
