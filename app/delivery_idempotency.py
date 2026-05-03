from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class DeliveryIdempotencyResult:
    idempotency_key: str
    already_accepted: bool
    sent_message_ids: list[int]
    attempt: dict[str, Any] | None = None


def build_delivery_idempotency_key(*, operation_kind: str, delivery_id: int | None = None, rule_id: int | None = None, target_id: str | None = None, media_group_id: str | None = None, source_message_ids: list[int] | None = None) -> str:
    kind = str(operation_kind or "").strip().lower()
    target = str(target_id or "")
    if kind == "single":
        return f"delivery:{int(delivery_id or 0)}:target:{target}:single"
    if kind == "album":
        if media_group_id:
            return f"rule:{int(rule_id or 0)}:target:{target}:media_group:{media_group_id}:album"
        sorted_ids = ",".join(str(int(x)) for x in sorted(int(x) for x in (source_message_ids or [])))
        return f"rule:{int(rule_id or 0)}:target:{target}:album_sources:{sorted_ids}"
    if kind == "video_send":
        return f"delivery:{int(delivery_id or 0)}:target:{target}:video_send:v1"
    return f"delivery:{int(delivery_id or 0)}:target:{target}:{kind or 'unknown'}"


def extract_sent_message_ids_from_attempt(attempt: dict[str, Any] | None) -> list[int]:
    if not isinstance(attempt, dict):
        return []
    raw = attempt.get("sent_message_ids_json")
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    result: list[int] = []
    for value in raw:
        try:
            result.append(int(value))
        except Exception:
            continue
    return result
