from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def normalize_telethon_target(chat_id: int | str) -> int | str:
    value = str(chat_id).strip()
    if value.lstrip("-").isdigit():
        return int(value)
    return chat_id


def _normalize_telegram_channel_id_for_compare(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.startswith("-100"):
        normalized = raw[4:]
    elif raw.startswith("-"):
        normalized = raw[1:]
    else:
        normalized = raw
    return normalized if normalized.isdigit() else None


def _extract_telethon_message_peer_id(message: Any) -> str | None:
    if message is None:
        return None
    for candidate in [getattr(message, "chat_id", None), getattr(getattr(message, "peer_id", None), "channel_id", None), getattr(getattr(message, "to_id", None), "channel_id", None), getattr(getattr(message, "peer_id", None), "chat_id", None), getattr(getattr(message, "to_id", None), "chat_id", None)]:
        normalized = _normalize_telegram_channel_id_for_compare(candidate)
        if normalized is not None:
            return normalized
    return None



@dataclass(frozen=True)
class TelethonResolvedDelivery:
    ok: bool
    method: str
    message_id: int | None
    message_ids: list[int]
    grouped_id: int | None
    recovered: bool
    error_text: str | None = None
    raw_message_ids: list[int] | None = None


def normalize_caption_fingerprint(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def collect_album_groups(messages) -> dict[int, list[Any]]:
    groups: dict[int, list[Any]] = {}
    for msg in messages or []:
        gid = getattr(msg, "grouped_id", None)
        if gid is None:
            continue
        groups.setdefault(int(gid), []).append(msg)
    for items in groups.values():
        items.sort(key=lambda x: int(getattr(x, "id", 0) or 0))
    return groups


async def verify_raw_album_ids(*, telethon_client, target_id: int | str, raw_message_ids: list[int], expected_count: int, expected_caption: str | None, started_at: datetime) -> TelethonResolvedDelivery:
    if not raw_message_ids:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, False, "Telethon не вернул ID сообщений", raw_message_ids=[])
    msgs = await telethon_client.get_messages(normalize_telethon_target(target_id), ids=raw_message_ids)
    if not isinstance(msgs, list):
        msgs = [msgs]
    expected = [int(x) for x in raw_message_ids if x]
    mapped = {int(getattr(m, "id", 0) or 0): m for m in msgs if m}
    min_dt = started_at if getattr(started_at, "tzinfo", None) else started_at.replace(tzinfo=timezone.utc)
    min_dt = min_dt - timedelta(seconds=60)
    target_peer = _normalize_telegram_channel_id_for_compare(target_id)
    fp = normalize_caption_fingerprint(expected_caption or "") if expected_caption else ""
    valid: list[Any] = []
    for mid in expected:
        m = mapped.get(mid)
        if not m:
            continue
        peer = _extract_telethon_message_peer_id(m)
        if target_peer and peer and peer != target_peer:
            continue
        dt = getattr(m, "date", None)
        if dt is not None:
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt < min_dt:
                continue
        valid.append(m)
    if len(valid) < expected_count:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [int(getattr(x, 'id', 0) or 0) for x in valid], None, False, "Не удалось подтвердить ID отправленного альбома в целевом канале.", raw_message_ids=expected)
    gid = int(getattr(valid[0], "grouped_id", 0) or 0)
    same_group = [m for m in valid if int(getattr(m, "grouped_id", 0) or 0) == gid] if gid else list(valid)
    if fp:
        captions = "\n".join(str(getattr(m, "message", "") or "") for m in same_group)
        normalized_captions = normalize_caption_fingerprint(captions)
        if normalized_captions and fp not in normalized_captions:
            return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], gid or None, False, "Не совпадает подпись отправленного альбома.", raw_message_ids=expected)
    ids = sorted(int(getattr(m, "id", 0) or 0) for m in same_group)[:expected_count]
    return TelethonResolvedDelivery(True, "telethon_source", ids[0] if ids else None, ids, gid or None, False, raw_message_ids=expected)


async def recover_album_ids_by_scan(*, telethon_client, target_id: int | str, expected_count: int, expected_caption: str | None, started_at: datetime, scan_limit: int = 80) -> TelethonResolvedDelivery:
    entity = normalize_telethon_target(target_id)
    try:
        recent = await telethon_client.get_messages(entity, limit=scan_limit)
    except TypeError:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Не удалось восстановить ID альбома сканированием канала.")
    if not isinstance(recent, list):
        recent = [recent]
    groups = collect_album_groups(recent)
    fp = normalize_caption_fingerprint(expected_caption or "") if expected_caption else ""
    base_dt = started_at if getattr(started_at, "tzinfo", None) else started_at.replace(tzinfo=timezone.utc)
    candidates = []
    for gid, items in groups.items():
        if len(items) < expected_count:
            continue
        dt = getattr(items[0], "date", None)
        if dt is not None and getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        captions = "\n".join(str(getattr(m, "message", "") or "") for m in items)
        if fp and fp not in normalize_caption_fingerprint(captions):
            continue
        after_penalty = 0 if (dt is None or dt >= base_dt) else 1
        delta = abs((dt - base_dt).total_seconds()) if dt is not None else 10**9
        score = (after_penalty, delta)
        ids = sorted(int(getattr(m, "id", 0) or 0) for m in items)[:expected_count]
        candidates.append((score, gid, ids))
    if not candidates:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Не удалось восстановить ID альбома сканированием канала.")
    candidates.sort(key=lambda x: x[0])
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Найдено несколько кандидатов альбома с одинаковым приоритетом.")
    _, gid, ids = candidates[0]
    return TelethonResolvedDelivery(True, "telethon_source_recovered_by_scan", ids[0] if ids else None, ids, gid, True)
