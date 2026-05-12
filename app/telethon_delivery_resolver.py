from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

WINDOW_BEFORE_SECONDS = 60
WINDOW_AFTER_SECONDS = 300
CAPTION_FINGERPRINT_LENGTH = 160


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
    for candidate in [
        getattr(message, "chat_id", None),
        getattr(getattr(message, "peer_id", None), "channel_id", None),
        getattr(getattr(message, "to_id", None), "channel_id", None),
        getattr(getattr(message, "peer_id", None), "chat_id", None),
        getattr(getattr(message, "to_id", None), "chat_id", None),
    ]:
        normalized = _normalize_telegram_channel_id_for_compare(candidate)
        if normalized is not None:
            return normalized
    return None


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


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
    normalized = str(text or "").lower().replace("\xa0", " ").replace("\n", " ").replace("\r", " ")
    normalized = " ".join(normalized.split())
    return normalized[:CAPTION_FINGERPRINT_LENGTH]


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
    base = _to_utc(started_at) or datetime.now(timezone.utc)
    min_dt = base - timedelta(seconds=WINDOW_BEFORE_SECONDS)
    expected = [int(x) for x in raw_message_ids if x]
    msgs = await telethon_client.get_messages(normalize_telethon_target(target_id), ids=expected)
    if not isinstance(msgs, list):
        msgs = [msgs]
    mapped = {int(getattr(m, "id", 0) or 0): m for m in msgs if m}
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
        dt = _to_utc(getattr(m, "date", None))
        if dt is not None and dt < min_dt:
            continue
        valid.append(m)
    if len(valid) < expected_count:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [int(getattr(x, "id", 0) or 0) for x in valid], None, False, "Не удалось подтвердить ID отправленного альбома в целевом канале.", raw_message_ids=expected)

    if expected_count > 1:
        grouped_ids = {int(getattr(m, "grouped_id", 0) or 0) for m in valid}
        if 0 in grouped_ids or len(grouped_ids) != 1:
            return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, False, "У отправленного альбома отсутствует корректный grouped_id.", raw_message_ids=expected)
        gid = next(iter(grouped_ids))
        group_rows = await telethon_client.get_messages(normalize_telethon_target(target_id), ids=[int(getattr(m, "id", 0) or 0) for m in valid])
        if not isinstance(group_rows, list):
            group_rows = [group_rows]
        same_group = [m for m in group_rows if int(getattr(m, "grouped_id", 0) or 0) == gid]
        if len(same_group) < expected_count:
            return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], gid, False, "Не удалось подтвердить полный состав отправленного альбома.", raw_message_ids=expected)
    else:
        gid = int(getattr(valid[0], "grouped_id", 0) or 0) or None
        same_group = list(valid)

    if fp:
        captions = "\n".join(str(getattr(m, "message", "") or "") for m in same_group)
        normalized = normalize_caption_fingerprint(captions)
        if normalized and fp not in normalized:
            return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], gid, False, "Не совпадает подпись отправленного альбома.", raw_message_ids=expected)

    ids = sorted(int(getattr(m, "id", 0) or 0) for m in same_group)[:expected_count]
    return TelethonResolvedDelivery(True, "telethon_source", ids[0] if ids else None, ids, gid, False, raw_message_ids=expected)


async def recover_album_ids_by_scan(*, telethon_client, target_id: int | str, expected_count: int, expected_caption: str | None, started_at: datetime) -> TelethonResolvedDelivery:
    base = _to_utc(started_at) or datetime.now(timezone.utc)
    min_dt = base - timedelta(seconds=WINDOW_BEFORE_SECONDS)
    max_dt = base + timedelta(seconds=WINDOW_AFTER_SECONDS)
    fp = normalize_caption_fingerprint(expected_caption or "") if expected_caption else ""
    entity = normalize_telethon_target(target_id)

    collected: list[Any] = []
    try:
        async for msg in telethon_client.iter_messages(entity, limit=None):
            dt = _to_utc(getattr(msg, "date", None))
            if dt is not None and dt < min_dt:
                break
            if dt is None or dt <= max_dt:
                collected.append(msg)
    except Exception:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Не удалось восстановить ID альбома сканированием канала.")

    groups = collect_album_groups(collected)
    candidates: list[tuple[tuple[int, int, float, int], int, list[int]]] = []
    for gid, items in groups.items():
        msg_dt = _to_utc(getattr(items[0], "date", None))
        if msg_dt is None or msg_dt < min_dt or msg_dt > max_dt:
            continue
        media_ok = len(items) >= expected_count
        if not media_ok:
            continue
        captions = "\n".join(str(getattr(m, "message", "") or "") for m in items)
        caption_norm = normalize_caption_fingerprint(captions)
        caption_match = 1 if (not fp or (caption_norm and fp in caption_norm)) else 0
        if fp and caption_match == 0:
            continue
        after_penalty = 0 if msg_dt >= base else 1
        delta = abs((msg_dt - base).total_seconds())
        score = (-caption_match, 0 if media_ok else 1, after_penalty, delta)
        ids = sorted(int(getattr(m, "id", 0) or 0) for m in items)[:expected_count]
        candidates.append((score, gid, ids))

    if not candidates:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Не удалось восстановить ID альбома сканированием канала.")
    candidates.sort(key=lambda x: x[0])
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        return TelethonResolvedDelivery(False, "telethon_source_unverified", None, [], None, True, "Найдено несколько кандидатов альбома с одинаковым приоритетом.")
    _, gid, ids = candidates[0]
    return TelethonResolvedDelivery(True, "telethon_source_recovered_by_scan", ids[0] if ids else None, ids, gid, True)
