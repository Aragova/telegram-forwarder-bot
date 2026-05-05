from __future__ import annotations

import logging
from typing import Any

from aiogram.types import MessageEntity
from telethon.tl import types as tl_types

logger = logging.getLogger("forwarder")


def serialize_message_entities(entities) -> list[dict[str, Any]]:
    if not entities:
        return []
    payload: list[dict[str, Any]] = []
    for entity in entities:
        if hasattr(entity, "model_dump"):
            item = entity.model_dump(exclude_none=True)
        elif hasattr(entity, "dict"):
            item = entity.dict(exclude_none=True)
        else:
            item = {
                "type": getattr(entity, "type", None),
                "offset": getattr(entity, "offset", None),
                "length": getattr(entity, "length", None),
                "url": getattr(entity, "url", None),
                "user": getattr(entity, "user", None),
                "language": getattr(entity, "language", None),
                "custom_emoji_id": getattr(entity, "custom_emoji_id", None),
            }
            item = {k: v for k, v in item.items() if v is not None}
        user = item.get("user")
        if user is not None and not isinstance(user, (str, int, float, bool, dict, list, type(None))):
            item["user"] = getattr(user, "model_dump", lambda **kwargs: {"id": getattr(user, "id", None)})(exclude_none=True)
        payload.append(item)
    return payload


def deserialize_message_entities(raw_entities: list[dict[str, Any]] | None) -> list[MessageEntity] | None:
    if not raw_entities:
        return None
    items: list[MessageEntity] = []
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(MessageEntity(**raw))
        except Exception as exc:
            logger.warning("Некорректная entity в saved_post пропущена: %s | raw=%s", exc, raw)
            continue
    return items or None


def saved_post_requires_premium_send(content: dict[str, Any]) -> bool:
    entities = content.get("entities") or []
    caption_entities = content.get("caption_entities") or []
    return any(
        str(item.get("type")) == "custom_emoji" or item.get("custom_emoji_id")
        for item in [*entities, *caption_entities]
        if isinstance(item, dict)
    )


def normalize_saved_post_entities(raw_entities: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not raw_entities:
        return []

    entities_payload = raw_entities
    if isinstance(raw_entities, str):
        import json

        try:
            parsed = json.loads(raw_entities)
            entities_payload = parsed
        except Exception as exc:
            logger.warning("Некорректный JSON entities в saved_post: %s", exc)
            return []
    if isinstance(entities_payload, dict):
        entities_payload = [entities_payload]
    if not isinstance(entities_payload, list):
        return []

    normalized: list[dict[str, Any]] = []
    for raw in entities_payload:
        if not isinstance(raw, dict):
            logger.warning("Битая entity в saved_post пропущена: %s", raw)
            continue
        try:
            entity_type = str(raw.get("type") or "")
            offset = int(raw.get("offset"))
            length = int(raw.get("length"))
        except Exception:
            logger.warning("Битая entity в saved_post пропущена: %s", raw)
            continue

        item: dict[str, Any] = {"type": entity_type, "offset": offset, "length": length}
        if raw.get("url") is not None:
            item["url"] = str(raw.get("url"))
        if raw.get("language") is not None:
            item["language"] = str(raw.get("language"))
        if raw.get("custom_emoji_id") is not None:
            item["custom_emoji_id"] = str(raw.get("custom_emoji_id"))
        normalized.append(item)
    return normalized


def saved_post_entities_to_telethon(raw_entities: list[dict[str, Any]] | None) -> list:
    normalized = normalize_saved_post_entities(raw_entities)
    if not normalized:
        return []

    items: list = []
    custom_emoji_count = 0
    for raw in normalized:
        entity_type = str(raw.get("type") or "")
        offset = int(raw.get("offset"))
        length = int(raw.get("length"))
        try:
            if entity_type == "bold":
                items.append(tl_types.MessageEntityBold(offset=offset, length=length))
            elif entity_type == "italic":
                items.append(tl_types.MessageEntityItalic(offset=offset, length=length))
            elif entity_type == "underline":
                items.append(tl_types.MessageEntityUnderline(offset=offset, length=length))
            elif entity_type in {"strikethrough", "strike"}:
                items.append(tl_types.MessageEntityStrike(offset=offset, length=length))
            elif entity_type == "code":
                items.append(tl_types.MessageEntityCode(offset=offset, length=length))
            elif entity_type == "pre":
                items.append(tl_types.MessageEntityPre(offset=offset, length=length, language=str(raw.get("language") or "")))
            elif entity_type == "text_link":
                url = str(raw.get("url") or "")
                if not url:
                    raise ValueError("missing url")
                items.append(tl_types.MessageEntityTextUrl(offset=offset, length=length, url=url))
            elif entity_type == "url":
                items.append(tl_types.MessageEntityUrl(offset=offset, length=length))
            elif entity_type == "mention":
                items.append(tl_types.MessageEntityMention(offset=offset, length=length))
            elif entity_type == "custom_emoji":
                custom_emoji_id = raw.get("custom_emoji_id")
                if custom_emoji_id is None:
                    raise ValueError("missing custom_emoji_id")
                items.append(tl_types.MessageEntityCustomEmoji(offset=offset, length=length, document_id=int(custom_emoji_id)))
                custom_emoji_count += 1
        except Exception:
            continue
    logger.info("SAVED_POST_TELETHON_ENTITIES_BUILT | total=%s | custom_emoji_count=%s", len(items), custom_emoji_count)
    return items
