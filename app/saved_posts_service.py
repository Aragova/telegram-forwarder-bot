from __future__ import annotations

from typing import Any

from aiogram.types import MessageEntity


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


def build_saved_post_content_from_aiogram_message(message) -> dict[str, Any]:
    kind = "text"
    media = None
    if getattr(message, "photo", None):
        kind = "photo"
        ph = message.photo[-1]
        media = {"file_id": ph.file_id, "file_unique_id": getattr(ph, "file_unique_id", None), "width": getattr(ph, "width", 0), "height": getattr(ph, "height", 0), "duration": 0, "mime_type": None, "file_name": None}
    elif getattr(message, "video", None):
        kind = "video"
        v = message.video
        media = {"file_id": v.file_id, "file_unique_id": getattr(v, "file_unique_id", None), "width": getattr(v, "width", 0), "height": getattr(v, "height", 0), "duration": getattr(v, "duration", 0), "mime_type": getattr(v, "mime_type", None), "file_name": getattr(v, "file_name", None)}
    elif getattr(message, "animation", None):
        kind = "animation"
        a = message.animation
        media = {"file_id": a.file_id, "file_unique_id": getattr(a, "file_unique_id", None), "width": getattr(a, "width", 0), "height": getattr(a, "height", 0), "duration": getattr(a, "duration", 0), "mime_type": getattr(a, "mime_type", None), "file_name": getattr(a, "file_name", None)}
    elif getattr(message, "document", None):
        kind = "document"
        d = message.document
        media = {"file_id": d.file_id, "file_unique_id": getattr(d, "file_unique_id", None), "width": 0, "height": 0, "duration": 0, "mime_type": getattr(d, "mime_type", None), "file_name": getattr(d, "file_name", None)}

    return {
        "schema_version": 1,
        "kind": kind,
        "text": message.text or "",
        "caption": message.caption or "",
        "entities": serialize_message_entities(getattr(message, "entities", None)),
        "caption_entities": serialize_message_entities(getattr(message, "caption_entities", None)),
        "media": media,
        "forward_origin": {"chat_id": str(message.chat.id) if getattr(message, "chat", None) else None, "message_id": getattr(message, "message_id", None)},
    }


def deserialize_message_entities(raw_entities: list[dict[str, Any]] | None) -> list[MessageEntity] | None:
    if not raw_entities:
        return None
    items: list[MessageEntity] = []
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        try:
            items.append(MessageEntity(**raw))
        except Exception:
            continue
    return items or None


def get_saved_post_preview_caption(content: dict[str, Any]) -> str:
    kind = str(content.get("kind") or "text")
    text = str(content.get("text") or "").strip()
    caption = str(content.get("caption") or "").strip()

    if kind == "text":
        if text:
            return text
        return "Рекламный текст пустой."

    label_map = {
        "photo": "Фото",
        "video": "Видео",
        "animation": "Анимация",
        "document": "Документ",
    }
    label = label_map.get(kind, "Пост")
    if caption:
        return f"{label}: {caption}"
    return f"{label} без подписи."


def get_saved_post_short_description(content: dict[str, Any]) -> str:
    kind = str(content.get("kind") or "text")
    mapping = {"text": "текст", "photo": "фото", "video": "видео", "animation": "анимация", "document": "документ"}
    return mapping.get(kind, kind)
