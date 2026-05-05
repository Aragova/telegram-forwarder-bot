from __future__ import annotations

from typing import Any

from app.saved_post_entities import serialize_message_entities


def summarize_aiogram_message_for_saved_post(message) -> dict[str, Any]:
    return {
        "message_id": getattr(message, "message_id", None),
        "content_type": getattr(message, "content_type", None),
        "has_text": bool(getattr(message, "text", None)),
        "text_len": len(getattr(message, "text", None) or ""),
        "text_entities_count": len(getattr(message, "entities", None) or []),
        "has_caption": bool(getattr(message, "caption", None)),
        "caption_len": len(getattr(message, "caption", None) or ""),
        "caption_entities_count": len(getattr(message, "caption_entities", None) or []),
        "has_photo": bool(getattr(message, "photo", None)),
        "photo_count": len(getattr(message, "photo", None) or []),
        "has_video": bool(getattr(message, "video", None)),
        "has_animation": bool(getattr(message, "animation", None)),
        "has_document": bool(getattr(message, "document", None)),
        "media_group_id": getattr(message, "media_group_id", None),
        "forward_origin_type": type(getattr(message, "forward_origin", None)).__name__ if getattr(message, "forward_origin", None) else None,
    }


def summarize_saved_post_entities(content: dict[str, Any]) -> dict[str, Any]:
    entities = content.get("entities") or []
    caption_entities = content.get("caption_entities") or []
    custom_emoji_entities = sum(1 for item in entities if isinstance(item, dict) and item.get("custom_emoji_id"))
    custom_emoji_caption_entities = sum(1 for item in caption_entities if isinstance(item, dict) and item.get("custom_emoji_id"))
    return {
        "entities_count": len(entities),
        "caption_entities_count": len(caption_entities),
        "entities_custom_emoji_count": custom_emoji_entities,
        "caption_entities_custom_emoji_count": custom_emoji_caption_entities,
    }


def build_saved_post_content_from_aiogram_message(message) -> dict[str, Any]:
    text = ""
    caption = ""
    entities: list[dict[str, Any]] = []
    caption_entities: list[dict[str, Any]] = []
    media = None

    if getattr(message, "photo", None):
        kind = "photo"
        ph = message.photo[-1]
        caption = message.caption or ""
        caption_entities = serialize_message_entities(getattr(message, "caption_entities", None))
        media = {"file_id": ph.file_id, "file_unique_id": getattr(ph, "file_unique_id", None), "width": getattr(ph, "width", 0), "height": getattr(ph, "height", 0), "duration": 0, "mime_type": None, "file_name": None}
    elif getattr(message, "video", None):
        kind = "video"
        v = message.video
        caption = message.caption or ""
        caption_entities = serialize_message_entities(getattr(message, "caption_entities", None))
        media = {"file_id": v.file_id, "file_unique_id": getattr(v, "file_unique_id", None), "width": getattr(v, "width", 0), "height": getattr(v, "height", 0), "duration": getattr(v, "duration", 0), "mime_type": getattr(v, "mime_type", None), "file_name": getattr(v, "file_name", None)}
    elif getattr(message, "animation", None):
        kind = "animation"
        a = message.animation
        caption = message.caption or ""
        caption_entities = serialize_message_entities(getattr(message, "caption_entities", None))
        media = {"file_id": a.file_id, "file_unique_id": getattr(a, "file_unique_id", None), "width": getattr(a, "width", 0), "height": getattr(a, "height", 0), "duration": getattr(a, "duration", 0), "mime_type": getattr(a, "mime_type", None), "file_name": getattr(a, "file_name", None)}
    elif getattr(message, "document", None):
        kind = "document"
        d = message.document
        caption = message.caption or ""
        caption_entities = serialize_message_entities(getattr(message, "caption_entities", None))
        media = {"file_id": d.file_id, "file_unique_id": getattr(d, "file_unique_id", None), "width": 0, "height": 0, "duration": 0, "mime_type": getattr(d, "mime_type", None), "file_name": getattr(d, "file_name", None)}
    elif getattr(message, "text", None):
        kind = "text"
        text = message.text or ""
        entities = serialize_message_entities(getattr(message, "entities", None))
    else:
        raise ValueError("Неподдерживаемый тип рекламного поста")

    return {
        "schema_version": 1,
        "kind": kind,
        "text": text,
        "caption": caption,
        "entities": entities,
        "caption_entities": caption_entities,
        "media": media,
        "forward_origin": {"chat_id": str(message.chat.id) if getattr(message, "chat", None) else None, "message_id": getattr(message, "message_id", None)},
    }



def build_saved_post_media_item_from_aiogram_message(message) -> dict[str, Any]:
    if getattr(message, "photo", None):
        ph = message.photo[-1]
        return {"kind": "photo", "file_id": ph.file_id, "file_unique_id": getattr(ph, "file_unique_id", None), "width": getattr(ph, "width", 0), "height": getattr(ph, "height", 0), "duration": 0, "mime_type": None, "file_name": None}
    if getattr(message, "video", None):
        v = message.video
        return {"kind": "video", "file_id": v.file_id, "file_unique_id": getattr(v, "file_unique_id", None), "width": getattr(v, "width", 0), "height": getattr(v, "height", 0), "duration": getattr(v, "duration", 0), "mime_type": getattr(v, "mime_type", None), "file_name": getattr(v, "file_name", None)}
    if getattr(message, "document", None):
        d = message.document
        return {"kind": "document", "file_id": d.file_id, "file_unique_id": getattr(d, "file_unique_id", None), "width": 0, "height": 0, "duration": 0, "mime_type": getattr(d, "mime_type", None), "file_name": getattr(d, "file_name", None)}
    raise ValueError("Неподдерживаемый элемент альбома")


def build_saved_post_album_content_from_aiogram_messages(messages: list[Any]) -> dict[str, Any]:
    if not messages:
        raise ValueError("Список сообщений альбома пуст")
    ordered = sorted(messages, key=lambda m: int(getattr(m, "message_id", 0) or 0))
    media_group_id = getattr(ordered[0], "media_group_id", None)
    if not media_group_id:
        raise ValueError("У альбома отсутствует media_group_id")
    if any(getattr(m, "media_group_id", None) != media_group_id for m in ordered):
        raise ValueError("Сообщения из разных media_group_id")
    media_items = [build_saved_post_media_item_from_aiogram_message(m) for m in ordered]
    source_message_ids = [
        int(getattr(m, "message_id", 0) or 0)
        for m in ordered
        if getattr(m, "message_id", None)
    ]

    caption = ""
    caption_entities: list[dict[str, Any]] = []
    for msg in ordered:
        current_caption = str(getattr(msg, "caption", "") or "").strip()
        if current_caption:
            caption = getattr(msg, "caption", "") or ""
            caption_entities = serialize_message_entities(getattr(msg, "caption_entities", None))
            break

    first = ordered[0]
    return {
        "schema_version": 2,
        "kind": "album",
        "text": "",
        "caption": caption,
        "entities": [],
        "caption_entities": caption_entities,
        "media": None,
        "media_items": media_items,
        "forward_origin": {
            "chat_id": str(first.chat.id) if getattr(first, "chat", None) else None,
            "message_id": getattr(first, "message_id", None),
            "message_ids": source_message_ids,
        },
        "source_message_ids": source_message_ids,
        "media_group_id": str(media_group_id),
    }

def get_saved_post_preview_caption(content: dict[str, Any]) -> str:
    kind = str(content.get("kind") or "text")
    text = str(content.get("text") or "").strip()
    caption = str(content.get("caption") or "").strip()

    if kind == "text":
        if text:
            return text
        return "Рекламный текст пустой."

    if kind == "album":
        count = len(content.get('media_items') or [])
        if caption:
            return f"Альбом · {count} медиа: {caption}"
        return f"Альбом · {count} медиа без подписи."

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
    if kind == "album":
        return f"альбом · {len(content.get('media_items') or [])} медиа"
    mapping = {"text": "текст", "photo": "фото", "video": "видео", "animation": "анимация", "document": "документ"}
    return mapping.get(kind, kind)
