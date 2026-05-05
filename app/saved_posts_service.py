from __future__ import annotations

import logging
import mimetypes
import tempfile
from typing import Any
from pathlib import Path

from aiogram.types import MessageEntity
from telethon.tl import types as tl_types

logger = logging.getLogger("forwarder")


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


def _guess_media_extension(kind: str, media: dict[str, Any]) -> str:
    if kind == "photo":
        return ".jpg"
    if kind == "video":
        return ".mp4"
    if kind == "animation":
        mime_type = str(media.get("mime_type") or "").lower()
        return ".gif" if "gif" in mime_type else ".mp4"
    if kind == "document":
        file_name = str(media.get("file_name") or "")
        suffix = Path(file_name).suffix
        if suffix:
            return suffix
        mime_type = str(media.get("mime_type") or "")
        guessed = mimetypes.guess_extension(mime_type)
        return guessed or ".bin"
    return ".bin"


async def download_saved_post_media_for_telethon(*, bot, content: dict[str, Any], temp_dir: str | Path) -> Path:
    media = content.get("media") or {}
    file_id = str(media.get("file_id") or "")
    if not file_id:
        raise ValueError("В saved post отсутствует media.file_id")
    kind = str(content.get("kind") or "document")
    ext = _guess_media_extension(kind, media)
    temp_dir_path = Path(temp_dir)
    temp_dir_path.mkdir(parents=True, exist_ok=True)
    file_info = await bot.get_file(file_id)
    local_path = temp_dir_path / f"saved_post_{file_id}{ext}"
    await bot.download_file(file_info.file_path, destination=local_path)
    return local_path


async def send_saved_post_content_via_telethon(
    *,
    bot,
    telethon_client,
    chat_id: int | str,
    content: dict[str, Any],
    temp_dir: str | Path,
) -> dict[str, Any]:
    kind = str(content.get("kind") or "text")
    logger.info("SAVED_POST_TELETHON_BUILDER_SEND_START | target_id=%s | kind=%s", chat_id, kind)
    local_path: Path | None = None
    try:
        if kind == "text":
            entities = saved_post_entities_to_telethon(content.get("entities"))
            sent = await telethon_client.send_message(entity=chat_id, message=content.get("text") or "", formatting_entities=entities)
        else:
            local_path = await download_saved_post_media_for_telethon(bot=bot, content=content, temp_dir=temp_dir)
            caption_entities = saved_post_entities_to_telethon(content.get("caption_entities"))
            sent = await telethon_client.send_file(
                entity=chat_id,
                file=str(local_path),
                caption=content.get("caption") or "",
                formatting_entities=caption_entities,
            )
        logger.info("SAVED_POST_TELETHON_BUILDER_SEND_DONE | target_id=%s | message_id=%s | method=telethon_builder", chat_id, sent.id)
        return {"ok": True, "message_id": sent.id, "chat_id": str(chat_id), "kind": kind, "method": "telethon_builder"}
    except Exception as exc:
        logger.warning("SAVED_POST_TELETHON_BUILDER_SEND_FAILED | target_id=%s | error=%s", chat_id, exc)
        raise
    finally:
        if local_path:
            try:
                local_path.unlink(missing_ok=True)
            except Exception:
                pass


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


async def send_saved_post_content(
    *,
    bot,
    chat_id: int | str,
    content: dict[str, Any],
    reply_markup=None,
) -> dict[str, Any]:
    kind = str(content.get("kind") or "text")

    if kind == "text":
        msg = await bot.send_message(
            chat_id=chat_id,
            text=content.get("text") or "",
            entities=deserialize_message_entities(content.get("entities")),
            reply_markup=reply_markup,
        )
    elif kind == "photo":
        media = content.get("media") or {}
        msg = await bot.send_photo(
            chat_id=chat_id,
            photo=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "video":
        media = content.get("media") or {}
        msg = await bot.send_video(
            chat_id=chat_id,
            video=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "animation":
        media = content.get("media") or {}
        msg = await bot.send_animation(
            chat_id=chat_id,
            animation=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    elif kind == "document":
        media = content.get("media") or {}
        msg = await bot.send_document(
            chat_id=chat_id,
            document=media["file_id"],
            caption=content.get("caption"),
            caption_entities=deserialize_message_entities(content.get("caption_entities")),
            reply_markup=reply_markup,
        )
    else:
        raise ValueError(f"Unsupported saved post kind: {kind}")

    return {
        "ok": True,
        "message_id": msg.message_id,
        "chat_id": str(chat_id),
        "kind": kind,
    }
