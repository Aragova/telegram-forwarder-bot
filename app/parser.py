from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("forwarder")

def detect_message_media_kind(message) -> str:
    if getattr(message, "video", None):
        return "video"

    if getattr(message, "photo", None):
        return "image"

    if getattr(message, "gif", None):
        return "video"

    media = getattr(message, "media", None)
    if not media:
        return "text"

    document = getattr(message, "document", None)
    if document and getattr(document, "mime_type", None):
        mime = (document.mime_type or "").lower()
        if mime.startswith("video/"):
            return "video"
        if mime.startswith("image/"):
            return "image"
        return "document"

    return "text"

def _normalize_entity_type(entity) -> str | None:
    class_name = entity.__class__.__name__

    mapping = {
        "MessageEntityBold": "bold",
        "MessageEntityItalic": "italic",
        "MessageEntityUnderline": "underline",
        "MessageEntityStrike": "strike",
        "MessageEntitySpoiler": "spoiler",
        "MessageEntityCode": "code",
        "MessageEntityPre": "pre",
        "MessageEntityTextUrl": "text_link",
        "MessageEntityUrl": "url",
        "MessageEntityMention": "mention",
        "MessageEntityMentionName": "text_mention",
        "InputMessageEntityMentionName": "text_mention",
        "MessageEntityEmail": "email",
        "MessageEntityPhone": "phone",
        "MessageEntityHashtag": "hashtag",
        "MessageEntityCashtag": "cashtag",
        "MessageEntityBotCommand": "bot_command",
        "MessageEntityBlockquote": "blockquote",
        "MessageEntityCustomEmoji": "custom_emoji",
    }

    normalized = mapping.get(class_name)
    if not normalized:
        logger.warning("parser_entity_unknown_type: unknown entity class=%s", class_name)

    return normalized

def _utf16_text_length(text: str) -> int:
    if not text:
        return 0
    return len(text.encode("utf-16-le")) // 2


def _is_valid_entity_range_utf16(text: str, offset: int, length: int) -> bool:
    if offset < 0 or length <= 0:
        return False

    utf16_len = _utf16_text_length(text)
    if offset > utf16_len:
        return False

    if offset + length > utf16_len:
        return False

    return True
def _serialize_message_entities(message) -> list[dict[str, Any]]:
    raw_entities = getattr(message, "entities", None) or []
    result: list[dict[str, Any]] = []

    if not raw_entities:
        return result

    text = getattr(message, "raw_text", None) or message.message or message.text or ""
    text_utf16_len = _utf16_text_length(text)
    skipped_count = 0

    for index, entity in enumerate(raw_entities, start=1):
        try:
            entity_type = _normalize_entity_type(entity)
            if not entity_type:
                skipped_count += 1
                continue

            offset = int(getattr(entity, "offset", 0) or 0)
            length = int(getattr(entity, "length", 0) or 0)

            if offset < 0 or length <= 0 or offset + length > text_utf16_len:
                logger.warning(
                    "parser_entity_invalid_range: type=%s offset=%s length=%s text_len=%s text_utf16_len=%s",
                    entity_type,
                    offset,
                    length,
                    len(text),
                    text_utf16_len,
                )
                skipped_count += 1
                continue

            item: dict[str, Any] = {
                "type": entity_type,
                "offset": offset,
                "length": length,
            }

            url = getattr(entity, "url", None)
            if url:
                item["url"] = str(url)

            language = getattr(entity, "language", None)
            if language:
                item["language"] = str(language)

            user_id = getattr(entity, "user_id", None)
            if user_id is not None:
                try:
                    item["user_id"] = int(user_id)
                except Exception:
                    item["user_id"] = str(user_id)

            document_id = getattr(entity, "document_id", None)
            if document_id is not None:
                try:
                    item["custom_emoji_id"] = str(int(document_id))
                except Exception:
                    item["custom_emoji_id"] = str(document_id)

            if entity_type == "custom_emoji" and not item.get("custom_emoji_id"):
                logger.warning(
                    "parser_custom_emoji_missing_id: offset=%s length=%s",
                    offset,
                    length,
                )

            result.append(item)

        except Exception as exc:
            skipped_count += 1
            logger.exception(
                "parser_entity_parse_failed: index=%s entity_class=%s error=%s",
                index,
                entity.__class__.__name__,
                exc,
            )

    logger.info(
        "parser_entities_serialized: total=%s saved=%s skipped=%s text_len=%s text_utf16_len=%s",
        len(raw_entities),
        len(result),
        skipped_count,
        len(text),
        text_utf16_len,
    )

    return result

def message_to_content_dict(message):
    text = getattr(message, "raw_text", None) or message.message or message.text or ""

    try:
        entities = _serialize_message_entities(message)
    except Exception as exc:
        logger.exception(
            "parser_message_entities_build_failed: message_id=%s error=%s",
            getattr(message, "id", None),
            exc,
        )
        entities = []

    content = {
        "text": text,
        "entities": entities,
        "has_media": bool(getattr(message, "media", None)),
        "media_kind": detect_message_media_kind(message),
        "date": message.date.isoformat() if getattr(message, "date", None) else None,
    }

    logger.info(
        "parser_message_content_built: message_id=%s media_kind=%s has_media=%s text_len=%s entities=%s",
        getattr(message, "id", None),
        content["media_kind"],
        content["has_media"],
        len(text),
        len(entities),
    )

    return content

async def parse_channel_history(client, db, channel_id: str, clean_start: bool = False):
    entity = int(channel_id) if str(channel_id).lstrip("-").isdigit() else channel_id
    channel = await client.get_entity(entity)

    if clean_start:
        deleted_count = db.delete_channel_posts(channel_id, None)
        logger.info(
            "parse_channel_history: clean_start enabled, source=%s, deleted_old_posts=%s",
            channel_id,
            deleted_count,
        )

    batch = []
    saved = 0
    total_seen = 0
    skipped_empty = 0
    messages_with_entities = 0
    messages_with_custom_emoji = 0
    content_build_errors = 0

    async for message in client.iter_messages(channel, limit=None, reverse=True, wait_time=1):
        total_seen += 1

        if not message.text and not message.media and not message.message:
            skipped_empty += 1
            continue

        try:
            content = message_to_content_dict(message)

            entities = content.get("entities") or []
            if entities:
                messages_with_entities += 1

            if any((entity.get("type") == "custom_emoji") for entity in entities):
                messages_with_custom_emoji += 1

            batch.append(
                (
                    int(message.id),
                    str(channel_id),
                    None,
                    content,
                    str(message.grouped_id) if getattr(message, "grouped_id", None) else None,
                )
            )

        except Exception as exc:
            content_build_errors += 1
            logger.exception(
                "parse_channel_history_message_failed: source=%s message_id=%s error=%s",
                channel_id,
                getattr(message, "id", None),
                exc,
            )
            continue

        if len(batch) >= 100:
            try:
                saved += db.save_post_batch(batch)
            except Exception as exc:
                logger.exception(
                    "parse_channel_history_batch_save_failed: source=%s batch_size=%s error=%s",
                    channel_id,
                    len(batch),
                    exc,
                )
                raise
            finally:
                batch = []

    if batch:
        try:
            saved += db.save_post_batch(batch)
        except Exception as exc:
            logger.exception(
                "parse_channel_history_final_batch_save_failed: source=%s batch_size=%s error=%s",
                channel_id,
                len(batch),
                exc,
            )
            raise

    logger.info(
        "История канала %s обработана | просмотрено=%s | сохранено=%s | пустых=%s | с_entities=%s | с_custom_emoji=%s | ошибок_контента=%s",
        channel_id,
        total_seen,
        saved,
        skipped_empty,
        messages_with_entities,
        messages_with_custom_emoji,
        content_build_errors,
    )

    return saved

async def parse_group_history(client, db, chat_id: str, thread_id: int, clean_start: bool = False):
    entity = int(chat_id) if str(chat_id).lstrip("-").isdigit() else chat_id
    chat = await client.get_entity(entity)

    if clean_start:
        deleted_count = db.delete_channel_posts(chat_id, thread_id)
        logger.info(
            "parse_group_history: clean_start enabled, source=%s:%s, deleted_old_posts=%s",
            chat_id,
            thread_id,
            deleted_count,
        )

    batch = []
    saved = 0
    total_seen = 0
    skipped_empty = 0
    messages_with_entities = 0
    messages_with_custom_emoji = 0
    content_build_errors = 0

    async for message in client.iter_messages(chat, limit=None, reverse=True, reply_to=thread_id, wait_time=1):
        total_seen += 1

        if not message.text and not message.media and not message.message:
            skipped_empty += 1
            continue

        try:
            content = message_to_content_dict(message)

            entities = content.get("entities") or []
            if entities:
                messages_with_entities += 1

            if any((entity.get("type") == "custom_emoji") for entity in entities):
                messages_with_custom_emoji += 1

            batch.append(
                (
                    int(message.id),
                    str(chat_id),
                    int(thread_id),
                    content,
                    str(message.grouped_id) if getattr(message, "grouped_id", None) else None,
                )
            )

        except Exception as exc:
            content_build_errors += 1
            logger.exception(
                "parse_group_history_message_failed: source=%s:%s message_id=%s error=%s",
                chat_id,
                thread_id,
                getattr(message, "id", None),
                exc,
            )
            continue

        if len(batch) >= 100:
            try:
                saved += db.save_post_batch(batch)
            except Exception as exc:
                logger.exception(
                    "parse_group_history_batch_save_failed: source=%s:%s batch_size=%s error=%s",
                    chat_id,
                    thread_id,
                    len(batch),
                    exc,
                )
                raise
            finally:
                batch = []

    if batch:
        try:
            saved += db.save_post_batch(batch)
        except Exception as exc:
            logger.exception(
                "parse_group_history_final_batch_save_failed: source=%s:%s batch_size=%s error=%s",
                chat_id,
                thread_id,
                len(batch),
                exc,
            )
            raise

    logger.info(
        "История темы %s:%s обработана | просмотрено=%s | сохранено=%s | пустых=%s | с_entities=%s | с_custom_emoji=%s | ошибок_контента=%s",
        chat_id,
        thread_id,
        total_seen,
        saved,
        skipped_empty,
        messages_with_entities,
        messages_with_custom_emoji,
        content_build_errors,
    )

    return saved
