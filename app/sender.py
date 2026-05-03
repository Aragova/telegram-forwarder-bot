from __future__ import annotations
import asyncio
import html
import logging, mimetypes, random, re, time
import json
from typing import Any
from telethon.tl import types as tl_types
from pathlib import Path
from aiogram.methods import CopyMessages
from aiogram.types import MessageEntity
from aiogram.types import FSInputFile, InputMediaDocument, InputMediaPhoto, InputMediaVideo
from telethon import functions, types
from .config import settings
from .repository_models import utc_now_iso
from .telegram_client import ReactionClientInfo
from .video_processor import VideoProcessor
from .scheduler_service import SchedulerService
from .reaction_runtime_resolver import ReactionRuntimeResolver
from telethon.tl.types import (
    MessageEntityBold,
    MessageEntityItalic,
    MessageEntityUnderline,
    MessageEntityStrike,
    MessageEntityCode,
    MessageEntityPre,
    MessageEntityTextUrl,
    MessageEntityUrl,
    MessageEntityMention,
    MessageEntityCustomEmoji,
)

logger = logging.getLogger("forwarder")
MAX_INVALID_MP4_RETRY = 1
MAX_NORMAL_REACTION_ATTEMPTS = 3

async def run_db(callable_obj, *args, **kwargs):
    """
    Уводит sync DB-работу из event loop в thread pool.
    """
    return await asyncio.to_thread(callable_obj, *args, **kwargs)

REACTION_POOL = ["❤", "🔥", "🥰", "🤩", "😍", "⚡", "🍌", "🏆", "🍓", "💋", "💘", "🦄", "😘", "😎"]
NORMAL_REACTION_POOL = ["🔥", "❤", "🥰", "😍", "😘", "💋", "🍓", "⚡"]
DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK = False
DEBUG_FORCE_SKIP_COPY_SINGLE = False
DEBUG_FORCE_SKIP_COPY_ALBUM = False

def _telethon_entities_to_bot(entities):
    if not entities:
        return None

    result = []

    for e in entities:
        try:
            if isinstance(e, MessageEntityBold):
                result.append(MessageEntity(type="bold", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityItalic):
                result.append(MessageEntity(type="italic", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityUnderline):
                result.append(MessageEntity(type="underline", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityStrike):
                result.append(MessageEntity(type="strikethrough", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityCode):
                result.append(MessageEntity(type="code", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityPre):
                result.append(MessageEntity(type="pre", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityTextUrl):
                result.append(
                    MessageEntity(
                        type="text_link",
                        offset=e.offset,
                        length=e.length,
                        url=e.url,
                    )
                )

            elif isinstance(e, MessageEntityUrl):
                result.append(MessageEntity(type="url", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityMention):
                result.append(MessageEntity(type="mention", offset=e.offset, length=e.length))

            elif isinstance(e, MessageEntityCustomEmoji):
                result.append(
                    MessageEntity(
                        type="custom_emoji",
                        offset=e.offset,
                        length=e.length,
                        custom_emoji_id=str(e.document_id),
                    )
                )

        except Exception:
            continue

    return result or None

def _build_text_with_entities(message):
    text = message.text or message.message or ""
    entities = getattr(message, "entities", None)

    if not text:
        return None, None

    bot_entities = _telethon_entities_to_bot(entities)

    return text, bot_entities

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

def _format_bytes_ru(num_bytes: int | float | None) -> str:
    try:
        value = float(num_bytes or 0)
    except Exception:
        value = 0.0

    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def _format_speed_ru(bytes_per_sec: float | int | None) -> str:
    try:
        speed = float(bytes_per_sec or 0)
    except Exception:
        speed = 0.0
    return f"{_format_bytes_ru(speed)}/с"


def _format_eta_ru(seconds: float | int | None) -> str:
    try:
        sec = max(0, int(seconds or 0))
    except Exception:
        sec = 0

    minutes, seconds = divmod(sec, 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours} ч {minutes} мин {seconds} сек"
    if minutes > 0:
        return f"{minutes} мин {seconds} сек"
    return f"{seconds} сек"

def _normalize_source_text(text: str) -> str:
    if not text:
        return ""

    import re

    # 🔥 УБИРАЕМ МУСОР ОТ СКАНЕРА
    text = re.sub(r"\*{2,}", "*", text)
    text = re.sub(r"\[\*\*", "[", text)
    text = re.sub(r"\*\*\]", "]", text)
    text = re.sub(r"\*\*\(", "(", text)
    text = re.sub(r"\)\*\*", ")", text)

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Убираем самые частые битые markdown-конструкции
    text = text.replace("**[**", "[")
    text = text.replace("]**(", "](")
    text = text.replace("****", "")
    text = text.replace("***", "")
    text = re.sub(r"\*\*(\s*)\*\*", r"\1", text)

    # Схлопываем слишком длинные хвосты из пустых строк
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def _markdownish_to_html(text: str) -> str:
    if not text:
        return ""

    text = _normalize_source_text(text)

    placeholders: dict[str, str] = {}

    def _store(value: str) -> str:
        key = f"__HTML_PLACEHOLDER_{len(placeholders)}__"
        placeholders[key] = value
        return key

    # [**text**](url)
    text = re.sub(
        r"\[\*\*(.+?)\*\*\]\((https?://[^\s)]+)\)",
        lambda m: _store(f'<a href="{html.escape(m.group(2), quote=True)}"><b>{html.escape(m.group(1))}</b></a>'),
        text,
        flags=re.DOTALL,
    )

    # [text](url)
    text = re.sub(
        r"\[(.+?)\]\((https?://[^\s)]+)\)",
        lambda m: _store(f'<a href="{html.escape(m.group(2), quote=True)}">{html.escape(m.group(1))}</a>'),
        text,
        flags=re.DOTALL,
    )

    # **text**
    text = re.sub(
        r"\*\*(.+?)\*\*",
        lambda m: _store(f"<b>{html.escape(m.group(1))}</b>"),
        text,
        flags=re.DOTALL,
    )

    text = html.escape(text)

    for key, value in placeholders.items():
        text = text.replace(html.escape(key), value)

    return text.strip()


def _prepare_html_text(text: str | None) -> str | None:
    prepared = _markdownish_to_html(text or "")
    return prepared or None


def _normalize_reaction_emoji(value: str | None) -> str:
    return (value or "").replace("\ufe0f", "").strip()

def _detect_message_media_kind(message) -> str:
    """
    Возвращает:
    - "video"    если сообщение содержит видео
    - "image"    если сообщение содержит фото/изображение
    - "document" если есть файл, но это не image/video
    - "text"     если медиа нет
    """
    if not message:
        return "text"

    if getattr(message, "video", None):
        return "video"

    if getattr(message, "photo", None):
        return "image"

    media = getattr(message, "media", None)
    if not media:
        return "text"

    try:
        if isinstance(media, types.MessageMediaDocument):
            doc = media.document
            if doc and getattr(doc, "mime_type", None):
                mime = (doc.mime_type or "").lower()
                if mime.startswith("video/"):
                    return "video"
                if mime.startswith("image/"):
                    return "image"
            return "document"
    except Exception:
        pass

    return "text"

class SenderService:
    def __init__(self, bot, telethon_client, reaction_clients: list[ReactionClientInfo], db):
        self.bot = bot
        self.telethon = telethon_client
        self.reaction_clients = reaction_clients or []
        self.db = db
        self.scheduler_service = SchedulerService(self.db)

        self.video_processor = VideoProcessor(
            bot=self.bot,
            telethon_client=self.telethon,
        )

    def _extract_sent_message_id(self, sent_msg) -> int | None:
        ids = self._extract_sent_message_ids(sent_msg)
        return ids[0] if ids else None

    def _extract_sent_message_ids(self, sent_result) -> list[int]:
        def _extract_one(item) -> list[int]:
            if item is None:
                return []
            if isinstance(item, (list, tuple)):
                nested: list[int] = []
                for nested_item in item:
                    nested.extend(_extract_one(nested_item))
                return nested
            if isinstance(item, dict):
                values = []
                for key in ("message_id", "id"):
                    val = item.get(key)
                    if val is not None:
                        try:
                            values.append(int(val))
                        except Exception:
                            pass
                if values:
                    return values
                for nested_key in ("message", "result", "data"):
                    if nested_key in item:
                        nested_values = _extract_one(item.get(nested_key))
                        if nested_values:
                            return nested_values
                return values
            for attr in ("message_id", "id"):
                try:
                    val = getattr(item, attr, None)
                    if val is not None:
                        return [int(val)]
                except Exception:
                    continue
            for key in ("message", "result", "data"):
                nested_obj = getattr(item, key, None)
                if nested_obj is not None:
                    nested_values = _extract_one(nested_obj)
                    if nested_values:
                        return nested_values
            return []

        if sent_result is None:
            return []
        raw_items = list(sent_result) if isinstance(sent_result, (list, tuple)) else [sent_result]
        ids: list[int] = []
        for item in raw_items:
            ids.extend(_extract_one(item))
        return [x for x in ids if isinstance(x, int)]

    async def _validate_reaction_target_message(self, *, rule_id: int | None, source_channel: str, target_id: str, source_message_ids: list[int], sent_message_id: int | None, delivery_id: int | None = None, max_age_seconds: int = 300) -> int | None:
        logger.info("REACTION_TARGET_VALIDATE_START | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
        if sent_message_id is None or int(sent_message_id) <= 0:
            logger.warning("REACTION_SKIPPED_INVALID_TARGET_MESSAGE | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
            return None
        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        msg = await self.telethon.get_messages(entity, ids=int(sent_message_id))
        if not msg:
            logger.warning("REACTION_SKIPPED_TARGET_MESSAGE_NOT_FOUND | rule_id=%s | delivery_id=%s | target_id=%s | sent_message_id=%s", rule_id, delivery_id, target_id, sent_message_id)
            return None
        now_ts = int(time.time())
        msg_ts = int(getattr(msg, "date").timestamp()) if getattr(msg, "date", None) else 0
        age_seconds = now_ts - msg_ts if msg_ts else 10**9
        if age_seconds > int(max_age_seconds):
            logger.warning("REACTION_BLOCKED_STALE_SENT_MESSAGE_ID | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | message_date=%s | age_seconds=%s | max_age_seconds=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, getattr(msg, "date", None), age_seconds, max_age_seconds, source_message_ids)
            return None
        if str(source_channel) == str(target_id) and int(sent_message_id) in {int(x) for x in (source_message_ids or [])}:
            logger.warning("REACTION_BLOCKED_SOURCE_MESSAGE_ID | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | sent_message_id=%s | source_message_ids=%s", rule_id, delivery_id, source_channel, target_id, sent_message_id, source_message_ids)
            return None
        logger.info("REACTION_TARGET_VALIDATE_OK | rule_id=%s | delivery_id=%s | target_id=%s | sent_message_id=%s | message_date=%s | age_seconds=%s", rule_id, delivery_id, target_id, sent_message_id, getattr(msg, "date", None), age_seconds)
        return int(sent_message_id)


    async def _validate_sent_message_ids_for_delivery(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_START | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
        )

        valid_ids: list[int] = []
        for candidate_id in normalized_candidates:
            try:
                validated = await self._validate_reaction_target_message(
                    rule_id=rule_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=source_message_ids or [],
                    sent_message_id=candidate_id,
                    delivery_id=delivery_id,
                    max_age_seconds=max_age_seconds,
                )
            except Exception as exc:
                logger.warning(
                    "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_ITEM_FAILED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | sent_message_id=%s | error=%s",
                    rule_id,
                    delivery_id,
                    method,
                    target_id,
                    candidate_id,
                    exc,
                )
                continue
            if validated:
                valid_ids.append(int(validated))

        if valid_ids:
            logger.info(
                "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                valid_ids,
            )
            return valid_ids

        reason = "no_candidate_ids" if not normalized_candidates else "all_candidates_rejected"
        logger.warning(
            "DELIVERY_SENT_MESSAGE_IDS_VALIDATE_EMPTY | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | reason=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            normalized_candidates,
            reason,
        )
        return []

    async def _confirm_target_delivery_message_ids(
        self,
        *,
        rule_id: int | None,
        delivery_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        candidate_sent_message_ids: list[int],
        method: str,
        max_age_seconds: int = 300,
    ) -> list[int]:
        normalized_candidates: list[int] = []
        for value in candidate_sent_message_ids or []:
            try:
                normalized_candidates.append(int(value))
            except Exception:
                continue

        logger.info(
            "DELIVERY_TARGET_CONFIRM_START | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            source_channel,
            target_id,
            source_message_ids,
            normalized_candidates,
        )

        if not hasattr(self.telethon, "get_messages"):
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_SKIPPED_NO_GET_MESSAGES | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s",
                rule_id,
                delivery_id,
                method,
                target_id,
                normalized_candidates,
            )
            return normalized_candidates

        if not normalized_candidates:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "no_candidate_ids"
            )
            return []

        valid_ids = await self._validate_sent_message_ids_for_delivery(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=normalized_candidates,
            method=method,
            max_age_seconds=max_age_seconds,
        )

        if not valid_ids:
            logger.warning(
                "DELIVERY_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | method=%s | source_channel=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s | reason=%s",
                rule_id, delivery_id, method, source_channel, target_id, source_message_ids, normalized_candidates, "all_candidates_rejected"
            )
            return []

        logger.info(
            "DELIVERY_TARGET_CONFIRM_OK | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | valid_sent_message_ids=%s",
            rule_id,
            delivery_id,
            method,
            target_id,
            valid_ids,
        )
        return valid_ids

    async def _confirm_target_delivery_message_ids_with_retry(
        self,
        **kwargs,
    ) -> list[int]:
        attempts = (0.0, 0.7, 1.5)
        last_reason = "target_message_not_found_after_send"
        for attempt_no, delay_seconds in enumerate(attempts, start=1):
            if delay_seconds > 0:
                logger.info(
                    "DELIVERY_TARGET_CONFIRM_RETRY | rule_id=%s | delivery_id=%s | attempt=%s | delay=%s | candidate_sent_message_ids=%s | reason=%s",
                    kwargs.get("rule_id"), kwargs.get("delivery_id"), attempt_no, delay_seconds, kwargs.get("candidate_sent_message_ids"), last_reason
                )
                await asyncio.sleep(delay_seconds)
            valid_ids = await self._confirm_target_delivery_message_ids(**kwargs)
            if valid_ids:
                return valid_ids
        return []

    def _normalize_video_caption_entities(self, raw_entities) -> list[dict]:
        if not raw_entities:
            return []

        parsed = raw_entities

        try:
            # 1) строка -> пробуем обычный JSON
            if isinstance(parsed, str):
                raw_text = parsed.strip()
                if not raw_text:
                    return []

                try:
                    parsed = json.loads(raw_text)
                except Exception:
                    # 2) fallback: иногда в базе лежит python-подобная строка
                    import ast
                    try:
                        parsed = ast.literal_eval(raw_text)
                    except Exception:
                        logger.warning(
                            "VIDEO_CAPTION_MODE | не удалось распарсить caption entities | type=%s | preview=%r",
                            type(raw_entities),
                            raw_text[:300],
                        )
                        return []

            # 3) если после первого json.loads получили снова строку -> пробуем ещё раз
            if isinstance(parsed, str):
                parsed = parsed.strip()
                if not parsed:
                    return []
                try:
                    parsed = json.loads(parsed)
                except Exception:
                    logger.warning(
                        "VIDEO_CAPTION_MODE | caption entities остались строкой после повторного parse | preview=%r",
                        parsed[:300],
                    )
                    return []

            if isinstance(parsed, dict):
                parsed = [parsed]

            if not isinstance(parsed, list):
                logger.warning(
                    "VIDEO_CAPTION_MODE | caption entities не список после нормализации | type=%s",
                    type(parsed),
                )
                return []

            normalized: list[dict] = []

            for item in parsed:
                if not isinstance(item, dict):
                    continue

                entity_type = str(item.get("type") or "").strip().lower()
                offset = item.get("offset")
                length = item.get("length")

                try:
                    offset = int(offset)
                    length = int(length)
                except Exception:
                    continue

                if not entity_type or offset < 0 or length <= 0:
                    continue

                normalized_item = {
                    "type": entity_type,
                    "offset": offset,
                    "length": length,
                }

                if item.get("url"):
                    normalized_item["url"] = str(item.get("url"))
                if item.get("language"):
                    normalized_item["language"] = str(item.get("language"))
                if item.get("custom_emoji_id"):
                    normalized_item["custom_emoji_id"] = str(item.get("custom_emoji_id"))

                normalized.append(normalized_item)

            return normalized

        except Exception as exc:
            logger.warning(
                "VIDEO_CAPTION_MODE | normalize caption entities failed | error=%s | raw_type=%s",
                exc,
                type(raw_entities),
            )
            return []

    def _content_from_message_or_post(self, message=None, post_row=None) -> dict:
        def _row_value(row_obj, key: str, default=None):
            if row_obj is None:
                return default

            try:
                if isinstance(row_obj, dict):
                    return row_obj.get(key, default)
            except Exception:
                pass

            try:
                return row_obj[key]
            except Exception:
                pass

            try:
                return getattr(row_obj, key)
            except Exception:
                pass

            return default

        if post_row is not None:
            content = _row_value(post_row, "content_json")

            if isinstance(content, dict):
                return content

            if isinstance(content, str) and content.strip():
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    logger.warning(
                        "CONTENT_FROM_POST_ROW | не удалось распарсить content_json | type=%s",
                        type(post_row).__name__,
                    )

        if message is not None:
            text = (
                getattr(message, "raw_text", None)
                or getattr(message, "text", None)
                or getattr(message, "message", None)
                or ""
            )

            raw_entities = getattr(message, "entities", None) or []
            entities_payload: list[dict] = []
            text_utf16_len = _utf16_text_length(text)

            for entity in raw_entities:
                try:
                    offset = int(getattr(entity, "offset", 0) or 0)
                    length = int(getattr(entity, "length", 0) or 0)

                    if offset < 0 or length <= 0 or offset + length > text_utf16_len:
                        logger.warning(
                            "CONTENT_FROM_MESSAGE | skip invalid entity type=%s offset=%s length=%s text_utf16_len=%s",
                            entity.__class__.__name__,
                            offset,
                            length,
                            text_utf16_len,
                        )
                        continue

                    item = {
                        "offset": offset,
                        "length": length,
                    }

                    if isinstance(entity, types.MessageEntityBold):
                        item["type"] = "bold"
                    elif isinstance(entity, types.MessageEntityItalic):
                        item["type"] = "italic"
                    elif isinstance(entity, types.MessageEntityUnderline):
                        item["type"] = "underline"
                    elif isinstance(entity, types.MessageEntityStrike):
                        item["type"] = "strike"
                    elif isinstance(entity, types.MessageEntitySpoiler):
                        item["type"] = "spoiler"
                    elif isinstance(entity, types.MessageEntityCode):
                        item["type"] = "code"
                    elif isinstance(entity, types.MessageEntityPre):
                        item["type"] = "pre"
                        item["language"] = str(getattr(entity, "language", "") or "")
                    elif isinstance(entity, types.MessageEntityTextUrl):
                        item["type"] = "text_link"
                        item["url"] = str(getattr(entity, "url", "") or "")
                    elif isinstance(entity, types.MessageEntityUrl):
                        item["type"] = "url"
                    elif isinstance(entity, types.MessageEntityMention):
                        item["type"] = "mention"
                    elif isinstance(entity, types.MessageEntityEmail):
                        item["type"] = "email"
                    elif isinstance(entity, types.MessageEntityPhone):
                        item["type"] = "phone"
                    elif isinstance(entity, types.MessageEntityHashtag):
                        item["type"] = "hashtag"
                    elif isinstance(entity, types.MessageEntityCashtag):
                        item["type"] = "cashtag"
                    elif isinstance(entity, types.MessageEntityBotCommand):
                        item["type"] = "bot_command"
                    elif isinstance(entity, types.MessageEntityBlockquote):
                        item["type"] = "blockquote"
                    elif isinstance(entity, types.MessageEntityCustomEmoji):
                        item["type"] = "custom_emoji"
                        item["custom_emoji_id"] = str(int(getattr(entity, "document_id")))
                    else:
                        logger.warning(
                            "CONTENT_FROM_MESSAGE | unknown entity type=%s",
                            entity.__class__.__name__,
                        )
                        continue

                    entities_payload.append(item)

                except Exception as exc:
                    logger.warning(
                        "CONTENT_FROM_MESSAGE | failed to serialize entity=%r error=%s",
                        entity,
                        exc,
                    )

            logger.info(
                "CONTENT_FROM_MESSAGE | text_len=%s | text_utf16_len=%s | entities_in=%s | entities_out=%s",
                len(text),
                text_utf16_len,
                len(raw_entities),
                len(entities_payload),
            )

            return {
                "text": text,
                "entities": entities_payload,
                "has_media": bool(getattr(message, "media", None)),
                "media_kind": _detect_message_media_kind(message),
                "date": getattr(getattr(message, "date", None), "isoformat", lambda: None)(),
            }

        return {
            "text": "",
            "entities": [],
            "has_media": False,
            "media_kind": "text",
            "date": None,
        }

    def _video_caption_requires_premium(self, caption: str | None, caption_entities) -> bool:
        entities = self._normalize_video_caption_entities(caption_entities)

        for entity in entities:
            entity_type = str(entity.get("type") or "").strip().lower()
            if entity_type == "custom_emoji":
                return True

        return False

    def _build_video_caption_delivery_payload(self, rule) -> dict[str, Any]:
        caption = getattr(rule, "video_caption", None)
        raw_caption_entities = getattr(rule, "video_caption_entities_json", None)

        caption_text = caption or ""
        caption_entities = self._normalize_video_caption_entities(raw_caption_entities)
        caption_delivery_mode = self._get_rule_video_caption_delivery_mode(rule)

        requires_premium = self._video_caption_requires_premium(
            caption_text,
            caption_entities,
        )

        has_any_entities = bool(caption_entities)

        # SaaS-логика:
        # builder_first  -> всегда premium
        # copy_first     -> всегда plain
        # auto           -> premium, если есть ЛЮБЫЕ entities
        #                   (не только custom emoji), иначе plain
        if caption_delivery_mode == "builder_first":
            selected_mode = "premium"
        elif caption_delivery_mode == "copy_first":
            selected_mode = "plain"
        else:
            selected_mode = "premium" if has_any_entities else "plain"

        caption_entities_json = None
        if caption_entities:
            try:
                caption_entities_json = json.dumps(caption_entities, ensure_ascii=False)
            except Exception as exc:
                logger.warning(
                    "VIDEO_CAPTION_MODE | не удалось сериализовать caption entities в json | error=%s",
                    exc,
                )
                caption_entities_json = None

        if caption_entities_json and isinstance(caption_entities_json, str):
            try:
                json.loads(caption_entities_json)
            except Exception:
                logger.warning(
                    "VIDEO_CAPTION_MODE | caption_entities_json битый, сбрасываю в None"
                )
                caption_entities_json = None

        logger.info(
            "VIDEO_CAPTION_MODE | payload built | mode=%s | selected_mode=%s | has_caption=%s | entities=%s | requires_premium=%s",
            caption_delivery_mode,
            selected_mode,
            bool(caption_text),
            len(caption_entities),
            requires_premium,
        )

        return {
            "caption": caption_text,
            "caption_entities": caption_entities,
            "caption_entities_json": caption_entities_json,
            "caption_delivery_mode": caption_delivery_mode,
            "requires_premium": requires_premium,
            "has_any_entities": has_any_entities,
            "selected_mode": selected_mode,
        }

    def _build_telethon_entities_from_content(self, content: dict | None, text: str) -> list:
        if not content:
            return []

        raw_entities = content.get("entities") or []
        if not raw_entities:
            return []

        built: list = []
        text_utf16_len = _utf16_text_length(text or "")

        for item in raw_entities:
            try:
                entity_type = str(item.get("type") or "").strip()
                offset = int(item.get("offset", 0) or 0)
                length = int(item.get("length", 0) or 0)

                if offset < 0 or length <= 0 or offset + length > text_utf16_len:
                    logger.warning(
                        "ENTITY_FROM_CONTENT | skip invalid entity type=%s offset=%s length=%s text_utf16_len=%s",
                        entity_type,
                        offset,
                        length,
                        text_utf16_len,
                    )
                    continue

                if entity_type == "bold":
                    built.append(types.MessageEntityBold(offset=offset, length=length))
                elif entity_type == "italic":
                    built.append(types.MessageEntityItalic(offset=offset, length=length))
                elif entity_type == "underline":
                    built.append(types.MessageEntityUnderline(offset=offset, length=length))
                elif entity_type == "strike":
                    built.append(types.MessageEntityStrike(offset=offset, length=length))
                elif entity_type == "spoiler":
                    built.append(types.MessageEntitySpoiler(offset=offset, length=length))
                elif entity_type == "code":
                    built.append(types.MessageEntityCode(offset=offset, length=length))
                elif entity_type == "pre":
                    built.append(
                        types.MessageEntityPre(
                            offset=offset,
                            length=length,
                            language=str(item.get("language") or "")
                        )
                    )
                elif entity_type == "text_link":
                    url = str(item.get("url") or "").strip()
                    if url:
                        built.append(
                            types.MessageEntityTextUrl(
                                offset=offset,
                                length=length,
                                url=url,
                            )
                        )
                elif entity_type == "url":
                    built.append(types.MessageEntityUrl(offset=offset, length=length))
                elif entity_type == "mention":
                    built.append(types.MessageEntityMention(offset=offset, length=length))
                elif entity_type == "email":
                    built.append(types.MessageEntityEmail(offset=offset, length=length))
                elif entity_type == "phone":
                    built.append(types.MessageEntityPhone(offset=offset, length=length))
                elif entity_type == "hashtag":
                    built.append(types.MessageEntityHashtag(offset=offset, length=length))
                elif entity_type == "cashtag":
                    built.append(types.MessageEntityCashtag(offset=offset, length=length))
                elif entity_type == "bot_command":
                    built.append(types.MessageEntityBotCommand(offset=offset, length=length))
                elif entity_type == "blockquote":
                    built.append(types.MessageEntityBlockquote(offset=offset, length=length))
                elif entity_type == "custom_emoji":
                    custom_emoji_id = item.get("custom_emoji_id")
                    if custom_emoji_id:
                        built.append(
                            types.MessageEntityCustomEmoji(
                                offset=offset,
                                length=length,
                                document_id=int(custom_emoji_id),
                            )
                        )
                else:
                    logger.warning(
                        "ENTITY_FROM_CONTENT | unknown entity type=%s",
                        entity_type,
                    )

            except Exception as exc:
                logger.warning(
                    "ENTITY_FROM_CONTENT | failed to build entity=%r error=%s",
                    item,
                    exc,
                )

        logger.info(
            "ENTITY_FROM_CONTENT | total=%s | built=%s | text_len=%s | text_utf16_len=%s",
            len(raw_entities),
            len(built),
            len(text or ""),
            text_utf16_len,
        )

        return built

    def _build_text_and_entities_from_content(self, content: dict | None) -> tuple[str, list]:
        content = content or {}
        text = str(content.get("text") or "")
        entities = self._build_telethon_entities_from_content(content, text)
        return text, entities

    def _serialize_pipeline_verify_result(self, verify_result: dict | None) -> dict:
        payload = dict(verify_result or {})
        return {
            "ok": bool(payload.get("ok")),
            "error_text": payload.get("error_text"),
            "grouped_id": payload.get("grouped_id"),
            "count": payload.get("count"),
            "first_message_id": payload.get("first_message_id"),
        }

    def _schedule_video_event_log(
        self,
        *,
        event_type: str,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        loop.create_task(
            run_db(
                self._log_video_event_sync,
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status=status,
                error_text=error_text,
                extra=extra,
            )
        )

    def _get_post_row_for_rule_message_sync(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        return self._get_post_row_for_rule_message(rule, source_channel, message_id)

    def _get_rule_intro_items_sync(self, rule):
        return self._get_rule_intro_items(rule)

    def _resolve_repost_caption_delivery_strategy_sync(
        self,
        *,
        rule,
        source_channel: str,
        message_ids: list[int],
        is_album: bool,
    ) -> dict[str, Any]:
        return self._resolve_repost_caption_delivery_strategy(
            rule=rule,
            source_channel=source_channel,
            message_ids=message_ids,
            is_album=is_album,
        )

    def _mark_delivery_sent_sync(self, delivery_id: int, *, sent_message_id: int | None = None, sent_message_ids: list[int] | None = None, target_id: str | None = None, delivery_method: str | None = None) -> None:
        if hasattr(self.db, "mark_delivery_sent_with_target_message"):
            self.db.mark_delivery_sent_with_target_message(delivery_id, sent_message_id=sent_message_id, sent_message_ids=sent_message_ids, target_id=target_id, delivery_method=delivery_method)
            return
        self.db.mark_delivery_sent(delivery_id)

    def _mark_many_deliveries_sent_sync(self, delivery_ids: list[int]) -> None:
        self.db.mark_many_deliveries_sent(delivery_ids)

    def _mark_delivery_faulty_sync(self, delivery_id: int, error_text: str) -> None:
        self.db.mark_delivery_faulty(delivery_id, error_text)

    def _get_post_id_by_delivery_sync(self, delivery_id: int) -> int | None:
        return self.db.get_post_id_by_delivery(delivery_id)

    def _log_video_event_sync(
        self,
        *,
        event_type: str,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        self.db.log_video_event(
            event_type=event_type,
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status=status,
            error_text=error_text,
            extra=extra,
        )

    def _finalize_video_failure_sync(
        self,
        *,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        source_message_id: int,
        error_text: str,
        fallback_mode: str | None = None,
        caption_delivery_mode: str | None = None,
        selected_mode: str | None = None,
        caption_requires_premium: bool | None = None,
    ) -> None:
        extra = {
            "source_channel": source_channel,
            "target_id": target_id,
            "target_thread_id": target_thread_id,
            "source_message_id": source_message_id,
        }

        if fallback_mode is not None:
            extra["fallback_mode"] = fallback_mode
        if caption_delivery_mode is not None:
            extra["caption_delivery_mode"] = caption_delivery_mode
        if selected_mode is not None:
            extra["selected_mode"] = selected_mode
        if caption_requires_premium is not None:
            extra["caption_requires_premium"] = caption_requires_premium

        self.db.log_video_event(
            event_type="video_processing_failed",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="faulty",
            error_text=error_text,
            extra=extra,
        )
        self.db.mark_delivery_faulty(delivery_id, error_text)

    def _finalize_video_success_sync(
        self,
        *,
        delivery_id: int,
        rule_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        source_message_id: int,
        sent_message_id: int | None,
        fallback_mode: str,
        caption_delivery_mode: str,
        selected_mode: str,
        caption_requires_premium: bool,
    ) -> None:
        self.db.log_video_event(
            event_type="video_processing_completed",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="sent",
            extra={
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "source_message_id": source_message_id,
                "sent_message_id": sent_message_id,
                "candidate_sent_message_ids": candidate_sent_message_ids,
                "valid_sent_message_ids": valid_sent_message_ids,
                "fallback_mode": fallback_mode,
                "caption_delivery_mode": caption_delivery_mode,
                "selected_mode": selected_mode,
                "caption_requires_premium": caption_requires_premium,
            },
        )
        self.db.mark_delivery_sent(delivery_id)

    def _log_delivery_pipeline_step_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        event_type: str,
        pipeline_stage: str,
        pipeline_result: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        """
        Единый лог промежуточного pipeline-шага.

        ВАЖНО:
        - не помечает доставку как faulty
        - не является финальной ошибкой
        - нужен для прозрачной диагностики
        """
        base_extra = {
            "pipeline_stage": pipeline_stage,
            "pipeline_result": pipeline_result,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="processing",
                error_text=error_text,
                extra=base_extra,
            )

        item_kind = "АЛЬБОМ" if len(source_message_ids) > 1 else "ОДИНОЧНЫЙ"
        log_line = (
            f"ПРАВИЛО {rule_id} | {item_kind} | ШАГ {pipeline_stage} → "
            f"{pipeline_result.upper()}"
        )
        if error_text:
            logger.warning("%s | %s", log_line, error_text)
        else:
            logger.info("%s", log_line)

    async def _log_delivery_pipeline_step(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        event_type: str,
        pipeline_stage: str,
        pipeline_result: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        await run_db(
            self._log_delivery_pipeline_step_sync,
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            event_type=event_type,
            pipeline_stage=pipeline_stage,
            pipeline_result=pipeline_result,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            extra=extra,
        )

    def _log_delivery_final_success_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        reaction_message_id: int | None = None,
        verify_result: dict | None = None,
        extra: dict | None = None,
    ) -> None:
        """
        Единый финальный лог успешной доставки.
        """
        verify_payload = self._serialize_pipeline_verify_result(verify_result)

        base_extra = {
            "final_method": final_method,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
            "sent_message_id": sent_message_id,
            "sent_message_ids": sent_message_ids or ([sent_message_id] if sent_message_id is not None else []),
            "first_sent_message_id": sent_message_id,
            "reaction_message_id": reaction_message_id if reaction_message_id is not None else sent_message_id,
            "verify_ok": verify_payload.get("ok"),
            "verify_grouped_id": verify_payload.get("grouped_id"),
            "verify_count": verify_payload.get("count"),
            "verify_first_message_id": verify_payload.get("first_message_id"),
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type="delivery_sent",
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="sent",
                extra=base_extra,
            )

        logger.info(
            "ПРАВИЛО %s | ДОСТАВКА | ИТОГ → УСПЕХ (method=%s, source=%s, target=%s, count=%s)",
            rule_id,
            final_method,
            source_channel,
            target_id,
            len(source_message_ids),
        )

    async def _log_delivery_final_success(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        sent_message_id: int | None = None,
        sent_message_ids: list[int] | None = None,
        reaction_message_id: int | None = None,
        verify_result: dict | None = None,
        extra: dict | None = None,
    ) -> None:
        await run_db(
            self._log_delivery_final_success_sync,
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            sent_message_id=sent_message_id,
            sent_message_ids=sent_message_ids,
            reaction_message_id=reaction_message_id,
            verify_result=verify_result,
            extra=extra,
        )

    def _log_delivery_final_failure_sync(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str,
        attempts_debug: list[dict] | None = None,
        extra: dict | None = None,
    ) -> None:
        """
        Единый финальный лог неуспешной доставки.

        ВАЖНО:
        - только тут пишем delivery_failed / faulty
        - все промежуточные шаги не считаются финальными ошибками
        """
        base_extra = {
            "final_method": final_method,
            "source_channel": source_channel,
            "target_id": target_id,
            "source_message_ids": source_message_ids,
            "attempts": attempts_debug or [],
        }
        if extra:
            base_extra.update(extra)

        for delivery_id in delivery_ids:
            post_id = self._get_post_id_by_delivery(delivery_id)

            self.db.log_delivery_event(
                event_type="delivery_failed",
                delivery_id=delivery_id,
                rule_id=rule_id,
                post_id=post_id,
                status="faulty",
                error_text=error_text,
                extra=base_extra,
            )

            self.db.mark_delivery_faulty(delivery_id, error_text)

        logger.error(
            "ПРАВИЛО %s | ДОСТАВКА | ИТОГ → ОШИБКА (method=%s, source=%s, target=%s, count=%s) | %s",
            rule_id,
            final_method,
            source_channel,
            target_id,
            len(source_message_ids),
            error_text,
        )

    async def _log_delivery_final_failure(
        self,
        *,
        rule_id: int,
        delivery_ids: list[int],
        final_method: str,
        source_channel: str,
        target_id: str,
        source_message_ids: list[int],
        error_text: str,
        attempts_debug: list[dict] | None = None,
        extra: dict | None = None,
    ) -> None:
        await run_db(
            self._log_delivery_final_failure_sync,
            rule_id=rule_id,
            delivery_ids=delivery_ids,
            final_method=final_method,
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=error_text,
            attempts_debug=attempts_debug,
            extra=extra,
        )

    def _stage_name_ru(self, stage: str | None) -> str:
        mapping = {
            "pipeline": "общий процесс",
            "download": "скачивание",
            "probe": "анализ видео",
            "trim": "обрезка",
            "normalize": "нормализация",
            "intro": "подготовка заставки",
            "concat": "склейка",
            "thumbnail": "создание превью",
            "send": "отправка",
        }
        return mapping.get(stage or "", stage or "неизвестный этап")

    def _log_human_video_event(
        self,
        *,
        event_type: str,
        status: str | None = None,
        error_text: str | None = None,
        extra: dict | None = None,
    ) -> None:
        payload = dict(extra or {})
        stage = payload.get("stage")
        stage_name = self._stage_name_ru(stage)

        if event_type == "video_stage_started":
            logger.info("▶️ Начат этап: %s", stage_name)
            return

        if event_type == "video_stage_completed":
            if stage == "download":
                file_size_mb = payload.get("file_size_mb")
                if file_size_mb is not None:
                    logger.info("✅ Скачивание завершено: %.1f МБ", float(file_size_mb))
                else:
                    logger.info("✅ Завершён этап: %s", stage_name)
            else:
                logger.info("✅ Завершён этап: %s", stage_name)
            return

        if event_type == "video_stage_failed":
            if error_text:
                logger.error("❌ Ошибка на этапе «%s»: %s", stage_name, error_text)
            else:
                logger.error("❌ Ошибка на этапе «%s»", stage_name)
            return

        if event_type == "video_ffmpeg_progress":
            operation = payload.get("operation")
            percent = payload.get("percent")
            processed_sec = payload.get("processed_sec")
            total_sec = payload.get("total_sec")
            speed = payload.get("speed")

            parts = []
            if operation:
                parts.append(str(operation))
            elif stage_name:
                parts.append(stage_name.capitalize())

            if percent is not None:
                parts.append(f"{float(percent):.1f}%")
            if processed_sec is not None and total_sec is not None:
                parts.append(f"{float(processed_sec):.1f} / {float(total_sec):.1f} сек")
            if speed:
                parts.append(f"скорость {speed}")

            logger.info("🎬 %s", " | ".join(parts))
            return

        if event_type == "video_send_retry":
            attempt = payload.get("attempt")
            max_retries = payload.get("max_retries")
            if attempt is not None and max_retries is not None:
                logger.warning("🔁 Повторная попытка отправки: %s из %s", attempt, max_retries)
            elif attempt is not None:
                logger.warning("🔁 Повторная попытка отправки: %s", attempt)
            else:
                logger.warning("🔁 Повторная попытка отправки")
            return

    def _get_rule_intro_items(self, rule):
        horizontal_intro = None
        vertical_intro = None

        horizontal_id = getattr(rule, "video_intro_horizontal_id", None)
        vertical_id = getattr(rule, "video_intro_vertical_id", None)

        try:
            if horizontal_id:
                horizontal_intro = self.db.get_intro_by_id(int(horizontal_id))
        except Exception:
            horizontal_intro = None

        try:
            if vertical_id:
                vertical_intro = self.db.get_intro_by_id(int(vertical_id))
        except Exception:
            vertical_intro = None

        return horizontal_intro, vertical_intro

    def _is_self_loop_rule(self, rule) -> bool:
        return (
            str(rule.source_id) == str(rule.target_id)
            and rule.source_thread_id == rule.target_thread_id
        )

    def _get_post_id_by_delivery(self, delivery_id: int) -> int | None:
        return self.db.get_post_id_by_delivery(delivery_id)

    def _handle_process_rule_exception_sync(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        post_id: int | None,
        message_id: int,
        source_channel: str,
        target_id: str,
        target_thread_id: int | None,
        media_group_id: str | None,
        schedule_mode: str,
        interval: int,
        error_text: str,
    ) -> None:
        self.db.log_delivery_event(
            event_type="delivery_process_exception",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="faulty",
            error_text=error_text,
            extra={
                "message_id": message_id,
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "media_group_id": media_group_id,
                "schedule_mode": schedule_mode,
            },
        )

        self.db.mark_delivery_faulty(delivery_id, error_text)

        if schedule_mode == "fixed":
            self.scheduler_service.touch_after_send(rule_id, interval)

    def _touch_rule_after_send_sync(self, rule_id: int, interval: int) -> None:
        self.scheduler_service.touch_after_send(rule_id, interval)

    def _prepare_album_delivery_sync(
        self,
        rule_id: int,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str,
    ) -> dict[str, Any]:
        if self.db.is_album_already_sent(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        ):
            album_rows = self.db.get_album_pending_for_rule(
                rule_id,
                source_channel,
                source_thread_id,
                media_group_id,
            )

            if album_rows:
                self.db.mark_many_deliveries_sent(
                    [int(r["delivery_id"]) for r in album_rows]
                )

            return {
                "already_sent": True,
                "album_rows": album_rows,
            }

        album_rows = self.db.get_album_pending_for_rule(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        )

        if self.db.is_album_already_sent(
            rule_id,
            source_channel,
            source_thread_id,
            media_group_id,
        ):
            if album_rows:
                self.db.mark_many_deliveries_sent(
                    [int(r["delivery_id"]) for r in album_rows]
                )

            return {
                "already_sent": True,
                "album_rows": album_rows,
            }

        return {
            "already_sent": False,
            "album_rows": album_rows,
        }

    def _take_due_delivery_sync(self, rule_id: int, schedule_mode: str) -> dict[str, Any] | None:
        due = self.db.take_due_delivery(rule_id, utc_now_iso())
        if not due:
            return None

        delivery_id = int(due["delivery_id"])
        post_id = self.db.get_post_id_by_delivery(delivery_id)

        self.db.log_delivery_event(
            event_type="delivery_started",
            delivery_id=delivery_id,
            rule_id=rule_id,
            post_id=post_id,
            status="processing",
            extra={
                "message_id": int(due["message_id"]),
                "source_channel": str(due["source_channel"]),
                "target_id": str(due["target_id"]),
                "target_thread_id": due["target_thread_id"],
                "media_group_id": str(due["media_group_id"]) if due["media_group_id"] is not None else None,
                "schedule_mode": schedule_mode,
            },
        )

        return {
            "due": due,
            "post_id": post_id,
        }

    def _clone_telethon_entities(self, entities, text: str | None = None) -> list:
        if not entities:
            return []

        text_value = text or ""
        text_utf16_len = _utf16_text_length(text_value)
        cloned: list = []

        for entity in entities:
            try:
                offset = int(getattr(entity, "offset", 0) or 0)
                length = int(getattr(entity, "length", 0) or 0)

                if length <= 0:
                    logger.warning(
                        "ENTITY_SANITIZE | skipped zero-length entity type=%s offset=%s length=%s",
                        entity.__class__.__name__,
                        offset,
                        length,
                    )
                    continue

                if text_value and (offset < 0 or offset + length > text_utf16_len):
                    logger.warning(
                        "ENTITY_SANITIZE | skipped invalid entity type=%s offset=%s length=%s text_len=%s text_utf16_len=%s",
                        entity.__class__.__name__,
                        offset,
                        length,
                        len(text_value),
                        text_utf16_len,
                    )
                    continue

                entity_dict = entity.to_dict()
                entity_dict.pop("_", None)
                cloned.append(type(entity)(**entity_dict))

            except Exception:
                logger.exception("ENTITY_SANITIZE | clone failed for entity=%r", entity)

        logger.info(
            "ENTITY_SANITIZE | total=%s | kept=%s | text_len=%s | text_utf16_len=%s",
            len(entities or []),
            len(cloned),
            len(text_value),
            text_utf16_len,
        )

        return cloned

    async def _send_text_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        text: str,
        entities,
    ) -> int | None:
        try:
            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            formatting_entities = self._clone_telethon_entities(entities, text)

            logger.info(
                "TELETHON_TEXT_SEND | START | target=%s | thread=%s | text_len=%s | entities_in=%s | entities_out=%s",
                target_id,
                target_thread_id,
                len(text or ""),
                len(entities or []),
                len(formatting_entities or []),
            )

            send_kwargs = {
                "entity": entity,
                "message": text or "",
                "formatting_entities": formatting_entities or None,
                "link_preview": False,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_message(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_TEXT_SEND | OK | target=%s | thread=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                sent_id,
            )
            return sent_id

        except Exception as exc:
            logger.warning(
                "TELETHON_TEXT_SEND | FAILED | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )
            return None

    async def _send_file_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        message,
        file_path: Path | None = None,
        force_document: bool = False,
        post_row: dict | None = None,
    ) -> int | None:
        content = self._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self._build_text_and_entities_from_content(content)
        formatting_entities = self._clone_telethon_entities(raw_entities, raw_text)

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        media_kind = _detect_message_media_kind(message)
        supports_streaming = media_kind == "video"

        try:
            logger.info(
                "TELETHON_FILE_SEND | START_ORIGINAL_MEDIA | target=%s | thread=%s | media_kind=%s | caption_len=%s | entities_in=%s | entities_out=%s | supports_streaming=%s",
                target_id,
                target_thread_id,
                media_kind,
                len(raw_text or ""),
                len(raw_entities or []),
                len(formatting_entities or []),
                supports_streaming,
            )

            send_kwargs = {
                "entity": entity,
                "file": getattr(message, "media", None),
                "caption": raw_text or "",
                "formatting_entities": formatting_entities or None,
                "force_document": force_document,
                "link_preview": False,
                "supports_streaming": supports_streaming,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_file(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_FILE_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                sent_id,
            )
            return sent_id

        except Exception as exc:
            logger.warning(
                "TELETHON_FILE_SEND | FAILED_ORIGINAL_MEDIA | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )

        if not file_path:
            logger.warning(
                "TELETHON_FILE_SEND | NO_FILE_PATH_FALLBACK | target=%s | thread=%s",
                target_id,
                target_thread_id,
            )
            return None

        try:
            logger.info(
                "TELETHON_FILE_SEND | START_FILE_PATH | target=%s | thread=%s | file=%s | media_kind=%s | caption_len=%s | entities_in=%s | entities_out=%s | supports_streaming=%s",
                target_id,
                target_thread_id,
                file_path.name,
                media_kind,
                len(raw_text or ""),
                len(raw_entities or []),
                len(formatting_entities or []),
                supports_streaming,
            )

            send_kwargs = {
                "entity": entity,
                "file": str(file_path),
                "caption": raw_text or "",
                "formatting_entities": formatting_entities or None,
                "force_document": force_document,
                "link_preview": False,
                "supports_streaming": supports_streaming,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_file(**send_kwargs)
            sent_id = int(sent.id) if sent else None

            logger.info(
                "TELETHON_FILE_SEND | OK_FILE_PATH | target=%s | thread=%s | file=%s | sent_message_id=%s",
                target_id,
                target_thread_id,
                file_path.name,
                sent_id,
            )
            return sent_id

        except Exception as exc:
            logger.warning(
                "TELETHON_FILE_SEND | FAILED_FILE_PATH | target=%s | thread=%s | file=%s | error=%s",
                target_id,
                target_thread_id,
                file_path.name if file_path else None,
                exc,
            )
            return None

    async def _send_album_via_telethon(
        self,
        *,
        messages,
        target_id,
        target_thread_id,
        post_rows: list[dict] | None = None,
    ) -> dict:
        downloaded_paths: list[Path] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для Telethon album send",
                }

            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id

            caption_text = ""
            caption_entities = None
            media_items = []

            for idx, message in enumerate(messages):
                media = getattr(message, "media", None)
                if not media:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": 0,
                        "error_text": "Один из элементов альбома не содержит media",
                    }
                media_items.append(media)

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self._clone_telethon_entities(caption_entities, caption_text)

            logger.info(
                "TELETHON_ALBUM_SEND | START_ORIGINAL_MEDIA | target=%s | thread=%s | items=%s | caption_len=%s | entities_in=%s | entities_out=%s",
                target_id,
                target_thread_id,
                len(media_items),
                len(caption_text or ""),
                len(caption_entities or []),
                len(formatting_entities or []),
            )

            send_kwargs = {
                "entity": entity,
                "file": media_items,
                "caption": caption_text or "",
                "formatting_entities": formatting_entities or None,
                "link_preview": False,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_file(**send_kwargs)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if sent_messages:
                first_id = int(sent_messages[0].id)
                logger.info(
                    "TELETHON_ALBUM_SEND | OK_ORIGINAL_MEDIA | target=%s | thread=%s | sent_count=%s | sent_message_ids=%s | first_message_id=%s",
                    target_id,
                    target_thread_id,
                    len(sent_messages),
                    [int(m.id) for m in sent_messages if m],
                    first_id,
                )
                return {
                    "ok": True,
                    "sent_message_id": first_id,
                    "sent_message_ids": [int(m.id) for m in sent_messages if m],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            logger.warning(
                "TELETHON_ALBUM_SEND | EMPTY_ORIGINAL_MEDIA | target=%s | thread=%s",
                target_id,
                target_thread_id,
            )

        except Exception as exc:
            logger.warning(
                "TELETHON_ALBUM_SEND | FAILED_ORIGINAL_MEDIA | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )

        try:
            files: list[str] = []
            caption_text = ""
            caption_entities = None

            for idx, message in enumerate(messages):
                file_path = await self.telethon.download_media(
                    message,
                    file=str(settings.media_cache_path),
                )
                if not file_path:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": len(files),
                        "error_text": f"Не удалось скачать элемент альбома {idx + 1}/{len(messages)}",
                    }

                path = Path(file_path)
                downloaded_paths.append(path)
                files.append(str(path))

                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self._clone_telethon_entities(caption_entities, caption_text)

            logger.info(
                "TELETHON_ALBUM_SEND | START_FILE_PATH | target=%s | thread=%s | items=%s | caption_len=%s | entities_in=%s | entities_out=%s",
                target_id,
                target_thread_id,
                len(files),
                len(caption_text or ""),
                len(caption_entities or []),
                len(formatting_entities or []),
            )

            send_kwargs = {
                "entity": entity,
                "file": files,
                "caption": caption_text or "",
                "formatting_entities": formatting_entities or None,
                "link_preview": False,
            }

            if target_thread_id is not None:
                send_kwargs["comment_to"] = int(target_thread_id)

            sent = await self.telethon.send_file(**send_kwargs)
            sent_messages = sent if isinstance(sent, list) else [sent]

            if not sent_messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Telethon send_file(album) вернул пустой результат",
                }

            first_id = int(sent_messages[0].id)
            logger.info(
                "TELETHON_ALBUM_SEND | OK_FILE_PATH | target=%s | thread=%s | sent_count=%s | sent_message_ids=%s | first_message_id=%s",
                target_id,
                target_thread_id,
                len(sent_messages),
                [int(m.id) for m in sent_messages if m],
                first_id,
            )
            return {
                "ok": True,
                "sent_message_id": first_id,
                "sent_message_ids": [int(m.id) for m in sent_messages if m],
                "sent_count": len(sent_messages),
                "error_text": None,
            }

        except Exception as exc:
            logger.exception(
                "TELETHON_ALBUM_SEND | FAILED_FILE_PATH | target=%s | thread=%s | error=%s",
                target_id,
                target_thread_id,
                exc,
            )
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass

    def _build_video_stage_logger(
        self,
        *,
        rule,
        delivery_id: int,
        post_id: int | None,
        source_channel: str,
        target_id: str,
        source_message_id: int,
    ):
        def stage_logger(
            event_type: str,
            status: str | None = None,
            error_text: str | None = None,
            extra: dict | None = None,
        ):
            payload = dict(extra or {})
            payload.setdefault("source_channel", source_channel)
            payload.setdefault("target_id", target_id)
            payload.setdefault("source_message_id", source_message_id)

            self._schedule_video_event_log(
                event_type=event_type,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                status=status,
                error_text=error_text,
                extra=payload,
            )

            self._log_human_video_event(
                event_type=event_type,
                status=status,
                error_text=error_text,
                extra=payload,
            )

        return stage_logger

    def _get_album_primary_text(self, messages, post_rows: list[dict] | None = None) -> str | None:
        for idx, message in enumerate(messages):
            post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
            content = self._content_from_message_or_post(message=message, post_row=post_row)
            raw_text, _raw_entities = self._build_text_and_entities_from_content(content)

            text_value = (raw_text or "").strip()
            if text_value:
                return text_value

        return None

    def _get_rule_video_caption_delivery_mode(self, rule) -> str:
        mode = str(getattr(rule, "video_caption_delivery_mode", "auto") or "auto").strip().lower()
        if mode not in ("copy_first", "builder_first", "auto"):
            return "auto"
        return mode

    def _resolve_repost_caption_delivery_strategy(
        self,
        *,
        rule,
        source_channel: str,
        message_ids: list[int],
        is_album: bool,
    ) -> dict[str, Any]:
        """
        Единый SaaS-резолвер режима подписи для REPOST-ветки.

        Возвращает:
        - configured_mode: что выставлено в правиле
        - requires_builder: требует ли контент builder/reupload
        - selected_path: какой путь реально запускать
            * copy_first
            * builder_first
        """
        configured_mode = self._get_rule_caption_delivery_mode(rule)

        if is_album:
            requires_builder = self._album_requires_builder(rule, source_channel, message_ids)
        else:
            first_message_id = int(message_ids[0]) if message_ids else 0
            requires_builder = self._single_requires_builder(rule, source_channel, first_message_id)

        if configured_mode == "builder_first":
            selected_path = "builder_first"
        elif configured_mode == "copy_first":
            selected_path = "copy_first"
        else:
            selected_path = "builder_first" if requires_builder else "copy_first"

        return {
            "configured_mode": configured_mode,
            "requires_builder": requires_builder,
            "selected_path": selected_path,
            "use_copy_first": selected_path == "copy_first",
        }

    def _get_rule_caption_delivery_mode(self, rule) -> str:
        """
        Режим подписи для обычного репоста.

        Использует ТОЛЬКО поле caption_delivery_mode.
        Не смешивать с video_caption_delivery_mode.
        """
        mode = str(getattr(rule, "caption_delivery_mode", "auto") or "auto").strip().lower()
        if mode not in ("copy_first", "builder_first", "auto"):
            return "auto"
        return mode

    def _content_requires_builder(self, content: dict | None) -> bool:
        """
        Для репоста считаем, что builder нужен,
        если в контенте есть ЛЮБЫЕ entities.

        Это делает auto-режим полностью одинаковым
        с video-веткой:
        - нет entities -> copy_first
        - есть entities -> builder_first
        """
        content = content or {}
        entities = content.get("entities") or []

        for entity in entities:
            try:
                entity_type = str(entity.get("type") or "").strip().lower()
                if entity_type:
                    return True
            except Exception:
                continue

        return False

    def _get_post_row_for_rule_message(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        try:
            return self.db.get_post(
                source_channel,
                getattr(rule, "source_thread_id", None),
                int(message_id),
            )
        except Exception as exc:
            logger.warning(
                "POST_ROW_LOOKUP | failed | source=%s | thread=%s | message_id=%s | error=%s",
                source_channel,
                getattr(rule, "source_thread_id", None),
                message_id,
                exc,
            )
            return None

    def _single_requires_builder(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> bool | dict[str, object]:
        post_row = self._get_post_row_for_rule_message(rule, source_channel, message_id)
        if not post_row:
            return False

        content = self._content_from_message_or_post(message=None, post_row=post_row)
        needs_builder = self._content_requires_builder(content)

        logger.info(
            "CAPTION_MODE_DETECT | single | rule_id=%s | message_id=%s | requires_builder=%s",
            getattr(rule, "id", None),
            message_id,
            needs_builder,
        )
        return needs_builder

    def _album_requires_builder(
        self,
        rule,
        source_channel: str,
        message_ids: list[int],
    ) -> bool:
        """
        Для альбома смотрим все элементы и особенно caption-элемент.
        Если хотя бы где-то есть custom_emoji -> builder required.
        """
        for message_id in message_ids:
            post_row = self._get_post_row_for_rule_message(rule, source_channel, int(message_id))
            if not post_row:
                continue

            content = self._content_from_message_or_post(message=None, post_row=post_row)
            if self._content_requires_builder(content):
                logger.info(
                    "CAPTION_MODE_DETECT | album | rule_id=%s | message_id=%s | requires_builder=True",
                    getattr(rule, "id", None),
                    message_id,
                )
                return True

        logger.info(
            "CAPTION_MODE_DETECT | album | rule_id=%s | requires_builder=False | items=%s",
            getattr(rule, "id", None),
            len(message_ids),
        )
        return False

    async def _fetch_album_messages(self, source_channel, message_ids):
        messages = []

        for mid in message_ids:
            msg = await self._fetch_message(source_channel, mid)
            if not msg:
                break
            messages.append(msg)

        return messages

    async def _verify_album_delivery(
        self,
        *,
        target_id,
        expected_count: int,
        sent_message_ids: list[int] | None,
        target_thread_id: int | None = None,
        target_grouped_id: int | None = None,
    ):
        try:
            if not sent_message_ids:
                return {
                    "ok": False,
                    "error_text": "reupload_album_sent_ids_missing",
                    "grouped_id": None,
                    "count": 0,
                    "first_message_id": None,
                    "sent_message_ids": [],
                }

            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            fetched = await self.telethon.get_messages(entity, ids=sent_message_ids)
            fetched_list = fetched if isinstance(fetched, list) else [fetched]
            fetched_list = [m for m in fetched_list if m]
            actual_ids = sorted(int(m.id) for m in fetched_list)

            if len(actual_ids) != len(sent_message_ids):
                return {
                    "ok": False,
                    "error_text": "verify_album_sent_ids_not_found",
                    "grouped_id": None,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids) if actual_ids else None,
                    "sent_message_ids": actual_ids,
                }

            grouped_ids = {int(m.grouped_id) for m in fetched_list if getattr(m, "grouped_id", None)}
            if expected_count > 1 and len(grouped_ids) > 1:
                return {
                    "ok": False,
                    "error_text": "verify_album_grouped_id_mismatch",
                    "grouped_id": None,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            grouped_id = next(iter(grouped_ids)) if grouped_ids else None
            if target_grouped_id and grouped_id and int(target_grouped_id) != int(grouped_id):
                return {
                    "ok": False,
                    "error_text": "verify_album_target_grouped_id_mismatch",
                    "grouped_id": grouped_id,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            if len(actual_ids) != expected_count:
                return {
                    "ok": False,
                    "error_text": "verify_album_count_mismatch",
                    "grouped_id": grouped_id,
                    "count": len(actual_ids),
                    "first_message_id": min(actual_ids),
                    "sent_message_ids": actual_ids,
                }

            return {
                "ok": True,
                "error_text": None,
                "grouped_id": grouped_id,
                "count": len(actual_ids),
                "first_message_id": min(actual_ids),
                "sent_message_ids": actual_ids,
            }

        except Exception as exc:
            logger.exception("verify_album_delivery: ошибка verify: %s", exc)
            return {
                "ok": False,
                "error_text": f"Ошибка verify: {exc}",
                "grouped_id": None,
                "count": 0,
                "first_message_id": None,
                "sent_message_ids": [],
            }

    async def _try_add_normal_reaction(self, client, entity, sent_message_id, session_name: str, rule_id: int | None = None) -> bool:
        emojis_to_try = NORMAL_REACTION_POOL[:]
        random.shuffle(emojis_to_try)
        emojis_to_try = emojis_to_try[:MAX_NORMAL_REACTION_ATTEMPTS]

        last_error = None

        for emoji in emojis_to_try:
            try:
                await client(
                    functions.messages.SendReactionRequest(
                        peer=entity,
                        msg_id=sent_message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji)],
                        big=False,
                        add_to_recent=False,
                    )
                )

                confirmed = await self._confirm_reaction(client, entity, sent_message_id, emoji)
                if confirmed:
                    logger.info("REACTION_VISIBLE_CONFIRMED | rule_id=%s | target_id=%s | message_id=%s | session=%s | reaction=%s", rule_id, entity, sent_message_id, session_name, emoji)
                    return True
                logger.warning("REACTION_NOT_VISIBLE_AFTER_SEND | rule_id=%s | target_id=%s | message_id=%s | session=%s | reaction=%s", rule_id, entity, sent_message_id, session_name, emoji)

            except Exception as exc:
                last_error = exc
                exc_text = str(exc).lower()
                if "floodwait" in exc.__class__.__name__.lower() or "flood wait" in exc_text:
                    logger.warning(
                        "NORMAL_REACTION_STOP_ON_FLOOD_WAIT | session=%s | message_id=%s | target_id=%s | error=%s",
                        session_name,
                        sent_message_id,
                        entity,
                        exc,
                    )
                    break
                logger.warning(
                    "Обычный реактор %s не смог поставить реакцию %s на сообщение %s в %s: %s",
                    session_name,
                    emoji,
                    sent_message_id,
                    entity,
                    exc,
                )

        logger.warning(
            "Обычный реактор %s не смог поставить ни одну реакцию на сообщение %s в %s. Последняя ошибка: %s",
            session_name,
            sent_message_id,
            entity,
            last_error,
        )
        return False

    async def _try_add_premium_reactions(self, client, entity, sent_message_id, session_name: str, fixed_reactions: list[str], rule_id: int | None = None) -> bool:
        cleaned = []
        for emoji in fixed_reactions:
            emoji = (emoji or "").strip()
            if emoji and emoji not in cleaned:
                cleaned.append(emoji)

        if not cleaned:
            logger.warning(
                "Premium-реактор %s не имеет закреплённого набора реакций",
                session_name,
            )
            return False

        # Пробуем сперва полный набор, потом 2, потом 1.
        # Fallback на следующий вариант только при исключении RPC.
        variants = []
        if len(cleaned) >= 3:
            variants.append(cleaned[:3])
        if len(cleaned) >= 2:
            variants.append(cleaned[:2])
        variants.append(cleaned[:1])

        last_error = None

        for variant in variants:
            try:
                await client(
                    functions.messages.SendReactionRequest(
                        peer=entity,
                        msg_id=sent_message_id,
                        reaction=[types.ReactionEmoji(emoticon=emoji) for emoji in variant],
                        big=False,
                        add_to_recent=False,
                    )
                )
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Premium-реактор %s не смог поставить реакции %s на сообщение %s в %s: %s",
                    session_name,
                    variant,
                    sent_message_id,
                    entity,
                    exc,
                )
                continue

            confirmed, visible_reactions = await self._confirm_reaction_set(client, entity, sent_message_id, variant)
            if confirmed:
                logger.info(
                    "PREMIUM_REACTION_VISIBLE_CONFIRMED | rule_id=%s | target_id=%s | message_id=%s | session=%s | requested_reactions=%s | visible_reactions=%s",
                    rule_id,
                    entity,
                    sent_message_id,
                    session_name,
                    variant,
                    visible_reactions,
                )
            else:
                logger.warning(
                    "PREMIUM_REACTION_NOT_VISIBLE_AFTER_SEND | rule_id=%s | target_id=%s | message_id=%s | session=%s | requested_reactions=%s | visible_reactions=%s",
                    rule_id,
                    entity,
                    sent_message_id,
                    session_name,
                    variant,
                    visible_reactions,
                )
            return bool(confirmed)

        logger.warning(
            "Premium-реактор %s не смог поставить ни один вариант реакций на сообщение %s в %s. Последняя ошибка: %s",
            session_name,
            sent_message_id,
            entity,
            last_error,
        )
        return False

    async def _confirm_reaction(self, client, entity, message_id: int, emoji: str) -> bool:
        try:
            normalized_expected = _normalize_reaction_emoji(emoji)
            msg = await client.get_messages(entity, ids=message_id)
            reactions = getattr(msg, "reactions", None)
            if not reactions:
                return False
            for result in getattr(reactions, "results", []) or []:
                reaction = getattr(result, "reaction", None)
                actual = _normalize_reaction_emoji(getattr(reaction, "emoticon", None))
                if actual == normalized_expected:
                    return True
            return False
        except Exception:
            return False

    async def _confirm_reaction_set(self, client, entity, message_id: int, emojis: list[str]) -> tuple[bool, list[str]]:
        try:
            msg = await client.get_messages(entity, ids=message_id)
            reactions = getattr(msg, "reactions", None)
            if not reactions:
                logger.warning(
                    "CONFIRM_REACTION_SET_DEBUG | target_id=%s | message_id=%s | expected=%s | observed=%s",
                    entity,
                    message_id,
                    emojis,
                    {"reactions": None},
                )
                return False
            actual_emojis: set[str] = set()
            observed_results: list[dict[str, Any]] = []
            for result in getattr(reactions, "results", []) or []:
                reaction = getattr(result, "reaction", None)
                emoticon = getattr(reaction, "emoticon", None)
                observed_results.append(
                    {
                        "result_class": result.__class__.__name__,
                        "reaction_class": reaction.__class__.__name__ if reaction else None,
                        "emoticon": emoticon,
                        "document_id": getattr(reaction, "document_id", None) if reaction else None,
                        "count": getattr(result, "count", None),
                    }
                )
                normalized_emoticon = _normalize_reaction_emoji(emoticon)
                if normalized_emoticon:
                    actual_emojis.add(normalized_emoticon)
            observed = {
                "reactions_class": reactions.__class__.__name__,
                "results": observed_results,
                "recent_reactions_len": len(getattr(reactions, "recent_reactions", []) or []),
            }
            expected = {
                _normalize_reaction_emoji(emoji)
                for emoji in (emojis or [])
                if _normalize_reaction_emoji(emoji)
            }
            visible = sorted(actual_emojis)
            confirmed = bool(expected.intersection(actual_emojis))
            if not confirmed:
                logger.warning(
                    "CONFIRM_REACTION_SET_DEBUG | target_id=%s | message_id=%s | expected=%s | observed=%s",
                    entity,
                    message_id,
                    emojis,
                    observed,
                )
            return confirmed, visible
        except Exception:
            return False, []

    async def _select_reaction_message_id(self, target_id, sent_message_ids: list[int] | None) -> tuple[int | None, str]:
        ids = sorted({int(x) for x in (sent_message_ids or []) if x is not None})
        if not ids:
            return None, "missing_sent_message_ids"

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        try:
            fetched = await self.telethon.get_messages(entity, ids=ids)
            fetched_list = fetched if isinstance(fetched, list) else [fetched]
            fetched_by_id = {int(m.id): m for m in fetched_list if m}
        except Exception:
            fetched_by_id = {}

        for mid in ids:
            msg = fetched_by_id.get(mid)
            if not msg:
                continue
            text_value = (
                getattr(msg, "raw_text", None)
                or getattr(msg, "text", None)
                or getattr(msg, "message", None)
                or ""
            ).strip()
            if text_value:
                return mid, "caption_message"

        return ids[0], "first_album_message"

    async def _add_reaction_if_possible(self, target_id, sent_message_id, rule_id: int | None = None):
        if not self.reaction_clients:
            return

        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        premium_reactors: list[ReactionClientInfo] = []
        normal_reactors: list[ReactionClientInfo] = []

        for reactor in self.reaction_clients:
            if reactor.is_premium and reactor.fixed_reactions:
                premium_reactors.append(reactor)
            else:
                normal_reactors.append(reactor)

        premium_accepted = False

        for reactor in premium_reactors:
            try:
                premium_result = await self._try_add_premium_reactions(
                    client=reactor.client,
                    entity=entity,
                    sent_message_id=sent_message_id,
                    session_name=reactor.session_name,
                    rule_id=rule_id,
                    fixed_reactions=reactor.fixed_reactions,
                )
                if premium_result:
                    premium_accepted = True

            except Exception as exc:
                logger.warning(
                    "Реактор %s упал на сообщении %s в %s: %s",
                    reactor.session_name,
                    sent_message_id,
                    target_id,
                    exc,
                )

        if premium_accepted:
            logger.info(
                "REACTION_NORMAL_CONTINUE_AFTER_PREMIUM | rule_id=%s | target_id=%s | message_id=%s | normal_count=%s",
                rule_id,
                entity,
                sent_message_id,
                len(normal_reactors),
            )

        for reactor in normal_reactors:
            try:
                await self._try_add_normal_reaction(
                    client=reactor.client,
                    entity=entity,
                    sent_message_id=sent_message_id,
                    session_name=reactor.session_name,
                    rule_id=rule_id,
                )
            except Exception as exc:
                logger.warning(
                    "Реактор %s упал на сообщении %s в %s: %s",
                    reactor.session_name,
                    sent_message_id,
                    target_id,
                    exc,
                )

    async def _add_reaction_for_rule_if_possible(
        self,
        *,
        rule,
        target_id,
        sent_message_id,
        source_channel: str = "",
        source_message_ids: list[int] | None = None,
        delivery_id: int | None = None,
        max_age_seconds: int = 300,
    ) -> None:
        rule_id = int(getattr(rule, "id", 0) or 0)
        if str(source_channel) and str(source_channel) == str(target_id):
            logger.info("SELF_TARGET_REPOST_DETECTED | rule_id=%s | source_id=%s | target_id=%s", rule_id, source_channel, target_id)
        validated_id = await self._validate_reaction_target_message(
            rule_id=rule_id,
            source_channel=str(source_channel or ""),
            target_id=str(target_id),
            source_message_ids=source_message_ids or [],
            sent_message_id=sent_message_id,
            delivery_id=delivery_id,
            max_age_seconds=max_age_seconds,
        )
        if not validated_id:
            return
        sent_message_id = validated_id

        resolver = ReactionRuntimeResolver(self.db)

        try:
            plan = resolver.resolve_for_rule(rule)
        except Exception as exc:
            logger.warning(
                "REACTION_RUNTIME_RESOLVE_FAILED | rule_id=%s | error_type=%s",
                rule_id,
                exc.__class__.__name__,
            )
            await self._add_reaction_if_possible(target_id, sent_message_id, rule_id=rule_id)
            return

        if plan.use_legacy_reactors:
            logger.info(
                "REACTION_RUNTIME_SELECTED | rule_id=%s | mode=legacy_admin | reason=%s | legacy_clients=%s",
                rule_id,
                plan.reason,
                len(self.reaction_clients or []),
            )
            await self._add_reaction_if_possible(target_id, sent_message_id, rule_id=rule_id)
            return

        if plan.mode == "disabled":
            logger.info(
                "REACTION_RUNTIME_SKIPPED | rule_id=%s | mode=disabled | reason=%s",
                rule_id,
                plan.reason,
            )
            return

        if plan.mode == "no_accounts":
            logger.info(
                "REACTION_RUNTIME_SKIPPED | rule_id=%s | mode=no_accounts | tenant_id=%s | reason=%s",
                rule_id,
                plan.tenant_id,
                plan.reason,
            )
            return

        if plan.use_tenant_reactors:
            logger.info(
                "REACTION_RUNTIME_SELECTED | rule_id=%s | mode=tenant_saas | tenant_id=%s | accounts=%s",
                rule_id,
                plan.tenant_id,
                len(plan.tenant_accounts),
            )
            account_ids = [int(a["id"]) for a in (plan.tenant_accounts or []) if a.get("id") is not None]
            try:
                job_id = await run_db(
                    self.db.enqueue_reaction_job,
                    tenant_id=int(plan.tenant_id or 0),
                    rule_id=rule_id,
                    target_id=target_id,
                    message_id=int(sent_message_id),
                    account_ids=account_ids,
                    max_attempts=3,
                )
                logger.info(
                    "REACTION_JOB_ENQUEUED | tenant_id=%s | rule_id=%s | job_id=%s | target_id=%s | message_id=%s | accounts=%s",
                    plan.tenant_id,
                    rule_id,
                    job_id,
                    target_id,
                    sent_message_id,
                    len(account_ids),
                )
            except Exception:
                logger.exception("REACTION_JOB_ENQUEUE_FAILED | tenant_id=%s | rule_id=%s", plan.tenant_id, rule_id)
            return


    async def process_rule_once(self, rule):
        schedule_mode = getattr(rule, "schedule_mode", "interval") or "interval"

        taken = await run_db(self._take_due_delivery_sync, rule.id, schedule_mode)
        if not taken:
            return False

        due = taken["due"]
        post_id = taken["post_id"]

        delivery_id = int(due["delivery_id"])
        source_channel = str(due["source_channel"])
        message_id = int(due["message_id"])
        media_group_id = due["media_group_id"]
        target_id = str(due["target_id"])
        target_thread_id = due["target_thread_id"]
        interval = int(due["interval"])

        try:
            rule_mode = getattr(rule, "mode", "repost") or "repost"

            # VIDEO-РЕЖИМ:
            # Даже если сообщение пришло из альбома, обрабатываем его как отдельный пост.
            if rule_mode == "video":
                ok = await self._deliver_single_video(
                    rule,
                    delivery_id,
                    message_id,
                    source_channel,
                    target_id,
                    target_thread_id,
                )

                if ok or schedule_mode == "fixed":
                    await run_db(self._touch_rule_after_send_sync, rule.id, interval)

                return ok

            # REPOST-РЕЖИМ:
            # Старая стабильная логика альбомов сохраняется как есть.

            if media_group_id:
                prepared_album = await run_db(
                    self._prepare_album_delivery_sync,
                    rule.id,
                    source_channel,
                    due["source_thread_id"],
                    str(media_group_id),
                )

                album_rows = prepared_album["album_rows"]

                if prepared_album["already_sent"]:
                    logger.warning(
                        "⛔ Альбом media_group_id=%s уже был отправлен по правилу %s, пропускаю повторную доставку",
                        media_group_id,
                        rule.id,
                    )
                    return True

                ok = await self._deliver_album(
                    rule,
                    album_rows,
                    source_channel,
                    target_id,
                    target_thread_id,
                )

                if ok or schedule_mode == "fixed":
                    await run_db(self._touch_rule_after_send_sync, rule.id, interval)

                return ok

            # Обычный одиночный репост
            ok = await self._deliver_single(
                rule,
                delivery_id,
                message_id,
                source_channel,
                target_id,
                target_thread_id,
            )

            if ok or schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, rule.id, interval)

            return ok

        except Exception as exc:
            logger.exception("Ошибка доставки rule=%s delivery=%s", rule.id, delivery_id)

            await run_db(
                self._handle_process_rule_exception_sync,
                rule_id=rule.id,
                delivery_id=delivery_id,
                post_id=post_id,
                message_id=message_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                media_group_id=str(media_group_id) if media_group_id is not None else None,
                schedule_mode=schedule_mode,
                interval=interval,
                error_text=str(exc),
            )
            return False

    async def execute_repost_single_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        source_thread_id: int | None = None,
        target_id: str,
        target_thread_id: int | None = None,
        mode: str = "repost",
        interval: int = 0,
        schedule_mode: str = "interval",
        media_group_id: str | None = None,
        job_type: str | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "repost")
        logger.info(
            "JOB EXECUTOR | repost_single | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        try:
            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для repost_single")
            ok = await self._deliver_single(
                rule,
                int(delivery_id),
                int(message_id),
                str(source_channel),
                str(target_id),
                target_thread_id,
            )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if ok:
                logger.info(
                    "JOB EXECUTOR | repost_single | success | rule_id=%s | delivery_id=%s",
                    rule_id,
                    delivery_id,
                )
            else:
                delivery_row = await run_db(self.db.get_delivery, int(delivery_id))
                uncertain_error = "copy_single_uncertain_no_fallback"
                if (
                    isinstance(delivery_row, dict)
                    and str(delivery_row.get("status") or "") == "faulty"
                    and uncertain_error in str(delivery_row.get("error_text") or "")
                ):
                    logger.warning(
                        "JOB EXECUTOR | repost_single | non_retryable_uncertain | rule_id=%s | delivery_id=%s",
                        rule_id,
                        delivery_id,
                    )
                    return {"ok": False, "retryable": False, "error_text": str(delivery_row.get("error_text") or uncertain_error)}
                logger.warning(
                    "JOB EXECUTOR | repost_single | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат",
                    rule_id,
                    delivery_id,
                )
            return bool(ok)
        except Exception as exc:
            logger.warning(
                "JOB EXECUTOR | repost_single | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | repost_single | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_repost_album_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int | None = None,
        message_id: int | None = None,
        source_channel: str,
        source_thread_id: int | None,
        media_group_id: str | None = None,
        target_id: str,
        target_thread_id: int | None,
        mode: str = "repost",
        interval: int = 0,
        schedule_mode: str = "interval",
        job_type: str | None = None,
        delivery_ids: list[int] | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "repost")
        logger.info(
            "JOB EXECUTOR | repost_album | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        try:
            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для repost_album")
            if not media_group_id:
                raise RuntimeError("Не указан media_group_id для repost_album")

            album_rows = await run_db(
                self.db.get_processing_album_for_rule,
                int(rule_id),
                str(source_channel),
                source_thread_id,
                str(media_group_id),
            )
            if not album_rows:
                raise RuntimeError(f"Не найден processing-альбом media_group_id={media_group_id}")

            ok = await self._deliver_album(
                rule,
                album_rows,
                str(source_channel),
                str(target_id),
                target_thread_id,
            )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if ok:
                logger.info(
                    "JOB EXECUTOR | repost_album | success | rule_id=%s | delivery_id=%s",
                    rule_id,
                    delivery_id,
                )
            else:
                logger.warning(
                    "JOB EXECUTOR | repost_album | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат",
                    rule_id,
                    delivery_id,
                )
            return bool(ok)
        except Exception as exc:
            logger.warning(
                "JOB EXECUTOR | repost_album | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | repost_album | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_video_delivery_from_job(
        self,
        *,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        source_thread_id: int | None = None,
        target_id: str,
        target_thread_id: int | None = None,
        mode: str = "video",
        interval: int = 0,
        schedule_mode: str = "interval",
        media_group_id: str | None = None,
        job_type: str | None = None,
    ) -> bool:
        normalized_schedule_mode = str(schedule_mode or "interval")
        normalized_mode = str(mode or "video")
        logger.info(
            "JOB EXECUTOR | video_delivery | start | rule_id=%s | delivery_id=%s | message_id=%s | mode=%s | schedule_mode=%s",
            rule_id,
            delivery_id,
            message_id,
            normalized_mode,
            normalized_schedule_mode,
        )
        try:
            rule = await run_db(self.db.get_rule, int(rule_id))
            if not rule:
                raise RuntimeError(f"Правило #{rule_id} не найдено для video_delivery")
            ok = await self._deliver_single_video(
                rule,
                int(delivery_id),
                int(message_id),
                str(source_channel),
                str(target_id),
                target_thread_id,
            )
            if ok or normalized_schedule_mode == "fixed":
                await run_db(self._touch_rule_after_send_sync, int(rule_id), int(interval))
            if ok:
                logger.info(
                    "JOB EXECUTOR | video_delivery | success | rule_id=%s | delivery_id=%s",
                    rule_id,
                    delivery_id,
                )
            else:
                logger.warning(
                    "JOB EXECUTOR | video_delivery | failed | rule_id=%s | delivery_id=%s | error=исполнитель вернул неуспешный результат",
                    rule_id,
                    delivery_id,
                )
            return bool(ok)
        except Exception as exc:
            logger.warning(
                "JOB EXECUTOR | video_delivery | failed | rule_id=%s | delivery_id=%s | error=%s",
                rule_id,
                delivery_id,
                exc,
            )
            logger.exception(
                "JOB EXECUTOR | video_delivery | ошибка выполнения rule_id=%s delivery_id=%s",
                rule_id,
                delivery_id,
            )
            raise

    async def execute_video_download_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        message_id: int,
        source_channel: str,
        target_id: str,
        invalid_file_attempts: int | None = None,
        **_: object,
    ) -> dict:
        logger.info(
            "VIDEO DOWNLOAD START | delivery_id=%s | rule_id=%s | job_id=%s | stage=download",
            delivery_id,
            rule_id,
            job_id,
        )
        message = await self._fetch_message(source_channel, message_id)
        if not message:
            logger.warning("VIDEO STAGE FAILED | не удалось получить сообщение для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        path = await self._download_video_source(
            message,
            delivery_id=int(delivery_id),
            rule_id=int(rule_id),
            source_channel=str(source_channel),
            target_id=str(target_id),
            source_message_id=int(message_id),
        )
        if not path:
            logger.warning("VIDEO STAGE FAILED | не удалось скачать видео для delivery_id=%s", delivery_id)
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": True,
                "error_text": "Не удалось скачать видео",
            }
        probe_ok, probe_error = await self._validate_mp4_file_for_pipeline(
            Path(path),
            delivery_id=int(delivery_id),
            job_id=job_id,
            stage="download",
        )
        if not probe_ok:
            previous_invalid_attempts = max(0, int(invalid_file_attempts or 0))
            current_job_attempt = max(1, int(job_attempt or 1))
            validation_attempt = previous_invalid_attempts + current_job_attempt
            final_failed = validation_attempt > MAX_INVALID_MP4_RETRY
            compact_error = (probe_error or "битый MP4").replace("битый MP4: ", "", 1)
            current_size = Path(path).stat().st_size if Path(path).is_file() else 0
            if final_failed:
                logger.warning(
                    "VIDEO FILE VALIDATION FINAL FAILED | stage=download | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=delivery_faulty_and_job_failed | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            else:
                logger.warning(
                    "VIDEO FILE VALIDATION FAILED | stage=download | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=удалён_файл_и_retry | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            if final_failed:
                error_text = f"Битый MP4 после повторной загрузки: {compact_error}"
            else:
                error_text = f"Битый MP4: {compact_error}, повторная загрузка 1/{MAX_INVALID_MP4_RETRY}"
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": not final_failed,
                "error_text": error_text,
                "invalid_source_file": True,
                "final_invalid_source_file": final_failed,
                "ffprobe_stderr": compact_error,
                "validation_attempt": validation_attempt,
                "max_validation_attempts": MAX_INVALID_MP4_RETRY,
                "path": str(path),
                "size": current_size,
            }
        downloaded_size = Path(path).stat().st_size if Path(path).is_file() else 0
        video_info = await self.video_processor.get_video_info(str(path), use_cache=False)
        logger.info(
            "VIDEO DOWNLOAD DONE | скачивание завершено для delivery_id=%s | path=%s | size=%s | duration=%s",
            delivery_id,
            path,
            downloaded_size,
            round(float(video_info.get("duration") or 0.0), 2) if isinstance(video_info, dict) else None,
        )
        return {"ok": True, "source_video_path": str(path), "fallback_to_legacy": False}

    async def execute_video_process_from_job(
        self,
        *,
        job_id: int | None = None,
        job_attempt: int | None = None,
        rule_id: int,
        delivery_id: int,
        source_video_path: str | None = None,
        artifact_version: int | None = None,
        invalid_file_attempts: int | None = None,
        **_: object,
    ) -> dict:
        logger.info(
            "VIDEO PROCESS START | delivery_id=%s | rule_id=%s | job_id=%s | stage=process",
            delivery_id,
            rule_id,
            job_id,
        )
        if int(artifact_version or 1) != 1:
            logger.warning("VIDEO FALLBACK TO LEGACY | неподдерживаемая artifact_version=%s | delivery_id=%s", artifact_version, delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not source_video_path:
            logger.warning("VIDEO STAGE FAILED | отсутствует video_file_path для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False, "error_text": "Отсутствует путь к исходному файлу"}
        if not Path(source_video_path).is_file():
            logger.warning("VIDEO STAGE FAILED | исходный файл не найден для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False, "error_text": "Исходный файл не найден"}
        source_path = Path(source_video_path)
        probe_ok, probe_error = await self._validate_mp4_file_for_pipeline(
            source_path,
            delivery_id=int(delivery_id),
            job_id=job_id,
            stage="process",
        )
        if not probe_ok:
            previous_invalid_attempts = max(0, int(invalid_file_attempts or 0))
            validation_attempt = previous_invalid_attempts + 1
            final_failed = validation_attempt > MAX_INVALID_MP4_RETRY
            compact_error = (probe_error or "битый MP4").replace("битый MP4: ", "", 1)
            current_size = source_path.stat().st_size if source_path.is_file() else 0
            if final_failed:
                logger.warning(
                    "VIDEO FILE VALIDATION FINAL FAILED | stage=process | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=delivery_faulty_and_job_failed | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    source_path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            else:
                logger.warning(
                    "VIDEO FILE VALIDATION FAILED | stage=process | delivery_id=%s | rule_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=удалён_файл_и_retry | validation_attempt=%s | max_validation_attempts=%s",
                    delivery_id,
                    rule_id,
                    job_id,
                    source_path,
                    current_size,
                    compact_error,
                    validation_attempt,
                    MAX_INVALID_MP4_RETRY,
                )
            if final_failed:
                error_text = f"Битый MP4 после повторной загрузки: {compact_error}"
            else:
                error_text = f"Битый MP4: {compact_error}, повторная загрузка 1/{MAX_INVALID_MP4_RETRY}"
            return {
                "ok": False,
                "fallback_to_legacy": False,
                "retryable": False,
                "restart_download": not final_failed,
                "error_text": error_text,
                "invalid_source_file": True,
                "final_invalid_source_file": final_failed,
                "ffprobe_stderr": compact_error,
                "validation_attempt": validation_attempt,
                "max_validation_attempts": MAX_INVALID_MP4_RETRY,
                "invalid_file_attempts": validation_attempt,
                "path": str(source_path),
                "size": current_size,
            }

        rule = await run_db(self.db.get_rule, int(rule_id))
        if not rule:
            logger.warning("VIDEO FALLBACK TO LEGACY | правило не найдено для process | delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        horizontal_intro, vertical_intro = await run_db(self._get_rule_intro_items_sync, rule)
        processed_result = await self.video_processor.build_processed_video(
            input_file_path=str(source_video_path),
            add_intro=bool(getattr(rule, "video_add_intro", False)),
            intro_name_horizontal=getattr(horizontal_intro, "file_name", None) if horizontal_intro else None,
            intro_name_vertical=getattr(vertical_intro, "file_name", None) if vertical_intro else None,
        )
        if not processed_result:
            logger.warning("VIDEO STAGE FAILED | VideoProcessor не смог обработать файл для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}
        logger.info(
            "VIDEO PROCESS DONE | обработка стадии завершена для delivery_id=%s | source=%s | processed=%s",
            delivery_id,
            source_video_path,
            processed_result.get("processed_video_path"),
        )
        return {"ok": True, "fallback_to_legacy": False, **processed_result}

    async def _validate_mp4_file_for_pipeline(
        self,
        file_path: Path,
        *,
        delivery_id: int,
        job_id: int | None,
        stage: str,
    ) -> tuple[bool, str | None]:
        if not file_path.exists() or not file_path.is_file():
            error_text = "Файл для проверки не найден"
            logger.warning(
                "VIDEO FILE VALIDATION FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=retry",
                stage,
                delivery_id,
                job_id,
                str(file_path),
                0,
                error_text,
            )
            return False, error_text

        file_size = file_path.stat().st_size
        probe_cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-print_format",
            "json",
            str(file_path),
        ]
        process = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        stderr_text = stderr.decode("utf-8", errors="ignore").strip()
        stdout_text = stdout.decode("utf-8", errors="ignore").strip()

        probe_payload = {}
        if stdout_text:
            try:
                probe_payload = json.loads(stdout_text)
            except Exception:
                probe_payload = {}

        streams = probe_payload.get("streams") if isinstance(probe_payload, dict) else None
        format_info = probe_payload.get("format") if isinstance(probe_payload, dict) else None
        empty_probe_data = (not isinstance(streams, list) or not streams) or (not isinstance(format_info, dict) or not format_info)

        if process.returncode == 0 and not empty_probe_data:
            return True, None

        compact_stderr = (stderr_text or "").replace("\n", " | ").strip()
        lower_stderr = compact_stderr.lower()
        if "moov atom not found" in lower_stderr:
            reason = "битый MP4: moov atom not found"
        elif "invalid data found when processing input" in lower_stderr:
            reason = "битый MP4: Invalid data found when processing input"
        elif empty_probe_data:
            reason = "битый MP4: ffprobe вернул пустые stream/format данные"
        else:
            fallback = compact_stderr or "ffprobe завершился с ошибкой"
            reason = f"битый MP4: {fallback[:500]}"

        removed = False
        try:
            file_path.unlink(missing_ok=True)
            removed = True
        except Exception as cleanup_exc:
            logger.warning(
                "VIDEO FILE VALIDATION CLEANUP FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | error=%s",
                stage,
                delivery_id,
                job_id,
                str(file_path),
                cleanup_exc,
            )

        action = "удалён_файл_и_retry" if removed else "retry_без_удаления"
        logger.warning(
            "VIDEO FILE VALIDATION FAILED | stage=%s | delivery_id=%s | job_id=%s | path=%s | size=%s | ffprobe_stderr=%s | action=%s",
            stage,
            delivery_id,
            job_id,
            str(file_path),
            file_size,
            compact_stderr,
            action,
        )
        return False, reason

    async def execute_video_send_from_job(
        self,
        *,
        processed_video_path: str | None = None,
        thumbnail_path: str | None = None,
        **payload: object,
    ) -> dict:
        return await self.execute_video_send_from_processed_job(
            processed_video_path=processed_video_path or payload.get("processed_file_path"),
            thumbnail_path=thumbnail_path,
            **payload,
        )

    async def execute_video_send_from_processed_job(
        self,
        *,
        processed_video_path: str | None = None,
        thumbnail_path: str | None = None,
        artifact_version: int | None = None,
        pipeline_version: int | None = None,
        **payload: object,
    ) -> dict:
        delivery_id = int(payload.get("delivery_id") or 0)
        rule_id = int(payload.get("rule_id") or 0)
        logger.info("VIDEO SEND START | старт отправки для delivery_id=%s | rule_id=%s | stage=send", delivery_id, rule_id)
        if int(artifact_version or 1) != 1 or int(pipeline_version or 1) != 1:
            logger.warning(
                "VIDEO FALLBACK TO LEGACY | неподдерживаемая версия контракта artifact=%s pipeline=%s | delivery_id=%s",
                artifact_version,
                pipeline_version,
                delivery_id,
            )
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not processed_video_path:
            logger.warning("VIDEO STAGE FAILED | отсутствует processed_file_path для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        if not Path(processed_video_path).is_file():
            logger.warning("VIDEO STAGE FAILED | обработанный файл не найден для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        rule = await run_db(self.db.get_rule, rule_id)
        if not rule:
            logger.warning("VIDEO FALLBACK TO LEGACY | правило не найдено для send | delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        caption_payload = self._build_video_caption_delivery_payload(rule)
        video_info = await self.video_processor.get_video_info(str(processed_video_path), use_cache=False)
        if not video_info:
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}

        sent_msg = await self.video_processor.send_with_retry(
            self.bot,
            payload.get("target_id"),
            payload.get("target_thread_id"),
            str(processed_video_path),
            str(thumbnail_path) if thumbnail_path else None,
            caption_payload["caption"] or "",
            video_info["duration"],
            caption_entities_json=caption_payload["caption_entities_json"],
            caption_send_mode=caption_payload["selected_mode"],
        )
        if not sent_msg:
            logger.warning("VIDEO STAGE RETRY | неуспешная отправка для delivery_id=%s", delivery_id)
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}

        sent_message_ids = self._extract_sent_message_ids(sent_msg)
        valid_sent_message_ids = await self._confirm_target_delivery_message_ids_with_retry(
            rule_id=rule_id,
            delivery_id=delivery_id,
            source_channel=str(payload.get("source_channel") or ""),
            target_id=str(payload.get("target_id") or ""),
            source_message_ids=[int(payload.get("message_id"))] if payload.get("message_id") else [],
            candidate_sent_message_ids=sent_message_ids,
            method="video_send",
            max_age_seconds=900,
        )
        sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
        logger.info("DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s", rule_id, delivery_id, "video_send", [], sent_message_ids, type(sent_msg).__name__)
        if sent_message_id:
            await self._add_reaction_if_possible(payload.get("target_id"), int(sent_message_id))
        else:
            logger.warning("DELIVERY_FALSE_SUCCESS_PREVENTED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | action=retry_or_faulty", rule_id, delivery_id, "video_send", payload.get("target_id"), sent_message_ids)
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}
        await run_db(self._mark_delivery_sent_sync, delivery_id, sent_message_id=sent_message_id, sent_message_ids=valid_sent_message_ids, target_id=str(payload.get("target_id") or ""), delivery_method="video_send")
        await run_db(self._touch_rule_after_send_sync, rule_id, int(payload.get("interval") or 0))
        logger.info("VIDEO SEND DONE | отправка завершена для delivery_id=%s", delivery_id)
        return {"ok": True, "fallback_to_legacy": False}

    async def _deliver_single(self, rule, delivery_id, message_id, source_channel, target_id, target_thread_id):
        post_id = await run_db(self._get_post_id_by_delivery_sync, delivery_id)
        delivery_ids = [int(delivery_id)]
        source_message_ids = [int(message_id)]

        strategy = await run_db(
            self._resolve_repost_caption_delivery_strategy_sync,
            rule=rule,
            source_channel=source_channel,
            message_ids=source_message_ids,
            is_album=False,
        )

        caption_mode = strategy["configured_mode"]
        requires_builder = strategy["requires_builder"]
        use_copy_first = strategy["use_copy_first"]

        await run_db(
            self.db.log_delivery_event,
            event_type="delivery_caption_mode_selected",
            delivery_id=delivery_id,
            rule_id=rule.id,
            post_id=post_id,
            status="processing",
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
                "selected_path": "copy_first" if use_copy_first else "builder_first",
                "message_id": message_id,
                "source_channel": source_channel,
                "target_id": target_id,
            },
        )

        logger.info(
            "CAPTION_MODE | single | rule=%s | delivery=%s | mode=%s | requires_builder=%s | selected_path=%s",
            rule.id,
            delivery_id,
            caption_mode,
            requires_builder,
            "copy_first" if use_copy_first else "builder_first",
        )

        post_row = await run_db(
            self._get_post_row_for_rule_message_sync,
            rule,
            source_channel,
            message_id,
        )

        # =========================================================
        # 1) COPY SINGLE
        # Выполняем только если текущий режим разрешает copy-first
        # =========================================================
        if use_copy_first:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                extra={
                    "attempt_no": 1,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            copy_result = await self._copy_single_via_bot(
                source_channel,
                target_id,
                message_id,
                target_thread_id,
            )
            copy_sent_ids = [int(x) for x in (copy_result.get("sent_ids") or []) if str(x).isdigit()]
            logger.info("COPY_SINGLE_RESULT_RAW_TYPE | rule_id=%s | delivery_id=%s | raw_type=%s", rule.id, delivery_id, copy_result.get("raw_result_type"))
            logger.info("COPY_SINGLE_EXTRACTED_SENT_IDS | rule_id=%s | delivery_id=%s | sent_ids=%s", rule.id, delivery_id, copy_sent_ids)
            valid_copy_sent_ids: list[int] = []
            if copy_sent_ids:
                logger.info("COPY_SINGLE_TARGET_CONFIRM_START | rule_id=%s | delivery_id=%s | target_id=%s | source_message_ids=%s | candidate_sent_message_ids=%s", rule.id, delivery_id, target_id, source_message_ids, copy_sent_ids)
                valid_copy_sent_ids = await self._confirm_target_delivery_message_ids_with_retry(
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=source_message_ids,
                    candidate_sent_message_ids=copy_sent_ids,
                    method="copy_single",
                )
                if valid_copy_sent_ids:
                    logger.info("COPY_SINGLE_TARGET_CONFIRM_OK | rule_id=%s | delivery_id=%s | valid_sent_message_ids=%s", rule.id, delivery_id, valid_copy_sent_ids)
                else:
                    logger.warning("COPY_SINGLE_TARGET_CONFIRM_FAILED | rule_id=%s | delivery_id=%s | candidate_sent_message_ids=%s", rule.id, delivery_id, copy_sent_ids)

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="ok" if valid_copy_sent_ids else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text=None if valid_copy_sent_ids else "copy_message не сработал",
                extra={
                    "attempt_no": 1,
                    "sent_message_id": valid_copy_sent_ids[0] if valid_copy_sent_ids else None,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            if valid_copy_sent_ids:
                candidate_sent_message_ids = valid_copy_sent_ids
                authoritative_sent_message_id = int(valid_copy_sent_ids[0])
                await self._add_reaction_for_rule_if_possible(
                    rule=rule,
                    target_id=target_id,
                    sent_message_id=authoritative_sent_message_id,
                    source_channel=str(source_channel or ""),
                    source_message_ids=source_message_ids,
                    delivery_id=delivery_id,
                )

                await self._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="copy_single",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    sent_message_id=authoritative_sent_message_id,
                    verify_result=None,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await run_db(
                    self._mark_delivery_sent_sync,
                    delivery_id,
                    sent_message_id=authoritative_sent_message_id,
                    sent_message_ids=candidate_sent_message_ids,
                    target_id=str(target_id),
                    delivery_method="copy_single",
                )
                await run_db(self._touch_rule_after_send_sync, rule.id, int(rule.interval_seconds or 0))
                return True
            if copy_result.get("attempted"):
                error_text = "copy_single_uncertain_no_fallback: copy_message was attempted but target confirmation failed; manual review required"
                logger.warning("COPY_SINGLE_UNCERTAIN_NO_FALLBACK | rule_id=%s | delivery_id=%s | reason=copy_attempted_without_verified_target_message", rule.id, delivery_id)
                await run_db(self._mark_delivery_faulty_sync, delivery_id, error_text)
                await self._log_delivery_final_failure(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="copy_single_uncertain_no_fallback",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    error_text=error_text,
                    attempts_debug=[
                        {"stage": "copy_single", "ok": False, "attempted": True, "candidate_sent_message_ids": copy_sent_ids},
                    ],
                    extra={"non_retryable": True, "manual_review_required": True},
                )
                return False
            logger.info("COPY_TO_REUPLOAD_FALLBACK_ALLOWED | rule_id=%s | delivery_id=%s | reason=copy_not_attempted", rule.id, delivery_id)
        else:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_single",
                pipeline_result="skipped",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="copy_single пропущен политикой caption mode",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                    "skip_reason": "builder_required_or_builder_first",
                },
            )

        # =========================================================
        # 2) SELF LOOP
        # =========================================================
        if self._is_self_loop_rule(rule) and use_copy_first:
            logger.info(
                "Self-loop: copy_single не сработал для %s/%s, проблемную доставку не создаю, потому что источник и получатель совпадают",
                source_channel,
                message_id,
            )

            try:
                await self._add_reaction_if_possible(target_id, int(message_id))
            except Exception as exc:
                logger.warning(
                    "SELF_LOOP_REACTION | single | не удалось поставить реакцию на исходное сообщение %s в %s: %s",
                    message_id,
                    target_id,
                    exc,
                )

            await self._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="self_loop_copy_only_single",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                sent_message_id=int(message_id),
                verify_result=None,
                extra={
                    "skip_reason": "self_loop_copy_not_supported",
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(
                self._mark_delivery_sent_sync,
                delivery_id,
                sent_message_id=int(message_id),
                sent_message_ids=[int(message_id)],
                target_id=str(target_id),
                delivery_method="self_loop_copy_only_single",
            )
            return True

        # =========================================================
        # 3) FETCH MESSAGE
        # =========================================================
        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="fetch_message",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        message = await self._fetch_message(source_channel, message_id)

        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="fetch_message",
            pipeline_result="ok" if message else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=None if message else "Сообщение не получено через MTProto",
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        if not message:
            await self._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="fetch_message_failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Сообщение не получено через MTProto",
                attempts_debug=[
                    {
                        "stage": "copy_single",
                        "ok": False,
                        "skipped": not use_copy_first,
                    },
                    {
                        "stage": "fetch_message",
                        "ok": False,
                        "error_text": "Сообщение не получено через MTProto",
                    },
                ],
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        content = self._content_from_message_or_post(message=message, post_row=post_row)
        built_text, _built_entities = self._build_text_and_entities_from_content(content)

        # =========================================================
        # 4) REUPLOAD SINGLE
        # =========================================================
        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_single",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "attempt_no": 1,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        sent_message_id = await self._reupload_message(
            message,
            target_id,
            target_thread_id,
            post_row=post_row,
        )
        candidate_sent_message_ids = [int(sent_message_id)] if sent_message_id else []
        valid_sent_message_ids = await self._confirm_target_delivery_message_ids_with_retry(
            rule_id=rule.id,
            delivery_id=delivery_id,
            source_channel=str(source_channel or ""),
            target_id=str(target_id),
            source_message_ids=source_message_ids,
            candidate_sent_message_ids=candidate_sent_message_ids,
            method="reupload_single",
        )

        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_single",
            pipeline_result="ok" if valid_sent_message_ids else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text=None if valid_sent_message_ids else "target_message_not_found_after_send",
            extra={
                "attempt_no": 1,
                "sent_message_id": sent_message_id,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        if valid_sent_message_ids:
            logger.info("REUPLOAD_SINGLE_TARGET_VERIFY_OK | rule_id=%s | delivery_id=%s | source_channel=%s | source_message_id=%s | target_id=%s | valid_sent_message_ids=%s", rule.id, delivery_id, source_channel, message_id, target_id, valid_sent_message_ids)
            authoritative_sent_message_id = int(valid_sent_message_ids[0])
            await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

            await self._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="reupload_single",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                sent_message_id=authoritative_sent_message_id,
                verify_result=None,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(
                self._mark_delivery_sent_sync,
                delivery_id,
                sent_message_id=authoritative_sent_message_id,
                sent_message_ids=valid_sent_message_ids,
                target_id=str(target_id),
                delivery_method="reupload_single",
            )
            return True

        logger.warning("REUPLOAD_SINGLE_TARGET_VERIFY_FAILED | rule_id=%s | delivery_id=%s | source_channel=%s | source_message_id=%s | target_id=%s | candidate_sent_message_ids=%s | reason=target_message_not_found_after_send", rule.id, delivery_id, source_channel, message_id, target_id, candidate_sent_message_ids)
        logger.warning("DELIVERY_FALSE_SUCCESS_PREVENTED | rule_id=%s | delivery_id=%s | method=%s | target_id=%s | candidate_sent_message_ids=%s | action=retry_or_faulty", rule.id, delivery_id, "reupload_single", target_id, candidate_sent_message_ids)

        # =========================================================
        # 5) DEBUG: fallback disabled
        # =========================================================
        if DEBUG_FORCE_DISABLE_BOTAPI_FALLBACK:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="text_fallback",
                pipeline_result="failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Bot API fallback принудительно отключён для диагностики",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await self._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="single_pipeline_final_failure",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Не удалось доставить сообщение через Telethon, Bot API fallback отключён",
                attempts_debug=[
                    {
                        "stage": "copy_single",
                        "ok": False,
                        "skipped": not use_copy_first,
                    },
                    {
                        "stage": "fetch_message",
                        "ok": True,
                    },
                    {
                        "stage": "reupload_single",
                        "ok": False,
                        "sent_message_id": None,
                    },
                    {
                        "stage": "text_fallback",
                        "ok": False,
                        "disabled": True,
                    },
                ],
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        # =========================================================
        # 6) TEXT FALLBACK
        # =========================================================
        html_text = _prepare_html_text(built_text)

        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="text_fallback",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        text_fallback_ok = False
        text_fallback_sent_message_id = None

        if html_text:
            try:
                sent = await self.bot.send_message(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    text=html_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )

                await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent.message_id,
                        )

                text_fallback_ok = True
                text_fallback_sent_message_id = sent.message_id

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="text_fallback",
                    pipeline_result="ok",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    extra={
                        "sent_message_id": sent.message_id,
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await self._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="text_fallback",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    sent_message_id=sent.message_id,
                    verify_result=None,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )

                await run_db(self._mark_delivery_sent_sync, delivery_id)
                return True

            except Exception as exc:
                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="text_fallback",
                    pipeline_result="failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=source_message_ids,
                    error_text=str(exc),
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                    },
                )
        else:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="text_fallback",
                pipeline_result="failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=source_message_ids,
                error_text="Текстовый fallback невозможен: текст пустой",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

        # =========================================================
        # 7) FINAL FAILURE
        # =========================================================
        await self._log_delivery_final_failure(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            final_method="single_pipeline_final_failure",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=source_message_ids,
            error_text="Не удалось доставить сообщение",
            attempts_debug=[
                {
                    "stage": "copy_single",
                    "ok": False,
                    "skipped": not use_copy_first,
                },
                {
                    "stage": "fetch_message",
                    "ok": True,
                },
                {
                    "stage": "reupload_single",
                    "ok": False,
                    "sent_message_id": None,
                },
                {
                    "stage": "text_fallback",
                    "ok": text_fallback_ok,
                    "sent_message_id": text_fallback_sent_message_id,
                },
            ],
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )
        return False

    async def _deliver_single_video(self, rule, delivery_id, message_id, source_channel, target_id, target_thread_id):
        post_id = await run_db(self._get_post_id_by_delivery_sync, delivery_id)

        await run_db(
            self._log_video_event_sync,
            event_type="video_processing_started",
            delivery_id=delivery_id,
            rule_id=rule.id,
            post_id=post_id,
            status="processing",
            extra={
                "source_channel": source_channel,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "source_message_id": message_id,
                "video_mode": True,
            },
        )

        try:
            message = await self._fetch_message(source_channel, message_id)
            if not message:
                await run_db(
                    self._finalize_video_failure_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    error_text="Сообщение не получено через MTProto",
                )
                return False

            media_kind = _detect_message_media_kind(message)
            horizontal_intro, vertical_intro = await run_db(
                self._get_rule_intro_items_sync,
                rule,
            )

            caption_payload = self._build_video_caption_delivery_payload(rule)

            rule_caption = caption_payload["caption"]
            rule_caption_entities = caption_payload["caption_entities"]
            caption_entities_json = caption_payload["caption_entities_json"]
            caption_delivery_mode = caption_payload["caption_delivery_mode"]
            requires_premium = caption_payload["requires_premium"]
            selected_mode = caption_payload["selected_mode"]

            trim_seconds = int(getattr(rule, "video_trim_seconds", 120) or 120)

            stage_logger = self._build_video_stage_logger(
                rule=rule,
                delivery_id=delivery_id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                source_message_id=message_id,
            )

            await run_db(
                self._log_video_event_sync,
                event_type="video_download_started",
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                status="processing",
                extra={
                    "source_channel": source_channel,
                    "target_id": target_id,
                    "target_thread_id": target_thread_id,
                    "source_message_id": message_id,
                    "media_kind": media_kind,
                    "trim_seconds": trim_seconds,
                    "horizontal_intro_id": getattr(horizontal_intro, "id", None),
                    "vertical_intro_id": getattr(vertical_intro, "id", None),
                    "horizontal_intro_name": getattr(horizontal_intro, "display_name", None),
                    "vertical_intro_name": getattr(vertical_intro, "display_name", None),
                    "has_rule_caption": bool(rule_caption),
                    "has_rule_caption_entities": bool(rule_caption_entities),
                    "caption_delivery_mode": caption_delivery_mode,
                    "selected_mode": selected_mode,
                    "caption_requires_premium": requires_premium,
                },
            )

            if media_kind != "video":
                await run_db(
                    self._log_video_event_sync,
                    event_type="video_processing_completed",
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    status="sent",
                    extra={
                        "source_channel": source_channel,
                        "target_id": target_id,
                        "target_thread_id": target_thread_id,
                        "source_message_id": message_id,
                        "media_kind": media_kind,
                        "skipped": True,
                        "skip_reason": "not_video",
                    },
                )

                await run_db(self._mark_delivery_sent_sync, delivery_id)
                return True

            source_video_path = await self._download_video_source(
                message,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                source_message_id=message_id,
            )

            if not source_video_path:
                await run_db(
                    self._finalize_video_failure_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    error_text="Не удалось скачать видео",
                )
                return False

            try:
                sent_msg = await self.video_processor.process_video(
                    video_file_id=None,
                    context=None,
                    destination_channel=target_id,
                    target_thread_id=target_thread_id,
                    add_intro=bool(getattr(rule, "video_add_intro", False)),
                    intro_name_horizontal=getattr(horizontal_intro, "file_name", None) if horizontal_intro else None,
                    intro_name_vertical=getattr(vertical_intro, "file_name", None) if vertical_intro else None,
                    caption=rule_caption or "",
                    caption_entities_json=caption_entities_json,
                    caption_send_mode=selected_mode,
                    input_file_path=str(source_video_path),
                    stage_logger=stage_logger,
                )
            finally:
                try:
                    source_video_path.unlink(missing_ok=True)
                except Exception:
                    pass

            if sent_msg:
                sent_message_ids = self._extract_sent_message_ids(sent_msg)
                valid_sent_message_ids = await self._confirm_target_delivery_message_ids_with_retry(
                    rule_id=rule.id,
                    delivery_id=delivery_id,
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=[int(message_id)],
                    candidate_sent_message_ids=sent_message_ids,
                    method="video_process",
                    max_age_seconds=900,
                )
                sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
                logger.info("DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s", rule.id, delivery_id, "video_process", [message_id], sent_message_ids, type(sent_msg).__name__)

                try:
                    if sent_message_id:
                        await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                            source_channel=source_channel,
                            source_message_ids=[message_id],
                            delivery_id=delivery_id,
                            max_age_seconds=900,
                        )
                    else:
                        logger.warning(
                            "VIDEO_REACTION | не удалось подтвердить sent_message_id после process_video | rule=%s | delivery=%s | target=%s",
                            rule.id,
                            delivery_id,
                            target_id,
                        )
                except Exception as exc:
                    logger.warning(
                        "Не удалось поставить реакцию под видео-сообщение %s в %s: %s",
                        sent_message_id,
                        target_id,
                        exc,
                    )

                await run_db(
                    self._finalize_video_success_sync,
                    delivery_id=delivery_id,
                    rule_id=rule.id,
                    post_id=post_id,
                    source_channel=source_channel,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    source_message_id=message_id,
                    sent_message_id=sent_message_id,
                    fallback_mode="deliver_single",
                    caption_delivery_mode=caption_delivery_mode,
                    selected_mode=selected_mode,
                    caption_requires_premium=requires_premium,
                )
                return True

            await run_db(
                self._finalize_video_failure_sync,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                source_message_id=message_id,
                error_text="Обычная доставка внутри video-ветки не сработала",
                fallback_mode="deliver_single",
                caption_delivery_mode=caption_delivery_mode,
                selected_mode=selected_mode,
                caption_requires_premium=requires_premium,
            )
            return False

        except Exception as exc:
            logger.exception("Ошибка video delivery rule=%s delivery=%s", rule.id, delivery_id)

            await run_db(
                self._finalize_video_failure_sync,
                delivery_id=delivery_id,
                rule_id=rule.id,
                post_id=post_id,
                source_channel=source_channel,
                target_id=target_id,
                target_thread_id=target_thread_id,
                source_message_id=message_id,
                error_text=str(exc),
            )
            return False

    async def _deliver_album(self, rule, album_rows, source_channel, target_id, target_thread_id):
        delivery_ids = [int(r["delivery_id"]) for r in album_rows]
        message_ids = [int(r["message_id"]) for r in album_rows]

        strategy = await run_db(
            self._resolve_repost_caption_delivery_strategy_sync,
            rule=rule,
            source_channel=source_channel,
            message_ids=message_ids,
            is_album=True,
        )

        caption_mode = strategy["configured_mode"]
        requires_builder = strategy["requires_builder"]
        use_copy_first = strategy["use_copy_first"]

        post_rows_by_message_id = {
            int(r["message_id"]): r
            for r in album_rows
        }

        source_messages = None
        first_source_caption = None
        final_error_text = None
        attempts_debug: list[dict] = []

        logger.info(
            "CAPTION_MODE | album | rule=%s | mode=%s | requires_builder=%s | selected_path=%s | items=%s",
            rule.id,
            caption_mode,
            requires_builder,
            "copy_first" if use_copy_first else "builder_first",
            len(message_ids),
        )

        # =========================================================
        # PREVIEW / caption text for verify
        # =========================================================
        try:
            fetched_preview = await self._fetch_album_messages(source_channel, message_ids)
            if fetched_preview:
                source_messages = fetched_preview
                first_source_caption = self._get_album_primary_text(
                    source_messages,
                    post_rows=[
                        post_rows_by_message_id.get(int(getattr(m, "id")))
                        for m in source_messages
                    ],
                )
        except Exception as exc:
            logger.warning(
                "Не удалось заранее получить preview альбома %s -> %s: %s",
                source_channel,
                target_id,
                exc,
            )
            source_messages = None
            first_source_caption = None

        # =========================================================
        # 1) COPY VIA BOT API
        # Выполняем только если режим разрешает copy-first
        # =========================================================
        if use_copy_first:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={
                    "attempt_no": 1,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            copy_result = await self._copy_album_via_bot(
                source_channel=source_channel,
                target_id=target_id,
                message_ids=message_ids,
                target_thread_id=target_thread_id,
            )
            attempts_debug.append({"stage": "copy_album", **copy_result})

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="ok" if copy_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=copy_result.get("error_text"),
                extra={
                    "attempt_no": 1,
                    "sent_message_id": copy_result.get("sent_message_id"),
                    "sent_count": copy_result.get("sent_count"),
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            if copy_result["ok"]:
                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verified = await self._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=copy_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                )
                attempts_debug.append({"stage": "verify_after_copy", **verified})

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": self._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await self._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=copy_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                    )
                    attempts_debug.append({"stage": "verify_after_copy_retry_only", **verified_retry})

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": self._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    sent_message_id = verified.get("first_message_id") or copy_result.get("sent_message_id")
                    await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

                    await self._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="copy_album_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                        },
                    )

                    await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            copy_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "copy_album пропущен политикой caption mode",
            }

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album",
                pipeline_result="skipped",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text="copy_album пропущен политикой caption mode",
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                    "skip_reason": "builder_required_or_builder_first",
                },
            )

        # =========================================================
        # 2) SELF LOOP
        # =========================================================
        if self._is_self_loop_rule(rule) and use_copy_first:
            logger.info(
                "Self-loop: copy_album не сработал для %s -> %s, проблемные доставки не создаю, потому что источник и получатель совпадают",
                source_channel,
                target_id,
            )

            first_album_message_id = int(message_ids[0]) if message_ids else None

            try:
                if first_album_message_id:
                    await self._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=first_album_message_id,
                    )
            except Exception as exc:
                logger.warning(
                    "SELF_LOOP_REACTION | album | не удалось поставить реакцию на исходное сообщение %s в %s: %s",
                    first_album_message_id,
                    target_id,
                    exc,
                )

            await self._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="self_loop_copy_only_album",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                sent_message_id=first_album_message_id,
                verify_result=None,
                extra={
                    "skip_reason": "self_loop_copy_not_supported",
                    "attempts": attempts_debug,
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
            return True

        # =========================================================
        # 3) RETRY COPY ONLY IF COPY REALLY FAILED
        # =========================================================
        if use_copy_first and not copy_result["ok"]:
            await asyncio.sleep(1.2)

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album_retry",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 2},
            )

            copy_retry_result = await self._copy_album_via_bot(
                source_channel=source_channel,
                target_id=target_id,
                message_ids=message_ids,
                target_thread_id=target_thread_id,
            )
            attempts_debug.append({"stage": "copy_album_retry", **copy_retry_result})

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="copy_album_retry",
                pipeline_result="ok" if copy_retry_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=copy_retry_result.get("error_text"),
                extra={
                    "attempt_no": 2,
                    "sent_message_id": copy_retry_result.get("sent_message_id"),
                    "sent_count": copy_retry_result.get("sent_count"),
                },
            )
            if copy_retry_result["ok"]:
                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy_retry",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verified = await self._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=copy_retry_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                )
                attempts_debug.append({"stage": "verify_after_copy_retry", **verified})

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_copy_retry",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": self._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only_second",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await self._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=copy_retry_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                    )
                    attempts_debug.append({"stage": "verify_after_copy_retry_only_second", **verified_retry})

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_copy_retry_only_second",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": self._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    sent_message_id = verified.get("first_message_id") or copy_retry_result.get("sent_message_id")
                    await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

                    await self._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="copy_album_retry_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                        },
                    )

                    await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            copy_retry_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Повторный copy не выполнялся",
            }

        # =========================================================
        # 4) FETCH SOURCE ALBUM
        # =========================================================
        if source_messages is None:
            source_messages = await self._fetch_album_messages(source_channel, message_ids)

        if source_messages is not None and first_source_caption is None:
            first_source_caption = self._get_album_primary_text(
                source_messages,
                post_rows=[
                    post_rows_by_message_id.get(int(getattr(m, "id")))
                    for m in source_messages
                ],
            )

        if len(source_messages) != len(message_ids):
            final_error_text = "Не удалось получить весь альбом через MTProto"

            await self._log_delivery_final_failure(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="fetch_album_failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=final_error_text,
                attempts_debug=attempts_debug,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )
            return False

        # =========================================================
        # 5) REUPLOAD AS ALBUM
        # =========================================================
        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_album",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            extra={
                "attempt_no": 1,
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        reupload_result = await self._reupload_album(
            messages=source_messages,
            target_id=target_id,
            target_thread_id=target_thread_id,
            post_rows=[
                post_rows_by_message_id.get(int(getattr(m, "id")))
                for m in source_messages
            ],
        )
        attempts_debug.append({"stage": "reupload_album", **reupload_result})

        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="reupload_album",
            pipeline_result="ok" if reupload_result["ok"] else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=reupload_result.get("error_text"),
            extra={
                "attempt_no": 1,
                "sent_message_id": reupload_result.get("sent_message_id"),
                "sent_count": reupload_result.get("sent_count"),
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )

        expected_count = len(message_ids)
        reupload_sent_count = int(reupload_result.get("sent_count") or 0)
        upload_confirmed_by_send_result = (
            reupload_result.get("ok") is True
            and reupload_sent_count >= expected_count
        )

        if reupload_result["ok"]:
            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="verify_after_reupload",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 1},
            )

            verified = await self._verify_album_delivery(
                target_id=target_id,
                expected_count=len(message_ids),
                sent_message_ids=reupload_result.get("sent_message_ids"),
                target_thread_id=target_thread_id,
                target_grouped_id=reupload_result.get("target_grouped_id"),
            )
            attempts_debug.append({"stage": "verify_after_reupload", **verified})

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="verify_after_reupload",
                pipeline_result="ok" if verified["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=verified.get("error_text"),
                extra={
                    "attempt_no": 1,
                    "verify_result": self._serialize_pipeline_verify_result(verified),
                },
            )

            if not verified["ok"]:
                await asyncio.sleep(1.5)

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry_only",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 2},
                )

                verified_retry = await self._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=reupload_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                    target_grouped_id=reupload_result.get("target_grouped_id"),
                )
                attempts_debug.append({"stage": "verify_after_reupload_retry_only", **verified_retry})

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry_only",
                    pipeline_result="ok" if verified_retry["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified_retry.get("error_text"),
                    extra={
                        "attempt_no": 2,
                        "verify_result": self._serialize_pipeline_verify_result(verified_retry),
                    },
                )
                if verified_retry["ok"]:
                    verified = verified_retry

            if verified["ok"]:
                sent_message_ids = verified.get("sent_message_ids") or reupload_result.get("sent_message_ids") or []
                reaction_message_id, reaction_target_reason = await self._select_reaction_message_id(
                    target_id=target_id,
                    sent_message_ids=sent_message_ids,
                )
                sent_message_id = (sent_message_ids[0] if sent_message_ids else None) or reupload_result.get("sent_message_id")

                if reaction_message_id:
                    await self._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=reaction_message_id,
                        source_channel=str(source_channel or ""),
                        source_message_ids=message_ids,
                        delivery_id=(delivery_ids[0] if delivery_ids else None),
                    )

                await self._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="reupload_album_verified",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    sent_message_id=sent_message_id,
                    sent_message_ids=sent_message_ids,
                    reaction_message_id=reaction_message_id,
                    verify_result=verified,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                        "reaction_target_reason": reaction_target_reason,
                    },
                )

                await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
                return True

            if upload_confirmed_by_send_result:
                logger.warning(
                    "REUPLOAD_ALBUM | verify не подтвердил альбом, но отправка уже подтверждена Telethon | "
                    "rule_id=%s | sent_count=%s | expected_count=%s | error=%s",
                    rule.id,
                    reupload_sent_count,
                    expected_count,
                    verified.get("error_text"),
                )

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload",
                    pipeline_result="soft_failed_upload_confirmed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "verify_result": self._serialize_pipeline_verify_result(verified),
                        "sent_count": reupload_sent_count,
                        "expected_count": expected_count,
                        "sent_message_id": reupload_result.get("sent_message_id"),
                        "source_message_ids": message_ids,
                        "reason": "upload_confirmed_by_telethon_send_result",
                    },
                )

                sent_message_ids = reupload_result.get("sent_message_ids") or []
                logger.info(
                    "DELIVERY_SENT_MESSAGE_IDS_EXTRACTED | rule_id=%s | delivery_id=%s | method=%s | source_message_ids=%s | sent_message_ids=%s | result_type=%s",
                    rule.id,
                    (delivery_ids[0] if delivery_ids else None),
                    "reupload_album_unverified_success",
                    message_ids,
                    sent_message_ids,
                    type(reupload_result).__name__,
                )
                valid_sent_message_ids = await self._validate_sent_message_ids_for_delivery(
                    rule_id=rule.id,
                    delivery_id=(delivery_ids[0] if delivery_ids else None),
                    source_channel=str(source_channel or ""),
                    target_id=str(target_id),
                    source_message_ids=message_ids,
                    candidate_sent_message_ids=sent_message_ids,
                    method="reupload_album_unverified_success",
                )
                sent_message_id = valid_sent_message_ids[0] if valid_sent_message_ids else None
                reaction_message_id, reaction_target_reason = await self._select_reaction_message_id(
                    target_id=target_id,
                    sent_message_ids=valid_sent_message_ids,
                )

                await self._log_delivery_final_success(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    final_method="reupload_album_unverified_success",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    sent_message_id=sent_message_id,
                    sent_message_ids=valid_sent_message_ids,
                    reaction_message_id=reaction_message_id,
                    verify_result=verified,
                    extra={
                        "caption_delivery_mode": caption_mode,
                        "requires_builder": requires_builder,
                        "reaction_target_reason": reaction_target_reason,
                        "verify_ok": False,
                        "verify_count": verified.get("count"),
                        "verify_grouped_id": verified.get("grouped_id"),
                        "verify_first_message_id": verified.get("first_message_id"),
                        "first_sent_message_id": reupload_result.get("sent_message_id"),
                        "candidate_sent_message_ids": sent_message_ids,
                        "valid_sent_message_ids": valid_sent_message_ids,
                        "sent_count": reupload_sent_count,
                        "expected_count": expected_count,
                    },
                )

                if reaction_message_id:
                    try:
                        logger.info(
                            "REACTION_AFTER_REUPLOAD_UNVERIFIED_VALIDATED | start | rule_id=%s | delivery_id=%s | target_id=%s | message_id=%s | valid_sent_message_ids=%s",
                            rule.id,
                            (delivery_ids[0] if delivery_ids else None),
                            target_id,
                            reaction_message_id,
                            valid_sent_message_ids,
                        )
                        await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=reaction_message_id,
                            source_channel=str(source_channel or ""),
                            source_message_ids=message_ids,
                            delivery_id=(delivery_ids[0] if delivery_ids else None),
                        )
                    except Exception as exc:
                        logger.warning(
                            "REACTION_AFTER_REUPLOAD_UNVERIFIED_VALIDATED | failed | rule_id=%s | delivery_id=%s | target_id=%s | message_id=%s | error=%s",
                            rule.id,
                            (delivery_ids[0] if delivery_ids else None),
                            target_id,
                            reaction_message_id,
                            exc,
                        )
                else:
                    logger.warning(
                        "REACTION_SKIPPED_UNVERIFIED_ALBUM_SENT_IDS | rule_id=%s | delivery_id=%s | source_channel=%s | target_id=%s | method=%s | candidate_sent_message_ids=%s | source_message_ids=%s | reason=%s",
                        rule.id,
                        (delivery_ids[0] if delivery_ids else None),
                        source_channel,
                        target_id,
                        "reupload_album_unverified_success",
                        sent_message_ids,
                        message_ids,
                        verified.get("error_text") or "verify_album_sent_ids_not_found",
                    )

                await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
                return True

        # =========================================================
        # 6) RETRY REUPLOAD ONLY IF REUPLOAD REALLY FAILED
        # =========================================================
        if not reupload_result["ok"]:
            await asyncio.sleep(1.2)

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_album_retry",
                pipeline_result="started",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                extra={"attempt_no": 2},
            )

            reupload_retry_result = await self._reupload_album(
                messages=source_messages,
                target_id=target_id,
                target_thread_id=target_thread_id,
                post_rows=[
                    post_rows_by_message_id.get(int(getattr(m, "id")))
                    for m in source_messages
                ],
            )
            attempts_debug.append({"stage": "reupload_album_retry", **reupload_retry_result})

            await self._log_delivery_pipeline_step(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                event_type="delivery_pipeline_step",
                pipeline_stage="reupload_album_retry",
                pipeline_result="ok" if reupload_retry_result["ok"] else "failed",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                error_text=reupload_retry_result.get("error_text"),
                extra={
                    "attempt_no": 2,
                    "sent_message_id": reupload_retry_result.get("sent_message_id"),
                    "sent_count": reupload_retry_result.get("sent_count"),
                },
            )

            if reupload_retry_result["ok"]:
                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry",
                    pipeline_result="started",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    extra={"attempt_no": 1},
                )

                verified = await self._verify_album_delivery(
                    target_id=target_id,
                    expected_count=len(message_ids),
                    sent_message_ids=reupload_retry_result.get("sent_message_ids"),
                    target_thread_id=target_thread_id,
                    target_grouped_id=reupload_retry_result.get("target_grouped_id"),
                )
                attempts_debug.append({"stage": "verify_after_reupload_retry", **verified})

                await self._log_delivery_pipeline_step(
                    rule_id=rule.id,
                    delivery_ids=delivery_ids,
                    event_type="delivery_pipeline_step",
                    pipeline_stage="verify_after_reupload_retry",
                    pipeline_result="ok" if verified["ok"] else "failed",
                    source_channel=source_channel,
                    target_id=target_id,
                    source_message_ids=message_ids,
                    error_text=verified.get("error_text"),
                    extra={
                        "attempt_no": 1,
                        "verify_result": self._serialize_pipeline_verify_result(verified),
                    },
                )

                if not verified["ok"]:
                    await asyncio.sleep(1.5)

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_reupload_retry_only_second",
                        pipeline_result="started",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        extra={"attempt_no": 2},
                    )

                    verified_retry = await self._verify_album_delivery(
                        target_id=target_id,
                        expected_count=len(message_ids),
                        sent_message_ids=reupload_retry_result.get("sent_message_ids"),
                        target_thread_id=target_thread_id,
                        target_grouped_id=reupload_retry_result.get("target_grouped_id"),
                    )
                    attempts_debug.append({"stage": "verify_after_reupload_retry_only_second", **verified_retry})

                    await self._log_delivery_pipeline_step(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        event_type="delivery_pipeline_step",
                        pipeline_stage="verify_after_reupload_retry_only_second",
                        pipeline_result="ok" if verified_retry["ok"] else "failed",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        error_text=verified_retry.get("error_text"),
                        extra={
                            "attempt_no": 2,
                            "verify_result": self._serialize_pipeline_verify_result(verified_retry),
                        },
                    )

                    if verified_retry["ok"]:
                        verified = verified_retry

                if verified["ok"]:
                    sent_message_ids = verified.get("sent_message_ids") or reupload_retry_result.get("sent_message_ids") or []
                    reaction_message_id, reaction_target_reason = await self._select_reaction_message_id(
                        target_id=target_id,
                        sent_message_ids=sent_message_ids,
                    )
                    sent_message_id = (sent_message_ids[0] if sent_message_ids else None) or reupload_retry_result.get("sent_message_id")

                    if reaction_message_id:
                        await self._add_reaction_for_rule_if_possible(
                        rule=rule,
                        target_id=target_id,
                        sent_message_id=reaction_message_id,
                    )

                    await self._log_delivery_final_success(
                        rule_id=rule.id,
                        delivery_ids=delivery_ids,
                        final_method="reupload_album_retry_verified",
                        source_channel=source_channel,
                        target_id=target_id,
                        source_message_ids=message_ids,
                        sent_message_id=sent_message_id,
                        sent_message_ids=sent_message_ids,
                        reaction_message_id=reaction_message_id,
                        verify_result=verified,
                        extra={
                            "caption_delivery_mode": caption_mode,
                            "requires_builder": requires_builder,
                            "reaction_target_reason": reaction_target_reason,
                        },
                    )

                    await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
                    return True
        else:
            reupload_retry_result = {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Повторный reupload не выполнялся, потому что первый reupload уже отработал",
            }

        # =========================================================
        # 7) EMERGENCY FALLBACK: ONE BY ONE
        # =========================================================
        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="send_album_one_by_one",
            pipeline_result="started",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            extra={"attempt_no": 1},
        )

        one_by_one_result = await self._send_album_one_by_one(
            messages=source_messages,
            target_id=target_id,
            target_thread_id=target_thread_id,
            post_rows=[
                post_rows_by_message_id.get(int(getattr(m, "id")))
                for m in source_messages
            ],
        )
        attempts_debug.append({"stage": "send_album_one_by_one", **one_by_one_result})

        await self._log_delivery_pipeline_step(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            event_type="delivery_pipeline_step",
            pipeline_stage="send_album_one_by_one",
            pipeline_result="ok" if one_by_one_result["ok"] else "failed",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=one_by_one_result.get("error_text"),
            extra={
                "attempt_no": 1,
                "sent_message_id": one_by_one_result.get("sent_message_id"),
                "sent_count": one_by_one_result.get("sent_count"),
            },
        )

        if one_by_one_result["ok"]:
            sent_message_id = one_by_one_result.get("sent_message_id")
            if sent_message_id:
                await self._add_reaction_for_rule_if_possible(
                            rule=rule,
                            target_id=target_id,
                            sent_message_id=sent_message_id,
                        )

            await self._log_delivery_final_success(
                rule_id=rule.id,
                delivery_ids=delivery_ids,
                final_method="one_by_one_fallback",
                source_channel=source_channel,
                target_id=target_id,
                source_message_ids=message_ids,
                sent_message_id=sent_message_id,
                verify_result=None,
                extra={
                    "caption_delivery_mode": caption_mode,
                    "requires_builder": requires_builder,
                },
            )

            await run_db(self._mark_many_deliveries_sent_sync, delivery_ids)
            return True

        # =========================================================
        # 8) FINAL FAILURE
        # =========================================================
        final_error_text = (
            one_by_one_result.get("error_text")
            or reupload_retry_result.get("error_text")
            or reupload_result.get("error_text")
            or copy_retry_result.get("error_text")
            or copy_result.get("error_text")
            or "Не удалось доставить альбом ни одним методом"
        )

        await self._log_delivery_final_failure(
            rule_id=rule.id,
            delivery_ids=delivery_ids,
            final_method="album_pipeline_final_failure",
            source_channel=source_channel,
            target_id=target_id,
            source_message_ids=message_ids,
            error_text=final_error_text,
            attempts_debug=attempts_debug,
            extra={
                "caption_delivery_mode": caption_mode,
                "requires_builder": requires_builder,
            },
        )
        return False

    async def _copy_single_via_bot(self, source_channel, target_id, message_id, target_thread_id):
        if DEBUG_FORCE_SKIP_COPY_SINGLE:
            logger.warning(
                "COPY_SINGLE | TEST MODE | принудительно пропускаю Bot API copy_message для проверки Telethon"
            )
            return {"attempted": False, "sent_ids": [], "fallback_allowed": True, "raw_result_type": "debug_skip"}

        try:
            sent = await self.bot.copy_message(
                chat_id=target_id,
                from_chat_id=source_channel,
                message_id=message_id,
                message_thread_id=target_thread_id,
            )
            sent_ids = self._extract_sent_message_ids(sent)
            return {"attempted": True, "sent_ids": sent_ids, "fallback_allowed": False, "raw_result_type": type(sent).__name__}
        except Exception as exc:
            logger.warning(
                "Не удалось скопировать сообщение %s/%s в %s: %s",
                source_channel,
                message_id,
                target_id,
                exc,
            )
            return {"attempted": True, "sent_ids": [], "fallback_allowed": False, "raw_result_type": "exception", "error_text": str(exc)}

    async def _copy_album_via_bot(self, source_channel, target_id, message_ids, target_thread_id):
        if DEBUG_FORCE_SKIP_COPY_ALBUM:
            logger.warning(
                "COPY_ALBUM | TEST MODE | принудительно пропускаю Bot API CopyMessages для проверки Telethon album send"
            )
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "Bot API copy_album принудительно отключён",
            }

        try:
            sent_messages = await self.bot(
                CopyMessages(
                    chat_id=target_id,
                    from_chat_id=source_channel,
                    message_ids=message_ids,
                    message_thread_id=target_thread_id,
                )
            )

            if sent_messages and len(sent_messages) > 0:
                return {
                    "ok": True,
                    "sent_message_id": sent_messages[0].message_id,
                    "sent_message_ids": [int(m.message_id) for m in sent_messages],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "CopyMessages вернул пустой результат",
            }

        except Exception as exc:
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

    async def _send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        sent_ids: list[int] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для one-by-one fallback",
                }

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                sent_message_id = await self._reupload_message(
                    message=message,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    post_row=post_row,
                )

                if not sent_message_id:
                    return {
                        "ok": False,
                        "sent_message_id": sent_ids[0] if sent_ids else None,
                        "sent_count": len(sent_ids),
                        "error_text": "Не удалось отправить один из элементов альбома в аварийном fallback",
                    }

                sent_ids.append(int(sent_message_id))

            return {
                "ok": True,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": None,
            }

        except Exception as exc:
            return {
                "ok": False,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": str(exc),
            }

    async def _reupload_album(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
        downloaded_paths: list[Path] = []

        try:
            if not messages:
                return {
                    "ok": False,
                    "sent_message_id": None,
                    "sent_count": 0,
                    "error_text": "Пустой список сообщений для reupload альбома",
                }

            logger.info(
                "REUPLOAD_ALBUM | START | target=%s | thread=%s | items=%s",
                target_id,
                target_thread_id,
                len(messages),
            )

            telethon_result = await self._send_album_via_telethon(
                messages=messages,
                target_id=target_id,
                target_thread_id=target_thread_id,
                post_rows=post_rows,
            )

            logger.info(
                "REUPLOAD_ALBUM | TELETHON_RESULT | ok=%s | sent_message_id=%s | sent_message_ids=%s | sent_count=%s | error=%s",
                telethon_result.get("ok"),
                telethon_result.get("sent_message_id"),
                telethon_result.get("sent_message_ids"),
                telethon_result.get("sent_count"),
                telethon_result.get("error_text"),
            )

            if telethon_result["ok"]:
                return telethon_result

            caption_index = None
            caption_text = None

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, _raw_entities = self._build_text_and_entities_from_content(content)

                text_value = (raw_text or "").strip()
                if text_value:
                    caption_index = idx
                    caption_text = text_value
                    break

            caption_html = None
            caption_plain = None

            if caption_text:
                normalized_caption = _normalize_source_text(caption_text)
                caption_plain = normalized_caption or caption_text

                try:
                    prepared_html = _prepare_html_text(caption_text)
                except Exception:
                    prepared_html = None

                suspicious = False
                prepared_check = prepared_html or ""
                suspicious_patterns = [
                    "*🔥",
                    "**FireFolder",
                    "__HTML_PLACEHOLDER_",
                    "***",
                    "[**",
                    "]**(",
                ]
                for pattern in suspicious_patterns:
                    if pattern in prepared_check:
                        suspicious = True
                        break

                if prepared_html and not suspicious:
                    caption_html = prepared_html
                else:
                    caption_html = None

            media_items = []

            for idx, message in enumerate(messages):
                file_path = await self.telethon.download_media(
                    message,
                    file=str(settings.media_cache_path),
                )
                if not file_path:
                    return {
                        "ok": False,
                        "sent_message_id": None,
                        "sent_count": 0,
                        "error_text": f"Не удалось скачать элемент альбома {idx + 1}/{len(messages)}",
                    }

                path = Path(file_path)
                downloaded_paths.append(path)

                input_file = FSInputFile(path)
                mime, _ = mimetypes.guess_type(path.name)
                mime = (mime or "").lower()

                item_caption = None
                item_parse_mode = None

                if caption_index == idx and caption_text:
                    if caption_html:
                        item_caption = caption_html
                        item_parse_mode = "HTML"
                    else:
                        item_caption = caption_plain
                        item_parse_mode = None

                if mime.startswith("image/"):
                    media_items.append(
                        InputMediaPhoto(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )
                elif mime.startswith("video/"):
                    media_items.append(
                        InputMediaVideo(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )
                else:
                    media_items.append(
                        InputMediaDocument(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                        )
                    )

            sent_messages = await self.bot.send_media_group(
                chat_id=target_id,
                media=media_items,
                message_thread_id=target_thread_id,
            )

            if sent_messages and len(sent_messages) > 0:
                return {
                    "ok": True,
                    "sent_message_id": sent_messages[0].message_id,
                    "sent_message_ids": [int(m.message_id) for m in sent_messages],
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": telethon_result.get("error_text") or "send_media_group вернул пустой результат",
            }

        except Exception as exc:
            logger.exception("reupload_album: ошибка reupload альбома: %s", exc)
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
            }

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass

    async def _fetch_message(self, source_channel, message_id):
        try:
            entity = int(source_channel) if str(source_channel).lstrip("-").isdigit() else source_channel
            return await self.telethon.get_messages(entity, ids=message_id)
        except Exception as exc:
            logger.warning("Telethon не смог получить сообщение %s/%s: %s", source_channel, message_id, exc); return None

    async def _download_video_source(
        self,
        message,
        *,
        delivery_id: int | None = None,
        rule_id: int | None = None,
        post_id: int | None = None,
        source_channel: str | None = None,
        target_id: str | None = None,
        source_message_id: int | None = None,
    ):
        started_at = time.monotonic()
        last_emit_at = 0.0
        last_emit_percent = -1

        cache_dir = settings.media_cache_path
        cache_dir.mkdir(parents=True, exist_ok=True)

        message_id = source_message_id or getattr(message, "id", None) or "unknown"
        delivery_part = delivery_id if delivery_id is not None else "manual"
        ext = getattr(getattr(message, "file", None), "ext", None) or ".mp4"
        if not str(ext).startswith("."):
            ext = f".{ext}"
        download_target_path = cache_dir / f"video_src_{delivery_part}_{message_id}_{int(time.time() * 1000)}{ext}"

        def progress_callback(current: int, total: int):
            nonlocal last_emit_at, last_emit_percent

            now = time.monotonic()
            elapsed = max(now - started_at, 0.001)
            speed = current / elapsed if elapsed > 0 else 0.0
            percent = int((current / total) * 100) if total else 0
            remaining_bytes = max(total - current, 0)
            eta_sec = (remaining_bytes / speed) if speed > 0 else 0.0

            should_emit = False
            if now - last_emit_at >= 1.0:
                should_emit = True
            if percent >= last_emit_percent + 5:
                should_emit = True
            if current == total and total > 0:
                should_emit = True

            if not should_emit:
                return

            last_emit_at = now
            last_emit_percent = percent

            logger.info(
                "📥 Скачивание видео: %s%% | %s из %s | скорость %s | осталось %s",
                percent,
                _format_bytes_ru(current),
                _format_bytes_ru(total),
                _format_speed_ru(speed),
                _format_eta_ru(eta_sec),
            )

            if delivery_id is not None and rule_id is not None:
                try:
                    self._schedule_video_event_log(
                        event_type="video_download_progress",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="processing",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "percent": percent,
                            "downloaded_bytes": current,
                            "total_bytes": total,
                            "speed_bytes_per_sec": round(speed, 2),
                            "eta_sec": int(eta_sec),
                            "downloaded_human": _format_bytes_ru(current),
                            "total_human": _format_bytes_ru(total),
                            "speed_human": _format_speed_ru(speed),
                            "eta_human": _format_eta_ru(eta_sec),
                        },
                    )
                except Exception:
                    pass

        try:
            logger.info("📥 Начинаю скачивание исходного видео...")

            file_path = await self.telethon.download_media(
                message,
                file=str(download_target_path),
                progress_callback=progress_callback,
            )

            if not file_path:
                logger.warning("Не удалось скачать исходное видео: путь не получен")
                return None

            path = Path(file_path)
            if not path.exists() or not path.is_file():
                logger.warning("Не удалось скачать исходное видео: файл не найден после скачивания")
                return None

            file_size = path.stat().st_size
            if file_size <= 0:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                logger.warning("Не удалось скачать исходное видео: файл пустой")
                return None

            elapsed_total = time.monotonic() - started_at
            avg_speed = file_size / elapsed_total if elapsed_total > 0 else 0.0

            logger.info(
                "✅ Скачивание завершено: %s за %.1f сек | средняя скорость %s",
                _format_bytes_ru(file_size),
                elapsed_total,
                _format_speed_ru(avg_speed),
            )

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self._log_video_event_sync,
                        event_type="video_download_completed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="completed",
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                            "file_path": str(path),
                            "downloaded_bytes": file_size,
                            "total_bytes": file_size,
                            "downloaded_human": _format_bytes_ru(file_size),
                            "elapsed_sec": round(elapsed_total, 2),
                            "avg_speed_bytes_per_sec": round(avg_speed, 2),
                            "avg_speed_human": _format_speed_ru(avg_speed),
                        },
                    )
                except Exception:
                    pass

            return path

        except Exception as exc:
            logger.warning("Не удалось скачать исходное видео: %s", exc)

            if delivery_id is not None and rule_id is not None:
                try:
                    await run_db(
                        self._log_video_event_sync,
                        event_type="video_download_failed",
                        delivery_id=delivery_id,
                        rule_id=rule_id,
                        post_id=post_id,
                        status="failed",
                        error_text=str(exc),
                        extra={
                            "source_channel": source_channel,
                            "target_id": target_id,
                            "source_message_id": source_message_id,
                            "stage": "download",
                        },
                    )
                except Exception:
                    pass

            return None

    async def _reupload_message(self, message, target_id, target_thread_id, post_row: dict | None = None):
        content = self._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self._build_text_and_entities_from_content(content)

        if not getattr(message, "media", None):
            logger.info(
                "REUPLOAD_MESSAGE | TEXT_ONLY | target=%s | thread=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            sent_message_id = await self._send_text_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                text=raw_text,
                entities=raw_entities,
            )
            if sent_message_id:
                logger.info(
                    "REUPLOAD_MESSAGE | TELETHON_TEXT_USED | sent_message_id=%s",
                    sent_message_id,
                )
                return sent_message_id

            html_text = _prepare_html_text(raw_text)
            if html_text:
                logger.info("REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | START")
                sent = await self.bot.send_message(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    text=html_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                logger.info(
                    "REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | OK | sent_message_id=%s",
                    sent.message_id,
                )
                return sent.message_id

            logger.warning("REUPLOAD_MESSAGE | TEXT_ONLY | ALL_METHODS_FAILED")
            return None

        file_path = await self.telethon.download_media(message, file=str(settings.media_cache_path))
        if not file_path:
            logger.warning("REUPLOAD_MESSAGE | DOWNLOAD_FAILED")
            return None

        try:
            path = Path(file_path)
            mime, _ = mimetypes.guess_type(path.name)
            mime = (mime or "").lower()

            logger.info(
                "REUPLOAD_MESSAGE | MEDIA | target=%s | thread=%s | file=%s | mime=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                path.name,
                mime,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            sent_message_id = await self._send_file_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                message=message,
                file_path=path,
                force_document=not (mime.startswith("image/") or mime.startswith("video/")),
                post_row=post_row,
            )
            if sent_message_id:
                logger.info(
                    "REUPLOAD_MESSAGE | TELETHON_FILE_USED | sent_message_id=%s",
                    sent_message_id,
                )
                return sent_message_id

            html_text = _prepare_html_text(raw_text)
            input_file = FSInputFile(path)

            logger.info("REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | START | mime=%s", mime)

            if mime.startswith("image/"):
                sent = await self.bot.send_photo(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    photo=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                )
            elif mime.startswith("video/"):
                sent = await self.bot.send_video(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    video=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                    supports_streaming=True,
                )
            else:
                sent = await self.bot.send_document(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    document=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                )

            logger.info(
                "REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | OK | sent_message_id=%s",
                sent.message_id,
            )
            return sent.message_id

        finally:
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception:
                pass
