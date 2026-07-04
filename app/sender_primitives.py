from __future__ import annotations

import html
import re

from aiogram.types import MessageEntity
from telethon import types
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

MAX_INVALID_MP4_RETRY = 1
MAX_NORMAL_REACTION_ATTEMPTS = 3

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
