from __future__ import annotations

import json
import logging
from typing import Any

from telethon import types

from .delivery_content_helpers import (
    _iter_content_entities,
    content_requires_builder,
    extract_text_from_content,
    normalize_caption_delivery_mode,
)
from .sender_primitives import (
    _detect_message_media_kind,
    _utf16_text_length,
)

logger = logging.getLogger("forwarder")


class SenderContentHelpers:
    def __init__(self, owner):
        self.owner = owner

    def caption_entity_counts(self, entities) -> dict[str, int]:
        counts = {"total": 0, "custom_emoji": 0, "urls": 0, "bold": 0, "italic": 0}
        for entity in entities or []:
            try:
                entity_type = str(entity.get("type") if isinstance(entity, dict) else getattr(entity, "type", entity.__class__.__name__) or "").lower()
            except Exception:
                entity_type = ""
            counts["total"] += 1
            if entity_type in {"custom_emoji", "messageentitycustomemoji"} or "customemoji" in entity_type:
                counts["custom_emoji"] += 1
            elif entity_type in {"url", "text_link", "messageentityurl", "messageentitytexturl"}:
                counts["urls"] += 1
            elif entity_type in {"bold", "messageentitybold"}:
                counts["bold"] += 1
            elif entity_type in {"italic", "messageentityitalic"}:
                counts["italic"] += 1
        return counts


    def log_caption_entity_inventory(self, *, source: str, rule_id=None, message_ids=None, entities=None) -> None:
        counts = self.caption_entity_counts(entities or [])
        logger.info(
            "CAPTION_ENTITY_INVENTORY | source=%s | rule_id=%s | message_ids=%s | total=%s | custom_emoji=%s | urls=%s | bold=%s | italic=%s",
            source,
            rule_id,
            message_ids or [],
            counts["total"],
            counts["custom_emoji"],
            counts["urls"],
            counts["bold"],
            counts["italic"],
        )


    def content_from_message_or_post(self, message=None, post_row=None) -> dict:
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

        post_content = None
        if post_row is not None:
            content = _row_value(post_row, "content_json")

            if isinstance(content, dict):
                post_content = content

            if post_content is None and isinstance(content, str) and content.strip():
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict):
                        post_content = parsed
                except Exception:
                    logger.warning(
                        "CONTENT_FROM_POST_ROW | не удалось распарсить content_json | type=%s",
                        type(post_row).__name__,
                    )

            if post_content is not None and message is None:
                return post_content

        live_content = None
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

            live_content = {
                "text": text,
                "entities": entities_payload,
                "has_media": bool(getattr(message, "media", None)),
                "media_kind": _detect_message_media_kind(message),
                "date": getattr(getattr(message, "date", None), "isoformat", lambda: None)(),
            }

        if post_content is not None:
            live_entities = (live_content or {}).get("entities") or []
            post_entities = list(_iter_content_entities(post_content))
            if not post_entities and live_entities:
                post_text = str(post_content.get("text") or "")
                live_text = str((live_content or {}).get("text") or "")
                normalized_post_text = post_text.replace("\r\n", "\n").replace("\r", "\n")
                normalized_live_text = live_text.replace("\r\n", "\n").replace("\r", "\n")

                if post_text == live_text or normalized_post_text == normalized_live_text:
                    merged = dict(post_content)
                    merged["entities"] = live_entities
                    if not merged.get("text") and live_text:
                        merged["text"] = live_text
                    merged.setdefault("has_media", (live_content or {}).get("has_media", False))
                    merged.setdefault("media_kind", (live_content or {}).get("media_kind", "text"))
                    merged.setdefault("date", (live_content or {}).get("date"))
                    logger.warning(
                        "CONTENT_FROM_MESSAGE_OR_POST | merged_live_entities | post_entities=%s | live_entities=%s | result_entities=%s",
                        len(post_entities),
                        len(live_entities),
                        len(merged.get("entities") or []),
                    )
                    return merged

                logger.warning(
                    "CONTENT_FROM_MESSAGE_OR_POST | prefer_live_content_text_mismatch | post_text_len=%s | live_text_len=%s | post_entities=%s | live_entities=%s",
                    len(post_text),
                    len(live_text),
                    len(post_entities),
                    len(live_entities),
                )
                return live_content
            return post_content

        if live_content is not None:
            return live_content

        return {
            "text": "",
            "entities": [],
            "has_media": False,
            "media_kind": "text",
            "date": None,
        }


    def build_telethon_entities_from_content(self, content: dict | None, text: str) -> list:
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


    def build_text_and_entities_from_content(self, content: dict | None) -> tuple[str, list]:
        text = extract_text_from_content(content)
        raw_entities = (content or {}).get("entities") or []
        in_counts = self.caption_entity_counts(raw_entities)
        entities = self.build_telethon_entities_from_content(content, text)
        out_counts = self.caption_entity_counts(entities)
        logger.info(
            "CAPTION_ENTITY_BUILD_RESULT | source=album | rule_id=%s | total_in=%s | total_out=%s | custom_emoji_in=%s | custom_emoji_out=%s",
            (content or {}).get("rule_id"),
            in_counts["total"],
            out_counts["total"],
            in_counts["custom_emoji"],
            out_counts["custom_emoji"],
        )
        return text, entities


    def clone_telethon_entities(self, entities, text: str | None = None) -> list:
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


    def get_album_primary_text(self, messages, post_rows: list[dict] | None = None) -> str | None:
        for idx, message in enumerate(messages):
            post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
            content = self.content_from_message_or_post(message=message, post_row=post_row)
            raw_text, _raw_entities = self.build_text_and_entities_from_content(content)

            text_value = (raw_text or "").strip()
            if text_value:
                return text_value

        return None


    def get_rule_video_caption_delivery_mode(self, rule) -> str:
        return normalize_caption_delivery_mode(getattr(rule, "video_caption_delivery_mode", "auto"))


    def resolve_repost_caption_delivery_strategy(
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
        configured_mode = self.get_rule_caption_delivery_mode(rule)

        if is_album:
            requires_builder = self.album_requires_builder(rule, source_channel, message_ids)
        else:
            first_message_id = int(message_ids[0]) if message_ids else 0
            requires_builder = self.single_requires_builder(rule, source_channel, first_message_id)

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


    def get_rule_caption_delivery_mode(self, rule) -> str:
        """
        Режим подписи для обычного репоста.

        Использует ТОЛЬКО поле caption_delivery_mode.
        Не смешивать с video_caption_delivery_mode.
        """
        return normalize_caption_delivery_mode(getattr(rule, "caption_delivery_mode", "auto"))


    def content_requires_builder(self, content: dict | None) -> bool:
        return content_requires_builder(content)


    def get_post_row_for_rule_message(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> dict | None:
        try:
            return self.owner.db.get_post(
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


    def single_requires_builder(
        self,
        rule,
        source_channel: str,
        message_id: int,
    ) -> bool | dict[str, object]:
        post_row = self.get_post_row_for_rule_message(rule, source_channel, message_id)
        if not post_row:
            return False

        content = self.content_from_message_or_post(message=None, post_row=post_row)
        needs_builder = self.content_requires_builder(content)

        counts = self.caption_entity_counts(list(_iter_content_entities(content)))
        logger.info(
            "CAPTION_MODE_DETECT | single | rule_id=%s | message_id=%s | entities=%s | custom_emoji=%s | requires_builder=%s",
            getattr(rule, "id", None),
            message_id,
            counts["total"],
            counts["custom_emoji"],
            needs_builder,
        )
        return needs_builder


    def album_requires_builder(
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
            post_row = self.get_post_row_for_rule_message(rule, source_channel, int(message_id))
            if not post_row:
                continue

            content = self.content_from_message_or_post(message=None, post_row=post_row)
            if self.content_requires_builder(content):
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

