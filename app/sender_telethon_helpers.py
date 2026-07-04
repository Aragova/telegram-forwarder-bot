from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .config import settings
from .sender_primitives import _detect_message_media_kind

logger = logging.getLogger("forwarder")


class SenderTelethonHelpers:
    def __init__(self, owner: Any):
        self.owner = owner

    async def send_text_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        text: str,
        entities,
    ) -> int | None:
        try:
            entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
            formatting_entities = self.owner._clone_telethon_entities(entities, text)

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

            sent = await self.owner.telethon.send_message(**send_kwargs)
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

    async def send_file_via_telethon(
        self,
        *,
        target_id,
        target_thread_id,
        message,
        file_path: Path | None = None,
        force_document: bool = False,
        post_row: dict | None = None,
    ) -> int | None:
        content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)
        formatting_entities = self.owner._clone_telethon_entities(raw_entities, raw_text)

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

            sent = await self.owner.telethon.send_file(**send_kwargs)
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

            sent = await self.owner.telethon.send_file(**send_kwargs)
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

    async def send_album_via_telethon(
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
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self.owner._clone_telethon_entities(caption_entities, caption_text)

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

            sent = await self.owner.telethon.send_file(**send_kwargs)
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
                file_path = await self.owner.telethon.download_media(
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
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

                if raw_text and not caption_text:
                    caption_text = raw_text
                    caption_entities = raw_entities

            formatting_entities = self.owner._clone_telethon_entities(caption_entities, caption_text)

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

            sent = await self.owner.telethon.send_file(**send_kwargs)
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


    async def verify_album_delivery(
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
            fetched = await self.owner.telethon.get_messages(entity, ids=sent_message_ids)
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

