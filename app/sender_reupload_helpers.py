from __future__ import annotations

import logging
import mimetypes
from pathlib import Path

from aiogram.types import FSInputFile, InputMediaDocument, InputMediaPhoto, InputMediaVideo

from .config import settings
from . import sender_primitives as _sender_primitives
from .telegram_send_result import telegram_send_result_from_raw

logger = logging.getLogger("forwarder")


class SenderReuploadHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
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
                sent_message_id = await self.reupload_message(
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

    async def reupload_album(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None):
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

            telethon_result = await self.owner._send_album_via_telethon(
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

            send_result = telegram_send_result_from_raw(
                telethon_result,
                method="reupload_album",
                fallback_sent_ids=telethon_result.get("sent_message_ids"),
                error_text=telethon_result.get("error_text"),
            )
            log_fn = logger.info if send_result.ok else logger.warning
            log_fn(
                "TELEGRAM_SEND_RESULT | method=%s | ok=%s | sent_message_ids=%s | sent_message_id=%s | raw_result_type=%s | error_text=%s | retryable=%s",
                send_result.method, send_result.ok, send_result.sent_message_ids, send_result.sent_message_id, send_result.raw_result_type, send_result.error_text, send_result.retryable
            )
            if send_result.ok:
                telethon_result["sent_message_ids"] = send_result.sent_message_ids
                telethon_result["sent_message_id"] = send_result.sent_message_id
                return telethon_result

            caption_index = None
            caption_text = None

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, _raw_entities = self.owner._build_text_and_entities_from_content(content)

                text_value = (raw_text or "").strip()
                if text_value:
                    caption_index = idx
                    caption_text = text_value
                    break

            caption_html = None
            caption_plain = None

            if caption_text:
                normalized_caption = _sender_primitives._normalize_source_text(caption_text)
                caption_plain = normalized_caption or caption_text

                try:
                    prepared_html = _sender_primitives._prepare_html_text(caption_text)
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
                file_path = await self.owner.telethon.download_media(
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

            sent_messages = await self.owner.bot.send_media_group(
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

    async def reupload_message(self, message, target_id, target_thread_id, post_row: dict | None = None):
        content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

        if not getattr(message, "media", None):
            logger.info(
                "REUPLOAD_MESSAGE | TEXT_ONLY | target=%s | thread=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            sent_message_id = await self.owner._send_text_via_telethon(
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

            html_text = _sender_primitives._prepare_html_text(raw_text)
            if html_text:
                logger.info("REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | START")
                sent = await self.owner.bot.send_message(
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

        file_path = await self.owner.telethon.download_media(message, file=str(settings.media_cache_path))
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

            sent_message_id = await self.owner._send_file_via_telethon(
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

            html_text = _sender_primitives._prepare_html_text(raw_text)
            input_file = FSInputFile(path)

            logger.info("REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | START | mime=%s", mime)

            if mime.startswith("image/"):
                sent = await self.owner.bot.send_photo(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    photo=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                )
            elif mime.startswith("video/"):
                sent = await self.owner.bot.send_video(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    video=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text else None,
                    supports_streaming=True,
                )
            else:
                sent = await self.owner.bot.send_document(
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
