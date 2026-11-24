from __future__ import annotations

import logging
import mimetypes
from pathlib import Path

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, TelegramRetryAfter
from telethon.errors import FloodWaitError
from .transport_policy import TransportRateLimited
from .send_intent import PublicationWriteUnavailable, possible_write
from .publication_transport_guard import PublicationContentError
from .telegram_publication import botapi_publication_ids
from aiogram.types import FSInputFile, InputMediaDocument, InputMediaPhoto, InputMediaVideo

from .config import settings
from . import sender_primitives as _sender_primitives
from .telegram_send_result import telegram_send_result_from_raw
from .telethon_authoritative_resolver import TelethonSendOutcome, telethon_transport_failed, telethon_not_attempted

logger = logging.getLogger("forwarder")


class SenderReuploadHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def send_album_one_by_one(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None, caption_override: dict | None = None):
        sent_ids: list[int] = []
        accepted_transport_count = 0

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
                outcome = await self.reupload_message(
                    message=message,
                    target_id=target_id,
                    target_thread_id=target_thread_id,
                    post_row=post_row,
                    **({"caption_override": (caption_override if idx == 0 else {"text": "", "entities": []})}
                       if caption_override is not None else {}),
                )

                if isinstance(outcome, TelethonSendOutcome):
                    if outcome.transport_accepted:
                        accepted_transport_count += 1
                    if outcome.transport_accepted and outcome.authoritative_resolved and outcome.authoritative_message_id:
                        sent_ids.append(int(outcome.authoritative_message_id))
                        continue
                    if outcome.transport_attempted and not outcome.transport_accepted:
                        return {
                            "ok": False,
                            "transport_attempted": True,
                            "transport_accepted": False,
                            "authoritative_resolved": False,
                            "sent_message_id": sent_ids[0] if sent_ids else None,
                            "sent_message_ids": sent_ids[:],
                            "sent_count": len(sent_ids),
                            "error_text": outcome.error_text or "uncertain_one_by_one_transport",
                            "manual_review_required": True,
                            "non_retryable": True,
                            "action": "no_second_send",
                        }
                    if outcome.transport_accepted and not outcome.authoritative_resolved:
                        return {
                            "ok": False,
                            "transport_accepted": True,
                            "authoritative_resolved": False,
                            "sent_message_id": None,
                            "sent_message_ids": [],
                            "sent_count": accepted_transport_count,
                            "returned_candidate_id": outcome.returned_candidate_id,
                            "returned_candidate_ids": outcome.returned_candidate_ids or [],
                            "resolved_authoritative_message_ids_before_unresolved": sent_ids[:],
                            "resolution_method": outcome.resolution_method,
                            "error_text": outcome.error_text,
                            "manual_review_required": True,
                            "non_retryable": True,
                            "action": "no_second_send",
                        }
                    return {
                        "ok": False,
                        "transport_accepted": bool(sent_ids),
                        "authoritative_resolved": False,
                        "sent_message_id": sent_ids[0] if sent_ids else None,
                        "sent_message_ids": sent_ids[:],
                        "sent_count": len(sent_ids),
                        "error_text": outcome.error_text or "Не удалось отправить один из элементов альбома в аварийном fallback",
                    }

                if not outcome:
                    return {
                        "ok": False,
                        "sent_message_id": sent_ids[0] if sent_ids else None,
                        "sent_message_ids": sent_ids[:],
                        "sent_count": len(sent_ids),
                        "error_text": "Не удалось отправить один из элементов альбома в аварийном fallback",
                    }
                legacy_sent_message_id = outcome
                sent_ids.append(int(legacy_sent_message_id))

            return {
                "ok": True,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": None,
            }

        except (TelegramRetryAfter, FloodWaitError, TransportRateLimited, PublicationWriteUnavailable) as exc:
            if not sent_ids and not accepted_transport_count and not possible_write():
                if isinstance(exc, (TelegramRetryAfter, FloodWaitError)):
                    delay = exc.retry_after if isinstance(exc, TelegramRetryAfter) else exc.seconds
                    raise TransportRateLimited(
                        retry_after_seconds=max(1, int(delay)), backend="telegram",
                        op_name="send_album_one_by_one", key=str(target_id),
                    ) from exc
                raise
            # A later rejection cannot undo an earlier accepted or uncertain send.
            return {
                "ok": False,
                "transport_attempted": True,
                "transport_accepted": bool(sent_ids or accepted_transport_count),
                "authoritative_resolved": False,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": str(exc),
                "manual_review_required": True,
                "non_retryable": True,
                "action": "no_second_send",
            }

        except Exception as exc:
            # A raised send may have committed even when the first ID is absent.
            return {
                "ok": False,
                "transport_attempted": True,
                "transport_accepted": bool(sent_ids),
                "authoritative_resolved": False,
                "sent_message_id": sent_ids[0] if sent_ids else None,
                "sent_message_ids": sent_ids[:],
                "sent_count": len(sent_ids),
                "error_text": str(exc),
            }

    async def reupload_album(self, messages, target_id, target_thread_id, post_rows: list[dict] | None = None, is_self_loop: bool = False, caption_override: dict | None = None):
        downloaded_paths: list[Path] = []
        botapi_entered = False

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
                is_self_loop=is_self_loop,
                **({"caption_override": caption_override} if caption_override is not None else {}),
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
            if telethon_result.get("transport_attempted") or telethon_result.get("transport_accepted"):
                logger.warning("REUPLOAD_ALBUM | TELETHON_UNCERTAIN | no_botapi_fallback=True")
                return telethon_result

            caption_index = 0 if caption_override is not None else None
            caption_text = str(caption_override["text"]) if caption_override is not None else None
            caption_entities = None
            if caption_override is not None:
                _text, tl_entities = self.owner._build_text_and_entities_from_content(caption_override)
                caption_entities = _sender_primitives._telethon_entities_to_bot(tl_entities)

            for idx, message in enumerate(messages):
                post_row = post_rows[idx] if post_rows and idx < len(post_rows) else None
                content = self.owner._content_from_message_or_post(message=message, post_row=post_row)
                raw_text, _raw_entities = self.owner._build_text_and_entities_from_content(content)

                text_value = (raw_text or "").strip()
                if text_value and caption_override is None:
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
                item_entities = None

                if caption_index == idx and caption_text:
                    if caption_override is not None:
                        item_caption = caption_text
                        item_entities = caption_entities
                    elif caption_html:
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
                            caption_entities=item_entities,
                        )
                    )
                elif mime.startswith("video/"):
                    media_items.append(
                        InputMediaVideo(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                            caption_entities=item_entities,
                        )
                    )
                else:
                    media_items.append(
                        InputMediaDocument(
                            media=input_file,
                            caption=item_caption,
                            parse_mode=item_parse_mode,
                            caption_entities=item_entities,
                        )
                    )

            if not 2 <= len(media_items) <= 10:
                return {"ok": False, "sent_count": 0, "error_text": "Недопустимый размер медиагруппы"}
            botapi_entered = True
            sent_messages = await self.owner.bot.send_media_group(
                chat_id=target_id,
                media=media_items,
                message_thread_id=target_thread_id,
            )

            ids = botapi_publication_ids(sent_messages, len(media_items))
            if ids:
                return {
                    "ok": True, "transport_attempted": True,
                    "transport_accepted": True, "authoritative_resolved": True,
                    "sent_message_id": ids[0], "sent_message_ids": ids,
                    "sent_count": len(ids), "error_text": None,
                }
            return {
                "ok": False, "transport_attempted": True,
                "transport_accepted": True, "authoritative_resolved": False,
                "accepted_unresolved": True, "sent_message_ids": [],
                "raw_message_ids": [getattr(m, "message_id", None) for m in (sent_messages or [])],
                "sent_count": len(sent_messages or []),
                "error_text": "botapi_accepted_partial_or_zero_ids_no_resend",
            }

        except FloodWaitError as exc:
            raise TransportRateLimited(
                retry_after_seconds=max(1, int(exc.seconds)), backend="telethon",
                op_name="reupload_album", key=str(target_id),
            ) from exc
        except (TelegramRetryAfter, TransportRateLimited, PublicationWriteUnavailable):
            raise
        except (TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, PublicationContentError) as exc:
            return {"ok": False, "sent_message_id": None, "sent_count": 0,
                    "definitely_rejected": True, "error_text": str(exc)}
        except Exception as exc:
            logger.exception("reupload_album: ошибка reupload альбома: %s", exc)
            return {
                "ok": False, "sent_message_id": None, "sent_count": 0,
                "transport_attempted": botapi_entered,
                "transport_accepted": False,
                "error_text": str(exc),
            }

        finally:
            for path in downloaded_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass

    async def reupload_message(self, message, target_id, target_thread_id, post_row: dict | None = None, is_self_loop: bool = False, caption_override: dict | None = None) -> TelethonSendOutcome:
        content = caption_override if caption_override is not None else self.owner._content_from_message_or_post(message=message, post_row=post_row)
        raw_text, raw_entities = self.owner._build_text_and_entities_from_content(content)

        if not getattr(message, "media", None) or (
            caption_override is not None
            and _sender_primitives._is_link_preview_only(message)
        ):
            logger.info(
                "REUPLOAD_MESSAGE | TEXT_ONLY | target=%s | thread=%s | text_len=%s | entities=%s",
                target_id,
                target_thread_id,
                len(raw_text or ""),
                len(raw_entities or []),
            )

            telethon_outcome = await self.owner._send_text_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                text=raw_text,
                entities=raw_entities,
                source_message_ids=({int(getattr(message, "id"))} if is_self_loop and getattr(message, "id", None) else None),
                **({"literal_caption": True} if caption_override is not None else {}),
            )
            if isinstance(telethon_outcome, int) and telethon_outcome > 0:
                return TelethonSendOutcome(True, True, True, int(telethon_outcome), [int(telethon_outcome)], resolution_method="legacy_telethon_helper")
            if getattr(telethon_outcome, "transport_attempted", False) or getattr(telethon_outcome, "transport_accepted", False):
                logger.warning("REUPLOAD_MESSAGE | TELETHON_TEXT_UNCERTAIN | no_botapi_fallback=True")
                return telethon_outcome

            html_text = raw_text if caption_override is not None else _sender_primitives._prepare_html_text(raw_text)
            bot_entities = _sender_primitives._telethon_entities_to_bot(raw_entities) if caption_override is not None else None
            if html_text:
                logger.info("REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | START")
                sent = await self.owner.bot.send_message(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    text=html_text,
                    parse_mode=None if caption_override is not None else "HTML",
                    entities=bot_entities,
                    disable_web_page_preview=True,
                )
                ids = botapi_publication_ids(sent, 1)
                if not ids:
                    return TelethonSendOutcome(True, True, False, None, [], resolution_method="botapi_pending_unresolved")
                logger.info("REUPLOAD_MESSAGE | BOTAPI_TEXT_FALLBACK | OK | sent_message_id=%s", ids[0])
                return TelethonSendOutcome(True, True, True, ids[0], ids, resolution_method="botapi_text_fallback")

            logger.warning("REUPLOAD_MESSAGE | TEXT_ONLY | ALL_METHODS_FAILED")
            return telethon_outcome if isinstance(telethon_outcome, TelethonSendOutcome) else telethon_not_attempted("text_only_all_methods_failed")

        reader = getattr(self.owner, "telethon_read", None) or self.owner.telethon
        if reader is None:
            raise PublicationWriteUnavailable("MTProto source reader is not ready")
        try:
            file_path = await reader.download_media(message, file=str(settings.media_cache_path))
        except FloodWaitError as exc:
            raise TransportRateLimited(
                retry_after_seconds=max(1, int(exc.seconds)), backend="telethon",
                op_name="download_media", key=str(target_id),
            ) from exc
        if not file_path:
            logger.warning("REUPLOAD_MESSAGE | DOWNLOAD_FAILED")
            return telethon_not_attempted("download_failed_before_send")

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

            telethon_outcome = await self.owner._send_file_via_telethon(
                target_id=target_id,
                target_thread_id=target_thread_id,
                message=message,
                file_path=path,
                force_document=not (mime.startswith("image/") or mime.startswith("video/")),
                post_row=post_row,
                is_self_loop=is_self_loop,
                **({"caption_override": caption_override} if caption_override is not None else {}),
            )
            if isinstance(telethon_outcome, int) and telethon_outcome > 0:
                return TelethonSendOutcome(True, True, True, int(telethon_outcome), [int(telethon_outcome)], resolution_method="legacy_telethon_helper")
            if getattr(telethon_outcome, "transport_attempted", False) or getattr(telethon_outcome, "transport_accepted", False):
                logger.warning("REUPLOAD_MESSAGE | TELETHON_FILE_UNCERTAIN | no_botapi_fallback=True")
                return telethon_outcome

            html_text = raw_text if caption_override is not None else _sender_primitives._prepare_html_text(raw_text)
            bot_entities = _sender_primitives._telethon_entities_to_bot(raw_entities) if caption_override is not None else None
            input_file = FSInputFile(path)

            logger.info("REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | START | mime=%s", mime)

            if mime.startswith("image/"):
                sent = await self.owner.bot.send_photo(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    photo=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text and caption_override is None else None,
                    caption_entities=bot_entities,
                )
            elif mime.startswith("video/"):
                sent = await self.owner.bot.send_video(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    video=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text and caption_override is None else None,
                    caption_entities=bot_entities,
                    supports_streaming=True,
                )
            else:
                sent = await self.owner.bot.send_document(
                    chat_id=target_id,
                    message_thread_id=target_thread_id,
                    document=input_file,
                    caption=html_text,
                    parse_mode="HTML" if html_text and caption_override is None else None,
                    caption_entities=bot_entities,
                )

            ids = botapi_publication_ids(sent, 1)
            if not ids:
                return TelethonSendOutcome(True, True, False, None, [], resolution_method="botapi_pending_unresolved")
            logger.info("REUPLOAD_MESSAGE | BOTAPI_MEDIA_FALLBACK | OK | sent_message_id=%s", ids[0])
            return TelethonSendOutcome(True, True, True, ids[0], ids, resolution_method="botapi_media_fallback")

        except (TelegramRetryAfter, FloodWaitError, TransportRateLimited, PublicationWriteUnavailable):
            raise
        except (TelegramBadRequest, TelegramForbiddenError, TelegramEntityTooLarge, PublicationContentError) as exc:
            return telethon_not_attempted(str(exc))
        except Exception as exc:
            return telethon_transport_failed(str(exc))
        finally:
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception:
                pass
