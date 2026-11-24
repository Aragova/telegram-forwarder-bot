from __future__ import annotations

import logging

from aiogram.methods import CopyMessages
from aiogram.exceptions import TelegramBadRequest

from .telegram_publication import botapi_publication_ids

from .sender_primitives import (
    DEBUG_FORCE_SKIP_COPY_ALBUM,
    DEBUG_FORCE_SKIP_COPY_SINGLE,
)

logger = logging.getLogger("forwarder")


class SenderBotApiCopyHelpers:
    def __init__(self, owner):
        self.owner = owner

    async def copy_single_via_bot(
        self,
        source_channel,
        target_id,
        message_id,
        target_thread_id,
    ):
        if DEBUG_FORCE_SKIP_COPY_SINGLE:
            logger.warning(
                "COPY_SINGLE | TEST MODE | принудительно пропускаю Bot API copy_message для проверки Telethon"
            )
            return {
                "attempted": False,
                "sent_ids": [],
                "fallback_allowed": True,
                "raw_result_type": "debug_skip",
            }

        try:
            sent = await self.owner.bot.copy_message(
                chat_id=target_id,
                from_chat_id=source_channel,
                message_id=message_id,
                message_thread_id=target_thread_id,
            )
            if getattr(sent, "message_id", None) == 0:
                logger.info(
                    "TELEGRAM_BOTAPI_VIDEO_PROCESSING_PENDING | target_id=%s | source_message_id=%s | action=no_second_send",
                    target_id, message_id,
                )
            sent_ids = botapi_publication_ids(sent,1)
            return {
                "accepted_unresolved": not bool(sent_ids),
                "attempted": True,
                "sent_ids": sent_ids,
                "fallback_allowed": False,
                "raw_result_type": type(sent).__name__,
                "raw_result": sent,
            }
        except Exception as exc:
            logger.warning(
                "Не удалось скопировать сообщение %s/%s в %s: %s",
                source_channel,
                message_id,
                target_id,
                exc,
            )
            return {
                "attempted": True,
                "sent_ids": [],
                "fallback_allowed": False,
                "raw_result_type": "exception",
                "error_text": str(exc),
                "definitely_rejected": isinstance(exc, TelegramBadRequest),
            }

    async def copy_album_via_bot(
        self,
        source_channel,
        target_id,
        message_ids,
        target_thread_id,
    ):
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
            sent_messages = await self.owner.bot(
                CopyMessages(
                    chat_id=target_id,
                    from_chat_id=source_channel,
                    message_ids=message_ids,
                    message_thread_id=target_thread_id,
                )
            )

            if sent_messages and len(sent_messages) > 0:
                confirmed_ids = botapi_publication_ids(sent_messages,len(message_ids))
                if not confirmed_ids:
                    logger.info(
                        "TELEGRAM_BOTAPI_ALBUM_PROCESSING_PENDING | target_id=%s | source_message_ids=%s | action=no_second_send",
                        target_id, message_ids,
                    )
                if not confirmed_ids:
                    return {
                        "ok": False, "attempted": True, "definitely_rejected": False,
                        "accepted_unresolved": True, "sent_message_id": None,
                        "sent_message_ids": [], "sent_count": len(sent_messages),
                        "raw_message_ids": [m.message_id for m in sent_messages],
                        "error_text": "botapi_accepted_partial_or_zero_ids_no_resend",
                    }
                return {
                    "ok": True,
                    "attempted": True,
                    "sent_message_id": confirmed_ids[0],
                    "sent_message_ids": confirmed_ids,
                    "sent_count": len(sent_messages),
                    "error_text": None,
                }

            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": "CopyMessages вернул пустой результат",
                "attempted": True,
                "definitely_rejected": False,
            }

        except Exception as exc:
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
                "attempted": True,
                "definitely_rejected": isinstance(exc, TelegramBadRequest),
            }
