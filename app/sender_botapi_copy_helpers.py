from __future__ import annotations

import logging

from aiogram.methods import CopyMessages
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

from .transport_policy import TransportRateLimited

from .sender_primitives import (
    DEBUG_FORCE_SKIP_COPY_ALBUM,
    DEBUG_FORCE_SKIP_COPY_SINGLE,
)

logger = logging.getLogger("forwarder")


def _propagate_copy_rate_limit(exc: Exception, op_name: str) -> None:
    # A definite rejected copy must reach the worker's deferred retry path.
    # Never call this helper for extraction/verification errors after the RPC.
    if isinstance(exc, TransportRateLimited):
        raise exc
    if isinstance(exc, TelegramRetryAfter):
        raise TransportRateLimited(
            retry_after_seconds=max(1, int(exc.retry_after)),
            backend="bot",
            op_name=op_name,
            key=f"sender.bot.{op_name}",
        ) from exc


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

        rpc_returned = False
        try:
            sent = await self.owner.bot.copy_message(
                chat_id=target_id,
                from_chat_id=source_channel,
                message_id=message_id,
                message_thread_id=target_thread_id,
            )
            rpc_returned = True
            sent_ids = self.owner._extract_sent_message_ids(sent)
            return {
                "attempted": True,
                "sent_ids": sent_ids,
                "fallback_allowed": False,
                "raw_result_type": type(sent).__name__,
                "raw_result": sent,
            }
        except Exception as exc:
            if not rpc_returned:
                _propagate_copy_rate_limit(exc, "copy_message")
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
                "definitely_rejected": not rpc_returned and isinstance(exc, TelegramBadRequest),
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

        original_ids = list(message_ids)
        sorted_ids = sorted(original_ids)
        rpc_returned = False
        try:
            sent_messages = await self.owner.bot(
                CopyMessages(
                    chat_id=target_id,
                    from_chat_id=source_channel,
                    message_ids=sorted_ids,
                    message_thread_id=target_thread_id,
                )
            )

            rpc_returned = True
            if sent_messages and len(sent_messages) > 0:
                sent_ids = [int(m.message_id) for m in sent_messages]
                if len(sent_ids) == len(sorted_ids) and len(set(sorted_ids)) == len(sorted_ids):
                    by_source = dict(zip(sorted_ids, sent_ids))
                    sent_ids = [by_source[source_id] for source_id in original_ids]
                return {
                    "ok": True,
                    "sent_message_id": sent_ids[0],
                    "sent_message_ids": sent_ids,
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
            if not rpc_returned:
                _propagate_copy_rate_limit(exc, "copy_messages")
            return {
                "ok": False,
                "sent_message_id": None,
                "sent_count": 0,
                "error_text": str(exc),
                "attempted": True,
                "definitely_rejected": not rpc_returned and isinstance(exc, TelegramBadRequest),
            }
