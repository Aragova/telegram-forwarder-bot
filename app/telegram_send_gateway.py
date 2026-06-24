from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .telegram_send_result import TelegramSendResult, telegram_send_result_from_raw


@dataclass(slots=True)
class TelegramSendGateway:
    bot: Any
    telethon_client: Any | None = None

    async def copy_message(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        raw_result = await self.bot.copy_message(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="copy_message")

    async def send_message(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        raw_result = await self.bot.send_message(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="send_message")

    async def send_video(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        raw_result = await self.bot.send_video(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="send_video")

    async def send_document(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        raw_result = await self.bot.send_document(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="send_document")

    async def send_media_group(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        raw_result = await self.bot.send_media_group(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="send_media_group")

    async def telethon_send_file(self, *args: Any, **kwargs: Any) -> TelegramSendResult:
        if self.telethon_client is None:
            raise RuntimeError("telethon_client_is_not_configured")
        raw_result = await self.telethon_client.send_file(*args, **kwargs)
        return telegram_send_result_from_raw(raw_result, method="telethon_send_file")
