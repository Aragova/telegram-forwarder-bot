from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from app.saved_post_renderer import normalize_telethon_target


@dataclass(frozen=True)
class RepostCampaignDeleteResult:
    ok: bool
    method: str
    target_id: str
    message_id: int
    error_text: str | None = None
    message_ids: list[int] | None = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "method": self.method,
            "target_id": self.target_id,
            "message_id": self.message_id,
            "error_text": self.error_text,
            "message_ids": self.message_ids,
        }


class RepostCampaignDeleteService:
    def __init__(self, *, bot, telethon_client=None, logger_=None):
        self.bot = bot
        self.telethon_client = telethon_client
        self.logger = logger_ or logging.getLogger("forwarder")

    async def _delete_with_bot_api(self, *, target_id: int | str, message_id: int) -> RepostCampaignDeleteResult:
        self.logger.info("REPOST_CAMPAIGN_DELETE_TRY_BOT_API | target_id=%s | message_id=%s", target_id, message_id)
        await self.bot.delete_message(chat_id=target_id, message_id=message_id)
        return RepostCampaignDeleteResult(ok=True, method="bot_api", target_id=str(target_id), message_id=message_id, message_ids=[message_id])

    async def _delete_with_telethon(self, *, target_id: int | str, message_id: int) -> RepostCampaignDeleteResult:
        if self.telethon_client is None:
            raise RuntimeError("Telethon client недоступен")
        self.logger.info("REPOST_CAMPAIGN_DELETE_TRY_TELETHON | target_id=%s | message_id=%s", target_id, message_id)
        entity = normalize_telethon_target(target_id)
        await self.telethon_client.delete_messages(entity, [int(message_id)])
        return RepostCampaignDeleteResult(ok=True, method="telethon", target_id=str(target_id), message_id=message_id, message_ids=[message_id])

    async def delete_message(self, *, target_id: int | str, message_id: int, render_mode: str | None = None) -> RepostCampaignDeleteResult:
        prefer_telethon = (render_mode or "").strip().lower() == "telethon_builder"
        bot_error = None
        telethon_error = None

        try_first = [self._delete_with_telethon, self._delete_with_bot_api] if prefer_telethon else [self._delete_with_bot_api, self._delete_with_telethon]
        for method in try_first:
            try:
                result = await method(target_id=target_id, message_id=int(message_id))
                self.logger.info("REPOST_CAMPAIGN_DELETE_DONE | target_id=%s | message_id=%s | method=%s", target_id, message_id, result.method)
                return result
            except Exception as exc:
                if method == self._delete_with_bot_api:
                    bot_error = str(exc)
                else:
                    telethon_error = str(exc)

        error_text = f"Bot API: {bot_error}; Telethon: {telethon_error}"
        self.logger.warning("REPOST_CAMPAIGN_DELETE_FAILED | target_id=%s | message_id=%s | error=%s", target_id, message_id, error_text)
        return RepostCampaignDeleteResult(
            ok=False,
            method="failed",
            target_id=str(target_id),
            message_id=int(message_id),
            error_text=error_text,
        )


    async def delete_messages(self, *, target_id: int | str, message_ids: list[int], render_mode: str | None = None) -> RepostCampaignDeleteResult:
        ids = [int(x) for x in (message_ids or [])]
        if not ids:
            return RepostCampaignDeleteResult(ok=False, method="failed", target_id=str(target_id), message_id=0, message_ids=[], error_text="Нет сообщений для удаления")
        if len(ids) == 1:
            return await self.delete_message(target_id=target_id, message_id=ids[0], render_mode=render_mode)
        prefer_telethon = (render_mode or "").strip().lower() == "telethon_builder"
        try:
            if prefer_telethon:
                entity = normalize_telethon_target(target_id)
                await self.telethon_client.delete_messages(entity, ids)
                return RepostCampaignDeleteResult(ok=True, method="telethon", target_id=str(target_id), message_id=ids[0], message_ids=ids)
            for mid in ids:
                await self.bot.delete_message(chat_id=target_id, message_id=mid)
            return RepostCampaignDeleteResult(ok=True, method="bot_api", target_id=str(target_id), message_id=ids[0], message_ids=ids)
        except Exception as exc:
            if not prefer_telethon:
                try:
                    entity = normalize_telethon_target(target_id)
                    await self.telethon_client.delete_messages(entity, ids)
                    return RepostCampaignDeleteResult(ok=True, method="telethon", target_id=str(target_id), message_id=ids[0], message_ids=ids)
                except Exception as tele_exc:
                    return RepostCampaignDeleteResult(ok=False, method="failed", target_id=str(target_id), message_id=ids[0], message_ids=ids, error_text=f"Bot API: {exc}; Telethon: {tele_exc}")
            return RepostCampaignDeleteResult(ok=False, method="failed", target_id=str(target_id), message_id=ids[0], message_ids=ids, error_text=str(exc))


async def run_repost_campaign_delete_loop(*, runtime, interval_seconds: int = 10, batch_limit: int = 50):
    logger = logging.getLogger("forwarder")
    while True:
        try:
            result = await runtime.process_due_deletions(limit=batch_limit)
            if result.get("claimed", 0) > 0 or result.get("failed", 0) > 0:
                logger.info("REPOST_CAMPAIGN_DELETE_LOOP_TICK | result=%s", result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("REPOST_CAMPAIGN_DELETE_LOOP_FAILED | error=%s", exc)
        await asyncio.sleep(interval_seconds)
