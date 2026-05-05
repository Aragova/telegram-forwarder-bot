from __future__ import annotations

import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class RepostCampaignTargetCheckResult:
    ok: bool
    target_id: str
    target_thread_id: int | None = None
    title: str | None = None
    error_text: str | None = None
    can_view: bool = False
    can_publish: bool = False
    can_delete: bool | None = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "target_id": self.target_id,
            "target_thread_id": self.target_thread_id,
            "title": self.title,
            "error_text": self.error_text,
            "can_view": self.can_view,
            "can_publish": self.can_publish,
            "can_delete": self.can_delete,
        }


class RepostCampaignTargetCheckService:
    def __init__(self, *, telethon_client, logger_=None):
        self.telethon_client = telethon_client
        self.logger = logger_ or logging.getLogger("forwarder")

    async def check_target(self, *, target_id: str | int, target_thread_id: int | None = None) -> RepostCampaignTargetCheckResult:
        target_id_str = str(target_id)
        self.logger.info("REPOST_CAMPAIGN_TARGET_CHECK_START | target_id=%s | thread_id=%s", target_id_str, target_thread_id)
        try:
            entity_ref = int(target_id_str) if target_id_str.lstrip("-").isdigit() else target_id_str
            entity = await self.telethon_client.get_entity(entity_ref)
        except Exception:
            error_text = "Аккаунт-парсер не видит канал/группу"
            self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_FAILED | target_id=%s | error=%s", target_id_str, error_text)
            return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, error_text=error_text)

        title = getattr(entity, "title", None) or getattr(entity, "username", None) or target_id_str
        try:
            try:
                permissions = await self.telethon_client.get_permissions(entity, "me")
            except Exception:
                me = await self.telethon_client.get_me()
                permissions = await self.telethon_client.get_permissions(entity, me)
            is_admin = bool(getattr(permissions, "is_admin", False))
            is_creator = bool(getattr(permissions, "is_creator", False))
            admin_rights = getattr(permissions, "admin_rights", None)
            can_post_messages = bool(getattr(admin_rights, "post_messages", False)) if admin_rights else False
            can_delete_messages = bool(getattr(admin_rights, "delete_messages", False)) if admin_rights else None
            can_publish = bool(is_creator or (is_admin and can_post_messages))
            if not can_publish:
                error_text = "Аккаунт-парсер не имеет права публиковать в канал/группу"
                self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_FAILED | target_id=%s | error=%s", target_id_str, error_text)
                return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, title=title, error_text=error_text, can_view=True, can_publish=False, can_delete=can_delete_messages)
            self.logger.info("REPOST_CAMPAIGN_TARGET_CHECK_DONE | target_id=%s | title=%s | can_publish=%s | can_delete=%s", target_id_str, title, True, can_delete_messages)
            return RepostCampaignTargetCheckResult(ok=True, target_id=target_id_str, target_thread_id=target_thread_id, title=title, can_view=True, can_publish=True, can_delete=can_delete_messages)
        except Exception:
            error_text = "Не удалось подтвердить право публикации. Проверьте, что аккаунт-парсер добавлен как администратор."
            self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_FAILED | target_id=%s | error=%s", target_id_str, error_text)
            return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, title=title, error_text=error_text, can_view=True)
