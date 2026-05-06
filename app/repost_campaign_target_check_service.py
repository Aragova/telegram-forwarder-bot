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
            error_text = "ViMi пока не видит этот канал/группу. Проверьте, что канал добавлен правильно и доступен для аккаунта ViMi."
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
            banned_rights = getattr(permissions, "banned_rights", None)
            can_post_messages = bool(getattr(admin_rights, "post_messages", False)) if admin_rights else False
            can_delete_messages = bool(getattr(admin_rights, "delete_messages", False)) if admin_rights and hasattr(admin_rights, "delete_messages") else None
            banned_send_messages = bool(getattr(banned_rights, "send_messages", False)) if banned_rights else False
            is_broadcast_channel = bool(getattr(entity, "broadcast", False))
            is_megagroup = bool(getattr(entity, "megagroup", False))
            entity_type = type(entity).__name__
            if is_creator:
                can_publish = True
            elif is_broadcast_channel:
                can_publish = bool(is_admin and can_post_messages)
            elif is_megagroup or not is_broadcast_channel:
                can_publish = bool((is_admin or is_creator) and not banned_send_messages)
            else:
                can_publish = bool(is_creator or is_admin or can_post_messages)
            can_delete = True if is_creator else can_delete_messages
            self.logger.info(
                "REPOST_CAMPAIGN_TARGET_PERMISSIONS | target_id=%s | title=%s | entity_type=%s | broadcast=%s | megagroup=%s | is_admin=%s | is_creator=%s | can_post_messages=%s | can_delete_messages=%s | banned_send_messages=%s | can_publish=%s | can_delete=%s",
                target_id_str, title, entity_type, is_broadcast_channel, is_megagroup, is_admin, is_creator, can_post_messages, can_delete_messages, banned_send_messages, can_publish, can_delete,
            )
            if not can_publish:
                error_text = "ViMi пока не видит право публикации в этом канале/группе. Проверьте роль администратора и разрешение на отправку сообщений."
                self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_FAILED | target_id=%s | error=%s", target_id_str, error_text)
                return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, title=title, error_text=error_text, can_view=True, can_publish=False, can_delete=can_delete)
            self.logger.info("REPOST_CAMPAIGN_TARGET_CHECK_DONE | target_id=%s | title=%s | can_publish=%s | can_delete=%s", target_id_str, title, True, can_delete)
            return RepostCampaignTargetCheckResult(ok=True, target_id=target_id_str, target_thread_id=target_thread_id, title=title, can_view=True, can_publish=True, can_delete=can_delete)
        except Exception:
            error_text = "Не удалось проверить доступ к публикации. Проверьте, что аккаунт ViMi добавлен в администраторы канала/группы."
            self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_FAILED | target_id=%s | error=%s", target_id_str, error_text)
            return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, title=title, error_text=error_text, can_view=True)
