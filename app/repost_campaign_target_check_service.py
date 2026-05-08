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
    publish_status: str = "unknown"
    delete_status: str = "unknown"
    publish_error_text: str | None = None
    delete_error_text: str | None = None
    source: str = "telethon"
    details: dict | None = None

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
            "publish_status": self.publish_status,
            "delete_status": self.delete_status,
            "publish_error_text": self.publish_error_text,
            "delete_error_text": self.delete_error_text,
            "source": self.source,
            "details": self.details,
        }


class RepostCampaignTargetCheckService:
    def __init__(self, *, telethon_client, bot=None, logger_=None):
        self.telethon_client = telethon_client
        self.bot = bot
        self.logger = logger_ or logging.getLogger("forwarder")

    async def check_target(self, *, target_id: str | int, target_thread_id: int | None = None) -> RepostCampaignTargetCheckResult:
        target_id_str = str(target_id)
        try:
            entity_ref = int(target_id_str) if target_id_str.lstrip("-").isdigit() else target_id_str
            entity = await self.telethon_client.get_entity(entity_ref)
        except Exception:
            return RepostCampaignTargetCheckResult(ok=False, target_id=target_id_str, target_thread_id=target_thread_id, error_text="ViMi пока не видит этот канал/группу.", publish_error_text="ViMi пока не видит этот канал/группу.")

        title = getattr(entity, "title", None) or getattr(entity, "username", None) or target_id_str
        telethon_status = await self._check_telethon(entity)
        bot_status = await self._check_bot_api(target_id_str=target_id_str)
        source = "combined" if self.bot else "telethon"

        publish_status = self._merge_publish(telethon_status.get("publish"), bot_status.get("publish"))
        delete_status = self._merge_delete(telethon_status.get("delete"), bot_status.get("delete"))
        can_delete = True if delete_status == "confirmed" else (False if delete_status == "denied" else None)
        can_publish = publish_status == "confirmed"
        ok = can_publish and delete_status in {"confirmed", "unknown"}

        publish_error = None if publish_status == "confirmed" else "Не удалось подтвердить право публикации" if publish_status == "unknown" else "ViMi пока не видит право публикации в этом канале/группе."
        delete_error = None if delete_status == "confirmed" else "Не удалось подтвердить право удаления" if delete_status == "unknown" else "Нет права удаления"
        error_text = publish_error if publish_status in {"denied", "unknown"} else (delete_error if delete_status == "denied" else None)
        return RepostCampaignTargetCheckResult(
            ok=ok,
            target_id=target_id_str,
            target_thread_id=target_thread_id,
            title=title,
            error_text=error_text,
            can_view=True,
            can_publish=can_publish,
            can_delete=can_delete,
            publish_status=publish_status,
            delete_status=delete_status,
            publish_error_text=publish_error,
            delete_error_text=delete_error,
            source=source,
            details={"telethon": telethon_status, "bot_api": bot_status} if self.bot else {"telethon": telethon_status},
        )

    async def _check_telethon(self, entity) -> dict:
        try:
            try:
                permissions = await self.telethon_client.get_permissions(entity, "me")
            except Exception:
                me = await self.telethon_client.get_me()
                permissions = await self.telethon_client.get_permissions(entity, me)
            is_admin = bool(getattr(permissions, "is_admin", False))
            is_creator = bool(getattr(permissions, "is_creator", False))
            admin_rights = getattr(permissions, "admin_rights", None)
            is_broadcast_channel = bool(getattr(entity, "broadcast", False))
            banned_rights = getattr(permissions, "banned_rights", None)
            banned_send = bool(getattr(banned_rights, "send_messages", False)) if banned_rights else False
            is_megagroup = bool(getattr(entity, "megagroup", False))
            if is_creator:
                publish = "confirmed"
                delete = "confirmed"
            elif banned_send:
                publish = "denied"
                delete = "unknown"
            elif is_broadcast_channel and not is_admin:
                publish = "denied"
                delete = "denied"
            elif is_megagroup and is_admin:
                publish = "confirmed"
                delete = "confirmed" if admin_rights and bool(getattr(admin_rights, "delete_messages", False)) else "unknown"
            elif is_admin and admin_rights is None:
                publish = "confirmed"
                delete = "unknown"
            else:
                can_post = bool(getattr(admin_rights, "post_messages", False)) if admin_rights else bool(is_admin)
                can_delete = bool(getattr(admin_rights, "delete_messages", False)) if admin_rights and hasattr(admin_rights, "delete_messages") else None
                publish = "confirmed" if can_post else "denied"
                delete = "confirmed" if can_delete is True else ("denied" if can_delete is False else "unknown")
            return {"publish": publish, "delete": delete}
        except Exception:
            return {"publish": "unknown", "delete": "unknown"}

    async def _check_bot_api(self, *, target_id_str: str) -> dict:
        if not self.bot:
            return {"publish": "unknown", "delete": "unknown"}
        try:
            me = await self.bot.get_me()
            _ = await self.bot.get_chat(target_id_str)
            member = await self.bot.get_chat_member(target_id_str, me.id)
            status = getattr(member, "status", "")
            if status == "creator":
                return {"publish": "confirmed", "delete": "confirmed"}
            if status != "administrator":
                return {"publish": "denied", "delete": "denied"}
            can_post = getattr(member, "can_post_messages", None)
            can_delete = getattr(member, "can_delete_messages", None)
            publish = "confirmed" if can_post in (True, None) else "denied"
            delete = "confirmed" if can_delete is True else ("denied" if can_delete is False else "unknown")
            return {"publish": publish, "delete": delete}
        except Exception:
            return {"publish": "unknown", "delete": "unknown"}

    def _merge_publish(self, t: str | None, b: str | None) -> str:
        values = {t or "unknown", b or "unknown"}
        if "confirmed" in values:
            return "confirmed"
        if values == {"denied", "unknown"} or values == {"denied"}:
            return "denied"
        return "unknown"

    def _merge_delete(self, t: str | None, b: str | None) -> str:
        values = {t or "unknown", b or "unknown"}
        if "confirmed" in values:
            return "confirmed"
        if values == {"denied"}:
            return "denied"
        if "denied" in values and "unknown" in values:
            return "unknown"
        return "unknown"
