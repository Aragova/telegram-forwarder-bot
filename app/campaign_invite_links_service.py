"""Service helpers for Telegram campaign invite links."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

try:
    from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramForbiddenError
except ImportError:  # pragma: no cover - keeps tests importable without aiogram
    TelegramAPIError = TelegramBadRequest = TelegramForbiddenError = Exception


INVITE_LINK_MODE_JOIN_REQUEST = "join_request"
INVITE_LINK_MODE_DIRECT_JOIN = "direct_join"

INVITE_LINK_STATUS_ACTIVE = "active"
INVITE_LINK_STATUS_REVOKED = "revoked"
INVITE_LINK_STATUS_FAILED = "failed"

_ALLOWED_LINK_MODES = {INVITE_LINK_MODE_JOIN_REQUEST, INVITE_LINK_MODE_DIRECT_JOIN}
_MAX_INVITE_LINK_NAME_LENGTH = 32


def build_invite_link_hash(invite_link: str) -> str:
    normalized = (invite_link or "").strip()
    if not normalized:
        raise ValueError("invite_link is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_invite_link_name(
    *,
    rule_id: int,
    campaign_run_id: int | None = None,
    ad_target_title: str | None = None,
) -> str:
    base = f"ViMi · run #{campaign_run_id}" if campaign_run_id is not None else f"ViMi · rule #{rule_id}"
    target_title = (ad_target_title or "").strip()
    if target_title:
        available = _MAX_INVITE_LINK_NAME_LENGTH - len(base) - len(" · ")
        if available > 0:
            base = f"{base} · {target_title[:available].strip()}".strip()
    return (base[:_MAX_INVITE_LINK_NAME_LENGTH].strip() or "ViMi")


def serialize_telegram_invite_link_payload(invite_link_obj: Any) -> dict[str, Any]:
    raw: Any
    if invite_link_obj is None:
        return {}
    if isinstance(invite_link_obj, dict):
        raw = invite_link_obj
    elif hasattr(invite_link_obj, "model_dump"):
        raw = invite_link_obj.model_dump(mode="json", exclude_none=True)
    elif hasattr(invite_link_obj, "dict"):
        raw = invite_link_obj.dict(exclude_none=True)
    else:
        raw = {
            key: getattr(invite_link_obj, key, None)
            for key in ("invite_link", "name", "creates_join_request")
            if getattr(invite_link_obj, key, None) is not None
        }
    if not isinstance(raw, dict):
        return {}
    return {str(key): _json_safe(value) for key, value in raw.items() if _json_safe(value) is not None}


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items() if _json_safe(v) is not None}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value if _json_safe(item) is not None]
    return str(value)


def _get_field(obj: Any, field: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(field)
    return getattr(obj, field, None)


def _exception_text(exc: Exception) -> str:
    return str(exc).lower()


def _is_permission_error(exc: Exception) -> bool:
    text = _exception_text(exc)
    return isinstance(exc, TelegramForbiddenError) or any(
        marker in text for marker in ("not enough rights", "not administrator", "not an administrator", "forbidden", "can't create invite", "cannot create invite", "can't revoke", "cannot revoke")
    )


def _is_chat_unavailable_error(exc: Exception) -> bool:
    text = _exception_text(exc)
    return any(marker in text for marker in ("chat not found", "bot is not a member", "not found", "chat_id_invalid"))


def _is_already_revoked_error(exc: Exception) -> bool:
    text = _exception_text(exc)
    return any(marker in text for marker in ("invite link is invalid", "invalid invite link", "already revoked", "invite link not found"))


class CampaignInviteLinksService:
    def __init__(self, *, repo, bot, logger=None):
        self.repo = repo
        self.bot = bot
        self.logger = logger or logging.getLogger(__name__)

    async def create_invite_link(
        self,
        *,
        rule_id: int,
        destination_chat_id: str,
        destination_chat_title: str | None,
        link_mode: str,
        created_by: int | None = None,
        campaign_run_id: int | None = None,
        campaign_run_message_id: int | None = None,
        saved_post_id: int | None = None,
        ad_target_id: str | None = None,
        ad_target_thread_id: int | None = None,
        ad_target_title: str | None = None,
        invite_link_name: str | None = None,
    ) -> dict:
        if not (destination_chat_id or "").strip():
            return {"ok": False, "error_code": "destination_chat_required", "error_text": "Не выбран канал для вступления."}
        if link_mode not in _ALLOWED_LINK_MODES:
            return {"ok": False, "error_code": "invalid_link_mode", "error_text": "Некорректный режим рекламной ссылки."}

        creates_join_request = link_mode == INVITE_LINK_MODE_JOIN_REQUEST
        final_invite_link_name = (invite_link_name or build_invite_link_name(rule_id=rule_id, campaign_run_id=campaign_run_id, ad_target_title=ad_target_title))[:_MAX_INVITE_LINK_NAME_LENGTH].strip()
        self.logger.info("CAMPAIGN_INVITE_LINK_CREATE_STARTED rule_id=%s chat_id=%s", rule_id, destination_chat_id)
        try:
            telegram_link = await self.bot.create_chat_invite_link(chat_id=destination_chat_id, name=final_invite_link_name, creates_join_request=creates_join_request)
        except (TelegramForbiddenError, TelegramBadRequest, TelegramAPIError) as exc:
            return self._telegram_create_error(exc)

        payload = serialize_telegram_invite_link_payload(telegram_link)
        invite_link = _get_field(telegram_link, "invite_link")
        try:
            invite_link_hash = build_invite_link_hash(str(invite_link or ""))
        except ValueError:
            self.logger.warning("CAMPAIGN_INVITE_LINK_CREATE_FAILED reason=empty_invite_link rule_id=%s", rule_id)
            return {"ok": False, "error_code": "telegram_api_error", "error_text": "Telegram не создал ссылку. Попробуйте ещё раз."}

        invite_link_id = self.repo.create_campaign_invite_link_record(
            rule_id=rule_id, destination_chat_id=destination_chat_id, destination_chat_title=destination_chat_title,
            campaign_run_id=campaign_run_id, campaign_run_message_id=campaign_run_message_id, saved_post_id=saved_post_id,
            ad_target_id=ad_target_id, ad_target_thread_id=ad_target_thread_id, ad_target_title=ad_target_title,
            link_mode=link_mode, invite_link=invite_link, invite_link_hash=invite_link_hash,
            invite_link_name=final_invite_link_name, creates_join_request=creates_join_request,
            telegram_payload_json=payload, created_by=created_by, status=INVITE_LINK_STATUS_ACTIVE,
        )
        if invite_link_id is None:
            self.logger.warning("CAMPAIGN_INVITE_LINK_CREATE_FAILED reason=repository_save_failed rule_id=%s", rule_id)
            return {"ok": False, "error_code": "repository_save_failed", "error_text": "Telegram-ссылка создана, но ViMi не смог сохранить её в базе.", "invite_link": invite_link}
        self.logger.info("CAMPAIGN_INVITE_LINK_CREATED invite_link_id=%s rule_id=%s", invite_link_id, rule_id)
        return {"ok": True, "invite_link_id": invite_link_id, "invite_link": invite_link, "invite_link_hash": invite_link_hash, "creates_join_request": creates_join_request, "link_mode": link_mode}

    async def revoke_invite_link(self, *, invite_link_id: int, actor_id: int | None = None) -> dict:
        self.logger.info("CAMPAIGN_INVITE_LINK_REVOKE_STARTED invite_link_id=%s actor_id=%s", invite_link_id, actor_id)
        link = self.repo.get_campaign_invite_link(invite_link_id)
        if not link:
            return {"ok": False, "error_code": "invite_link_not_found", "error_text": "Рекламная ссылка не найдена."}
        if link.get("status") == INVITE_LINK_STATUS_REVOKED:
            return {"ok": True, "already_revoked": True, "invite_link_id": invite_link_id}
        try:
            revoked_payload = await self.bot.revoke_chat_invite_link(chat_id=link["destination_chat_id"], invite_link=link["invite_link"])
        except (TelegramForbiddenError, TelegramBadRequest, TelegramAPIError) as exc:
            if _is_already_revoked_error(exc):
                self.repo.mark_campaign_invite_link_revoked(invite_link_id, telegram_payload_json={"telegram_error": str(exc)})
                self.logger.info("CAMPAIGN_INVITE_LINK_REVOKED invite_link_id=%s already_revoked_on_telegram=1", invite_link_id)
                return {"ok": True, "invite_link_id": invite_link_id, "revoked": True, "already_revoked_on_telegram": True}
            if _is_permission_error(exc):
                self.logger.warning("CAMPAIGN_INVITE_LINK_REVOKE_FAILED invite_link_id=%s error=%s", invite_link_id, exc)
                return {"ok": False, "error_code": "telegram_permission_denied", "error_text": "ViMi не может отозвать ссылку. Проверьте права бота в канале."}
            self.logger.warning("CAMPAIGN_INVITE_LINK_REVOKE_FAILED invite_link_id=%s error=%s", invite_link_id, exc)
            return {"ok": False, "error_code": "telegram_api_error", "error_text": "Telegram не отозвал ссылку. Попробуйте ещё раз."}
        payload = serialize_telegram_invite_link_payload(revoked_payload)
        self.repo.mark_campaign_invite_link_revoked(invite_link_id, telegram_payload_json=payload)
        self.logger.info("CAMPAIGN_INVITE_LINK_REVOKED invite_link_id=%s", invite_link_id)
        return {"ok": True, "invite_link_id": invite_link_id, "revoked": True}

    def _telegram_create_error(self, exc: Exception) -> dict:
        self.logger.warning("CAMPAIGN_INVITE_LINK_CREATE_FAILED error=%s", exc)
        if _is_permission_error(exc):
            return {"ok": False, "error_code": "telegram_permission_denied", "error_text": "ViMi не может создать ссылку. Бот должен быть администратором канала и иметь право приглашать пользователей."}
        if _is_chat_unavailable_error(exc):
            return {"ok": False, "error_code": "telegram_chat_not_available", "error_text": "Канал для вступления недоступен для ViMi. Проверьте, что бот добавлен в канал."}
        return {"ok": False, "error_code": "telegram_api_error", "error_text": "Telegram не создал ссылку. Попробуйте ещё раз."}
