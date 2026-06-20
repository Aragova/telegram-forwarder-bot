"""Service for recording campaign invite link Telegram events."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from app.campaign_invite_links_service import build_invite_link_hash

ACTIVE_INVITE_LINK_STATUS = "active"
JOINED_MEMBER_STATUSES = {"member", "administrator", "creator"}
LEFT_MEMBER_STATUSES = {"left", "kicked"}


def _get_field(obj: Any, field: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(field)
    return getattr(obj, field, None)


def _status_to_str(status: Any) -> str | None:
    if status is None:
        return None
    value = getattr(status, "value", status)
    text = str(value).strip().lower()
    return text or None


def build_telegram_user_id_hash(user_id: int | str) -> str:
    normalized = str(user_id).strip()
    if not normalized:
        raise ValueError("telegram user id is required")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def sanitize_telegram_user_payload(user: Any) -> dict[str, Any]:
    if user is None:
        return {}
    allowed_fields = ("is_bot", "first_name", "last_name", "username", "language_code", "is_premium")
    payload: dict[str, Any] = {}
    for field in allowed_fields:
        value = _get_field(user, field)
        if value is not None:
            payload[field] = value
    return payload


def extract_invite_link_text(invite_link_obj: Any) -> str | None:
    invite_link = _get_field(invite_link_obj, "invite_link")
    if invite_link is None:
        return None
    invite_link_text = str(invite_link).strip()
    return invite_link_text or None


def sanitize_update_payload(update_obj: Any) -> dict[str, Any]:
    try:
        invite_link_obj = _get_field(update_obj, "invite_link")
        old_chat_member = _get_field(update_obj, "old_chat_member")
        new_chat_member = _get_field(update_obj, "new_chat_member")
        chat = _get_field(update_obj, "chat")
        payload = {
            "update_type": update_obj.__class__.__name__,
            "chat_id": _get_field(chat, "id"),
            "invite_link": extract_invite_link_text(invite_link_obj),
            "date": _get_field(update_obj, "date"),
            "event_at": _get_field(update_obj, "date"),
            "old_status": _status_to_str(_get_field(old_chat_member, "status")),
            "new_status": _status_to_str(_get_field(new_chat_member, "status")),
            "has_invite_link": invite_link_obj is not None,
        }
        return {key: value for key, value in payload.items() if value is not None}
    except Exception:
        return {}


def _resolve_member_event_type(old_status: str | None, new_status: str | None) -> str:
    if new_status in JOINED_MEMBER_STATUSES and old_status in LEFT_MEMBER_STATUSES:
        return "member_joined"
    if new_status == "left":
        return "member_left"
    if new_status == "kicked":
        return "member_kicked"
    return "member_unknown"


class CampaignInviteLinkEventsService:
    def __init__(self, *, repo, logger=None):
        self.repo = repo
        self.logger = logger or logging.getLogger(__name__)

    async def handle_chat_join_request(self, join_request) -> dict:
        invite_link_text = extract_invite_link_text(_get_field(join_request, "invite_link"))
        if not invite_link_text:
            return {"ok": False, "skipped": True, "reason": "missing_invite_link"}
        link = self.repo.get_campaign_invite_link_by_hash(build_invite_link_hash(invite_link_text))
        if not link:
            return {"ok": False, "skipped": True, "reason": "invite_link_not_tracked"}
        if link.get("status") != ACTIVE_INVITE_LINK_STATUS:
            return {"ok": False, "skipped": True, "reason": "invite_link_not_active"}
        user = _get_field(join_request, "from_user")
        telegram_user_id_hash = build_telegram_user_id_hash(_get_field(user, "id"))
        self.repo.create_campaign_invite_link_event(
            invite_link_id=link["id"], rule_id=link["rule_id"], campaign_run_id=link.get("campaign_run_id"),
            campaign_run_message_id=link.get("campaign_run_message_id"), destination_chat_id=link["destination_chat_id"],
            ad_target_id=link.get("ad_target_id"), ad_target_thread_id=link.get("ad_target_thread_id"),
            event_type="join_request_created", telegram_user_id_hash=telegram_user_id_hash,
            telegram_update_id=_get_field(join_request, "update_id"),
            telegram_user_payload_json=sanitize_telegram_user_payload(user), raw_update_json=sanitize_update_payload(join_request),
            event_at=_get_field(join_request, "date"),
        )
        return {"ok": True, "event_type": "join_request_created", "invite_link_id": link["id"], "rule_id": link["rule_id"]}

    async def handle_chat_member_updated(self, member_update) -> dict:
        invite_link_text = extract_invite_link_text(_get_field(member_update, "invite_link"))
        if not invite_link_text:
            return {"ok": False, "skipped": True, "reason": "missing_invite_link"}
        link = self.repo.get_campaign_invite_link_by_hash(build_invite_link_hash(invite_link_text))
        if not link:
            return {"ok": False, "skipped": True, "reason": "invite_link_not_tracked"}
        if link.get("status") != ACTIVE_INVITE_LINK_STATUS:
            return {"ok": False, "skipped": True, "reason": "invite_link_not_active"}
        old_chat_member = _get_field(member_update, "old_chat_member")
        new_chat_member = _get_field(member_update, "new_chat_member")
        user = _get_field(new_chat_member, "user") or _get_field(old_chat_member, "user")
        if user is None:
            return {"ok": False, "skipped": True, "reason": "missing_user"}
        old_status = _status_to_str(_get_field(old_chat_member, "status"))
        new_status = _status_to_str(_get_field(new_chat_member, "status"))
        event_type = _resolve_member_event_type(old_status, new_status)
        self.repo.create_campaign_invite_link_event(
            invite_link_id=link["id"], rule_id=link["rule_id"], campaign_run_id=link.get("campaign_run_id"),
            campaign_run_message_id=link.get("campaign_run_message_id"), destination_chat_id=link["destination_chat_id"],
            ad_target_id=link.get("ad_target_id"), ad_target_thread_id=link.get("ad_target_thread_id"),
            event_type=event_type, telegram_user_id_hash=build_telegram_user_id_hash(_get_field(user, "id")),
            telegram_update_id=_get_field(member_update, "update_id"),
            telegram_user_payload_json=sanitize_telegram_user_payload(user), raw_update_json=sanitize_update_payload(member_update),
            event_at=_get_field(member_update, "date"),
        )
        return {"ok": True, "event_type": event_type, "invite_link_id": link["id"], "rule_id": link["rule_id"]}
