from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.repost_campaign_service import build_campaign_delete_after_iso, format_campaign_show_seconds_ru
from app.repost_campaign_view_model import format_campaign_error_text
from app.saved_post_renderer import normalize_telethon_target
from app.saved_posts_service import get_saved_post_short_description


def build_telegram_message_url(*, target_id: int | str, message_id: int | None, username: str | None = None) -> str | None:
    if not message_id:
        return None
    if username:
        uname = str(username).strip().lstrip("@")
        if uname:
            return f"https://t.me/{uname}/{int(message_id)}"
    target = str(target_id or "").strip()
    if target.startswith("-100"):
        return f"https://t.me/c/{target[4:]}/{int(message_id)}"
    if target.startswith("-"):
        return f"https://t.me/c/{target.lstrip('-')}/{int(message_id)}"
    if target:
        return f"https://t.me/{target}/{int(message_id)}"
    return None


@dataclass(frozen=True)
class RepostCampaignActionResult:
    ok: bool
    action: str
    rule_id: int
    saved_post_id: int | None = None
    target_id: str | None = None
    message_id: int | None = None
    method: str | None = None
    kind: str | None = None
    error_text: str | None = None
    premium_required: bool = False
    extra: dict[str, Any] | None = None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "action": self.action,
            "rule_id": self.rule_id,
            "saved_post_id": self.saved_post_id,
            "target_id": self.target_id,
            "message_id": self.message_id,
            "method": self.method,
            "kind": self.kind,
            "error_text": self.error_text,
            "premium_required": self.premium_required,
            "extra": self.extra or {},
        }


class RepostCampaignRuntimeService:
    FINAL_VIEWS_MAX_ATTEMPTS = 3
    FINAL_VIEWS_RETRY_DELAY_SECONDS = 60

    def __init__(self, *, repo, renderer, deleter=None, target_checker=None, telethon_client=None, logger_=None):
        self.repo = repo
        self.renderer = renderer
        self.deleter = deleter
        self.target_checker = target_checker
        self.telethon_client = telethon_client
        self.logger = logger_ or logging.getLogger("forwarder")

    def _get_repost_rule_and_saved_post(self, *, rule_id: int, action: str):
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return RepostCampaignActionResult(ok=False, action=action, rule_id=rule_id, error_text="Правило не найдено")
        if (getattr(rule, "mode", "repost") or "repost").strip().lower() != "repost":
            return RepostCampaignActionResult(
                ok=False,
                action=action,
                rule_id=rule_id,
                error_text="Рекламная кампания доступна только для режима репоста",
            )

        saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
        if not saved_post_id:
            return RepostCampaignActionResult(ok=False, action=action, rule_id=rule_id, error_text="Рекламный пост не выбран")

        saved_post_id_int = int(saved_post_id)
        saved_post = self.repo.get_saved_post(saved_post_id_int)
        if not saved_post:
            return RepostCampaignActionResult(
                ok=False,
                action=action,
                rule_id=rule_id,
                saved_post_id=saved_post_id_int,
                error_text="Рекламный пост не найден",
            )

        return rule, saved_post



    def _extract_sent_message_ids(self, message: dict) -> list[int]:
        ids = message.get("sent_message_ids")
        if isinstance(ids, list) and ids:
            return [int(x) for x in ids]
        ids_json = message.get("sent_message_ids_json")
        if isinstance(ids_json, str) and ids_json.strip():
            try:
                parsed = json.loads(ids_json)
                if isinstance(parsed, list) and parsed:
                    return [int(x) for x in parsed]
            except Exception:
                pass
        if isinstance(ids_json, list) and ids_json:
            return [int(x) for x in ids_json]
        mid = message.get("sent_message_id")
        return [int(mid)] if mid else []

    def _normalize_telegram_channel_id_for_compare(self, value) -> str | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if raw.startswith('-100'):
            normalized = raw[4:]
        elif raw.startswith('-'):
            normalized = raw[1:]
        else:
            normalized = raw
        return normalized if normalized.isdigit() else None

    def _extract_telethon_message_peer_id(self, message) -> str | None:
        if message is None:
            return None
        candidates = [
            getattr(message, 'chat_id', None),
            getattr(getattr(message, 'peer_id', None), 'channel_id', None),
            getattr(getattr(message, 'to_id', None), 'channel_id', None),
            getattr(getattr(message, 'peer_id', None), 'chat_id', None),
            getattr(getattr(message, 'to_id', None), 'chat_id', None),
        ]
        for candidate in candidates:
            normalized = self._normalize_telegram_channel_id_for_compare(candidate)
            if normalized is not None:
                return normalized
        return None
    def get_campaign_target(self, *, rule_id: int, target_row_id: int) -> dict | None:
        targets = self.repo.list_rule_repost_campaign_targets(rule_id, active_only=False) or []
        return next((t for t in targets if int(t.get("id") or 0) == int(target_row_id)), None)

    def set_campaign_target_active(
        self,
        *,
        rule_id: int,
        target_row_id: int,
        is_active: bool,
        admin_id: int | None = None,
    ) -> dict:
        _ = admin_id
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "error_text": "Правило не найдено"}
        target = self.get_campaign_target(rule_id=rule_id, target_row_id=target_row_id)
        if not target:
            return {"ok": False, "error_text": "Канал/группа не найдены в кампании"}
        try:
            raw_update_result = self.repo.set_rule_repost_campaign_target_active(target_row_id, is_active)
            ok = bool(raw_update_result)
            if ok:
                self.logger.info("REPOST_CAMPAIGN_TARGET_STATUS_UPDATED | rule_id=%s | target_row_id=%s | is_active=%s | raw_update_result=%s", rule_id, target_row_id, is_active, raw_update_result)
            else:
                self.logger.warning("REPOST_CAMPAIGN_TARGET_ACTION_SAVE_FAILED | action=%s | rule_id=%s | target_row_id=%s | target_id=%s | current_is_active=%s | last_check_error=%s | raw_update_result=%s", "resume" if is_active else "pause", rule_id, target_row_id, target.get("target_id"), target.get("is_active"), target.get("last_check_error"), raw_update_result)
            return {
                "ok": ok,
                "action": "resume" if is_active else "pause",
                "rule_id": rule_id,
                "target_row_id": target_row_id,
                "target_id": target.get("target_id"),
                "target_title": target.get("title") or target.get("target_id"),
                "is_active": is_active,
                "error_text": None if ok else "Не удалось обновить канал/группу. Обновите список и повторите действие.",
                "extra": {"target_id": target.get("target_id"), "current_is_active": target.get("is_active"), "last_check_error": target.get("last_check_error")},
            }
        except Exception as exc:
            self.logger.warning("REPOST_CAMPAIGN_TARGET_STATUS_FAILED | rule_id=%s | target_row_id=%s | error=%s", rule_id, target_row_id, exc)
            return {"ok": False, "error_text": "Не удалось обновить статус канала/группы"}

    def remove_campaign_target(self, *, rule_id: int, target_row_id: int, admin_id: int | None = None) -> dict:
        _ = admin_id
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "error_text": "Правило не найдено"}
        target = self.get_campaign_target(rule_id=rule_id, target_row_id=target_row_id)
        if not target:
            return {"ok": False, "error_text": "Канал/группа не найдены в кампании"}
        try:
            ok = bool(self.repo.remove_rule_repost_campaign_target(target_row_id))
            if ok:
                self.logger.info("REPOST_CAMPAIGN_TARGET_REMOVED | rule_id=%s | target_row_id=%s", rule_id, target_row_id)
            else:
                self.logger.warning("REPOST_CAMPAIGN_TARGET_ACTION_SAVE_FAILED | action=%s | rule_id=%s | target_row_id=%s | target_id=%s | current_is_active=%s | last_check_error=%s", "remove", rule_id, target_row_id, target.get("target_id"), target.get("is_active"), target.get("last_check_error"))
            return {
                "ok": ok,
                "action": "remove",
                "rule_id": rule_id,
                "target_row_id": target_row_id,
                "target_id": target.get("target_id"),
                "target_title": target.get("title") or target.get("target_id"),
                "error_text": None if ok else "Не удалось обновить канал/группу. Обновите список и повторите действие.",
                "extra": {"target_id": target.get("target_id"), "current_is_active": target.get("is_active"), "last_check_error": target.get("last_check_error")},
            }
        except Exception as exc:
            self.logger.warning("REPOST_CAMPAIGN_TARGET_REMOVE_FAILED | rule_id=%s | target_row_id=%s | error=%s", rule_id, target_row_id, exc)
            return {"ok": False, "error_text": "Не удалось удалить канал/группу"}


    async def check_campaign_target(self, *, rule_id: int, target_row_id: int, admin_id: int | None = None) -> dict:
        _ = admin_id
        if not self.target_checker:
            return {"ok": False, "rule_id": rule_id, "target_row_id": target_row_id, "error_text": "Сервис проверки прав недоступен"}
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "rule_id": rule_id, "target_row_id": target_row_id, "error_text": "Правило не найдено"}
        target = self.get_campaign_target(rule_id=rule_id, target_row_id=target_row_id)
        if not target:
            return {"ok": False, "rule_id": rule_id, "target_row_id": target_row_id, "error_text": "Канал/группа не найдены в кампании"}
        check = await self.target_checker.check_target(target_id=target["target_id"], target_thread_id=target.get("target_thread_id"))
        last_check_error = None
        publish_status = getattr(check, "publish_status", ("confirmed" if getattr(check, "can_publish", False) else "denied"))
        delete_status = getattr(check, "delete_status", ("confirmed" if getattr(check, "can_delete", None) is True else ("denied" if getattr(check, "can_delete", None) is False else "unknown")))
        publish_error_text = getattr(check, "publish_error_text", None)
        delete_error_text = getattr(check, "delete_error_text", None)
        source = getattr(check, "source", "telethon")
        details = getattr(check, "details", None)
        if publish_status == "denied":
            last_check_error = publish_error_text or "Нет права публикации"
        elif publish_status == "unknown":
            last_check_error = "Не удалось подтвердить право публикации"
        elif delete_status == "denied":
            last_check_error = delete_error_text or "Нет права удаления"
        saved = bool(self.repo.update_rule_repost_campaign_target_check_result(
            target_row_id,
            title=check.title,
            last_check_error=last_check_error,
            can_post=check.can_publish,
            can_delete=check.can_delete,
            publish_status=publish_status,
            delete_permission_status=delete_status,
            publish_error_text=publish_error_text,
            delete_error_text=delete_error_text,
            check_source=source,
        ))
        if not saved:
            self.logger.warning(
                "REPOST_CAMPAIGN_TARGET_CHECK_SAVE_FAILED | rule_id=%s | target_row_id=%s | title=%s | ok=%s",
                rule_id,
                target_row_id,
                check.title,
                check.ok,
            )
        self.logger.info("REPOST_CAMPAIGN_TARGET_CHECKED | rule_id=%s | target_row_id=%s | ok=%s | error=%s", rule_id, target_row_id, check.ok, check.error_text)
        save_error_text = "Проверка выполнена, но не удалось сохранить результат. Обновите список и повторите."
        error_text = check.error_text if not check.ok else (save_error_text if not saved else None)
        return {
            "ok": bool(check.ok and saved),
            "check_ok": bool(check.ok),
            "saved": saved,
            "save_error": (not saved),
            "rule_id": rule_id,
            "target_row_id": target_row_id,
            "target_id": check.target_id,
            "target_title": check.title or target.get("title") or target.get("target_id"),
            "error_text": error_text,
            "can_view": check.can_view,
            "can_publish": check.can_publish,
            "can_delete": check.can_delete,
            "publish_status": publish_status,
            "delete_status": delete_status,
            "publish_error_text": publish_error_text,
            "delete_error_text": delete_error_text,
            "source": source,
            "details": details,
        }

    async def check_campaign_targets(self, *, rule_id: int, active_only: bool = False, admin_id: int | None = None, limit: int = 50) -> dict:
        _ = admin_id
        if not self.target_checker:
            return {"ok": False, "rule_id": rule_id, "error_text": "Сервис проверки прав недоступен", "checked": 0, "passed": 0, "failed": 0, "items": []}
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "rule_id": rule_id, "error_text": "Правило не найдено", "checked": 0, "passed": 0, "failed": 0, "items": []}
        targets = self.repo.list_rule_repost_campaign_targets(rule_id, active_only=active_only) or []
        targets = targets[: max(0, int(limit or 0))]
        self.logger.info("REPOST_CAMPAIGN_TARGET_CHECK_BATCH_STARTED | rule_id=%s | count=%s", rule_id, len(targets))
        items=[]
        for t in targets:
            items.append(await self.check_campaign_target(rule_id=rule_id, target_row_id=int(t.get("id") or 0), admin_id=admin_id))
        passed=sum(1 for i in items if i.get("ok"))
        failed=len(items)-passed
        self.logger.info("REPOST_CAMPAIGN_TARGET_CHECK_BATCH_DONE | rule_id=%s | checked=%s | passed=%s | failed=%s", rule_id, len(items), passed, failed)
        return {"ok": True, "rule_id": rule_id, "checked": len(items), "passed": passed, "failed": failed, "items": items}

    async def preview_saved_post(self, *, rule_id: int, admin_chat_id: int | str, reply_markup=None) -> RepostCampaignActionResult:
        loaded = self._get_repost_rule_and_saved_post(rule_id=rule_id, action="preview_saved_post")
        if isinstance(loaded, RepostCampaignActionResult):
            return loaded

        rule, saved_post = loaded
        saved_post_id = int(getattr(rule, "repost_campaign_saved_post_id"))
        content = saved_post.get("content_json") or saved_post.get("content") or {}
        render_result = await self.renderer.send(chat_id=admin_chat_id, content=content, reply_markup=reply_markup)

        if not render_result.ok:
            return RepostCampaignActionResult(
                ok=False,
                action="preview_saved_post",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                method=render_result.method,
                kind=render_result.kind,
                error_text=render_result.error_text,
                premium_required=render_result.premium_required,
            )

        self.logger.info(
            "REPOST_CAMPAIGN_SAVED_POST_PREVIEW_SENT | rule_id=%s | saved_post_id=%s | kind=%s | method=%s",
            rule_id,
            saved_post_id,
            render_result.kind,
            render_result.method,
        )
        return RepostCampaignActionResult(
            ok=True,
            action="preview_saved_post",
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            target_id=str(admin_chat_id),
            message_id=render_result.message_id,
            method=render_result.method,
            kind=render_result.kind,
            premium_required=render_result.premium_required,
        )

    async def preview_saved_post_in_main_target(
        self,
        *,
        rule_id: int,
        admin_chat_id: int | str,
        reply_markup: Any | None = None,
    ) -> RepostCampaignActionResult:
        _ = admin_chat_id, reply_markup
        loaded = self._get_repost_rule_and_saved_post(rule_id=rule_id, action="preview_saved_post_in_main_target")
        if isinstance(loaded, RepostCampaignActionResult):
            return loaded

        rule, saved_post = loaded
        saved_post_id = int(getattr(rule, "repost_campaign_saved_post_id"))
        target_id = getattr(rule, "target_id", None)
        if not target_id:
            return RepostCampaignActionResult(
                ok=False,
                action="preview_saved_post_in_main_target",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="У правила не задан основной канал получателя",
            )
        content = saved_post.get("content_json") or saved_post.get("content") or {}
        render_result = await self.renderer.send(chat_id=target_id, content=content, reply_markup=None)
        if not render_result.ok:
            return RepostCampaignActionResult(
                ok=False,
                action="preview_saved_post_in_main_target",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                target_id=str(target_id),
                method=render_result.method,
                kind=render_result.kind,
                error_text=render_result.error_text,
                premium_required=render_result.premium_required,
            )
        message_ids = getattr(render_result, "message_ids", None)
        preview_url = build_telegram_message_url(target_id=target_id, message_id=render_result.message_id, username=getattr(rule, "target_username", None))
        return RepostCampaignActionResult(
            ok=True,
            action="preview_saved_post_in_main_target",
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            target_id=str(target_id),
            message_id=render_result.message_id,
            method=render_result.method,
            kind=render_result.kind,
            premium_required=render_result.premium_required,
            extra={
                "rule_id": rule_id,
                "saved_post_id": saved_post.get("id"),
                "target_id": str(target_id),
                "target_title": getattr(rule, "target_title", None) or str(target_id),
                "kind": content.get("kind"),
                "method": render_result.method,
                "message_id": render_result.message_id,
                "message_ids": message_ids,
                "preview_url": preview_url,
                "render_result": render_result.to_dict(),
            },
        )

    async def delete_preview_messages(
        self,
        *,
        target_id: int | str,
        message_id: int | None = None,
        message_ids: list[int] | None = None,
        render_mode: str | None = None,
    ) -> RepostCampaignActionResult:
        ids = [int(x) for x in (message_ids or []) if x]
        if not ids and message_id:
            ids = [int(message_id)]
        if not self.deleter:
            return RepostCampaignActionResult(ok=False, action="delete_preview_messages", rule_id=0, error_text="Сервис удаления недоступен")
        if not ids:
            return RepostCampaignActionResult(ok=False, action="delete_preview_messages", rule_id=0, error_text="Нет сообщений предпросмотра для удаления")
        try:
            self.logger.info("REPOST_CAMPAIGN_PREVIEW_DELETE_STARTED | target_id=%s | message_ids=%s", target_id, ids)
            delete_result = None
            if len(ids) > 1 and hasattr(self.deleter, "delete_messages"):
                delete_result = await self.deleter.delete_messages(target_id=target_id, message_ids=ids, render_mode=render_mode)
            else:
                delete_result = await self.deleter.delete_message(target_id=target_id, message_id=ids[0], render_mode=render_mode)
            if not getattr(delete_result, "ok", False):
                error_text = getattr(delete_result, "error_text", None) or "Не удалось удалить предпросмотр"
                self.logger.warning("REPOST_CAMPAIGN_PREVIEW_DELETE_FAILED | target_id=%s | message_ids=%s | error=%s", target_id, ids, error_text)
                return RepostCampaignActionResult(
                    ok=False,
                    action="delete_preview_messages",
                    rule_id=0,
                    target_id=str(target_id),
                    message_id=ids[0] if ids else None,
                    method=render_mode,
                    error_text=error_text,
                    extra={
                        "target_id": str(target_id),
                        "message_id": message_id,
                        "message_ids": ids,
                        "render_mode": render_mode,
                        "delete_result": delete_result.to_dict() if hasattr(delete_result, "to_dict") else None,
                    },
                )
            self.logger.info("REPOST_CAMPAIGN_PREVIEW_DELETE_DONE | target_id=%s | message_ids=%s", target_id, ids)
            return RepostCampaignActionResult(
                ok=True,
                action="delete_preview_messages",
                rule_id=0,
                target_id=str(target_id),
                message_id=ids[0],
                method=render_mode,
                extra={
                    "target_id": str(target_id),
                    "message_id": message_id,
                    "message_ids": ids,
                    "render_mode": render_mode,
                    "delete_result": delete_result.to_dict() if hasattr(delete_result, "to_dict") else None,
                },
            )
        except Exception as exc:
            self.logger.warning("REPOST_CAMPAIGN_PREVIEW_DELETE_FAILED | target_id=%s | message_ids=%s | error=%s", target_id, ids, exc)
            return RepostCampaignActionResult(
                ok=False,
                action="delete_preview_messages",
                rule_id=0,
                target_id=str(target_id),
                message_id=ids[0] if ids else None,
                method=render_mode,
                error_text="Не удалось удалить предпросмотр",
                extra={"target_id": str(target_id), "message_id": message_id, "message_ids": ids, "render_mode": render_mode},
            )

    async def test_send_saved_post_to_main_target(self, *, rule_id: int, admin_id: int | None = None) -> RepostCampaignActionResult:
        loaded = self._get_repost_rule_and_saved_post(rule_id=rule_id, action="test_send_saved_post")
        if isinstance(loaded, RepostCampaignActionResult):
            return loaded

        rule, saved_post = loaded
        saved_post_id = int(getattr(rule, "repost_campaign_saved_post_id"))
        target_id = getattr(rule, "target_id", None)
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
        if not target_id:
            return RepostCampaignActionResult(
                ok=False,
                action="test_send_saved_post",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="У правила не задан основной канал получателя",
            )

        run_id = self.repo.create_campaign_run(
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            run_type="test",
            status="sending",
            show_seconds=show_seconds,
            started_by=admin_id,
            targets_total=1,
        )
        if run_id is None:
            return RepostCampaignActionResult(
                ok=False,
                action="test_send_saved_post",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="Не удалось создать запись запуска рекламной кампании",
            )
        self.logger.info(
            "REPOST_CAMPAIGN_RUN_CREATED | rule_id=%s | saved_post_id=%s | run_id=%s | run_type=test",
            rule_id,
            saved_post_id,
            run_id,
        )
        run_message_id = self.repo.create_campaign_run_message(
            run_id=run_id,
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            target_kind="main",
            target_id=str(target_id),
            target_thread_id=getattr(rule, "target_thread_id", None),
            target_title=getattr(rule, "target_title", None),
            show_seconds=show_seconds,
            delete_after_at=None,
        )
        if run_message_id is None:
            self.repo.update_campaign_run_status(run_id, status="failed", targets_success=0, targets_failed=1, error_text="Не удалось создать запись публикации рекламной кампании", finish=True)
            return RepostCampaignActionResult(
                ok=False,
                action="test_send_saved_post",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="Не удалось создать запись публикации рекламной кампании",
                extra={"campaign_run_id": run_id},
            )
        self.logger.info(
            "REPOST_CAMPAIGN_RUN_MESSAGE_CREATED | run_id=%s | message_id=%s | target_id=%s | target_kind=main",
            run_id,
            run_message_id,
            target_id,
        )
        self.repo.mark_campaign_run_message_sending(run_message_id, render_mode=None)

        content = saved_post.get("content_json") or saved_post.get("content") or {}
        render_result = await self.renderer.send(chat_id=target_id, content=content)
        if not render_result.ok:
            self.repo.mark_campaign_run_message_failed(
                run_message_id,
                error_text=render_result.error_text or "unknown error",
                render_mode=render_result.method,
            )
            self.repo.update_campaign_run_status(
                run_id,
                status="failed",
                render_mode=render_result.method,
                targets_success=0,
                targets_failed=1,
                error_text=render_result.error_text,
                finish=True,
            )
            self.logger.warning("REPOST_CAMPAIGN_RUN_FAILED | run_id=%s | error=%s", run_id, render_result.error_text)
            self.logger.warning(
                "REPOST_CAMPAIGN_TEST_SEND_FAILED | rule_id=%s | saved_post_id=%s | target_id=%s | error=%s | method=%s",
                rule_id,
                saved_post_id,
                target_id,
                render_result.error_text,
                render_result.method,
            )
            return RepostCampaignActionResult(
                ok=False,
                action="test_send_saved_post",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                target_id=str(target_id),
                method=render_result.method,
                kind=render_result.kind,
                error_text=render_result.error_text,
                premium_required=render_result.premium_required,
                extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id},
            )

        self.repo.mark_campaign_run_message_sent(
            run_message_id,
            sent_message_id=render_result.message_id,
            sent_message_ids=getattr(render_result, "message_ids", None),
            render_mode=render_result.method,
        )
        self.repo.update_campaign_run_status(
            run_id,
            status="sent",
            render_mode=render_result.method,
            targets_success=1,
            targets_failed=0,
            finish=True,
            report={"target_id": str(target_id), "message_id": render_result.message_id, "message_ids": getattr(render_result, "message_ids", None), "method": render_result.method},
        )
        self.logger.info(
            "REPOST_CAMPAIGN_RUN_FINISHED | run_id=%s | status=%s | method=%s | sent_message_id=%s",
            run_id,
            "sent",
            render_result.method,
            render_result.message_id,
        )
        self.logger.info(
            "REPOST_CAMPAIGN_TEST_SEND_DONE | rule_id=%s | saved_post_id=%s | target_id=%s | message_id=%s | method=%s",
            rule_id,
            saved_post_id,
            target_id,
            render_result.message_id,
            render_result.method,
        )
        return RepostCampaignActionResult(
            ok=True,
            action="test_send_saved_post",
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            target_id=str(target_id),
            message_id=render_result.message_id,
            method=render_result.method,
            kind=render_result.kind,
            premium_required=render_result.premium_required,
            extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id},
        )

    async def launch_campaign_now(self, *, rule_id: int, admin_id: int | None = None, run_type: str = "manual") -> RepostCampaignActionResult:
        readiness = self.build_campaign_launch_readiness(rule_id=rule_id)
        if not readiness.get("can_launch"):
            return RepostCampaignActionResult(
                ok=False,
                action="launch_campaign",
                rule_id=rule_id,
                saved_post_id=readiness.get("saved_post_id"),
                error_text="Кампания не готова к запуску",
                extra={
                    "launch_readiness": readiness,
                    "block_reasons": readiness.get("block_reasons") or [],
                    "warnings": readiness.get("warnings") or [],
                },
            )
        if readiness.get("active_placement") or int(readiness.get("delete_failed") or 0) > 0:
            return RepostCampaignActionResult(
                ok=False,
                action="launch_campaign",
                rule_id=rule_id,
                saved_post_id=readiness.get("saved_post_id"),
                error_text="Кампания уже активна" if readiness.get("active_placement") else "Есть проблемы удаления в предыдущем запуске.",
                extra={
                    "active_placement": bool(readiness.get("active_placement")),
                    "last_run_id": readiness.get("active_run_id"),
                    "launch_readiness": readiness,
                },
            )
        loaded = self._get_repost_rule_and_saved_post(rule_id=rule_id, action="launch_campaign")
        if isinstance(loaded, RepostCampaignActionResult):
            return loaded
        rule, saved_post = loaded
        saved_post_id = int(getattr(rule, "repost_campaign_saved_post_id"))
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
        if show_seconds <= 0:
            return RepostCampaignActionResult(
                ok=False,
                action="launch_campaign",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="Срок показа не задан",
            )

        main_key = (str(getattr(rule, "target_id", "")), getattr(rule, "target_thread_id", None))
        targets: list[dict[str, Any]] = [{
            "target_kind": "main",
            "target_id": main_key[0],
            "target_thread_id": main_key[1],
            "target_title": getattr(rule, "target_title", None) or "Основной канал",
        }]
        seen = {main_key}
        extra_targets = readiness.get("ready_extra_targets") or []
        for row in extra_targets:
            key = (str(row.get("target_id") or ""), row.get("target_thread_id"))
            if key in seen:
                continue
            seen.add(key)
            targets.append({
                "target_kind": "extra",
                "target_id": key[0],
                "target_thread_id": key[1],
                "target_title": row.get("title") or row.get("target_id"),
            })

        run_id = self.repo.create_campaign_run(
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            run_type=run_type,
            status="sending",
            show_seconds=show_seconds,
            started_by=admin_id,
            targets_total=len(targets),
        )
        if run_id is None:
            return RepostCampaignActionResult(
                ok=False,
                action="launch_campaign",
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                error_text="Не удалось создать запись запуска рекламной кампании",
            )

        self.logger.info(
            "REPOST_CAMPAIGN_LAUNCH_STARTED | rule_id=%s | saved_post_id=%s | targets=%s | run_id=%s | run_type=%s",
            rule_id, saved_post_id, len(targets), run_id, run_type
        )
        content = saved_post.get("content_json") or saved_post.get("content") or {}
        methods: set[str] = set()
        success_count = 0
        failed_count = 0
        first_error_text = None
        any_premium_required = False
        for target in targets:
            delete_after_at = build_campaign_delete_after_iso(show_seconds) if show_seconds > 0 else None
            run_message_id = self.repo.create_campaign_run_message(
                run_id=run_id,
                rule_id=rule_id,
                saved_post_id=saved_post_id,
                target_kind=target["target_kind"],
                target_id=target["target_id"],
                target_thread_id=target["target_thread_id"],
                target_title=target["target_title"],
                show_seconds=show_seconds,
                delete_after_at=delete_after_at,
            )
            if run_message_id is None:
                failed_count += 1
                if first_error_text is None:
                    first_error_text = "Не удалось создать запись публикации рекламной кампании"
                continue
            self.repo.mark_campaign_run_message_sending(run_message_id, render_mode=None)
            self.logger.info(
                "REPOST_CAMPAIGN_LAUNCH_TARGET_SENDING | run_id=%s | target_kind=%s | target_id=%s",
                run_id, target["target_kind"], target["target_id"]
            )
            render_result = await self.renderer.send(chat_id=target["target_id"], content=content)
            any_premium_required = any_premium_required or bool(getattr(render_result, "premium_required", False))
            album_ids = list(getattr(render_result, "message_ids", None) or [])
            is_album_without_ids = bool(render_result.ok and render_result.kind == "album" and not album_ids)
            if render_result.ok and not is_album_without_ids:
                self.repo.mark_campaign_run_message_sent(
                    run_message_id,
                    sent_message_id=render_result.message_id,
                    sent_message_ids=album_ids or getattr(render_result, "message_ids", None),
                    render_mode=render_result.method,
                )
                success_count += 1
                if render_result.method:
                    methods.add(render_result.method)
                self.logger.info(
                    "REPOST_CAMPAIGN_LAUNCH_TARGET_SENT | run_id=%s | target_id=%s | message_id=%s | message_ids=%s | method=%s",
                    run_id, target["target_id"], render_result.message_id, album_ids, render_result.method
                )
            else:
                self.repo.mark_campaign_run_message_failed(
                    run_message_id,
                    error_text="Не удалось подтвердить ID отправленного альбома." if is_album_without_ids else (getattr(render_result, "error_text", None) or "unknown error"),
                    render_mode=getattr(render_result, "method", None),
                )
                failed_count += 1
                if first_error_text is None:
                    first_error_text = "Не удалось подтвердить ID отправленного альбома." if is_album_without_ids else (getattr(render_result, "error_text", None) or "unknown error")
                if getattr(render_result, "method", None):
                    methods.add(getattr(render_result, "method"))
                self.logger.warning(
                    "REPOST_CAMPAIGN_LAUNCH_TARGET_FAILED | run_id=%s | target_id=%s | error=%s | method=%s",
                    run_id, target["target_id"], getattr(render_result, "error_text", None), getattr(render_result, "method", None)
                )

        total = len(targets)
        final_status = "failed"
        if success_count == total:
            final_status = "sent"
        elif success_count > 0:
            final_status = "partial"
        render_mode = None
        if len(methods) == 1:
            render_mode = next(iter(methods))
        elif len(methods) > 1:
            render_mode = "mixed"
        report = {
            "targets_total": total,
            "targets_success": success_count,
            "targets_failed": failed_count,
            "methods": sorted(list(methods)),
            "main_target_id": str(getattr(rule, "target_id", "")),
            "extra_targets": len(targets) - 1,
        }
        self.repo.update_campaign_run_status(
            run_id,
            status=final_status,
            render_mode=render_mode,
            targets_success=success_count,
            targets_failed=failed_count,
            error_text=first_error_text,
            report=report,
            finish=True,
        )
        self.logger.info(
            "REPOST_CAMPAIGN_LAUNCH_FINISHED | run_id=%s | status=%s | success=%s | failed=%s",
            run_id, final_status, success_count, failed_count
        )
        return RepostCampaignActionResult(
            ok=success_count > 0,
            action="launch_campaign",
            rule_id=rule_id,
            saved_post_id=saved_post_id,
            method=render_mode,
            premium_required=any_premium_required,
            error_text=first_error_text if success_count == 0 else None,
            extra={
                "campaign_run_id": run_id,
                "targets_total": total,
                "targets_success": success_count,
                "targets_failed": failed_count,
                "final_status": final_status,
                "show_seconds": show_seconds,
                "extra_targets": len(targets) - 1,
                "launch_readiness": readiness,
                "will_send_total": readiness.get("will_send_total"),
                "will_skip_total": readiness.get("will_skip_total"),
                "extra_ready": readiness.get("extra_ready"),
                "extra_paused": readiness.get("extra_paused"),
                "extra_problem": readiness.get("extra_problem"),
            },
        )

    def build_campaign_launch_readiness(self, *, rule_id: int) -> dict:
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "rule_id": rule_id, "ready": False, "can_launch": False, "can_launch_ready_only": False, "block_reasons": ["Правило не найдено"], "warnings": []}
        saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
        saved_post_exists = bool(saved_post_id and self.repo.get_saved_post(int(saved_post_id)))
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
        main_target_id = getattr(rule, "target_id", None)
        main_target_ready = bool(main_target_id)
        targets = self.repo.list_rule_repost_campaign_targets(rule_id, active_only=False) or []
        ready_extra_targets = []
        paused_targets = []
        problem_targets = []
        for row in targets:
            publish_status = (row.get("publish_status") or "").strip().lower()
            delete_status = (row.get("delete_permission_status") or "").strip().lower()
            can_post = row.get("can_post")
            if publish_status not in {"confirmed", "denied", "unknown"}:
                if can_post is True:
                    publish_status = "confirmed"
                elif can_post is False:
                    publish_status = "denied"
                else:
                    publish_status = "unknown"
            if delete_status not in {"confirmed", "denied", "unknown"}:
                if row.get("can_delete") is True:
                    delete_status = "confirmed"
                elif row.get("can_delete") is False:
                    delete_status = "denied"
                else:
                    delete_status = "unknown"
            legacy_error = bool(str(row.get("last_check_error") or "").strip())
            if not (row.get("publish_status") or row.get("delete_permission_status") or row.get("can_post") is not None or row.get("can_delete") is not None):
                has_problem = legacy_error
            else:
                has_problem = publish_status in {"denied", "unknown"} or delete_status == "denied"
            is_active = True if row.get("is_active") is None else bool(row.get("is_active"))
            if has_problem:
                problem_targets.append(row)
            elif is_active:
                ready_extra_targets.append(row)
            else:
                paused_targets.append(row)
        extra_active_problem = sum(1 for row in problem_targets if bool(row.get("is_active")))
        targets_publish_unknown = 0
        targets_publish_denied = 0
        targets_delete_unknown = 0
        targets_delete_denied = 0
        for row in targets:
            p = (row.get("publish_status") or "").strip().lower()
            d = (row.get("delete_permission_status") or "").strip().lower()
            if p == "unknown":
                targets_publish_unknown += 1
            elif p == "denied":
                targets_publish_denied += 1
            if d == "unknown":
                targets_delete_unknown += 1
            elif d == "denied":
                targets_delete_denied += 1
        main_targets_count = 1 if main_target_ready else 0
        will_send_total = main_targets_count + len(ready_extra_targets)
        will_skip_total = len(paused_targets) + len(problem_targets)
        block_reasons = []
        if not saved_post_exists:
            block_reasons.append("Не выбран рекламный пост.")
        if show_seconds <= 0:
            block_reasons.append("Не задано время показа.")
        if not main_target_ready:
            block_reasons.append("У правила не задан основной канал.")
        if extra_active_problem > 0:
            block_reasons.append("Есть активные каналы/группы, которые требуют настройки.")
        if will_send_total <= 0:
            block_reasons.append("Нет получателей для запуска кампании.")
        has_skipped = will_skip_total > 0
        last_run = self.repo.list_campaign_runs_for_rule(rule_id, limit=1)[0] if self.repo.list_campaign_runs_for_rule(rule_id, limit=1) else None
        last_run_details = self.get_campaign_run_details(rule_id=rule_id, run_id=int(last_run.get("id"))) if last_run else {}
        summary = (last_run_details or {}).get("summary") or {}
        delete_pending = int(summary.get("delete_pending") or 0)
        delete_failed = int(summary.get("delete_failed") or 0)
        active_placement = delete_pending > 0 or int(summary.get("delete_processing") or 0) > 0
        can_launch = saved_post_exists and show_seconds > 0 and main_target_ready and extra_active_problem == 0 and will_send_total > 0
        if active_placement:
            block_reasons.append("Предыдущая кампания ещё активна: публикации ожидают автоудаления.")
            can_launch = False
        if delete_failed > 0:
            block_reasons.append("Есть проблемы удаления в предыдущем запуске.")
            can_launch = False
        can_launch_ready_only = saved_post_exists and show_seconds > 0 and main_target_ready and will_send_total > 0 and has_skipped
        result = {
            "ok": True,
            "rule_id": rule_id,
            "ready": can_launch,
            "can_launch": can_launch,
            "can_launch_ready_only": can_launch_ready_only,
            "block_reasons": block_reasons,
            "warnings": [],
            "saved_post_id": int(saved_post_id) if saved_post_id else None,
            "saved_post_exists": saved_post_exists,
            "show_seconds": show_seconds,
            "main_target_id": str(main_target_id) if main_target_id else None,
            "main_target_ready": main_target_ready,
            "main_targets_count": main_targets_count,
            "extra_total": len(targets),
            "extra_ready": len(ready_extra_targets),
            "extra_paused": len(paused_targets),
            "extra_problem": len(problem_targets),
            "extra_active_problem": extra_active_problem,
            "will_send_total": will_send_total,
            "will_skip_total": will_skip_total,
            "ready_extra_targets": ready_extra_targets,
            "paused_targets": paused_targets,
            "problem_targets": problem_targets,
            "active_placement": active_placement,
            "active_run_id": int(last_run.get("id")) if last_run else None,
            "delete_pending": delete_pending,
            "delete_failed": delete_failed,
            "targets_delete_unknown": targets_delete_unknown,
            "targets_delete_denied": targets_delete_denied,
            "targets_publish_unknown": targets_publish_unknown,
            "targets_publish_denied": targets_publish_denied,
        }
        self.logger.info("REPOST_CAMPAIGN_LAUNCH_READINESS | rule_id=%s can_launch=%s will_send_total=%s extra_ready=%s extra_paused=%s extra_problem=%s extra_active_problem=%s", rule_id, can_launch, will_send_total, len(ready_extra_targets), len(paused_targets), len(problem_targets), extra_active_problem)
        return result

    def get_campaign_readiness(self, *, rule_id: int) -> dict:
        rule = self.repo.get_rule(rule_id)
        summary = self.repo.get_rule_repost_campaign_summary(rule_id)
        saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None) if rule else None
        show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0) if rule else 0
        targets_active = int(summary.get("targets_active", 0) or 0)
        targets_with_errors = int(summary.get("targets_with_errors", 0) or 0)

        warnings: list[str] = []
        if not saved_post_id:
            warnings.append("Рекламный пост не выбран")
        if show_seconds <= 0:
            warnings.append("Срок показа не задан")
        if targets_active <= 0:
            warnings.append("Активных каналов кампании пока нет")
        if targets_with_errors > 0:
            warnings.append(f"Есть каналы, которые требуют проверки: {targets_with_errors}")

        ready = bool(saved_post_id) and show_seconds > 0 and targets_active > 0
        return {
            "rule_id": rule_id,
            "post_selected": bool(saved_post_id),
            "saved_post_id": saved_post_id,
            "show_seconds_ok": show_seconds > 0,
            "show_seconds": show_seconds,
            "targets_active": targets_active,
            "targets_with_errors": targets_with_errors,
            "ready": ready,
            "warnings": warnings,
            "status": "ready" if ready else "warning",
            "post_status_text": "✅ выбран" if saved_post_id else "❌ не выбран",
            "show_seconds_status_text": f"✅ {format_campaign_show_seconds_ru(show_seconds)}" if show_seconds > 0 else "❌ не задан",
            "targets_status_text": (
                f"✅ {targets_active} активных" if targets_active > 0 else "❌ нет активных каналов"
            ),
            "checks_status_text": (
                "✅ ошибок нет" if targets_with_errors <= 0 else f"⚠️ требуют проверки: {targets_with_errors}"
            ),
            "summary_text": (
                "✅ Кампания готова к тестовому запуску"
                if ready
                else "⚠️ Кампания не готова: исправьте пункты выше"
            ),
        }

    def get_campaign_history(self, *, rule_id: int, limit: int = 10) -> dict:
        try:
            rule = self.repo.get_rule(rule_id)
            if not rule:
                return {
                    "ok": False,
                    "rule_id": rule_id,
                    "error_text": "Правило не найдено",
                    "runs": [],
                    "summary": {},
                }
            runs = self.repo.list_campaign_runs_for_rule(rule_id, limit=limit)
            total = len(runs)
            sent = sum(1 for row in runs if row.get("status") == "sent")
            failed = sum(1 for row in runs if row.get("status") == "failed")
            partial = sum(1 for row in runs if row.get("status") == "partial")
            sending = sum(1 for row in runs if row.get("status") in {"created", "sending"})
            last_run = runs[0] if runs else None
            self.logger.info("REPOST_CAMPAIGN_HISTORY_OPENED | rule_id=%s | runs=%s", rule_id, total)
            return {
                "ok": True,
                "rule_id": rule_id,
                "runs": runs,
                "summary": {
                    "total": total,
                    "sent": sent,
                    "failed": failed,
                    "partial": partial,
                    "sending": sending,
                    "last_run": last_run,
                },
            }
        except Exception as exc:
            self.logger.exception("REPOST_CAMPAIGN_HISTORY_FAILED | rule_id=%s | error=%s", rule_id, exc)
            return {
                "ok": False,
                "rule_id": rule_id,
                "error_text": "Не удалось загрузить историю кампаний",
                "runs": [],
                "summary": {},
            }

    def get_campaign_run_details(self, *, rule_id: int, run_id: int) -> dict:
        try:
            run = self.repo.get_campaign_run(run_id)
            if not run:
                return {"ok": False, "rule_id": rule_id, "run_id": run_id, "error_text": "Запуск кампании не найден"}
            if int(run.get("rule_id") or 0) != int(rule_id):
                return {
                    "ok": False,
                    "rule_id": rule_id,
                    "run_id": run_id,
                    "error_text": "Запуск не относится к этому правилу",
                }
            messages = self.repo.list_campaign_run_messages(run_id)
            total = len(messages)
            sent = sum(1 for row in messages if row.get("send_status") == "sent")
            failed = sum(1 for row in messages if row.get("send_status") == "failed")
            pending = sum(1 for row in messages if row.get("send_status") in {"pending", "sending"})
            delete_pending = sum(1 for row in messages if row.get("delete_status") == "pending")
            deleted = sum(1 for row in messages if row.get("delete_status") == "deleted")
            delete_failed = sum(1 for row in messages if row.get("delete_status") == "failed")
            self.logger.info(
                "REPOST_CAMPAIGN_RUN_DETAILS_OPENED | rule_id=%s | run_id=%s | messages=%s",
                rule_id,
                run_id,
                total,
            )
            return {
                "ok": True,
                "rule_id": rule_id,
                "run_id": run_id,
                "run": run,
                "messages": messages,
                "summary": {
                    "total": total,
                    "sent": sent,
                    "failed": failed,
                    "pending": pending,
                    "delete_pending": delete_pending,
                    "deleted": deleted,
                    "delete_failed": delete_failed,
                },
            }
        except Exception as exc:
            self.logger.exception(
                "REPOST_CAMPAIGN_RUN_DETAILS_FAILED | rule_id=%s | run_id=%s | error=%s",
                rule_id,
                run_id,
                exc,
            )
            return {
                "ok": False,
                "rule_id": rule_id,
                "run_id": run_id,
                "error_text": "Не удалось загрузить детали запуска",
            }

    def get_campaign_control_center(self, *, rule_id: int) -> dict:
        try:
            readiness = self.get_campaign_readiness(rule_id=rule_id)
            history = self.get_campaign_history(rule_id=rule_id, limit=5)
            last_run = (history.get("summary") or {}).get("last_run")
            last_run_details = None
            if last_run and last_run.get("id"):
                last_run_details = self.get_campaign_run_details(rule_id=rule_id, run_id=int(last_run["id"]))

            issues: list[str] = []
            for warning in readiness.get("warnings", []):
                issues.append(str(warning))

            details_summary = ((last_run_details or {}).get("summary") or {}) if (last_run_details or {}).get("ok") else {}
            failed = int(details_summary.get("failed") or 0)
            delete_failed = int(details_summary.get("delete_failed") or 0)
            delete_pending = int(details_summary.get("delete_pending") or 0)
            if failed > 0:
                issues.append(f"Ошибок отправки в последнем запуске: {failed}")
            if delete_failed > 0:
                issues.append(f"Ошибок удаления в последнем запуске: {delete_failed}")
            if delete_pending > 0:
                issues.append(f"Ожидают автоудаления: {delete_pending}")

            self.logger.info(
                "REPOST_CAMPAIGN_CONTROL_CENTER_OPENED | rule_id=%s | last_run_id=%s | issues=%s",
                rule_id,
                (last_run or {}).get("id"),
                len(issues),
            )
            return {
                "ok": True,
                "rule_id": rule_id,
                "readiness": readiness,
                "history": history,
                "last_run": last_run,
                "last_run_details": last_run_details,
                "issues": issues,
            }
        except Exception as exc:
            self.logger.exception("REPOST_CAMPAIGN_CONTROL_CENTER_FAILED | rule_id=%s | error=%s", rule_id, exc)
            return {
                "ok": False,
                "rule_id": rule_id,
                "error_text": "Не удалось загрузить центр управления кампанией",
                "readiness": None,
                "history": None,
                "last_run": None,
                "last_run_details": None,
                "issues": [],
            }

    async def delete_campaign_run_message_now(
        self,
        *,
        rule_id: int,
        run_id: int,
        run_message_id: int,
        admin_id: int | None = None,
    ) -> RepostCampaignActionResult:
        _ = admin_id
        if self.deleter is None:
            return RepostCampaignActionResult(
                ok=False,
                action="delete_campaign_run_message_now",
                rule_id=rule_id,
                error_text="Delete service недоступен",
                extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id},
            )
        self.logger.info(
            "REPOST_CAMPAIGN_MANUAL_DELETE_STARTED | rule_id=%s | run_id=%s | run_message_id=%s",
            rule_id,
            run_id,
            run_message_id,
        )
        run = self.repo.get_campaign_run(run_id)
        if not run:
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="Запуск кампании не найден")
        if int(run.get("rule_id") or 0) != int(rule_id):
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="Запуск не относится к этому правилу")

        message = self.repo.get_campaign_run_message(run_message_id)
        if not message:
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="Публикация кампании не найдена")
        if int(message.get("run_id") or 0) != int(run_id) or int(message.get("rule_id") or 0) != int(rule_id):
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="Публикация не относится к этому запуску")
        if (message.get("send_status") or "").strip().lower() != "sent":
            self.logger.info("REPOST_CAMPAIGN_MANUAL_DELETE_SKIPPED | rule_id=%s | run_id=%s | run_message_id=%s | reason=%s", rule_id, run_id, run_message_id, "send_status_not_sent")
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="Публикация ещё не была успешно отправлена")
        message_ids = self._extract_sent_message_ids(message)
        if not message_ids:
            self.logger.info("REPOST_CAMPAIGN_MANUAL_DELETE_SKIPPED | rule_id=%s | run_id=%s | run_message_id=%s | reason=%s", rule_id, run_id, run_message_id, "missing_sent_message_id")
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, error_text="У публикации нет Telegram message_id для удаления")
        if (message.get("delete_status") or "").strip().lower() == "deleted":
            self.logger.info("REPOST_CAMPAIGN_MANUAL_DELETE_SKIPPED | rule_id=%s | run_id=%s | run_message_id=%s | reason=%s", rule_id, run_id, run_message_id, "already_deleted")
            return RepostCampaignActionResult(
                ok=True, action="delete_campaign_run_message_now", rule_id=rule_id, target_id=str(message.get("target_id")), message_id=int(message.get("sent_message_id")), method="already_deleted",
                extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id, "delete_status": "deleted", "already_deleted": True},
            )
        snapshot_result = await self.collect_final_views_for_campaign_run_message(message)
        if snapshot_result.get("retry"):
            return RepostCampaignActionResult(
                ok=False,
                action="delete_campaign_run_message_now",
                rule_id=rule_id,
                error_text="Не удалось зафиксировать просмотры перед удалением. Повторите позже.",
            )
        if len(message_ids) > 1 and hasattr(self.deleter, "delete_messages"):
            result = await self.deleter.delete_messages(target_id=message["target_id"], message_ids=message_ids, render_mode=message.get("render_mode"))
        else:
            result = await self.deleter.delete_message(target_id=message["target_id"], message_id=message_ids[0], render_mode=message.get("render_mode"))
        if result.ok:
            self.repo.mark_campaign_run_message_deleted(run_message_id)
            self.logger.info("REPOST_CAMPAIGN_MANUAL_DELETE_DONE | rule_id=%s | run_id=%s | run_message_id=%s | target_id=%s | message_id=%s | method=%s", rule_id, run_id, run_message_id, message["target_id"], int(message["sent_message_id"]), result.method)
            return RepostCampaignActionResult(
                ok=True, action="delete_campaign_run_message_now", rule_id=rule_id, target_id=str(message["target_id"]), message_id=int(message["sent_message_id"]), method=result.method,
                extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id, "delete_status": "deleted"},
            )
        self.repo.mark_campaign_run_message_delete_failed(run_message_id, error_text=result.error_text or "unknown delete error")
        self.logger.warning("REPOST_CAMPAIGN_MANUAL_DELETE_FAILED | rule_id=%s | run_id=%s | run_message_id=%s | error=%s", rule_id, run_id, run_message_id, result.error_text)
        return RepostCampaignActionResult(
            ok=False, action="delete_campaign_run_message_now", rule_id=rule_id, target_id=str(message["target_id"]), message_id=int(message["sent_message_id"]), method=result.method, error_text=result.error_text,
            extra={"campaign_run_id": run_id, "campaign_run_message_id": run_message_id, "delete_status": "failed"},
        )

    async def delete_campaign_run_now(self, *, rule_id: int, run_id: int, admin_id: int | None = None) -> RepostCampaignActionResult:
        _ = admin_id
        if self.deleter is None:
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_now", rule_id=rule_id, error_text="Delete service недоступен", extra={"campaign_run_id": run_id})
        run = self.repo.get_campaign_run(run_id)
        if not run:
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_now", rule_id=rule_id, error_text="Запуск кампании не найден")
        if int(run.get("rule_id") or 0) != int(rule_id):
            return RepostCampaignActionResult(ok=False, action="delete_campaign_run_now", rule_id=rule_id, error_text="Запуск не относится к этому правилу")
        messages = self.repo.list_campaign_run_messages(run_id)
        deleted = failed = skipped = 0
        for message in messages:
            row_id = int(message.get("id") or 0)
            status = (message.get("delete_status") or "").strip().lower()
            if not row_id or status == "deleted" or status not in {"pending", "processing", "failed"}:
                skipped += 1
                continue
            result = await self.delete_campaign_run_message_now(rule_id=rule_id, run_id=run_id, run_message_id=row_id, admin_id=admin_id)
            if result.ok:
                deleted += 1
            else:
                failed += 1
        return RepostCampaignActionResult(
            ok=(failed == 0),
            action="delete_campaign_run_now",
            rule_id=rule_id,
            extra={"campaign_run_id": run_id, "deleted": deleted, "failed": failed, "skipped": skipped},
            error_text=None if failed == 0 else "Не все публикации удалось удалить",
        )

    async def process_due_deletions(self, *, limit: int = 50) -> dict[str, Any]:
        if self.deleter is None:
            return {"ok": False, "claimed": 0, "deleted": 0, "failed": 0, "error_text": "Delete service недоступен"}
        reset_count = self.repo.reset_stuck_campaign_delete_processing(stuck_seconds=300)
        rows = self.repo.claim_due_campaign_run_messages_for_delete(limit=limit)
        if not rows:
            return {"ok": True, "claimed": 0, "deleted": 0, "failed": 0, "reset_stuck": reset_count}
        self.logger.info("REPOST_CAMPAIGN_DELETE_BATCH_START | claimed=%s | reset_stuck=%s", len(rows), reset_count)
        deleted_count = 0
        failed_count = 0
        views_collected = 0
        views_unavailable = 0
        views_failed = 0
        for row in rows:
            row_id = int(row["id"])
            target_id = row["target_id"]
            views_status = str(row.get("views_final_status") or "pending").strip().lower()
            if views_status not in {"collected", "unavailable"}:
                self.logger.info("REPOST_CAMPAIGN_DELETE_WAIT_FINAL_VIEWS | rule_id=%s | run_id=%s | campaign_run_message_id=%s", row.get("rule_id"), row.get("run_id"), row_id)
                snapshot_result = await self.collect_final_views_for_campaign_run_message(row)
                if snapshot_result.get("retry"):
                    views_failed += 1
                    continue
                if snapshot_result.get("status") == "collected":
                    views_collected += 1
                elif snapshot_result.get("status") == "unavailable":
                    views_unavailable += 1
                self.logger.info("REPOST_CAMPAIGN_DELETE_AFTER_FINAL_VIEWS | rule_id=%s | run_id=%s | campaign_run_message_id=%s | views_final_status=%s", row.get("rule_id"), row.get("run_id"), row_id, snapshot_result.get("status"))
            message_ids = self._extract_sent_message_ids(row)
            if not message_ids:
                self.repo.mark_campaign_run_message_delete_failed(row_id, error_text="missing sent_message_id")
                failed_count += 1
                continue
            if len(message_ids) > 1 and hasattr(self.deleter, "delete_messages"):
                result = await self.deleter.delete_messages(target_id=target_id, message_ids=message_ids, render_mode=row.get("render_mode"))
            else:
                result = await self.deleter.delete_message(target_id=target_id, message_id=message_ids[0], render_mode=row.get("render_mode"))
            sent_message_id = message_ids[0]
            if result.ok:
                self.repo.mark_campaign_run_message_deleted(row_id)
                deleted_count += 1
                self.logger.info("REPOST_CAMPAIGN_DELETE_MESSAGE_DONE | row_id=%s | target_id=%s | sent_message_id=%s | method=%s", row_id, target_id, sent_message_id, result.method)
            else:
                self.repo.mark_campaign_run_message_delete_failed(row_id, error_text=result.error_text or "unknown delete error")
                failed_count += 1
                self.logger.warning("REPOST_CAMPAIGN_DELETE_MESSAGE_FAILED | row_id=%s | target_id=%s | sent_message_id=%s | error=%s", row_id, target_id, sent_message_id, result.error_text)
        self.logger.info("REPOST_CAMPAIGN_DELETE_BATCH_DONE | claimed=%s | deleted=%s | failed=%s | reset_stuck=%s", len(rows), deleted_count, failed_count, reset_count)
        run_ids = {int(row.get("run_id") or 0) for row in rows if int(row.get("run_id") or 0) > 0}
        for run_id in sorted(run_ids):
            self.logger.info("REPOST_CAMPAIGN_VIEWS_REPORT_AVAILABLE | rule_id=%s | run_id=%s", int((rows[0] or {}).get("rule_id") or 0), run_id)
        return {"ok": True, "claimed": len(rows), "views_collected": views_collected, "views_unavailable": views_unavailable, "views_failed": views_failed, "deleted": deleted_count, "failed": failed_count, "reset_stuck": reset_count}

    async def collect_final_views_for_campaign_run_message(self, message: dict) -> dict:
        row_id = int(message.get("id") or 0)
        rule_id = int(message.get("rule_id") or 0)
        run_id = int(message.get("run_id") or 0)
        target_id = str(message.get("target_id") or "")
        sent_message_id = int((self._extract_sent_message_ids(message) or [0])[0] or 0)
        views_status = str(message.get("views_final_status") or "pending").strip().lower()
        if views_status in {"collected", "unavailable"}:
            return {"ok": True, "skipped": True, "status": views_status}
        now_iso = datetime.now(timezone.utc).isoformat()
        attempt = int(message.get("views_final_attempt_count") or 0) + 1
        self.logger.info(
            "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_STARTED | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
            rule_id, run_id, row_id, target_id, sent_message_id, None, "processing", attempt,
        )
        if self.telethon_client is None:
            self.repo.mark_campaign_run_message_views_unavailable(row_id, error_text="Сервис сбора просмотров недоступен", collected_at=now_iso)
            self.logger.info(
                "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_UNAVAILABLE | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                rule_id, run_id, row_id, target_id, sent_message_id, 0, "unavailable", attempt,
            )
            return {"ok": True, "status": "unavailable"}
        self.repo.mark_campaign_run_message_views_processing(row_id)
        message_ids = self._extract_sent_message_ids(message)
        if not message_ids:
            self.repo.mark_campaign_run_message_views_unavailable(row_id, error_text="Нет message_id для сбора просмотров", collected_at=now_iso)
            self.logger.info(
                "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_UNAVAILABLE | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                rule_id, run_id, row_id, target_id, sent_message_id, 0, "unavailable", attempt,
            )
            return {"ok": True, "status": "unavailable"}
        message_id = int(message_ids[0])
        try:
            entity = normalize_telethon_target(message.get("target_id"))
            msg = await self.telethon_client.get_messages(entity=entity, ids=message_id)
            if msg is not None and getattr(msg, "views", None) is not None:
                views_count = int(getattr(msg, "views", 0) or 0)
                self.repo.mark_campaign_run_message_views_collected(row_id, views_count=views_count, collected_at=now_iso)
                self.logger.info(
                    "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_DONE | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                    rule_id, run_id, row_id, target_id, message_id, views_count, "collected", attempt,
                )
                return {"ok": True, "status": "collected", "views_count": views_count}
            self.repo.mark_campaign_run_message_views_unavailable(row_id, error_text="Telegram не вернул просмотры перед удалением", collected_at=now_iso)
            self.logger.info(
                "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_UNAVAILABLE | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                rule_id, run_id, row_id, target_id, message_id, 0, "unavailable", attempt,
            )
            return {"ok": True, "status": "unavailable"}
        except Exception as exc:
            attempt_count = attempt
            if attempt_count < self.FINAL_VIEWS_MAX_ATTEMPTS:
                next_retry = (datetime.now(timezone.utc) + timedelta(seconds=self.FINAL_VIEWS_RETRY_DELAY_SECONDS)).isoformat()
                self.repo.mark_campaign_run_message_views_failed(row_id, error_text=str(exc), next_retry_at=next_retry)
                self.logger.warning(
                    "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_FAILED | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                    rule_id, run_id, row_id, target_id, message_id, None, "failed", attempt,
                )
                return {"ok": False, "retry": True, "status": "failed"}
            self.repo.mark_campaign_run_message_views_unavailable(row_id, error_text=str(exc), collected_at=now_iso)
            self.logger.info(
                "REPOST_CAMPAIGN_FINAL_VIEWS_COLLECT_UNAVAILABLE | rule_id=%s | run_id=%s | campaign_run_message_id=%s | target_id=%s | sent_message_id=%s | views_count=%s | views_final_status=%s | attempt=%s",
                rule_id, run_id, row_id, target_id, message_id, 0, "unavailable", attempt,
            )
            return {"ok": True, "status": "unavailable"}

    async def build_campaign_posts_library(self, *, rule_id: int) -> dict:
        try:
            rule = self.repo.get_rule(rule_id)
            if not rule:
                return {"ok": False, "rule_id": rule_id, "items": [], "summary": {}, "error_text": "Правило не найдено"}
            current_saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
            runs = self.repo.list_campaign_runs_for_rule(rule_id, limit=500)
            runs_by_post: dict[int, list[dict]] = {}
            all_saved_post_ids = set()
            if current_saved_post_id:
                all_saved_post_ids.add(int(current_saved_post_id))
            for run in runs:
                saved_post_id = int(run.get("saved_post_id") or 0)
                if saved_post_id <= 0:
                    continue
                all_saved_post_ids.add(saved_post_id)
                runs_by_post.setdefault(saved_post_id, []).append(run)

            items = []
            total_runs = 0
            for saved_post_id in sorted(all_saved_post_ids, key=lambda x: x, reverse=True):
                post_runs = runs_by_post.get(saved_post_id, [])
                total_runs += len(post_runs)
                saved_post = self.repo.get_saved_post(saved_post_id)
                content = (saved_post or {}).get("content") or {}
                kind = str(content.get("kind") or "unknown")
                is_album = kind == "album"
                media_count = len(content.get("media_items") or []) if is_album else 0
                description = get_saved_post_short_description(content) if saved_post else "пост недоступен"

                placements_total = placements_sent = placements_failed = 0
                for run in post_runs:
                    run_id = int(run.get("id") or 0)
                    details = self.get_campaign_run_details(rule_id=rule_id, run_id=run_id)
                    msgs = details.get("messages") or []
                    placements_total += len(msgs)
                    placements_sent += sum(1 for m in msgs if (m.get("send_status") or "").lower() == "sent")
                    placements_failed += sum(1 for m in msgs if (m.get("send_status") or "").lower() == "failed")
                items.append({
                    "saved_post_id": saved_post_id, "saved_post": saved_post, "kind": kind, "description": description,
                    "is_album": is_album, "media_count": media_count, "is_current": int(saved_post_id) == int(current_saved_post_id or 0),
                    "runs_count": len(post_runs), "last_run_id": int(post_runs[0].get("id")) if post_runs else None,
                    "last_started_at": post_runs[0].get("started_at") if post_runs else None, "last_status": post_runs[0].get("status") if post_runs else None,
                    "placements_total": placements_total, "placements_sent": placements_sent, "placements_failed": placements_failed,
                    "views_total": None, "views_available": 0, "views_unavailable": 0, "views_lazy": True,
                    "stats_hint": "Просмотры доступны внутри карточки поста",
                    "last_run_views_total": None, "top_channels": [], "problem_channels": [],
                })
            summary = {
                "posts_total": len(items),
                "current_saved_post_id": int(current_saved_post_id) if current_saved_post_id else None,
                "runs_total": total_runs,
                "placements_total": sum(int(item.get("placements_total") or 0) for item in items),
                "views_total": None,
                "views_available": 0,
                "views_unavailable": 0,
                "views_mode": "lazy",
            }
            self.logger.info("REPOST_CAMPAIGN_POST_LIBRARY_BUILT_FAST | rule_id=%s | posts=%s | runs=%s | placements=%s | views_mode=lazy", rule_id, len(items), total_runs, summary.get("placements_total"))
            return {"ok": True, "rule_id": rule_id, "items": items, "summary": summary, "error_text": None}
        except Exception as exc:
            self.logger.exception("REPOST_CAMPAIGN_POST_LIBRARY_BUILD_FAILED | rule_id=%s | error=%s", rule_id, exc)
            return {"ok": False, "rule_id": rule_id, "items": [], "summary": {}, "error_text": "Не удалось собрать библиотеку постов"}

    async def build_campaign_post_stats(self, *, rule_id: int, saved_post_id: int, include_live_views: bool = True) -> dict:
        self.logger.info("REPOST_CAMPAIGN_POST_STATS_BUILD_STARTED | rule_id=%s | saved_post_id=%s | include_live_views=%s", rule_id, saved_post_id, include_live_views)
        library = await self.build_campaign_posts_library(rule_id=rule_id)
        if not library.get("ok"):
            return {"ok": False, "rule_id": rule_id, "saved_post_id": saved_post_id, "error_text": library.get("error_text") or "Не удалось загрузить статистику"}
        item = next((x for x in library.get("items") or [] if int(x.get("saved_post_id") or 0) == int(saved_post_id)), None)
        if not item:
            return {"ok": False, "rule_id": rule_id, "saved_post_id": saved_post_id, "error_text": "Пост не найден в библиотеке кампании"}
        views_total = None
        views_available = 0
        views_unavailable = 0
        top_channels = []
        problem_channels = []
        channels_stats = []
        if include_live_views:
            runs = self.repo.list_campaign_runs_for_rule(rule_id, limit=500)
            runs_for_post = [r for r in runs if int(r.get("saved_post_id") or 0) == int(saved_post_id)]
            channels_index = {}
            for run in runs_for_post:
                run_id = int(run.get("id") or 0)
                if run_id <= 0:
                    continue
                details = self.get_campaign_run_details(rule_id=rule_id, run_id=run_id)
                if not details.get("ok"):
                    continue
                for msg in details.get("messages") or []:
                    send_status = str(msg.get("send_status") or "").strip().lower()
                    message_ids = self._extract_sent_message_ids(msg)
                    if send_status != "sent" or not message_ids:
                        continue
                    target_id = str(msg.get("target_id") or "")
                    if not target_id:
                        continue
                    row = channels_index.get(target_id)
                    if row is None:
                        row = {
                            "target_title": msg.get("target_title") or target_id,
                            "target_id": target_id,
                            "views_total": 0,
                            "views_status": "ok",
                            "views_source": None,
                            "runs_count": 0,
                            "unavailable_count": 0,
                            "error_text": None,
                        }
                        channels_index[target_id] = row
                    row["runs_count"] = int(row.get("runs_count") or 0) + 1
                    final_status = str(msg.get("views_final_status") or "").strip().lower()
                    if final_status == "collected":
                        count = int(msg.get("views_final_count") or 0)
                        row["views_total"] = int(row.get("views_total") or 0) + count
                        row["views_status"] = "ok"
                        row["views_source"] = "final_snapshot"
                        views_available += 1
                        views_total = int(views_total or 0) + count
                        continue
                    if final_status == "unavailable":
                        row["unavailable_count"] = int(row.get("unavailable_count") or 0) + 1
                        if int(row.get("views_total") or 0) <= 0:
                            row["views_status"] = "unavailable"
                        else:
                            row["views_status"] = "ok"
                        row["views_source"] = "final_snapshot"
                        row["error_text"] = msg.get("views_final_error_text") or "Telegram не вернул просмотры перед удалением."
                        views_unavailable += 1
                        if views_total is None:
                            views_total = 0
                        continue
                    delete_status = str(msg.get("delete_status") or "").strip().lower()
                    if delete_status == "deleted":
                        row["views_status"] = "unavailable"
                        row["views_source"] = "final_snapshot"
                        row["error_text"] = "Публикация уже удалена, финальные просмотры недоступны."
                        views_unavailable += 1
                        if views_total is None:
                            views_total = 0
                        continue
                    if not include_live_views:
                        continue
                    report_message_id = int(message_ids[0] or 0)
                    if report_message_id <= 0:
                        row["views_status"] = "unavailable"
                        row["error_text"] = "Нет идентификатора сообщения для получения просмотров."
                        views_unavailable += 1
                        if views_total is None:
                            views_total = 0
                        continue
                    try:
                        normalized_entity = normalize_telethon_target(target_id)
                        try:
                            telethon_message = await self.telethon_client.get_messages(entity=normalized_entity, ids=report_message_id)
                        except Exception:
                            entity = await self.telethon_client.get_entity(normalized_entity)
                            telethon_message = await self.telethon_client.get_messages(entity=entity, ids=report_message_id)
                        views = getattr(telethon_message, "views", None) if telethon_message is not None else None
                        if views is None:
                            row["views_status"] = "unavailable"
                            row["views_source"] = "live"
                            row["error_text"] = "Публикация уже удалена или Telegram не вернул данные."
                            views_unavailable += 1
                            if views_total is None:
                                views_total = 0
                        else:
                            row["views_total"] = int(row.get("views_total") or 0) + int(views or 0)
                            row["views_status"] = "ok"
                            row["views_source"] = "live"
                            views_available += 1
                            views_total = int(views_total or 0) + int(views or 0)
                    except Exception:
                        row["views_status"] = "unavailable"
                        row["views_source"] = "live"
                        row["error_text"] = "Telegram не вернул просмотры по этому каналу."
                        views_unavailable += 1
                        if views_total is None:
                            views_total = 0
            channels_stats = list(channels_index.values())
            channels_stats.sort(key=lambda x: int(x.get("views_total") or 0), reverse=True)
            top_channels = [x for x in channels_stats if str(x.get("views_status")) == "ok"][:5]
            problem_channels = [x for x in channels_stats if str(x.get("views_status")) in {"unavailable", "failed", "problem"}]
        result = {"ok": True, "rule_id": rule_id, "saved_post_id": saved_post_id, "saved_post": item.get("saved_post"), "description": item.get("description"), "kind": item.get("kind"), "is_album": item.get("is_album"), "media_count": item.get("media_count"), "is_current": item.get("is_current"), "runs_count": item.get("runs_count"), "last_run_id": item.get("last_run_id"), "last_started_at": item.get("last_started_at"), "views_total": views_total, "views_available": views_available, "views_unavailable": views_unavailable, "views_mode": ("live" if include_live_views else "lazy"), "placements_total": item.get("placements_total"), "placements_sent": item.get("placements_sent"), "placements_failed": item.get("placements_failed"), "channels": top_channels, "runs": [], "top_channels": top_channels, "problem_channels": problem_channels, "channels_stats": channels_stats, "error_text": None}
        self.logger.info("REPOST_CAMPAIGN_POST_STATS_BUILT | rule_id=%s | saved_post_id=%s | views_mode=%s | views_total=%s", rule_id, saved_post_id, result.get("views_mode"), result.get("views_total"))
        return result

    def select_campaign_saved_post_from_library(self, *, rule_id: int, saved_post_id: int, admin_id: int | None = None) -> dict:
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return {"ok": False, "error_text": "Правило не найдено"}
        saved_post = self.repo.get_saved_post(int(saved_post_id))
        if not saved_post:
            return {"ok": False, "error_text": "Рекламный пост не найден"}
        current_saved_post_id = getattr(rule, "repost_campaign_saved_post_id", None)
        allowed = int(saved_post_id) == int(current_saved_post_id or 0)
        if not allowed:
            runs = self.repo.list_campaign_runs_for_rule(rule_id, limit=500)
            allowed = any(int(r.get("saved_post_id") or 0) == int(saved_post_id) for r in runs)
        if not allowed:
            return {"ok": False, "error_text": "Этот пост не относится к библиотеке кампании"}
        ok = bool(self.repo.set_rule_repost_campaign_saved_post(rule_id, int(saved_post_id)))
        if ok:
            self.logger.info("REPOST_CAMPAIGN_POST_SELECTED_FROM_LIBRARY | rule_id=%s | saved_post_id=%s | admin_id=%s", rule_id, saved_post_id, admin_id)
            return {"ok": True, "rule_id": rule_id, "saved_post_id": int(saved_post_id), "error_text": None}
        return {"ok": False, "error_text": "Не удалось выбрать пост для кампании"}

    async def build_campaign_views_report(self, *, rule_id: int, run_id: int) -> dict:
        if self.telethon_client is None:
            return {"ok": False, "rule_id": rule_id, "run_id": run_id, "status": "unavailable", "error_text": "Сервис сбора просмотров временно недоступен."}
        details = self.get_campaign_run_details(rule_id=rule_id, run_id=run_id)
        if not details.get("ok"):
            return {
                "ok": False,
                "rule_id": rule_id,
                "run_id": run_id,
                "run": None,
                "messages": [],
                "status": "not_found",
                "summary_text": "Запуск кампании не найден.",
                "error_text": details.get("error_text") or "Запуск кампании не найден.",
            }
        run = details.get("run") or {}
        messages = details.get("messages") or []
        items, top_items, problem_items = [], [], []
        views_total = views_available = views_unavailable = sent_total = 0
        for msg in messages:
            send_status = str(msg.get("send_status") or "")
            message_ids = self._extract_sent_message_ids(msg)
            item = {
                "target_kind": msg.get("target_kind") or "extra",
                "target_id": str(msg.get("target_id") or ""),
                "target_title": msg.get("target_title") or str(msg.get("target_id") or "Канал/группа"),
                "message_id": message_ids[0] if message_ids else None,
                "message_ids": message_ids,
                "is_album": len(message_ids) > 1,
                "album_items": len(message_ids),
                "send_status": send_status,
                "delete_status": msg.get("delete_status"),
                "deleted_at": msg.get("deleted_at"),
                "views": None,
                "views_status": "not_sent",
                "error_text": None,
            }
            if send_status.strip().lower() != "sent" or not message_ids:
                items.append(item)
                continue
            sent_total += 1
            final_status = str(msg.get("views_final_status") or "").strip().lower()
            if final_status == "collected":
                item["views"] = int(msg.get("views_final_count") or 0)
                item["views_status"] = "ok"
                item["views_source"] = "final_snapshot"
                views_total += int(item["views"])
                views_available += 1
                top_items.append(item)
                items.append(item)
                continue
            if final_status == "unavailable":
                item["views"] = 0
                item["views_status"] = "unavailable"
                item["views_source"] = "final_snapshot"
                item["error_text"] = msg.get("views_final_error_text") or "Telegram не вернул просмотры перед удалением."
                views_unavailable += 1
                problem_items.append(item)
                items.append(item)
                continue
            try:
                target_id = msg.get("target_id")
                normalized_entity = normalize_telethon_target(target_id)
                report_message_id = int(message_ids[0])
                try:
                    telethon_message = await self.telethon_client.get_messages(entity=normalized_entity, ids=report_message_id)
                except Exception:
                    entity = await self.telethon_client.get_entity(normalized_entity)
                    telethon_message = await self.telethon_client.get_messages(entity=entity, ids=report_message_id)
                if telethon_message is None:
                    item["views_status"] = "unavailable"
                    item["error_text"] = "Публикация уже удалена или Telegram не вернул данные."
                    views_unavailable += 1
                elif int(getattr(telethon_message, "id", 0) or 0) != report_message_id:
                    item["views_status"] = "failed"
                    item["error_text"] = "Telegram не подтвердил нужное сообщение. Просмотры не засчитаны."
                    views_unavailable += 1
                    self.logger.warning(
                        "REPOST_CAMPAIGN_VIEWS_MESSAGE_MISMATCH | rule_id=%s | run_id=%s | target_id=%s | expected_message_id=%s | actual_message_id=%s",
                        rule_id,
                        run_id,
                        target_id,
                        report_message_id,
                        getattr(telethon_message, "id", None),
                    )
                else:
                    expected_peer = self._normalize_telegram_channel_id_for_compare(target_id)
                    actual_peer = self._extract_telethon_message_peer_id(telethon_message)
                    if expected_peer and actual_peer and expected_peer != actual_peer:
                        item["views_status"] = "failed"
                        item["error_text"] = "Telegram вернул данные другого канала. Просмотры не засчитаны."
                        views_unavailable += 1
                        self.logger.warning(
                            "REPOST_CAMPAIGN_VIEWS_PEER_MISMATCH | rule_id=%s | run_id=%s | target_id=%s | expected_peer=%s | actual_peer=%s | message_id=%s",
                            rule_id,
                            run_id,
                            target_id,
                            expected_peer,
                            actual_peer,
                            report_message_id,
                        )
                    else:
                        if actual_peer is None:
                            self.logger.info(
                                "REPOST_CAMPAIGN_VIEWS_PEER_UNKNOWN | rule_id=%s | run_id=%s | target_id=%s | message_id=%s",
                                rule_id,
                                run_id,
                                target_id,
                                report_message_id,
                            )
                        views = getattr(telethon_message, "views", None)
                        if views is None:
                            item["views_status"] = "unavailable"
                            item["error_text"] = "Публикация уже удалена или Telegram не вернул данные."
                            views_unavailable += 1
                        else:
                            item["views_status"] = "ok"
                            item["views_source"] = "live"
                            item["views"] = int(views or 0)
                            views_total += int(views or 0)
                            views_available += 1
                            top_items.append(item)
                            self.logger.info(
                                "REPOST_CAMPAIGN_VIEWS_TARGET_COLLECTED | rule_id=%s | run_id=%s | target_id=%s | message_id=%s | views=%s | peer_id=%s",
                                rule_id,
                                run_id,
                                target_id,
                                report_message_id,
                                item["views"],
                                actual_peer,
                            )
            except Exception as exc:
                item["views_status"] = "failed"
                item["views_source"] = "unavailable"
                item["error_text"] = "Telegram не вернул просмотры по этому каналу."
                self.logger.warning(
                    "REPOST_CAMPAIGN_VIEWS_TARGET_FAILED | rule_id=%s | run_id=%s | target_id=%s | message_id=%s | error=%s",
                    rule_id,
                    run_id,
                    msg.get("target_id"),
                    message_ids[0] if message_ids else None,
                    format_campaign_error_text(exc, limit=160) or str(exc),
                )
                views_unavailable += 1
            if item["views_status"] in {"failed", "unavailable"}:
                problem_items.append(item)
            items.append(item)
        if sent_total <= 0:
            status = "unavailable"
            summary_text = "Просмотры не удалось получить. Возможно, публикации уже удалены или Telegram не вернул данные."
        elif views_available == sent_total:
            status = "ready"
            summary_text = "Просмотры получены по всем размещениям."
        elif views_available > 0 and views_unavailable > 0:
            status = "partial"
            summary_text = "Часть просмотров недоступна. Проверьте проблемные каналы."
        else:
            status = "unavailable"
            summary_text = "Просмотры не удалось получить. Возможно, публикации уже удалены или Telegram не вернул данные."
        top_items_sorted = sorted(top_items, key=lambda x: int(x.get("views") or 0), reverse=True)[:5]
        self.logger.info(
            "REPOST_CAMPAIGN_VIEWS_REPORT_BUILT | rule_id=%s | run_id=%s | status=%s | views_total=%s | views_available=%s | views_unavailable=%s | sent_total=%s",
            rule_id, run_id, status, views_total, views_available, views_unavailable, sent_total,
        )
        return {
            "ok": True,
            "rule_id": rule_id,
            "run_id": run_id,
            "run": run,
            "messages": messages,
            "status": status,
            "saved_post_id": run.get("saved_post_id"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
            "show_seconds": int(run.get("show_seconds") or 0),
            "targets_total": len(messages),
            "sent_total": sent_total,
            "views_total": views_total,
            "views_available": views_available,
            "views_unavailable": views_unavailable,
            "items": items,
            "top_items": top_items_sorted,
            "problem_items": problem_items,
            "summary_text": summary_text,
            "error_text": None,
        }
