from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from app.repost_campaign_service import build_campaign_delete_after_iso, format_campaign_show_seconds_ru


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
    def __init__(self, *, repo, renderer, deleter=None, target_checker=None, logger_=None):
        self.repo = repo
        self.renderer = renderer
        self.deleter = deleter
        self.target_checker = target_checker
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
            ok = bool(self.repo.set_rule_repost_campaign_target_active(target_row_id, is_active))
            if ok:
                self.logger.info("REPOST_CAMPAIGN_TARGET_STATUS_UPDATED | rule_id=%s | target_row_id=%s | is_active=%s", rule_id, target_row_id, is_active)
            else:
                self.logger.warning("REPOST_CAMPAIGN_TARGET_STATUS_FAILED | rule_id=%s | target_row_id=%s | error=%s", rule_id, target_row_id, "update_failed")
            return {
                "ok": ok,
                "rule_id": rule_id,
                "target_row_id": target_row_id,
                "target_title": target.get("title") or target.get("target_id"),
                "is_active": is_active,
                "error_text": None if ok else "Не удалось обновить статус канала/группы",
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
                self.logger.warning("REPOST_CAMPAIGN_TARGET_REMOVE_FAILED | rule_id=%s | target_row_id=%s | error=%s", rule_id, target_row_id, "remove_failed")
            return {
                "ok": ok,
                "rule_id": rule_id,
                "target_row_id": target_row_id,
                "target_title": target.get("title") or target.get("target_id"),
                "error_text": None if ok else "Не удалось удалить канал/группу",
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
        saved = bool(self.repo.update_rule_repost_campaign_target_check_result(target_row_id, title=check.title, last_check_error=None if check.ok else check.error_text))
        if not saved:
            self.logger.warning("REPOST_CAMPAIGN_TARGET_CHECK_SAVE_FAILED | rule_id=%s | target_row_id=%s", rule_id, target_row_id)
        self.logger.info("REPOST_CAMPAIGN_TARGET_CHECKED | rule_id=%s | target_row_id=%s | ok=%s | error=%s", rule_id, target_row_id, check.ok, check.error_text)
        return {"ok": check.ok, "rule_id": rule_id, "target_row_id": target_row_id, "target_id": check.target_id, "target_title": check.title or target.get("title") or target.get("target_id"), "error_text": check.error_text, "can_view": check.can_view, "can_publish": check.can_publish, "can_delete": check.can_delete}

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
            if len(ids) > 1 and hasattr(self.deleter, "delete_messages"):
                await self.deleter.delete_messages(target_id=target_id, message_ids=ids, render_mode=render_mode)
            else:
                await self.deleter.delete_message(target_id=target_id, message_id=ids[0], render_mode=render_mode)
            self.logger.info("REPOST_CAMPAIGN_PREVIEW_DELETE_DONE | target_id=%s | message_ids=%s", target_id, ids)
            return RepostCampaignActionResult(
                ok=True,
                action="delete_preview_messages",
                rule_id=0,
                target_id=str(target_id),
                message_id=ids[0],
                method=render_mode,
                extra={"target_id": str(target_id), "message_id": message_id, "message_ids": ids, "render_mode": render_mode},
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

    async def launch_campaign_now(self, *, rule_id: int, admin_id: int | None = None) -> RepostCampaignActionResult:
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
        extra_targets = self.repo.list_rule_repost_campaign_targets(rule_id, active_only=True) or []
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
            run_type="manual",
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
            "REPOST_CAMPAIGN_LAUNCH_STARTED | rule_id=%s | saved_post_id=%s | targets=%s | run_id=%s",
            rule_id, saved_post_id, len(targets), run_id
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
            if render_result.ok:
                self.repo.mark_campaign_run_message_sent(
                    run_message_id,
                    sent_message_id=render_result.message_id,
                    sent_message_ids=getattr(render_result, "message_ids", None),
                    render_mode=render_result.method,
                )
                success_count += 1
                if render_result.method:
                    methods.add(render_result.method)
                self.logger.info(
                    "REPOST_CAMPAIGN_LAUNCH_TARGET_SENT | run_id=%s | target_id=%s | message_id=%s | method=%s",
                    run_id, target["target_id"], render_result.message_id, render_result.method
                )
            else:
                self.repo.mark_campaign_run_message_failed(
                    run_message_id,
                    error_text=render_result.error_text or "unknown error",
                    render_mode=render_result.method,
                )
                failed_count += 1
                if first_error_text is None:
                    first_error_text = render_result.error_text or "unknown error"
                if render_result.method:
                    methods.add(render_result.method)
                self.logger.warning(
                    "REPOST_CAMPAIGN_LAUNCH_TARGET_FAILED | run_id=%s | target_id=%s | error=%s | method=%s",
                    run_id, target["target_id"], render_result.error_text, render_result.method
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
            },
        )

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
        for row in rows:
            row_id = int(row["id"])
            target_id = row["target_id"]
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
        return {"ok": True, "claimed": len(rows), "deleted": deleted_count, "failed": failed_count, "reset_stuck": reset_count}
