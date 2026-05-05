from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from app.repost_campaign_service import build_campaign_delete_after_iso, format_campaign_show_seconds_ru


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
    def __init__(self, *, repo, renderer, logger_=None):
        self.repo = repo
        self.renderer = renderer
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
            sent_message_ids=None,
            render_mode=render_result.method,
        )
        self.repo.update_campaign_run_status(
            run_id,
            status="sent",
            render_mode=render_result.method,
            targets_success=1,
            targets_failed=0,
            finish=True,
            report={"target_id": str(target_id), "message_id": render_result.message_id, "method": render_result.method},
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
                    sent_message_ids=None,
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
