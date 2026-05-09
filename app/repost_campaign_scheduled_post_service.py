from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.repost_campaign_runtime_service import RepostCampaignActionResult
from app.repost_campaign_schedule_service import (
    CAMPAIGN_SCHEDULE_TIMEZONE_LABEL,
    CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES,
    format_campaign_schedule_datetime,
)
from app.repost_campaign_service import format_campaign_show_seconds_ru, normalize_campaign_show_seconds

VIP_SCHEDULED_POST_TIMEZONE_OFFSET_MINUTES = 180
VIP_SCHEDULED_POST_TIMEZONE_LABEL = "UTC+3"
VIP_SCHEDULED_POST_MIN_START_DELAY_SECONDS = 60
VIP_SCHEDULED_POST_EDIT_LOCK_SECONDS = 120
VIP_SCHEDULED_POST_DEFAULT_LIMIT = 20


def scheduled_post_now_utc() -> datetime:
    return datetime.now(timezone.utc)


class RepostCampaignScheduledPostService:
    def __init__(self, *, repo, campaign_runtime, target_checker=None, logger_=None):
        self.repo = repo
        self.campaign_runtime = campaign_runtime
        self.target_checker = target_checker
        self.logger = logger_ or logging.getLogger("forwarder")

    def _can_edit(self, row: dict) -> bool:
        return str(row.get("status")) in {"draft", "ready"}

    def _can_cancel(self, row: dict) -> bool:
        return str(row.get("status")) in {"draft", "ready", "scheduled"}

    def _normalize_dt_utc(self, value: datetime | str | None) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _build_post_snapshot(self, saved_post: dict) -> dict:
        content = saved_post.get("content_json") or {}
        return {
            "id": int(saved_post.get("id") or 0),
            "title": saved_post.get("title"),
            "kind": content.get("kind"),
            "media_items_count": len(content.get("media_items") or []),
        }

    def _build_targets_from_current_campaign(self, *, rule, targets: list[dict], include_main_target: bool = True) -> list[dict]:
        out: list[dict[str, Any]] = []
        if include_main_target and getattr(rule, "target_id", None):
            out.append({"target_kind": "main", "target_id": str(rule.target_id), "target_thread_id": getattr(rule, "target_thread_id", None), "target_title": getattr(rule, "target_title", None), "is_active": True, "metadata": {"source": "current_campaign", "source_target_row_id": None}})
        for t in targets:
            out.append({"target_kind": "extra", "target_id": str(t.get("target_id")), "target_thread_id": t.get("target_thread_id"), "target_title": t.get("title"), "is_active": True, "metadata": {"source": "current_campaign", "source_target_row_id": t.get("id")}})
        return out

    def create_draft(self, *, rule_id: int, tenant_id: int = 1, created_by: int | None = None, title: str | None = None) -> RepostCampaignActionResult:
        rule = self.repo.get_rule(rule_id)
        if not rule:
            return RepostCampaignActionResult(ok=False, action="create_scheduled_post_draft", rule_id=rule_id, error_text="Правило не найдено")
        if str(getattr(rule, "mode", "") or "").strip().lower() != "repost":
            return RepostCampaignActionResult(ok=False, action="create_scheduled_post_draft", rule_id=rule_id, error_text="Запланированные рекламные посты доступны только для режима репоста")
        scheduled_post_id = self.repo.create_campaign_scheduled_post_draft(rule_id=rule_id, tenant_id=tenant_id, created_by=created_by, title=title)
        if not scheduled_post_id:
            return RepostCampaignActionResult(ok=False, action="create_scheduled_post_draft", rule_id=rule_id, error_text="Не удалось создать черновик запланированного поста")
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=rule_id, event_type="draft_created", status_to="draft", actor_id=created_by)
        self.logger.info("VIP_SCHEDULED_POST_DRAFT_CREATED | scheduled_post_id=%s | rule_id=%s", scheduled_post_id, rule_id)
        return RepostCampaignActionResult(ok=True, action="create_scheduled_post_draft", rule_id=rule_id, extra={"scheduled_post_id": scheduled_post_id, "status": "draft"})

    def update_draft_saved_post(self, *, scheduled_post_id: int, saved_post_id: int, actor_id: int | None = None) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not row or not self._can_edit(row):
            return RepostCampaignActionResult(ok=False, action="update_draft_saved_post", rule_id=int((row or {}).get("rule_id") or 0), error_text="Запланированный пост нельзя редактировать")
        saved_post = self.repo.get_saved_post(saved_post_id)
        if not saved_post:
            return RepostCampaignActionResult(ok=False, action="update_draft_saved_post", rule_id=int(row.get("rule_id") or 0), error_text="Рекламный пост не найден")
        self.repo.update_campaign_scheduled_post(scheduled_post_id, saved_post_id=int(saved_post_id), post_snapshot=self._build_post_snapshot(saved_post))
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=int(row.get("rule_id") or 0), event_type="saved_post_updated", actor_id=actor_id)
        self.logger.info("VIP_SCHEDULED_POST_SAVED_POST_UPDATED | scheduled_post_id=%s", scheduled_post_id)
        return RepostCampaignActionResult(ok=True, action="update_draft_saved_post", rule_id=int(row.get("rule_id") or 0), saved_post_id=int(saved_post_id), extra={"scheduled_post_id": scheduled_post_id})

    def update_draft_targets_from_current_campaign(self, *, scheduled_post_id: int, actor_id: int | None = None, include_main_target: bool = True) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not row or not self._can_edit(row):
            return RepostCampaignActionResult(ok=False, action="update_draft_targets", rule_id=int((row or {}).get("rule_id") or 0), error_text="Запланированный пост нельзя редактировать")
        rule_id = int(row.get("rule_id") or 0)
        rule = self.repo.get_rule(rule_id)
        targets = self.repo.list_rule_repost_campaign_targets(rule_id, active_only=True) or []
        snapshot = self._build_targets_from_current_campaign(rule=rule, targets=targets, include_main_target=include_main_target)
        total = self.repo.replace_campaign_scheduled_post_targets(scheduled_post_id=scheduled_post_id, rule_id=rule_id, targets=snapshot)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=rule_id, event_type="targets_snapshot_saved", actor_id=actor_id, extra={"targets_total": total})
        self.logger.info("VIP_SCHEDULED_POST_TARGETS_SNAPSHOT_SAVED | scheduled_post_id=%s | targets_total=%s", scheduled_post_id, total)
        return RepostCampaignActionResult(ok=True, action="update_draft_targets", rule_id=rule_id, extra={"scheduled_post_id": scheduled_post_id, "targets_total": total})

    def update_draft_show_seconds(self, *, scheduled_post_id: int, show_seconds: int, actor_id: int | None = None) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not row or not self._can_edit(row):
            return RepostCampaignActionResult(ok=False, action="update_draft_show_seconds", rule_id=int((row or {}).get("rule_id") or 0), error_text="Запланированный пост нельзя редактировать")
        try:
            normalized = normalize_campaign_show_seconds(show_seconds)
            if normalized <= 0:
                raise ValueError()
        except Exception:
            return RepostCampaignActionResult(ok=False, action="update_draft_show_seconds", rule_id=int(row.get("rule_id") or 0), error_text="Некорректный срок показа")
        self.repo.update_campaign_scheduled_post(scheduled_post_id, show_seconds=normalized)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=int(row.get("rule_id") or 0), event_type="show_seconds_updated", actor_id=actor_id, extra={"show_seconds": normalized})
        self.logger.info("VIP_SCHEDULED_POST_SHOW_SECONDS_UPDATED | scheduled_post_id=%s | show_seconds=%s", scheduled_post_id, normalized)
        return RepostCampaignActionResult(ok=True, action="update_draft_show_seconds", rule_id=int(row.get("rule_id") or 0), extra={"show_seconds": normalized})

    def update_draft_scheduled_at(self, *, scheduled_post_id: int, scheduled_at_utc: datetime, actor_id: int | None = None) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not row or not self._can_edit(row):
            return RepostCampaignActionResult(ok=False, action="update_draft_scheduled_at", rule_id=int((row or {}).get("rule_id") or 0), error_text="Запланированный пост нельзя редактировать")
        dt = self._normalize_dt_utc(scheduled_at_utc)
        min_dt = scheduled_post_now_utc() + timedelta(seconds=VIP_SCHEDULED_POST_MIN_START_DELAY_SECONDS)
        if not dt or dt < min_dt:
            return RepostCampaignActionResult(ok=False, action="update_draft_scheduled_at", rule_id=int(row.get("rule_id") or 0), error_text="Время запуска должно быть в будущем")
        self.repo.update_campaign_scheduled_post(scheduled_post_id, scheduled_at=dt.isoformat(), timezone_offset_minutes=VIP_SCHEDULED_POST_TIMEZONE_OFFSET_MINUTES, timezone_label=VIP_SCHEDULED_POST_TIMEZONE_LABEL)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=int(row.get("rule_id") or 0), event_type="scheduled_at_updated", actor_id=actor_id)
        self.logger.info("VIP_SCHEDULED_POST_SCHEDULED_AT_UPDATED | scheduled_post_id=%s", scheduled_post_id)
        return RepostCampaignActionResult(ok=True, action="update_draft_scheduled_at", rule_id=int(row.get("rule_id") or 0), extra={"scheduled_at": dt.isoformat()})

    def build_readiness(self, *, scheduled_post_id: int, now_utc: datetime | None = None) -> dict[str, Any]:
        now_utc = self._normalize_dt_utc(now_utc) or scheduled_post_now_utc()
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id) or {}
        rule_id = int(row.get("rule_id") or 0)
        saved_post_id = row.get("saved_post_id")
        block_reasons: list[str] = []
        warnings: list[str] = []
        rule = self.repo.get_rule(rule_id) if rule_id else None
        if not rule:
            block_reasons.append("Правило не найдено")
        elif str(getattr(rule, "mode", "") or "").strip().lower() != "repost":
            block_reasons.append("Запланированные рекламные посты доступны только для режима репоста")
        post_ready = bool(saved_post_id)
        if not post_ready:
            block_reasons.append("Рекламный пост не выбран")
        elif not self.repo.get_saved_post(int(saved_post_id)):
            post_ready = False
            block_reasons.append("Рекламный пост не найден")

        targets = self.repo.list_campaign_scheduled_post_targets(scheduled_post_id, active_only=True) or []
        targets_total = len(targets)
        if targets_total == 0:
            block_reasons.append("Каналы/группы не выбраны")
        ready_count = warning_count = blocked_count = 0
        for t in targets:
            ps = str(t.get("publish_status") or "unknown")
            ds = str(t.get("delete_status") or "unknown")
            cp = t.get("can_publish")
            if ps == "denied" or cp is False:
                blocked_count += 1
            elif ps == "confirmed" or cp is True:
                ready_count += 1
            else:
                warning_count += 1
            if ds == "unknown":
                warning_count += 1
        targets_ready = targets_total > 0 and blocked_count == 0

        show_seconds = int(row.get("show_seconds") or 0)
        if show_seconds <= 0:
            block_reasons.append("Срок показа не задан")
        scheduled_at = self._normalize_dt_utc(row.get("scheduled_at"))
        if not scheduled_at:
            block_reasons.append("Время запуска не задано")
        elif scheduled_at < now_utc + timedelta(seconds=VIP_SCHEDULED_POST_MIN_START_DELAY_SECONDS):
            block_reasons.append("Время запуска должно быть в будущем")

        launch = self.campaign_runtime.build_campaign_launch_readiness(rule_id=rule_id) if rule_id else {}
        if launch.get("active_placement"):
            warnings.append("Сейчас есть активный рекламный пост. При запуске ViMi проверит состояние повторно.")

        can_schedule = len(block_reasons) == 0 and blocked_count == 0
        can_launch = can_schedule
        scheduled_at_text = format_campaign_schedule_datetime(scheduled_at, timezone_offset_minutes=CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES, timezone_label=CAMPAIGN_SCHEDULE_TIMEZONE_LABEL)
        expected_delete_at_text = "—"
        if scheduled_at and show_seconds > 0:
            expected_delete_at_text = format_campaign_schedule_datetime(scheduled_at + timedelta(seconds=show_seconds), timezone_offset_minutes=CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES, timezone_label=CAMPAIGN_SCHEDULE_TIMEZONE_LABEL)
        readiness = {"ok": True, "can_schedule": can_schedule, "can_launch": can_launch, "scheduled_post_id": scheduled_post_id, "rule_id": rule_id, "status": row.get("status") or "draft", "saved_post_id": saved_post_id, "post_ready": post_ready, "post_status_text": "✅ Рекламный пост выбран" if post_ready else "❌ Рекламный пост не выбран", "targets_total": targets_total, "targets_ready_count": ready_count, "targets_warning_count": warning_count, "targets_blocked_count": blocked_count, "targets_ready": targets_ready, "targets_status_text": "✅ Каналы/группы выбраны" if targets_ready else "❌ Каналы/группы не готовы", "show_seconds": show_seconds, "show_seconds_ready": show_seconds > 0, "show_seconds_text": format_campaign_show_seconds_ru(show_seconds) if show_seconds > 0 else "—", "show_seconds_status_text": "✅ Срок показа задан" if show_seconds > 0 else "❌ Срок показа не задан", "scheduled_at": scheduled_at.isoformat() if scheduled_at else None, "scheduled_at_text": scheduled_at_text, "scheduled_at_ready": scheduled_at is not None, "scheduled_at_status_text": "✅ Время запуска задано" if scheduled_at else "❌ Время запуска не задано", "expected_delete_at_text": expected_delete_at_text, "active_placement": bool(launch.get("active_placement")), "delete_failed": int(launch.get("delete_failed") or 0), "block_reasons": block_reasons, "warnings": warnings, "summary_text": "✅ Запланированный пост готов" if can_schedule else "❌ Запланированный пост не готов"}
        self.repo.update_campaign_scheduled_post(scheduled_post_id, targets_total=targets_total, targets_ready=ready_count, targets_with_warnings=warning_count, targets_blocked=blocked_count, readiness_snapshot=readiness, preview={"scheduled_at_text": scheduled_at_text, "expected_delete_at_text": expected_delete_at_text})
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=rule_id, event_type="readiness_checked", extra={"can_schedule": can_schedule})
        self.logger.info("VIP_SCHEDULED_POST_READINESS_CHECKED | scheduled_post_id=%s | can_schedule=%s", scheduled_post_id, can_schedule)
        return readiness

    def schedule_post(self, *, scheduled_post_id: int, actor_id: int | None = None, now_utc: datetime | None = None) -> RepostCampaignActionResult:
        readiness = self.build_readiness(scheduled_post_id=scheduled_post_id, now_utc=now_utc)
        if not readiness.get("can_schedule"):
            return RepostCampaignActionResult(ok=False, action="schedule_scheduled_post", rule_id=int(readiness.get("rule_id") or 0), saved_post_id=readiness.get("saved_post_id"), error_text="Запланированный пост не готов", extra={"scheduled_post_id": scheduled_post_id, "readiness": readiness, "block_reasons": readiness["block_reasons"], "warnings": readiness["warnings"]})
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id) or {}
        ok = self.repo.schedule_campaign_scheduled_post(scheduled_post_id, scheduled_by=actor_id)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=int(readiness.get("rule_id") or 0), event_type="scheduled", status_from=row.get("status"), status_to="scheduled", actor_id=actor_id)
        self.logger.info("VIP_SCHEDULED_POST_SCHEDULED | scheduled_post_id=%s", scheduled_post_id)
        return RepostCampaignActionResult(ok=bool(ok), action="schedule_scheduled_post", rule_id=int(readiness.get("rule_id") or 0), saved_post_id=readiness.get("saved_post_id"), extra={"scheduled_post_id": scheduled_post_id, "scheduled_at": readiness.get("scheduled_at"), "scheduled_at_text": readiness.get("scheduled_at_text"), "expected_delete_at_text": readiness.get("expected_delete_at_text"), "readiness": readiness})

    def cancel_post(self, *, scheduled_post_id: int, actor_id: int | None = None, reason: str | None = None) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not row:
            return RepostCampaignActionResult(ok=False, action="cancel_scheduled_post", rule_id=0, error_text="Запланированный пост не найден")
        if not self._can_cancel(row):
            return RepostCampaignActionResult(ok=False, action="cancel_scheduled_post", rule_id=int(row.get("rule_id") or 0), error_text="Отмена недоступна для текущего статуса")
        ok = self.repo.cancel_campaign_scheduled_post(scheduled_post_id, cancelled_by=actor_id, reason=reason)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=int(row.get("rule_id") or 0), event_type="cancelled", status_from=row.get("status"), status_to="cancelled", actor_id=actor_id, extra={"reason": reason})
        self.logger.info("VIP_SCHEDULED_POST_CANCELLED | scheduled_post_id=%s", scheduled_post_id)
        return RepostCampaignActionResult(ok=bool(ok), action="cancel_scheduled_post", rule_id=int(row.get("rule_id") or 0), extra={"scheduled_post_id": scheduled_post_id})

    async def check_targets(self, *, scheduled_post_id: int, active_only: bool = True, actor_id: int | None = None, limit: int = 50) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_post(scheduled_post_id) or {}
        rule_id = int(row.get("rule_id") or 0)
        if not self.target_checker:
            return RepostCampaignActionResult(ok=False, action="check_scheduled_post_targets", rule_id=rule_id, error_text="Сервис проверки прав недоступен")
        targets = (self.repo.list_campaign_scheduled_post_targets(scheduled_post_id, active_only=active_only) or [])[: int(limit)]
        passed = failed = 0
        for t in targets:
            try:
                check = await self.target_checker.check_target(target_id=t["target_id"], target_thread_id=t.get("target_thread_id"))
                publish_status = getattr(check, "publish_status", "confirmed" if getattr(check, "can_publish", False) else "denied")
                delete_status = getattr(check, "delete_status", "unknown")
                status = "confirmed" if publish_status == "confirmed" else "denied" if publish_status == "denied" else "unknown"
                self.repo.update_campaign_scheduled_post_target_check_result(int(t.get("id") or 0), can_publish=getattr(check, "can_publish", None), can_delete=getattr(check, "can_delete", None), publish_status=publish_status, delete_status=delete_status, publish_error_text=getattr(check, "publish_error_text", None), delete_error_text=getattr(check, "delete_error_text", None), check_source=getattr(check, "source", "telethon"))
                self.repo.log_campaign_scheduled_post_check(scheduled_post_id=scheduled_post_id, rule_id=rule_id, target_id=t["target_id"], target_thread_id=t.get("target_thread_id"), target_row_id=int(t.get("id") or 0), check_type="full", status=status, source=getattr(check, "source", "telethon"), error_text=getattr(check, "publish_error_text", None), details=getattr(check, "details", None))
                if status == "denied":
                    failed += 1
                else:
                    passed += 1
            except Exception as exc:
                failed += 1
                self.repo.log_campaign_scheduled_post_check(scheduled_post_id=scheduled_post_id, rule_id=rule_id, target_id=t["target_id"], target_thread_id=t.get("target_thread_id"), target_row_id=int(t.get("id") or 0), check_type="full", status="failed", source="target_checker", error_text=str(exc), details={"exception": exc.__class__.__name__})
        readiness = self.build_readiness(scheduled_post_id=scheduled_post_id)
        self.repo.log_campaign_scheduled_post_event(scheduled_post_id=scheduled_post_id, rule_id=rule_id, event_type="targets_checked", actor_id=actor_id, extra={"checked": len(targets), "passed": passed, "failed": failed})
        self.logger.info("VIP_SCHEDULED_POST_TARGETS_CHECKED | scheduled_post_id=%s | checked=%s", scheduled_post_id, len(targets))
        return RepostCampaignActionResult(ok=True, action="check_scheduled_post_targets", rule_id=rule_id, extra={"checked": len(targets), "passed": passed, "failed": failed, "readiness": readiness})

    def list_posts(self, *, rule_id: int, statuses: list[str] | None = None, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or VIP_SCHEDULED_POST_DEFAULT_LIMIT), 100))
        return self.repo.list_campaign_scheduled_posts(rule_id=rule_id, statuses=statuses or None, limit=safe_limit)

    def get_post_details(self, *, scheduled_post_id: int) -> dict[str, Any] | None:
        post = self.repo.get_campaign_scheduled_post(scheduled_post_id)
        if not post:
            return None
        return {
            "post": post,
            "targets": self.repo.list_campaign_scheduled_post_targets(scheduled_post_id, active_only=False),
            "checks": self.repo.list_campaign_scheduled_post_checks(scheduled_post_id, limit=50),
            "events": self.repo.list_campaign_scheduled_post_events(scheduled_post_id, limit=50),
            "readiness": post.get("readiness_snapshot_json") or post.get("readiness_snapshot") or {},
        }
