from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from app.repost_campaign_placement_service import RepostCampaignPlacementService
from app.repost_campaign_runtime_service import RepostCampaignActionResult
from app.repost_campaign_top_time import normalize_repost_campaign_top_time_settings

CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES = 180
CAMPAIGN_SCHEDULE_TIMEZONE_LABEL = "UTC+3"
CAMPAIGN_SCHEDULE_LOOP_INTERVAL_SECONDS = 15
CAMPAIGN_SCHEDULE_STUCK_SECONDS = 300
CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_RETRY_SECONDS = 300
CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS = 288


def campaign_schedule_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_campaign_schedule_input_to_utc(
    text: str,
    *,
    now_utc: datetime | None = None,
    timezone_offset_minutes: int = 180,
) -> datetime | None:
    src = (text or "").strip()
    now_utc = now_utc or campaign_schedule_now_utc()
    local_now = now_utc + timedelta(minutes=timezone_offset_minutes)

    for fmt in ("%d.%m %H:%M", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M"):
        try:
            local_value = datetime.strptime(src, fmt)
        except Exception:
            continue

        if fmt == "%d.%m %H:%M":
            local_value = local_value.replace(year=local_now.year)

        scheduled_utc = (local_value - timedelta(minutes=timezone_offset_minutes)).replace(tzinfo=timezone.utc)
        if scheduled_utc < now_utc + timedelta(minutes=1):
            return None
        return scheduled_utc

    return None


def _coerce_schedule_datetime_utc(value) -> datetime:
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            return campaign_schedule_now_utc()
    except Exception:
        return campaign_schedule_now_utc()

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _campaign_schedule_clean_channel_next_retry_at(now_utc: datetime | None = None) -> datetime:
    now = now_utc or campaign_schedule_now_utc()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    return now + timedelta(seconds=CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_RETRY_SECONDS)


def _campaign_schedule_clean_channel_attempt_count(row: dict | None) -> int:
    try:
        return max(0, int((row or {}).get("clean_channel_wait_attempt_count") or 0))
    except (TypeError, ValueError):
        return 0


def _campaign_schedule_clean_channel_wait_reason(policy_state: dict | None) -> str:
    return "Чистый канал занят активной рекламой"


def format_campaign_schedule_datetime(
    value: datetime | str | None,
    *,
    timezone_offset_minutes: int = 180,
    timezone_label: str = "UTC+3",
) -> str:
    if value is None:
        return "—"

    dt: datetime
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt = value

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    local = dt.astimezone(timezone.utc) + timedelta(minutes=timezone_offset_minutes)
    return f"{local.strftime('%d.%m %H:%M')} {timezone_label}"


class RepostCampaignScheduleService:
    def __init__(self, *, repo, campaign_runtime, logger_=None):
        self.repo = repo
        self.campaign_runtime = campaign_runtime
        self.logger = logger_ or logging.getLogger("forwarder")

    def build_schedule_readiness(self, *, rule_id: int, scheduled_at_utc: datetime) -> dict:
        readiness = self.campaign_runtime.build_campaign_launch_readiness(rule_id=rule_id)
        readiness["scheduled_at"] = scheduled_at_utc.isoformat()
        readiness["scheduled_at_text"] = format_campaign_schedule_datetime(scheduled_at_utc)

        show_seconds = int(readiness.get("show_seconds") or 0)
        if show_seconds > 0:
            expected_delete_at = scheduled_at_utc + timedelta(seconds=show_seconds)
            readiness["expected_delete_at_text"] = format_campaign_schedule_datetime(expected_delete_at)
        else:
            readiness["expected_delete_at_text"] = "—"

        return readiness

    def build_scheduled_launch_policy_state(
        self,
        *,
        rule_id: int,
        scheduled_at_utc: datetime,
    ) -> dict[str, Any]:
        scheduled_at_text = format_campaign_schedule_datetime(scheduled_at_utc)
        base_readiness = self.campaign_runtime.build_campaign_launch_readiness(
            rule_id=rule_id,
            include_active_placement_block=False,
        )

        if not base_readiness.get("can_launch"):
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_POLICY | rule_id=%s | action=%s | clean_channel_enabled=%s | active_placements=%s | delete_problem=%s",
                rule_id,
                "base_block",
                True,
                0,
                0,
            )
            return {
                "ok": True,
                "rule_id": int(rule_id),
                "launch_mode": "scheduled",
                "scheduled_at": scheduled_at_utc.isoformat(),
                "scheduled_at_text": scheduled_at_text,
                "base_readiness": base_readiness,
                "clean_channel_settings": None,
                "clean_channel_enabled": True,
                "clean_channel_policy": None,
                "state": "base_block",
                "action": "base_block",
                "can_schedule": False,
                "can_launch_if_due_now": False,
                "requires_confirmation": False,
                "will_wait_if_busy": False,
                "will_launch_over_active": False,
                "blocking_text": "Кампания не готова к планированию",
                "warning_text": None,
                "active_placements_total": 0,
                "delete_problem_total": 0,
                "placements": [],
            }

        settings = self.repo.get_rule_repost_campaign_clean_channel_settings(rule_id) or {
            "ok": False,
            "rule_id": rule_id,
            "enabled": True,
        }
        enabled = True if not settings.get("ok") else bool(settings.get("enabled", True))

        placement_service = RepostCampaignPlacementService(self.repo, logger=self.logger)
        try:
            policy = placement_service.build_launch_policy_preview(
                rule_id=rule_id,
                clean_channel_enabled=enabled,
                launch_mode="scheduled",
                basic_only=True,
            )
        except Exception as exc:
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                exc,
            )
            policy = {
                "ok": False,
                "rule_id": int(rule_id),
                "launch_mode": "scheduled",
                "clean_channel_enabled": enabled,
                "state": "unknown",
                "action": "block",
                "error_text": "Не удалось проверить активные размещения",
                "active_placements_total": 0,
                "delete_problem_total": 0,
                "placements": [],
            }

        if not policy.get("ok"):
            error_text = policy.get("error_text") or "Не удалось проверить активные размещения"
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                error_text,
            )
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "launch_mode": "scheduled",
                "scheduled_at": scheduled_at_utc.isoformat(),
                "scheduled_at_text": scheduled_at_text,
                "base_readiness": base_readiness,
                "clean_channel_settings": settings,
                "clean_channel_enabled": enabled,
                "clean_channel_policy": policy,
                "state": "unknown",
                "action": "policy_error",
                "can_schedule": False,
                "can_launch_if_due_now": False,
                "requires_confirmation": False,
                "will_wait_if_busy": False,
                "will_launch_over_active": False,
                "blocking_text": "Не удалось проверить активные размещения",
                "warning_text": None,
                "active_placements_total": 0,
                "delete_problem_total": 0,
                "placements": [],
            }

        policy_action = str(policy.get("action") or "block")
        state = str(policy.get("state") or "unknown")
        active_placements_total = int(policy.get("active_placements_total") or 0)
        delete_problem_total = int(policy.get("delete_problem_total") or 0)
        placements = policy.get("placements") or []
        blocking_text = None
        warning_text = None

        if policy_action == "allow":
            action = "allow"
            can_launch_if_due_now = True
            requires_confirmation = False
            will_wait_if_busy = False
            will_launch_over_active = False
        elif enabled and policy_action == "block":
            action = "schedule_with_clean_channel_wait"
            can_launch_if_due_now = False
            requires_confirmation = False
            will_wait_if_busy = True
            will_launch_over_active = False
            warning_text = "Если к моменту запуска в канале будет активная реклама, ViMi подождёт и не опубликует новый рекламный пост поверх неё."
        elif (not enabled) and policy_action == "allow_with_warning":
            action = "schedule_with_overlap_warning"
            can_launch_if_due_now = True
            requires_confirmation = True
            will_wait_if_busy = False
            will_launch_over_active = True
            warning_text = "Чистый канал выключен. Если к моменту запуска в канале уже будет активная реклама, новая реклама может быть опубликована поверх старой."
        else:
            action = "policy_error"
            can_launch_if_due_now = False
            requires_confirmation = False
            will_wait_if_busy = False
            will_launch_over_active = False
            blocking_text = "Не удалось проверить активные размещения"

        ok = action != "policy_error"
        can_schedule = action in {"allow", "schedule_with_clean_channel_wait", "schedule_with_overlap_warning"}
        if not ok:
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_POLICY_FAILED | rule_id=%s | error=%s",
                rule_id,
                f"unexpected policy action: {policy_action}",
            )
        else:
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_POLICY | rule_id=%s | action=%s | clean_channel_enabled=%s | active_placements=%s | delete_problem=%s",
                rule_id,
                action,
                enabled,
                active_placements_total,
                delete_problem_total,
            )

        return {
            "ok": ok,
            "rule_id": int(rule_id),
            "launch_mode": "scheduled",
            "scheduled_at": scheduled_at_utc.isoformat(),
            "scheduled_at_text": scheduled_at_text,
            "base_readiness": base_readiness,
            "clean_channel_settings": settings,
            "clean_channel_enabled": enabled,
            "clean_channel_policy": policy,
            "state": state,
            "action": action,
            "can_schedule": can_schedule,
            "can_launch_if_due_now": can_launch_if_due_now,
            "requires_confirmation": requires_confirmation,
            "will_wait_if_busy": will_wait_if_busy,
            "will_launch_over_active": will_launch_over_active,
            "blocking_text": blocking_text,
            "warning_text": warning_text,
            "active_placements_total": active_placements_total if ok else 0,
            "delete_problem_total": delete_problem_total if ok else 0,
            "placements": placements if ok else [],
        }

    def schedule_campaign_launch(
        self,
        *,
        rule_id: int,
        scheduled_at_utc: datetime,
        created_by: int | None = None,
        scheduled_policy: dict[str, Any] | None = None,
    ) -> RepostCampaignActionResult:
        policy_blocked = False
        if scheduled_policy is None:
            readiness = self.build_schedule_readiness(rule_id=rule_id, scheduled_at_utc=scheduled_at_utc)
            if not readiness.get("can_launch"):
                return RepostCampaignActionResult(
                    ok=False,
                    action="schedule_campaign_launch",
                    rule_id=rule_id,
                    error_text="Кампания не готова к запуску",
                    extra={"launch_readiness": readiness},
                )
        else:
            policy_action = scheduled_policy.get("action")
            can_schedule = scheduled_policy.get("can_schedule") is True
            readiness = scheduled_policy.get("base_readiness") or self.build_schedule_readiness(rule_id=rule_id, scheduled_at_utc=scheduled_at_utc)
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_CREATE_POLICY | rule_id=%s | action=%s | can_schedule=%s",
                rule_id,
                policy_action,
                can_schedule,
            )
            policy_blocked = scheduled_policy.get("ok") is False or not can_schedule

        if not readiness.get("expected_delete_at_text"):
            show_seconds = int(readiness.get("show_seconds") or 0)
            if show_seconds > 0:
                expected_delete_at = scheduled_at_utc + timedelta(seconds=show_seconds)
                readiness = {
                    **readiness,
                    "expected_delete_at_text": format_campaign_schedule_datetime(expected_delete_at),
                }
            else:
                readiness = {**readiness, "expected_delete_at_text": "—"}

        if scheduled_policy is not None and policy_blocked:
            return RepostCampaignActionResult(
                ok=False,
                action="schedule_campaign_launch",
                rule_id=rule_id,
                error_text=scheduled_policy.get("blocking_text") or "Кампания не готова к запуску",
                extra={"launch_readiness": readiness, "scheduled_policy": scheduled_policy},
            )

        rule = self.repo.get_rule(rule_id)
        top_time_snapshot = normalize_repost_campaign_top_time_settings(
            enabled=bool(getattr(rule, "repost_campaign_top_time_enabled", False)),
            seconds=int(getattr(rule, "repost_campaign_top_time_seconds", 0) or 0),
        )
        preview = {**readiness, "scheduled_policy": scheduled_policy} if scheduled_policy is not None else dict(readiness)
        preview["top_time_snapshot"] = top_time_snapshot
        scheduled_launch_id = self.repo.create_campaign_scheduled_launch(
            rule_id=rule_id,
            saved_post_id=int(readiness.get("saved_post_id")),
            show_seconds=int(readiness.get("show_seconds") or 0),
            scheduled_at=scheduled_at_utc.isoformat(),
            timezone_offset_minutes=CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES,
            timezone_label=CAMPAIGN_SCHEDULE_TIMEZONE_LABEL,
            created_by=created_by,
            preview=preview,
            top_time_enabled_snapshot=bool(top_time_snapshot["enabled"]),
            top_time_seconds_snapshot=int(top_time_snapshot["seconds"]),
        )
        extra = {
            "scheduled_launch_id": scheduled_launch_id,
            "scheduled_at": scheduled_at_utc.isoformat(),
            "scheduled_at_text": format_campaign_schedule_datetime(scheduled_at_utc),
            "expected_delete_at_text": readiness.get("expected_delete_at_text"),
        }
        if scheduled_policy is not None:
            extra["scheduled_policy"] = scheduled_policy
        return RepostCampaignActionResult(
            ok=bool(scheduled_launch_id),
            action="schedule_campaign_launch",
            rule_id=rule_id,
            saved_post_id=int(readiness.get("saved_post_id")),
            extra=extra,
        )

    def cancel_scheduled_launch(self, *, scheduled_launch_id: int, cancelled_by: int | None = None) -> RepostCampaignActionResult:
        row = self.repo.get_campaign_scheduled_launch(scheduled_launch_id)
        if not row:
            return RepostCampaignActionResult(ok=False, action="cancel_scheduled_launch", rule_id=0, error_text="Запланированный запуск не найден")
        if row.get("status") != "scheduled":
            return RepostCampaignActionResult(ok=False, action="cancel_scheduled_launch", rule_id=int(row.get("rule_id") or 0), error_text="Запуск уже нельзя отменить")

        ok = self.repo.cancel_campaign_scheduled_launch(scheduled_launch_id, cancelled_by=cancelled_by)
        return RepostCampaignActionResult(ok=ok, action="cancel_scheduled_launch", rule_id=int(row.get("rule_id") or 0))

    async def process_due_scheduled_launches(self, *, worker_id: str, limit: int = 5) -> dict:
        reset_result = self.repo.reset_stuck_campaign_scheduled_launches(stuck_seconds=CAMPAIGN_SCHEDULE_STUCK_SECONDS)
        if isinstance(reset_result, dict) and reset_result.get("needs_review"):
            self.logger.warning(
                "CAMPAIGN_SCHEDULE_NEEDS_REVIEW | reason=stale_processing_with_campaign_run | count=%s",
                reset_result.get("needs_review"),
            )
        claimed = self.repo.claim_due_campaign_scheduled_launches(now_iso=campaign_schedule_now_utc().isoformat(), worker_id=worker_id, limit=limit)

        for row in claimed:
            rule_id = int(row["rule_id"])
            scheduled_launch_id = int(row["id"])
            created_by = row.get("created_by")
            scheduled_at_utc = _coerce_schedule_datetime_utc(row.get("scheduled_at"))
            policy_state = self.build_scheduled_launch_policy_state(
                rule_id=rule_id,
                scheduled_at_utc=scheduled_at_utc,
            )
            action = str(policy_state.get("action") or "")
            self.logger.info(
                "REPOST_CAMPAIGN_SCHEDULE_DUE_POLICY | scheduled_launch_id=%s | rule_id=%s | action=%s | can_schedule=%s | can_launch_if_due_now=%s",
                scheduled_launch_id,
                rule_id,
                action,
                policy_state.get("can_schedule"),
                policy_state.get("can_launch_if_due_now"),
            )

            if policy_state.get("ok") is False or action == "policy_error":
                self.repo.mark_campaign_scheduled_launch_needs_review(
                    scheduled_launch_id,
                    error_text="Не удалось проверить Чистый канал перед запуском",
                    campaign_run_id=None,
                )
                self.logger.warning(
                    "REPOST_CAMPAIGN_SCHEDULE_NEEDS_REVIEW | scheduled_launch_id=%s | rule_id=%s | reason=clean_channel_policy_error",
                    scheduled_launch_id,
                    rule_id,
                )
                continue

            if action == "base_block" or policy_state.get("can_schedule") is not True:
                self.repo.mark_campaign_scheduled_launch_failed(
                    scheduled_launch_id,
                    error_text="Кампания не готова к запуску в момент старта",
                )
                self.logger.warning(
                    "REPOST_CAMPAIGN_SCHEDULE_FAILED_AT_START | scheduled_launch_id=%s | rule_id=%s | action=%s",
                    scheduled_launch_id,
                    rule_id,
                    action,
                )
                continue

            if action == "schedule_with_clean_channel_wait":
                attempt_count = _campaign_schedule_clean_channel_attempt_count(row)
                if attempt_count >= CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS:
                    self.repo.mark_campaign_scheduled_launch_needs_review(
                        scheduled_launch_id,
                        error_text="Запуск слишком долго ждёт чистый канал",
                        campaign_run_id=None,
                    )
                    self.logger.warning(
                        "REPOST_CAMPAIGN_SCHEDULE_NEEDS_REVIEW | scheduled_launch_id=%s | rule_id=%s | reason=clean_channel_wait_limit",
                        scheduled_launch_id,
                        rule_id,
                    )
                    continue

                next_retry_at = _campaign_schedule_clean_channel_next_retry_at()
                self.repo.mark_campaign_scheduled_launch_waiting_clean_channel(
                    scheduled_launch_id,
                    next_retry_at=next_retry_at.isoformat(),
                    reason=_campaign_schedule_clean_channel_wait_reason(policy_state),
                    policy_snapshot=policy_state,
                )
                self.logger.info(
                    "REPOST_CAMPAIGN_SCHEDULE_WAITING_CLEAN_CHANNEL | scheduled_launch_id=%s | rule_id=%s | next_retry_at=%s | attempt=%s",
                    scheduled_launch_id,
                    rule_id,
                    next_retry_at.isoformat(),
                    attempt_count + 1,
                )
                continue

            if action not in {"allow", "schedule_with_overlap_warning"}:
                self.repo.mark_campaign_scheduled_launch_needs_review(
                    scheduled_launch_id,
                    error_text="Неизвестное состояние проверки Чистого канала",
                    campaign_run_id=None,
                )
                self.logger.warning(
                    "REPOST_CAMPAIGN_SCHEDULE_NEEDS_REVIEW | scheduled_launch_id=%s | rule_id=%s | reason=unknown_clean_channel_action",
                    scheduled_launch_id,
                    rule_id,
                )
                continue

            top_time_snapshot = {
                "enabled": bool(row.get("top_time_enabled_snapshot") or False),
                "seconds": int(row.get("top_time_seconds_snapshot") or 0),
            }
            captured_run_id: int | None = None

            def _remember_campaign_run(campaign_run_id: int) -> None:
                nonlocal captured_run_id
                captured_run_id = int(campaign_run_id)
                self.repo.set_campaign_scheduled_launch_campaign_run_id(
                    scheduled_launch_id,
                    int(campaign_run_id),
                )

            try:
                result = await self.campaign_runtime.launch_campaign_now(
                    rule_id=rule_id,
                    admin_id=created_by,
                    run_type="scheduled",
                    on_campaign_run_created=_remember_campaign_run,
                    ignore_active_placement_block=(action == "schedule_with_overlap_warning"),
                    top_time_snapshot=top_time_snapshot,
                )
            except Exception as exc:
                current = self.repo.get_campaign_scheduled_launch(scheduled_launch_id) or {}
                run_id = int(current.get("campaign_run_id") or captured_run_id or 0)
                if run_id:
                    self.repo.mark_campaign_scheduled_launch_needs_review(
                        scheduled_launch_id,
                        error_text=str(exc) or "Запуск прерван после создания campaign_run",
                        campaign_run_id=run_id,
                    )
                    self.logger.warning(
                        "CAMPAIGN_SCHEDULE_NEEDS_REVIEW | scheduled_launch_id=%s | campaign_run_id=%s | reason=stale_processing_with_campaign_run",
                        scheduled_launch_id,
                        run_id,
                    )
                else:
                    self.repo.mark_campaign_scheduled_launch_failed(
                        scheduled_launch_id,
                        error_text=str(exc) or "Ошибка запуска",
                    )
                continue

            run_id = int(((result.extra or {}) if result else {}).get("campaign_run_id") or captured_run_id or 0)
            if result and result.ok and run_id:
                self.repo.mark_campaign_scheduled_launch_launched(scheduled_launch_id, campaign_run_id=run_id)
            elif run_id:
                self.repo.mark_campaign_scheduled_launch_needs_review(
                    scheduled_launch_id,
                    error_text=(result.error_text if result else None) or "Запуск прерван после создания campaign_run",
                    campaign_run_id=run_id,
                )
                self.logger.warning(
                    "CAMPAIGN_SCHEDULE_NEEDS_REVIEW | scheduled_launch_id=%s | campaign_run_id=%s | reason=launch_failed_with_campaign_run",
                    scheduled_launch_id,
                    run_id,
                )
            else:
                self.repo.mark_campaign_scheduled_launch_failed(scheduled_launch_id, error_text=result.error_text if result else "Ошибка запуска")

        return {"claimed": len(claimed)}


async def run_repost_campaign_scheduled_launch_loop(
    *,
    runtime: RepostCampaignScheduleService,
    stop_event: asyncio.Event | None = None,
    interval_seconds: int = CAMPAIGN_SCHEDULE_LOOP_INTERVAL_SECONDS,
    worker_id: str | None = None,
):
    logger = runtime.logger
    resolved_worker_id = worker_id or f"{os.uname().nodename}:{os.getpid()}"
    logger.info("REPOST_CAMPAIGN_SCHEDULE_LOOP_STARTED | worker_id=%s", resolved_worker_id)

    while not (stop_event and stop_event.is_set()):
        try:
            await runtime.process_due_scheduled_launches(worker_id=resolved_worker_id)
        except Exception:
            logger.exception("REPOST_CAMPAIGN_SCHEDULE_LOOP_FAILED | worker_id=%s", resolved_worker_id)
        await asyncio.sleep(interval_seconds)
