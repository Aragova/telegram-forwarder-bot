from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from app.repost_campaign_runtime_service import RepostCampaignActionResult

CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES = 180
CAMPAIGN_SCHEDULE_TIMEZONE_LABEL = "UTC+3"
CAMPAIGN_SCHEDULE_LOOP_INTERVAL_SECONDS = 15
CAMPAIGN_SCHEDULE_STUCK_SECONDS = 300


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

    def schedule_campaign_launch(
        self,
        *,
        rule_id: int,
        scheduled_at_utc: datetime,
        created_by: int | None = None,
    ) -> RepostCampaignActionResult:
        readiness = self.build_schedule_readiness(rule_id=rule_id, scheduled_at_utc=scheduled_at_utc)
        if not readiness.get("can_launch"):
            return RepostCampaignActionResult(
                ok=False,
                action="schedule_campaign_launch",
                rule_id=rule_id,
                error_text="Кампания не готова к запуску",
                extra={"launch_readiness": readiness},
            )

        scheduled_launch_id = self.repo.create_campaign_scheduled_launch(
            rule_id=rule_id,
            saved_post_id=int(readiness.get("saved_post_id")),
            show_seconds=int(readiness.get("show_seconds") or 0),
            scheduled_at=scheduled_at_utc.isoformat(),
            timezone_offset_minutes=CAMPAIGN_SCHEDULE_TIMEZONE_OFFSET_MINUTES,
            timezone_label=CAMPAIGN_SCHEDULE_TIMEZONE_LABEL,
            created_by=created_by,
            preview=readiness,
        )
        return RepostCampaignActionResult(
            ok=bool(scheduled_launch_id),
            action="schedule_campaign_launch",
            rule_id=rule_id,
            saved_post_id=int(readiness.get("saved_post_id")),
            extra={
                "scheduled_launch_id": scheduled_launch_id,
                "scheduled_at": scheduled_at_utc.isoformat(),
                "scheduled_at_text": format_campaign_schedule_datetime(scheduled_at_utc),
                "expected_delete_at_text": readiness.get("expected_delete_at_text"),
            },
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

            readiness = self.campaign_runtime.build_campaign_launch_readiness(rule_id=rule_id)
            if (not readiness.get("can_launch")) or readiness.get("active_placement") or int(readiness.get("delete_failed") or 0) > 0:
                self.repo.mark_campaign_scheduled_launch_failed(scheduled_launch_id, error_text="Кампания не готова к запуску в момент старта")
                continue

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
