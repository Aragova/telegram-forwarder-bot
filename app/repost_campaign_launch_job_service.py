from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.repost_campaign_ui import (
    build_repost_campaign_launch_job_status_view,
)


ACTIVE_REPOST_CAMPAIGN_LAUNCH_JOB_STATUSES = {"pending", "processing"}
TERMINAL_REPOST_CAMPAIGN_LAUNCH_JOB_STATUSES = {"sent", "failed", "needs_review", "cancelled"}


@dataclass(frozen=True)
class RepostCampaignLaunchJobEnqueueResult:
    job: dict[str, Any]
    created: bool


class RepostCampaignLaunchJobService:
    """Durable worker для ручных запусков рекламной кампании."""

    def __init__(
        self,
        *,
        repo: Any,
        campaign_runtime: Any,
        bot: Any | None = None,
        logger_: logging.Logger | None = None,
        lock_ttl_seconds: int = 900,
    ) -> None:
        self.repo = repo
        self.campaign_runtime = campaign_runtime
        self.bot = bot
        self.logger = logger_ or logging.getLogger("forwarder")
        self.lock_ttl_seconds = int(lock_ttl_seconds)

    def enqueue_manual_launch(
        self,
        *,
        rule_id: int,
        admin_id: int | None,
        progress_chat_id: int | str | None,
        progress_message_id: int | None,
    ) -> RepostCampaignLaunchJobEnqueueResult:
        existing = self.repo.get_active_repost_campaign_launch_job_for_rule(int(rule_id))
        if existing:
            self.logger.info(
                "REPOST_CAMPAIGN_LAUNCH_JOB_DUPLICATE_SUPPRESSED | rule_id=%s | job_id=%s | status=%s",
                rule_id,
                existing.get("id"),
                existing.get("status"),
            )
            return RepostCampaignLaunchJobEnqueueResult(job=existing, created=False)

        payload = {
            "rule_id": int(rule_id),
            "admin_id": admin_id,
            "progress_chat_id": progress_chat_id,
            "progress_message_id": progress_message_id,
            "launch_type": "manual",
            "requested_at": datetime.now(timezone.utc).isoformat(),
        }
        job = self.repo.create_repost_campaign_launch_job(
            rule_id=int(rule_id),
            admin_id=admin_id,
            progress_chat_id=progress_chat_id,
            progress_message_id=progress_message_id,
            payload_json=payload,
        )
        self.logger.info(
            "REPOST_CAMPAIGN_LAUNCH_JOB_ENQUEUED | rule_id=%s | job_id=%s | admin_id=%s",
            rule_id,
            job.get("id") if job else None,
            admin_id,
        )
        return RepostCampaignLaunchJobEnqueueResult(job=job, created=True)

    async def process_due_jobs(self, *, worker_id: str, limit: int = 5) -> int:
        await self.recover_stale_jobs(worker_id=worker_id)
        jobs = self.repo.get_due_repost_campaign_launch_jobs(limit=max(1, int(limit)))
        processed = 0
        for job in jobs:
            leased = self.repo.lease_repost_campaign_launch_job(
                int(job["id"]),
                worker_id=worker_id,
                lock_ttl_seconds=self.lock_ttl_seconds,
            )
            if not leased:
                continue
            self.logger.info(
                "REPOST_CAMPAIGN_LAUNCH_JOB_LEASED | job_id=%s | rule_id=%s | worker_id=%s",
                leased.get("id"),
                leased.get("rule_id"),
                worker_id,
            )
            await self.run_once(job=leased, worker_id=worker_id)
            processed += 1
        return processed

    async def run_once(self, *, job: dict[str, Any], worker_id: str) -> dict[str, Any] | None:
        job_id = int(job["id"])
        rule_id = int(job["rule_id"])
        admin_id = job.get("admin_id")
        self.repo.mark_repost_campaign_launch_job_processing(job_id, worker_id=worker_id)
        self.logger.info("REPOST_CAMPAIGN_LAUNCH_JOB_STARTED | job_id=%s | rule_id=%s", job_id, rule_id)
        before_run_id = self._latest_campaign_run_id(rule_id)
        try:
            readiness = self.campaign_runtime.build_campaign_launch_readiness(rule_id=rule_id)
            if not readiness.get("can_launch"):
                error_text = "Кампания не готова к запуску"
                result_json = {"ok": False, "error_text": error_text, "extra": {"launch_readiness": readiness}}
                updated = self.repo.mark_repost_campaign_launch_job_failed(job_id, last_error=error_text, result_json=result_json)
                await self._update_progress_message(rule_id=rule_id, job=updated or {**job, "status": "failed", "result_json": result_json})
                self.logger.info("REPOST_CAMPAIGN_LAUNCH_JOB_BLOCKED | job_id=%s | rule_id=%s", job_id, rule_id)
                return updated

            result = await self.campaign_runtime.launch_campaign_now(rule_id=rule_id, admin_id=admin_id, run_type="manual")
            result_json = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
            campaign_run_id = (result_json.get("extra") or {}).get("campaign_run_id") or self._detect_new_campaign_run_id(rule_id, before_run_id)
            updated = self.repo.mark_repost_campaign_launch_job_sent(job_id, campaign_run_id=campaign_run_id, result_json=result_json)
            await self._update_progress_message(rule_id=rule_id, job=updated or {**job, "status": "sent", "result_json": result_json, "campaign_run_id": campaign_run_id})
            self.logger.info(
                "REPOST_CAMPAIGN_LAUNCH_JOB_SENT | job_id=%s | rule_id=%s | campaign_run_id=%s | ok=%s",
                job_id,
                rule_id,
                campaign_run_id,
                result_json.get("ok"),
            )
            return updated
        except Exception as exc:
            detected_run_id = job.get("campaign_run_id") or self._detect_new_campaign_run_id(rule_id, before_run_id)
            error_text = str(exc) or "Неизвестная ошибка запуска кампании"
            if detected_run_id:
                updated = self.repo.mark_repost_campaign_launch_job_needs_review(
                    job_id,
                    last_error=error_text,
                    campaign_run_id=detected_run_id,
                )
                await self._update_progress_message(rule_id=rule_id, job=updated or {**job, "status": "needs_review", "last_error": error_text, "campaign_run_id": detected_run_id})
                self.logger.warning(
                    "REPOST_CAMPAIGN_LAUNCH_JOB_NEEDS_REVIEW | job_id=%s | rule_id=%s | campaign_run_id=%s | error=%s",
                    job_id,
                    rule_id,
                    detected_run_id,
                    error_text,
                )
                return updated

            updated = self.repo.mark_repost_campaign_launch_job_failed(job_id, last_error=error_text, retryable=True)
            self.logger.warning(
                "REPOST_CAMPAIGN_LAUNCH_JOB_FAILED | job_id=%s | rule_id=%s | error=%s",
                job_id,
                rule_id,
                error_text,
            )
            return updated

    async def recover_stale_jobs(self, *, worker_id: str) -> dict[str, int]:
        summary = self.repo.recover_stale_repost_campaign_launch_jobs(worker_id=worker_id)
        if int((summary or {}).get("needs_review") or 0) > 0:
            self.logger.warning(
                "REPOST_CAMPAIGN_LAUNCH_JOB_NEEDS_REVIEW | reason=stale_processing_with_campaign_run | count=%s",
                int(summary.get("needs_review") or 0),
            )
        if int((summary or {}).get("requeued") or 0) > 0 or int((summary or {}).get("failed") or 0) > 0:
            self.logger.info("REPOST_CAMPAIGN_LAUNCH_JOB_RECOVERY_DONE | summary=%s", summary)
        return summary or {"requeued": 0, "needs_review": 0, "failed": 0}

    def _latest_campaign_run_id(self, rule_id: int) -> int | None:
        try:
            runs = self.repo.list_campaign_runs_for_rule(int(rule_id), limit=1) or []
        except Exception:
            return None
        if not runs:
            return None
        try:
            return int(runs[0].get("id"))
        except Exception:
            return None

    def _detect_new_campaign_run_id(self, rule_id: int, before_run_id: int | None) -> int | None:
        latest = self._latest_campaign_run_id(rule_id)
        if latest is not None and latest != before_run_id:
            return latest
        return None

    async def _update_progress_message(self, *, rule_id: int, job: dict[str, Any]) -> None:
        if self.bot is None:
            return
        chat_id = job.get("progress_chat_id") or (job.get("payload_json") or {}).get("progress_chat_id")
        message_id = job.get("progress_message_id") or (job.get("payload_json") or {}).get("progress_message_id")
        if not chat_id or not message_id:
            return
        try:
            text, keyboard = build_repost_campaign_launch_job_status_view(rule_id=rule_id, job=job)
            await self.bot.edit_message_text(chat_id=chat_id, message_id=int(message_id), text=text, reply_markup=keyboard)
        except Exception as exc:
            self.logger.warning(
                "REPOST_CAMPAIGN_LAUNCH_JOB_PROGRESS_UI_FAILED | job_id=%s | rule_id=%s | error=%s",
                job.get("id"),
                rule_id,
                exc,
            )


async def run_repost_campaign_launch_job_loop(
    *,
    service: RepostCampaignLaunchJobService,
    interval_seconds: int = 15,
    worker_id: str = "repost-campaign-launch:bot",
    batch_limit: int = 5,
) -> None:
    service.logger.info(
        "REPOST_CAMPAIGN_LAUNCH_JOB_LOOP_STARTED | worker_id=%s | interval_seconds=%s | batch_limit=%s",
        worker_id,
        interval_seconds,
        batch_limit,
    )
    await service.recover_stale_jobs(worker_id=worker_id)
    while True:
        try:
            await service.process_due_jobs(worker_id=worker_id, limit=batch_limit)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            service.logger.warning("REPOST_CAMPAIGN_LAUNCH_JOB_LOOP_FAILED | worker_id=%s | error=%s", worker_id, exc)
        await asyncio.sleep(max(1, int(interval_seconds)))
