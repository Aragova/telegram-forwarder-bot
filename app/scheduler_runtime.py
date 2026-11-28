from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Callable

from app.job_service import (
    build_dedup_key_for_album,
    build_dedup_key_for_single,
    build_dedup_key_for_video,
    enqueue_repost_album,
    enqueue_repost_single,
    enqueue_video_delivery,
)
from app.repository_models import GLOBAL_INTERVAL_GAP_SECONDS, utc_now_iso
from app.limit_service import LimitService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService
from app.worker_load_service import build_worker_load_snapshot
from app.worker_resource_policy import POLICY
from app.tenant_fairness_service import TenantFairnessService
from app.top_time_guard_service import TopTimeGuardService
from app.runtime_utils import wait_for_db_task

logger = logging.getLogger("forwarder")


def _top_time_resume_next_run_iso(resume_at: str) -> str:
    resume_dt = datetime.fromisoformat(str(resume_at))
    if resume_dt.tzinfo is None:
        resume_dt = resume_dt.replace(tzinfo=timezone.utc)
    return (resume_dt.astimezone(timezone.utc) + timedelta(seconds=GLOBAL_INTERVAL_GAP_SECONDS)).isoformat()


def _log_top_time_auto_postponed(repo, *, rule, pause: dict, resume_at: str, next_run_at: str) -> None:
    target_id = str(getattr(rule, "target_id", "") or "")
    target_thread_id = getattr(rule, "target_thread_id", None)
    pause_id = pause.get("id")
    logger.info(
        "TOP_TIME_GUARD_BLOCKED_AUTO_POST | rule_id=%s | target_id=%s | target_thread_id=%s | pause_id=%s | resume_at=%s | next_run_at=%s",
        int(rule.id),
        target_id,
        target_thread_id,
        pause_id,
        resume_at,
        next_run_at,
    )
    if hasattr(repo, "log_event"):
        repo.log_event(
            event_type="top_time_auto_postponed",
            rule_id=int(rule.id),
            target_id=target_id,
            target_thread_id=target_thread_id,
            status="postponed",
            extra={
                "pause_id": pause_id,
                "target_id": target_id,
                "target_thread_id": target_thread_id,
                "resume_at": resume_at,
                "next_run_at": next_run_at,
                "reason": "top_time_pause",
            },
        )

def _video_tenant_throttled(repo, fairness_service, tenant_id, system_mode):
    if hasattr(repo, "get_tenant_heavy_admission_counts"):
        try:
            counts = repo.get_tenant_heavy_admission_counts(tenant_id)
        except Exception:
            logger.warning("Scheduler: ошибка чтения счётчиков heavy tenant_id=%s", tenant_id, exc_info=True)
            return True
        if not isinstance(counts, dict) or any(
            type(counts.get(key)) is not int or counts[key] < 0
            for key in ("pending", "processing", "retry")
        ):
            logger.warning("Scheduler: недоступны счётчики heavy tenant_id=%s", tenant_id)
            return True
        pending, processing, retry = (counts[key] for key in ("pending", "processing", "retry"))
    else:
        pending = int((repo.get_tenant_job_counts(queue="heavy") if hasattr(repo, "get_tenant_job_counts") else {}).get(tenant_id) or 0)
        processing = int((repo.get_tenant_processing_counts(queue="heavy") if hasattr(repo, "get_tenant_processing_counts") else {}).get(tenant_id) or 0)
        retry = int((repo.get_tenant_retry_counts(queue="heavy") if hasattr(repo, "get_tenant_retry_counts") else {}).get(tenant_id) or 0)
    return fairness_service.should_throttle_tenant(
        tenant_id=tenant_id, system_mode=system_mode, queue="heavy",
        tenant_pending=pending, tenant_processing=processing,
        tenant_retry=retry, tenant_heavy_pending=pending,
    )


async def scheduler_tick(repo, *, now_iso: str | None = None, enabled: bool = True,
                         stop_event: asyncio.Event | None = None) -> dict[str, int]:
    if not enabled or (stop_event is not None and stop_event.is_set()):
        return {"created": 0, "duplicates": 0, "checked_rules": 0}

    created = 0
    duplicates = 0
    checked_rules = 0
    due_iso = now_iso or utc_now_iso()
    subscription_service = SubscriptionService(repo)
    usage_service = UsageService(repo)
    limit_service = LimitService(repo, subscription_service, usage_service)
    fairness_service = TenantFairnessService(repo)

    if hasattr(repo, "mark_expired_campaign_top_time_pauses_completed"):
        completed_count = await asyncio.to_thread(repo.mark_expired_campaign_top_time_pauses_completed, limit=500)
        if completed_count > 0:
            logger.info("TOP_TIME_PAUSES_COMPLETED | count=%s", completed_count)

    top_time_guard = TopTimeGuardService(repo, logger=logger)
    rules = await asyncio.to_thread(repo.get_all_rules)

    for rule in rules:
        if stop_event is not None and stop_event.is_set():
            break
        if not bool(getattr(rule, "is_active", False)):
            continue

        rule_mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()
        load_mode = "normal"
        if rule_mode == "video":
            load = await asyncio.to_thread(build_worker_load_snapshot, repo, POLICY)
            if load is None or getattr(load, "mode", None) not in {"normal", "degraded", "saturated"}:
                logger.warning("Scheduler: недоступен снимок нагрузки rule_id=%s", int(rule.id))
                continue
            load_mode = load.mode
            if load.heavy_hard_limit_exceeded:
                logger.warning(
                    "Новый heavy job не поставлен из-за saturation | rule_id=%s | heavy_pending=%s | hard_limit=%s",
                    int(rule.id),
                    load.heavy_pending,
                    POLICY.backlog_hard_limit_heavy,
                )
                continue
            if load.heavy_soft_limit_exceeded:
                logger.warning(
                    "Heavy backlog превысил мягкий порог | rule_id=%s | heavy_pending=%s | soft_limit=%s",
                    int(rule.id),
                    load.heavy_pending,
                    POLICY.backlog_soft_limit_heavy,
                )

        checked_rules += 1
        has_get_due_delivery = hasattr(repo, "get_due_delivery")
        if has_get_due_delivery:
            due = await asyncio.to_thread(repo.get_due_delivery, int(rule.id), due_iso)
        else:
            due = await asyncio.to_thread(repo.take_due_delivery, int(rule.id), due_iso)
        if not due:
            continue

        decision = await asyncio.to_thread(top_time_guard.build_guard_decision, rule, at_iso=due_iso)
        if decision.get("blocked") is True:
            resume_at = decision.get("resume_at")
            if resume_at:
                next_run_at = _top_time_resume_next_run_iso(str(resume_at))
                await asyncio.to_thread(repo.update_rule_next_run_at, int(rule.id), next_run_at)
                await asyncio.to_thread(
                    _log_top_time_auto_postponed, repo,
                    rule=rule,
                    pause=decision.get("pause") or {},
                    resume_at=str(resume_at),
                    next_run_at=next_run_at,
                )
            continue

        tenant_id = int(due.get("tenant_id") or await asyncio.to_thread(
            getattr(repo, "get_rule_tenant_id", lambda _x: 1), int(rule.id),
        ) or 1)

        can_enqueue, enqueue_reason = await asyncio.to_thread(limit_service.can_enqueue_job, tenant_id)
        if not can_enqueue:
            logger.warning(
                "Scheduler пропустил публикацию: лимит тарифа tenant_id=%s reason=%s",
                tenant_id,
                enqueue_reason or "unknown",
            )
            if hasattr(repo, "create_billing_event"):
                sub = await asyncio.to_thread(subscription_service.get_active_subscription, int(tenant_id)) or {}
                usage_today = await asyncio.to_thread(usage_service.get_today_usage, int(tenant_id)) or {}
                await asyncio.to_thread(
                    repo.create_billing_event, int(tenant_id),
                    "subscription_blocked_action" if str(enqueue_reason or "").strip().lower() == "подписка неактивна" else "limit_job_blocked",
                    event_source="scheduler_runtime",
                    metadata={
                        "tenant_id": int(tenant_id),
                        "plan_name": str(sub.get("plan_name") or "FREE"),
                        "action": "enqueue_publication",
                        "reason": str(enqueue_reason or "limit"),
                        "usage_snapshot": {
                            "jobs_count": int(usage_today.get("jobs_count") or 0),
                            "video_count": int(usage_today.get("video_count") or 0),
                        },
                    },
                )
            continue

        delivery_id = int(due["delivery_id"])

        # Only video rules can be deferred by this check, and normal mode
        # always allows them. Preserve per-tenant backpressure in degraded mode.
        if rule_mode == "video" and load_mode != "normal":
            if await asyncio.to_thread(_video_tenant_throttled, repo, fairness_service, tenant_id, load_mode):
                logger.warning("Scheduler временно ограничил tenant #%s: превышена нагрузка", tenant_id)
                continue

        if stop_event is not None and stop_event.is_set():
            break
        job_id: int | None = None
        if hasattr(repo, "take_due_delivery_and_create_job"):
            try:
                atomic_result = await asyncio.to_thread(repo.take_due_delivery_and_create_job, int(rule.id), due_iso)
            except Exception as exc:
                logger.warning("Ошибка атомарного enqueue delivery_id=%s: %s", delivery_id, exc)
                continue
            if not atomic_result:
                continue
            result_status = str(atomic_result.get("status") or "").strip().lower()
            if result_status == "duplicate":
                duplicates += 1
                duplicate_delivery_id = int(atomic_result.get("delivery_id") or delivery_id)
                logger.debug("Scheduler пропустил delivery %s (дубликат задачи)", duplicate_delivery_id)
                continue
            if result_status == "limit_blocked":
                logger.warning(
                    "Scheduler: атомарный допуск отклонён tenant_id=%s: %s",
                    atomic_result.get("tenant_id", tenant_id), atomic_result.get("reason"),
                )
                continue
            if result_status != "created":
                logger.warning("Ошибка атомарного enqueue delivery_id=%s: неизвестный статус %s", delivery_id, result_status or "none")
                continue
            job_id = atomic_result.get("job_id")
            if job_id is None:
                logger.warning("Ошибка атомарного enqueue delivery_id=%s: job_id не получен", delivery_id)
                continue
            delivery_id = int(atomic_result.get("delivery_id") or delivery_id)
            tenant_id = int(atomic_result.get("tenant_id") or tenant_id)
        else:
            if has_get_due_delivery:
                due_taken = await asyncio.to_thread(repo.take_due_delivery, int(rule.id), due_iso)
            else:
                due_taken = due
            if not due_taken:
                continue
            media_group_id = due_taken.get("media_group_id")
            if rule_mode == "video":
                dedup_key = build_dedup_key_for_video(delivery_id)
                if await asyncio.to_thread(repo.get_active_job_by_dedup_key, dedup_key):
                    duplicates += 1
                    logger.info("Scheduler пропустил дубль задачи video_delivery для delivery #%s", delivery_id)
                    continue
                job_id = await asyncio.to_thread(enqueue_video_delivery, repo, delivery_id)
            elif media_group_id:
                album_rows = await asyncio.to_thread(
                    repo.get_album_pending_for_rule,
                    int(rule.id),
                    str(due_taken["source_channel"]),
                    due_taken["source_thread_id"],
                    str(media_group_id),
                )
                delivery_ids = [int(row["delivery_id"]) for row in (album_rows or [])] or [delivery_id]
                dedup_key = build_dedup_key_for_album(int(rule.id), str(media_group_id), delivery_ids)
                if await asyncio.to_thread(repo.get_active_job_by_dedup_key, dedup_key):
                    duplicates += 1
                    logger.info(
                        "Scheduler пропустил дубль задачи repost_album для rule #%s media_group_id=%s",
                        int(rule.id),
                        media_group_id,
                    )
                    continue
                job_id = await asyncio.to_thread(enqueue_repost_album, repo, delivery_ids, str(media_group_id))
            else:
                dedup_key = build_dedup_key_for_single(delivery_id)
                if await asyncio.to_thread(repo.get_active_job_by_dedup_key, dedup_key):
                    duplicates += 1
                    logger.info("Scheduler пропустил дубль задачи repost_single для delivery #%s", delivery_id)
                    continue
                job_id = await asyncio.to_thread(enqueue_repost_single, repo, delivery_id)
            if job_id is None:
                duplicates += 1
                logger.info("Scheduler пропустил дубль задачи для delivery #%s", delivery_id)
                continue

            # Legacy repositories do not implement the atomic admission contract.
            await asyncio.to_thread(usage_service.increment_jobs, tenant_id, 1)

        created += 1
        logger.info("Scheduler атомарно создал задачу job_id=%s для delivery_id=%s", int(job_id), delivery_id)

    return {"created": created, "duplicates": duplicates, "checked_rules": checked_rules}


async def run_scheduler_loop(
    repo,
    *,
    interval_seconds: float = 1.0,
    is_enabled: Callable[[], bool] | None = None,
) -> None:
    logger.info("Scheduler loop запущен")
    stop_event = asyncio.Event()
    while not stop_event.is_set():
        try:
            enabled = is_enabled() if is_enabled else True
            tick = asyncio.create_task(scheduler_tick(repo, enabled=enabled, stop_event=stop_event))
            try:
                await asyncio.shield(tick)
            except asyncio.CancelledError:
                # asyncio.to_thread cannot cancel a running SQL transaction.
                # Stop admitting the next rule and join the current tick before
                # allowing shutdown to drain the consumers.
                stop_event.set()
                await wait_for_db_task(tick)
                raise
        except Exception as exc:
            if stop_event.is_set():
                raise
            logger.warning("Scheduler loop ошибка: %s", exc)
        await asyncio.sleep(max(0.2, float(interval_seconds)))
