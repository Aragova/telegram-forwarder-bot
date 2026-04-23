from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable

from app.job_service import (
    build_dedup_key_for_album,
    build_dedup_key_for_single,
    build_dedup_key_for_video,
    enqueue_repost_album,
    enqueue_repost_single,
    enqueue_video_delivery,
)
from app.repository_models import utc_now_iso
from app.limit_service import LimitService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService
from app.worker_load_service import build_worker_load_snapshot
from app.worker_resource_policy import POLICY

logger = logging.getLogger("forwarder")
_last_usage_reset_day: str | None = None


async def scheduler_tick(repo, *, now_iso: str | None = None, enabled: bool = True) -> dict[str, int]:
    if not enabled:
        return {"created": 0, "duplicates": 0, "checked_rules": 0}

    created = 0
    duplicates = 0
    checked_rules = 0
    due_iso = now_iso or utc_now_iso()
    subscription_service = SubscriptionService(repo)
    usage_service = UsageService(repo)
    limit_service = LimitService(repo, subscription_service, usage_service)

    rules = await asyncio.to_thread(repo.get_all_rules)

    for rule in rules:
        if not bool(getattr(rule, "is_active", False)):
            continue

        rule_mode = (getattr(rule, "mode", "repost") or "repost").strip().lower()
        if rule_mode == "video":
            load = await asyncio.to_thread(build_worker_load_snapshot, repo, POLICY)
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
        due = await asyncio.to_thread(repo.take_due_delivery, int(rule.id), due_iso)
        if not due:
            continue
        tenant_id = int(due.get("tenant_id") or getattr(repo, "get_rule_tenant_id", lambda _x: 1)(int(rule.id)) or 1)

        can_enqueue, enqueue_reason = limit_service.can_enqueue_job(tenant_id)
        if not can_enqueue:
            logger.warning("Scheduler пропустил enqueue | rule_id=%s | tenant_id=%s | причина=%s", int(rule.id), tenant_id, enqueue_reason)
            continue

        delivery_id = int(due["delivery_id"])
        media_group_id = due.get("media_group_id")

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
                str(due["source_channel"]),
                due["source_thread_id"],
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

        created += 1
        await asyncio.to_thread(usage_service.increment_jobs, tenant_id, 1)
        logger.info("Scheduler создал задачу #%s для delivery #%s", int(job_id), delivery_id)

    return {"created": created, "duplicates": duplicates, "checked_rules": checked_rules}


async def run_scheduler_loop(
    repo,
    *,
    interval_seconds: float = 1.0,
    is_enabled: Callable[[], bool] | None = None,
) -> None:
    logger.info("Scheduler loop запущен")
    while True:
        try:
            global _last_usage_reset_day
            today = datetime.now(timezone.utc).date().isoformat()
            if _last_usage_reset_day != today:
                _last_usage_reset_day = today
                await asyncio.to_thread(UsageService(repo).reset_daily_usage)
            enabled = is_enabled() if is_enabled else True
            await scheduler_tick(repo, enabled=enabled)
        except Exception as exc:
            logger.warning("Scheduler loop ошибка: %s", exc)
        await asyncio.sleep(max(0.2, float(interval_seconds)))
