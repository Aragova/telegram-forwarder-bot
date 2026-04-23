from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("forwarder")


async def watchdog_tick(
    repo,
    *,
    requeue_delay_seconds: int = 15,
    stuck_processing_seconds: int = 600,
) -> dict[str, int]:
    expired_jobs = await asyncio.to_thread(repo.get_expired_leased_jobs)
    stuck_jobs = await asyncio.to_thread(repo.get_stuck_processing_jobs, int(stuck_processing_seconds))

    requeued = 0
    if expired_jobs:
        for job in expired_jobs:
            logger.warning("Lease задачи истёк | job #%s", job.get("id"))
        requeued = int(await asyncio.to_thread(repo.requeue_expired_leases, int(requeue_delay_seconds)) or 0)
        if requeued > 0:
            logger.warning("Watchdog вернул задачу в очередь: %s шт.", requeued)
            logger.warning("Задача переведена в retry после истечения lease: %s шт.", requeued)

    for job in stuck_jobs:
        logger.warning(
            "Обнаружена зависшая задача #%s (status=processing, updated_at=%s)",
            job.get("id"),
            job.get("updated_at"),
        )

    requeued_stuck = 0
    if stuck_jobs and hasattr(repo, "requeue_stuck_processing_jobs"):
        requeued_stuck = int(
            await asyncio.to_thread(
                repo.requeue_stuck_processing_jobs,
                int(stuck_processing_seconds),
                int(requeue_delay_seconds),
            )
            or 0
        )
        if requeued_stuck > 0:
            logger.warning("Watchdog вернул зависшие processing-задачи в retry: %s шт.", requeued_stuck)

    return {
        "expired_leased": len(expired_jobs),
        "requeued": requeued,
        "stuck_processing": len(stuck_jobs),
        "requeued_stuck_processing": requeued_stuck,
    }


async def run_watchdog_loop(
    repo,
    *,
    interval_seconds: float = 10.0,
    requeue_delay_seconds: int = 15,
    stuck_processing_seconds: int = 600,
) -> None:
    logger.info("Job watchdog запущен")
    while True:
        try:
            await watchdog_tick(
                repo,
                requeue_delay_seconds=requeue_delay_seconds,
                stuck_processing_seconds=stuck_processing_seconds,
            )
        except Exception as exc:
            logger.warning("Job watchdog ошибка: %s", exc)
        await asyncio.sleep(max(1.0, float(interval_seconds)))
