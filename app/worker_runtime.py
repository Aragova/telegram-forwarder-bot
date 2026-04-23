from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("forwarder")


def _safe_payload(job: dict) -> dict:
    payload = job.get("payload_json")
    return payload if isinstance(payload, dict) else {}


def compute_retry_delay_seconds(queue: str, attempts: int, job_type: str) -> int:
    step = max(1, int(attempts))
    if (queue or "").strip().lower() == "heavy" or job_type == "video_delivery":
        schedule = [30, 90, 180]
    else:
        schedule = [10, 30, 60]
    return schedule[min(step - 1, len(schedule) - 1)]


async def _run_one_job(repo, sender_service, worker_id: str, queue: str) -> bool:
    leased = await asyncio.to_thread(repo.lease_jobs, queue, worker_id, 1, 30)
    if not leased:
        return False

    job = leased[0]
    job_id = int(job["id"])
    job_type = str(job["job_type"])
    payload = _safe_payload(job)

    logger.info("JOB LEASED | %s взял задачу #%s (%s)", worker_id, job_id, job_type)

    marked = await asyncio.to_thread(repo.mark_job_processing, job_id, worker_id)
    if not marked:
        return False

    logger.info("JOB PROCESSING | %s обрабатывает задачу #%s (%s)", worker_id, job_id, job_type)

    try:
        if job_type == "repost_single":
            ok = await sender_service.execute_repost_single_from_job(**payload)
        elif job_type == "repost_album":
            ok = await sender_service.execute_repost_album_from_job(**payload)
        elif job_type == "video_delivery":
            ok = await sender_service.execute_video_delivery_from_job(**payload)
        else:
            await asyncio.to_thread(repo.fail_job, job_id, f"Неподдерживаемый job_type: {job_type}")
            logger.warning("JOB FAILED | %s завершил задачу #%s с ошибкой: неподдерживаемый тип", worker_id, job_id)
            return True

        if ok:
            await asyncio.to_thread(repo.complete_job, job_id)
            logger.info("JOB DONE | %s завершил задачу #%s", worker_id, job_id)
            return True

        attempts = int(job.get("attempts") or 0) + 1
        max_attempts = int(job.get("max_attempts") or 3)
        if attempts >= max_attempts:
            await asyncio.to_thread(repo.fail_job, job_id, "Исполнитель вернул неуспешный результат")
            logger.warning("JOB FAILED | Задача #%s помечена как failed", job_id)
        else:
            delay = compute_retry_delay_seconds(queue, attempts, job_type)
            await asyncio.to_thread(repo.retry_job, job_id, "Исполнитель вернул неуспешный результат", delay)
            logger.warning("Задача переведена в retry | #%s | через %s сек", job_id, delay)
        return True

    except Exception as exc:
        attempts = int(job.get("attempts") or 0) + 1
        max_attempts = int(job.get("max_attempts") or 3)
        if attempts >= max_attempts:
            await asyncio.to_thread(repo.fail_job, job_id, str(exc))
            logger.warning("JOB FAILED | Задача #%s завершилась ошибкой: %s", job_id, exc)
        else:
            delay = compute_retry_delay_seconds(queue, attempts, job_type)
            await asyncio.to_thread(repo.retry_job, job_id, str(exc), delay)
            logger.warning("Задача переведена в retry | #%s | через %s сек: %s", job_id, delay, exc)
        return True


async def run_light_worker(repo, sender_service, worker_id: str = "light-worker-1") -> None:
    logger.info("Light worker запущен: %s", worker_id)
    while True:
        processed = await _run_one_job(repo, sender_service, worker_id, "light")
        await asyncio.sleep(0.2 if processed else 1.0)


async def run_heavy_worker(repo, sender_service, worker_id: str = "heavy-worker-1") -> None:
    logger.info("Heavy worker запущен: %s", worker_id)
    while True:
        processed = await _run_one_job(repo, sender_service, worker_id, "heavy")
        await asyncio.sleep(0.5 if processed else 1.5)
