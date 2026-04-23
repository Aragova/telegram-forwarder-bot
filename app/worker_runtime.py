from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

from .job_service import (
    enqueue_video_delivery_fallback,
    enqueue_video_process,
    enqueue_video_send,
)

logger = logging.getLogger("forwarder")


def _safe_payload(job: dict) -> dict:
    payload = job.get("payload_json")
    return payload if isinstance(payload, dict) else {}


def compute_retry_delay_seconds(queue: str, attempts: int, job_type: str) -> int:
    step = max(1, int(attempts))
    if (queue or "").strip().lower() == "heavy" or job_type.startswith("video_"):
        schedule = [30, 90, 180]
    else:
        schedule = [10, 30, 60]
    return schedule[min(step - 1, len(schedule) - 1)]


def _log_video_stage_event(event_text: str, delivery_id: int) -> None:
    logger.info("%s | delivery_id=%s | ts=%s", event_text, int(delivery_id), datetime.now(timezone.utc).isoformat())


def cleanup_video_artifacts(payload: dict, *, mode: str) -> None:
    unique_paths: list[str] = []
    for path in [
        payload.get("source_video_path") or payload.get("video_file_path"),
        payload.get("processed_video_path") or payload.get("processed_file_path"),
        payload.get("thumbnail_path"),
        *(payload.get("cleanup_paths") or []),
    ]:
        if not path:
            continue
        value = str(path)
        if value not in unique_paths:
            unique_paths.append(value)

    for path in unique_paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            continue
        except Exception as cleanup_exc:
            logger.warning("VIDEO CLEANUP WARNING | mode=%s | path=%s | error=%s", mode, path, cleanup_exc)


async def _run_one_job(repo, sender_service, worker_id: str, queue: str) -> bool:
    leased = await asyncio.to_thread(repo.lease_jobs, queue, worker_id, 1, 30)
    if not leased:
        return False

    job = leased[0]
    job_id = int(job["id"])
    job_type = str(job["job_type"])
    payload = _safe_payload(job)
    payload["job_id"] = job_id

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
        elif job_type == "video_download":
            _log_video_stage_event("VIDEO DOWNLOAD START | запуск стадии скачивания", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_download_from_job(**payload)
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                return True
            if result.get("ok"):
                next_payload = dict(payload)
                next_payload["source_video_path"] = result.get("source_video_path")
                next_payload["processed_video_path"] = None
                next_payload["thumbnail_path"] = None
                next_payload["cleanup_paths"] = [result.get("source_video_path")] if result.get("source_video_path") else []
                enqueue_video_process(repo, next_payload)
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO DOWNLOAD DONE | стадия скачивания завершена", int(payload.get("delivery_id") or 0))
                return True
            ok = False
        elif job_type == "video_process":
            _log_video_stage_event("VIDEO PROCESS START | запуск стадии обработки", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_process_from_job(**payload)
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                return True
            if result.get("ok"):
                next_payload = dict(payload)
                next_payload["source_video_path"] = payload.get("source_video_path") or payload.get("video_file_path")
                next_payload["processed_video_path"] = result.get("processed_video_path")
                next_payload["thumbnail_path"] = result.get("thumbnail_path")
                next_payload["cleanup_paths"] = result.get("cleanup_paths") or payload.get("cleanup_paths") or []
                enqueue_video_send(repo, next_payload)
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO PROCESS DONE | стадия обработки завершена", int(payload.get("delivery_id") or 0))
                return True
            ok = False
        elif job_type == "video_send":
            _log_video_stage_event("VIDEO SEND START | запуск стадии отправки", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_send_from_job(**payload)
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                return True
            ok = bool(result.get("ok"))
            if ok:
                cleanup_video_artifacts(payload, mode="success")
                _log_video_stage_event("VIDEO SEND DONE | стадия отправки завершена", int(payload.get("delivery_id") or 0))
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

        retryable = bool(payload.get("retryable", True))
        if isinstance(result, dict):
            retryable = bool(result.get("retryable", True))
        attempts = int(job.get("attempts") or 0) + 1
        max_attempts = int(job.get("max_attempts") or 3)
        if (not retryable) or attempts >= max_attempts:
            await asyncio.to_thread(repo.fail_job, job_id, "Исполнитель вернул неуспешный результат")
            if job_type.startswith("video_"):
                cleanup_video_artifacts(payload, mode="final_failure")
            logger.warning("JOB FAILED | Задача #%s помечена как failed", job_id)
        else:
            delay = compute_retry_delay_seconds(queue, attempts, job_type)
            await asyncio.to_thread(repo.retry_job, job_id, "Исполнитель вернул неуспешный результат", delay)
            logger.warning("VIDEO STAGE RETRY | задача переведена в retry | #%s | через %s сек", job_id, delay)
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
