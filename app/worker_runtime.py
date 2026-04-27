from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .job_service import (
    enqueue_video_download,
    enqueue_video_delivery_fallback,
    enqueue_video_process,
    enqueue_video_send,
)
from .limit_service import LimitService
from .subscription_service import SubscriptionService
from .usage_service import UsageService
from .worker_load_service import build_worker_load_snapshot
from .worker_resource_policy import POLICY, WorkerResourcePolicy

logger = logging.getLogger("forwarder")

_LIGHT_JOB_TYPES = {"repost_single", "repost_album"}
_HEAVY_STAGE_TYPES = {"video_download", "video_process", "video_send", "video_delivery"}


@dataclass(slots=True)
class _InFlightState:
    light_active: int = 0
    heavy_active: int = 0
    heavy_download_active: int = 0
    heavy_process_active: int = 0
    heavy_send_active: int = 0


class _ThroughputMetrics:
    def __init__(self, window_sec: int) -> None:
        self.window_sec = max(30, int(window_sec))
        self._events: deque[dict[str, Any]] = deque()
        self._stage_durations: dict[str, deque[float]] = defaultdict(deque)
        self._stage_retries: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def record_done(self, *, queue: str, job_type: str, duration_sec: float) -> None:
        now = time.time()
        async with self._lock:
            self._events.append({"ts": now, "queue": queue, "job_type": job_type})
            self._stage_durations[job_type].append(float(max(duration_sec, 0.0)))
            self._prune(now)

    async def record_retry(self, *, job_type: str) -> None:
        async with self._lock:
            self._stage_retries[job_type] += 1

    async def snapshot(self) -> dict[str, Any]:
        now = time.time()
        async with self._lock:
            self._prune(now)
            light_done = 0
            heavy_done = 0
            by_stage: dict[str, int] = defaultdict(int)
            for event in self._events:
                job_type = str(event.get("job_type") or "")
                if job_type in _LIGHT_JOB_TYPES:
                    light_done += 1
                if job_type in _HEAVY_STAGE_TYPES:
                    heavy_done += 1
                    by_stage[job_type] += 1

            avg_stage_sec: dict[str, float] = {}
            for stage, values in self._stage_durations.items():
                if values:
                    avg_stage_sec[stage] = round(sum(values) / len(values), 3)

            return {
                "window_sec": self.window_sec,
                "light_done": light_done,
                "heavy_done": heavy_done,
                "by_stage": dict(by_stage),
                "avg_stage_sec": avg_stage_sec,
                "stage_retries": dict(self._stage_retries),
            }

    def _prune(self, now_ts: float) -> None:
        border = now_ts - self.window_sec
        while self._events and float(self._events[0].get("ts") or 0.0) < border:
            self._events.popleft()
        for stage in list(self._stage_durations.keys()):
            values = self._stage_durations[stage]
            while len(values) > 5000:
                values.popleft()


_runtime_metrics = _ThroughputMetrics(POLICY.throughput_window_sec)


async def get_worker_runtime_metrics_snapshot() -> dict[str, Any]:
    return await _runtime_metrics.snapshot()


def get_worker_runtime_metrics_snapshot_sync() -> dict[str, Any]:
    loop = None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        return {"warning": "runtime_loop_active", "window_sec": POLICY.throughput_window_sec}
    return asyncio.run(_runtime_metrics.snapshot())


def _safe_payload(job: dict) -> dict:
    payload = job.get("payload_json")
    return payload if isinstance(payload, dict) else {}


def _heavy_stage_key(job_type: str) -> str | None:
    value = str(job_type or "").strip().lower()
    if value == "video_download":
        return "download"
    if value == "video_process":
        return "process"
    if value == "video_send":
        return "send"
    return None


def compute_retry_delay_seconds(queue: str, attempts: int, job_type: str, policy: WorkerResourcePolicy | None = None) -> int:
    cfg = policy or POLICY
    step = max(1, int(attempts))
    if (queue or "").strip().lower() == "heavy" or str(job_type).startswith("video_"):
        schedule = [30, 90, 180]
    else:
        schedule = [10, 30, 60]
    base = schedule[min(step - 1, len(schedule) - 1)]

    if (queue or "").strip().lower() != "heavy" and not str(job_type).startswith("video_"):
        return base

    lo = min(cfg.retry_jitter_min_sec, cfg.retry_jitter_max_sec)
    hi = max(cfg.retry_jitter_min_sec, cfg.retry_jitter_max_sec)
    if hi <= 0:
        return base
    jitter = random.randint(max(0, lo), max(0, hi))
    if jitter > 0:
        logger.info(
            "Retry heavy job отложен с jitter | job_type=%s | base=%s | jitter=%s | total=%s",
            job_type,
            base,
            jitter,
            base + jitter,
        )
    return base + jitter


def _log_video_stage_event(event_text: str, delivery_id: int) -> None:
    logger.info("%s | delivery_id=%s | ts=%s", event_text, int(delivery_id), datetime.now(timezone.utc).isoformat())


def _finalize_invalid_source_file(repo, *, payload: dict, job_id: int, error_text: str, result: dict[str, Any] | None = None) -> None:
    delivery_id = int(payload.get("delivery_id") or 0)
    rule_id = int(payload.get("rule_id") or 0)
    post_id = repo.get_post_id_by_delivery(delivery_id)
    details = dict(result or {})
    repo.mark_delivery_faulty(delivery_id, error_text)
    repo.log_video_event(
        event_type="video_invalid_source_file",
        delivery_id=delivery_id,
        rule_id=rule_id,
        post_id=post_id,
        status="faulty",
        error_text=error_text,
        extra={
            "delivery_id": delivery_id,
            "rule_id": rule_id,
            "job_id": int(job_id),
            "path": details.get("path"),
            "size": details.get("size"),
            "ffprobe_stderr": details.get("ffprobe_stderr"),
            "validation_attempt": details.get("validation_attempt"),
            "max_validation_attempts": details.get("max_validation_attempts"),
        },
    )


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


def _can_start_job(queue: str, job_type: str, state: _InFlightState, policy: WorkerResourcePolicy) -> tuple[bool, str | None]:
    if queue == "light":
        if state.light_active >= int(policy.light_max_concurrency):
            return False, "Light worker достиг лимита concurrency"
        return True, None

    if state.heavy_active >= int(policy.heavy_max_concurrency):
        return False, "Heavy worker достиг лимита concurrency"

    stage = _heavy_stage_key(job_type)
    if stage == "download" and state.heavy_download_active >= int(policy.heavy_download_max_concurrency):
        return False, "Heavy download stage достиг лимита concurrency"
    if stage == "process" and state.heavy_process_active >= int(policy.heavy_process_max_concurrency):
        return False, "Heavy process stage достиг лимита concurrency"
    if stage == "send" and state.heavy_send_active >= int(policy.heavy_send_max_concurrency):
        return False, "Heavy send stage достиг лимита concurrency"
    return True, None


def _inc_in_flight(queue: str, job_type: str, state: _InFlightState) -> None:
    if queue == "light":
        state.light_active += 1
        return
    state.heavy_active += 1
    stage = _heavy_stage_key(job_type)
    if stage == "download":
        state.heavy_download_active += 1
    elif stage == "process":
        state.heavy_process_active += 1
    elif stage == "send":
        state.heavy_send_active += 1


def _dec_in_flight(queue: str, job_type: str, state: _InFlightState) -> None:
    if queue == "light":
        state.light_active = max(0, state.light_active - 1)
        return
    state.heavy_active = max(0, state.heavy_active - 1)
    stage = _heavy_stage_key(job_type)
    if stage == "download":
        state.heavy_download_active = max(0, state.heavy_download_active - 1)
    elif stage == "process":
        state.heavy_process_active = max(0, state.heavy_process_active - 1)
    elif stage == "send":
        state.heavy_send_active = max(0, state.heavy_send_active - 1)


async def _process_job(repo, sender_service, worker_id: str, queue: str, job: dict, *, policy: WorkerResourcePolicy, state: _InFlightState) -> bool:
    job_id = int(job["id"])
    job_type = str(job["job_type"])
    payload = _safe_payload(job)
    payload.pop("job_id", None)
    payload.pop("tenant_id", None)
    tenant_id = int(payload.get("tenant_id") or getattr(repo, "get_rule_tenant_id", lambda _x: 1)(int(payload.get("rule_id") or 0)) or 1)
    subscription_service = SubscriptionService(repo)
    usage_service = UsageService(repo)
    limit_service = LimitService(repo, subscription_service, usage_service)

    can_enqueue, enqueue_reason = limit_service.can_enqueue_job(tenant_id)
    if not can_enqueue:
        logger.warning("Worker блокирует выполнение job #%s tenant_id=%s: %s", job_id, tenant_id, enqueue_reason)
        if hasattr(repo, "create_billing_event"):
            sub = subscription_service.get_active_subscription(int(tenant_id)) or {}
            usage_today = usage_service.get_today_usage(int(tenant_id)) or {}
            event_type = "subscription_blocked_action" if str(enqueue_reason or "").strip().lower() == "подписка неактивна" else "limit_job_blocked"
            repo.create_billing_event(
                int(tenant_id),
                event_type,
                event_source="worker_runtime",
                metadata={
                    "tenant_id": int(tenant_id),
                    "plan_name": str(sub.get("plan_name") or "FREE"),
                    "action": "worker_job_execution",
                    "reason": str(enqueue_reason or "limit"),
                    "usage_snapshot": {
                        "jobs_count": int(usage_today.get("jobs_count") or 0),
                        "video_count": int(usage_today.get("video_count") or 0),
                    },
                },
            )
        await asyncio.to_thread(repo.fail_job, job_id, enqueue_reason or "Подписка неактивна")
        return True

    if str(job_type).startswith("video_"):
        can_video, video_reason = limit_service.can_process_video(tenant_id)
        if not can_video:
            logger.warning("Видео не обработано: лимит тарифа tenant_id=%s reason=%s", tenant_id, video_reason or "unknown")
            if hasattr(repo, "create_billing_event"):
                sub = subscription_service.get_active_subscription(int(tenant_id)) or {}
                usage_today = usage_service.get_today_usage(int(tenant_id)) or {}
                repo.create_billing_event(
                    int(tenant_id),
                    "limit_video_blocked",
                    event_source="worker_runtime",
                    metadata={
                        "tenant_id": int(tenant_id),
                        "plan_name": str(sub.get("plan_name") or "FREE"),
                        "action": "video_processing",
                        "reason": str(video_reason or "limit"),
                        "usage_snapshot": {
                            "jobs_count": int(usage_today.get("jobs_count") or 0),
                            "video_count": int(usage_today.get("video_count") or 0),
                        },
                    },
                )
            friendly = (
                "🎬 Лимит видео на сегодня исчерпан. "
                "Новые видео будут доступны после обновления дневного лимита "
                "или после перехода на PRO."
            )
            await asyncio.to_thread(repo.fail_job, job_id, video_reason or friendly)
            return True

    can_start, reason = _can_start_job(queue, job_type, state, policy)
    if not can_start:
        logger.warning("%s | queue=%s | job_id=%s | job_type=%s", reason, queue, job_id, job_type)
        attempts = int(job.get("attempts") or 0) + 1
        delay = compute_retry_delay_seconds(queue, attempts, job_type, policy)
        await asyncio.to_thread(repo.retry_job, job_id, reason or "Лимит concurrency", delay)
        return True

    marked = await asyncio.to_thread(repo.mark_job_processing, job_id, worker_id)
    if not marked:
        return False

    _inc_in_flight(queue, job_type, state)
    started_at = time.perf_counter()

    logger.info("JOB PROCESSING | %s обрабатывает задачу #%s (%s)", worker_id, job_id, job_type)
    result: dict[str, Any] | None = None
    try:
        if job_type == "repost_single":
            ok = await sender_service.execute_repost_single_from_job(**payload)
        elif job_type == "repost_album":
            ok = await sender_service.execute_repost_album_from_job(**payload)
        elif job_type == "video_download":
            _log_video_stage_event("VIDEO DOWNLOAD START | запуск стадии скачивания", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_download_from_job(
                **payload,
                job_attempt=int(job.get("attempts") or 0) + 1,
            )
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
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
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
                return True
            ok = False
        elif job_type == "video_process":
            _log_video_stage_event("VIDEO PROCESS START | запуск стадии обработки", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_process_from_job(
                **payload,
                job_attempt=int(job.get("attempts") or 0) + 1,
            )
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
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
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
                return True
            if result.get("restart_download"):
                delivery_id = int(payload.get("delivery_id") or 0)
                enqueue_video_download(
                    repo,
                    delivery_id,
                    extra_payload={
                        "invalid_file_attempts": int(result.get("invalid_file_attempts") or int(payload.get("invalid_file_attempts") or 0) + 1),
                    },
                )
                await asyncio.to_thread(repo.complete_job, job_id)
                cleanup_video_artifacts(payload, mode="restart_download")
                _log_video_stage_event(
                    "VIDEO PROCESS RETRY DOWNLOAD | обнаружен битый файл, создана повторная download-задача",
                    delivery_id,
                )
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
                return True
            ok = False
        elif job_type == "video_send":
            _log_video_stage_event("VIDEO SEND START | запуск стадии отправки", int(payload.get("delivery_id") or 0))
            result = await sender_service.execute_video_send_from_job(**payload)
            if result.get("fallback_to_legacy"):
                enqueue_video_delivery_fallback(repo, int(payload.get("delivery_id") or 0))
                await asyncio.to_thread(repo.complete_job, job_id)
                _log_video_stage_event("VIDEO FALLBACK TO LEGACY | fallback в video_delivery", int(payload.get("delivery_id") or 0))
                await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
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
            # Учитываем видео usage один раз на завершение конечной стадии доставки.
            # Это защищает от накрутки на download/process/retry этапах.
            if str(job_type) in {"video_send", "video_delivery"}:
                await asyncio.to_thread(usage_service.increment_video, tenant_id, 1)
            logger.info("JOB DONE | %s завершил задачу #%s", worker_id, job_id)
            await _runtime_metrics.record_done(queue=queue, job_type=job_type, duration_sec=time.perf_counter() - started_at)
            return True

        retryable = bool(payload.get("retryable", True))
        error_text = "Исполнитель вернул неуспешный результат"
        if isinstance(result, dict):
            retryable = bool(result.get("retryable", True))
            result_error_text = str(result.get("error_text") or "").strip()
            if result_error_text:
                error_text = result_error_text
            if bool(result.get("final_invalid_source_file")):
                await asyncio.to_thread(
                    _finalize_invalid_source_file,
                    repo,
                    payload=payload,
                    job_id=job_id,
                    error_text=error_text,
                    result=result,
                )
        attempts = int(job.get("attempts") or 0) + 1
        max_attempts = int(job.get("max_attempts") or 3)
        if (not retryable) or attempts >= max_attempts:
            await asyncio.to_thread(repo.fail_job, job_id, error_text)
            if job_type.startswith("video_"):
                cleanup_video_artifacts(payload, mode="final_failure")
            logger.warning("JOB FAILED | Задача #%s помечена как failed", job_id)
        else:
            delay = compute_retry_delay_seconds(queue, attempts, job_type, policy)
            await asyncio.to_thread(repo.retry_job, job_id, error_text, delay)
            logger.warning("VIDEO STAGE RETRY | задача переведена в retry | #%s | через %s сек", job_id, delay)
            await _runtime_metrics.record_retry(job_type=job_type)
        return True

    except Exception as exc:
        attempts = int(job.get("attempts") or 0) + 1
        max_attempts = int(job.get("max_attempts") or 3)
        if attempts >= max_attempts:
            await asyncio.to_thread(repo.fail_job, job_id, str(exc))
            logger.warning("JOB FAILED | Задача #%s завершилась ошибкой: %s", job_id, exc)
        else:
            delay = compute_retry_delay_seconds(queue, attempts, job_type, policy)
            await asyncio.to_thread(repo.retry_job, job_id, str(exc), delay)
            logger.warning("Задача переведена в retry | #%s | через %s сек: %s", job_id, delay, exc)
            await _runtime_metrics.record_retry(job_type=job_type)
        return True
    finally:
        _dec_in_flight(queue, job_type, state)


async def _run_one_job(repo, sender_service, worker_id: str, queue: str, *, policy: WorkerResourcePolicy | None = None, state: _InFlightState | None = None) -> bool:
    cfg = policy or POLICY
    active_state = state or _InFlightState()

    lease_batch = cfg.lease_batch_size_light if queue == "light" else cfg.lease_batch_size_heavy
    queue_cap = cfg.light_max_concurrency if queue == "light" else cfg.heavy_max_concurrency
    active_now = active_state.light_active if queue == "light" else active_state.heavy_active
    lease_limit = max(1, min(int(lease_batch), max(int(queue_cap) - int(active_now), 1)))
    if hasattr(repo, "lease_fair_jobs"):
        leased = await asyncio.to_thread(repo.lease_fair_jobs, queue, worker_id, lease_limit, 30)
    else:
        leased = await asyncio.to_thread(repo.lease_jobs, queue, worker_id, lease_limit, 30)
    if not leased:
        return False

    job = leased[0]
    logger.info("JOB LEASED | %s взял задачу #%s (%s)", worker_id, int(job["id"]), str(job["job_type"]))
    # Возвращаем остальные в очередь, если взяли batch>1 и сейчас не запускаем их.
    for extra in leased[1:]:
        await asyncio.to_thread(repo.retry_job, int(extra["id"]), "Контролируемый leasing: слот пока недоступен", 1)

    return await _process_job(repo, sender_service, worker_id, queue, job, policy=cfg, state=active_state)


async def _worker_loop(repo, sender_service, *, queue: str, worker_id: str, policy: WorkerResourcePolicy, stop_event: asyncio.Event | None = None) -> None:
    state = _InFlightState()
    in_flight: set[asyncio.Task] = set()
    queue_limit = int(policy.light_max_concurrency if queue == "light" else policy.heavy_max_concurrency)

    logger.info("%s worker запущен: %s | max_concurrency=%s", queue.capitalize(), worker_id, queue_limit)

    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("Worker завершает работу в graceful shutdown | queue=%s", queue)
                break

            if queue == "heavy":
                load = await asyncio.to_thread(build_worker_load_snapshot, repo, policy)
                if load.heavy_hard_limit_exceeded:
                    logger.warning(
                        "Heavy backlog превысил жёсткий порог | heavy_pending=%s | hard_limit=%s",
                        load.heavy_pending,
                        policy.backlog_hard_limit_heavy,
                    )
                elif load.heavy_soft_limit_exceeded:
                    logger.warning(
                        "Система перешла в degraded mode | heavy_pending=%s | soft_limit=%s",
                        load.heavy_pending,
                        policy.backlog_soft_limit_heavy,
                    )
                if load.retry_storm_warning:
                    logger.warning(
                        "Retry storm warning | heavy_retry_in_flight=%s | cap=%s",
                        load.heavy_retry_in_flight,
                        policy.max_heavy_retries_in_flight,
                    )

            while in_flight and len(in_flight) >= queue_limit:
                done, pending = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED, timeout=0.3)
                in_flight = set(pending)
                if not done:
                    logger.warning("%s worker достиг лимита concurrency | active=%s", queue.capitalize(), len(in_flight))
                    break

            if len(in_flight) >= queue_limit:
                await asyncio.sleep(0.2 if queue == "light" else 0.5)
                continue

            available = max(queue_limit - len(in_flight), 0)
            batch_size = policy.lease_batch_size_light if queue == "light" else policy.lease_batch_size_heavy
            lease_limit = max(1, min(available, int(batch_size)))
            leased: list[dict[str, Any]] = []
            if hasattr(repo, "lease_fair_jobs"):
                try:
                    leased = await asyncio.to_thread(repo.lease_fair_jobs, queue, worker_id, lease_limit, 30)
                except Exception as exc:
                    logger.warning(
                        "Fair leasing недоступен, используем fallback к обычному lease | queue=%s | ошибка=%s",
                        queue,
                        exc,
                    )
                    leased = await asyncio.to_thread(repo.lease_jobs, queue, worker_id, lease_limit, 30)
            else:
                leased = await asyncio.to_thread(repo.lease_jobs, queue, worker_id, lease_limit, 30)
            if not leased:
                await asyncio.sleep(0.2 if queue == "light" else 0.6)
                continue

            for job in leased:
                if len(in_flight) >= queue_limit:
                    await asyncio.to_thread(repo.retry_job, int(job["id"]), "Контролируемый leasing: нет свободных execution slots", 1)
                    continue
                logger.info("JOB LEASED | %s взял задачу #%s (%s)", worker_id, int(job["id"]), str(job["job_type"]))
                task = asyncio.create_task(_process_job(repo, sender_service, worker_id, queue, job, policy=policy, state=state))
                in_flight.add(task)
                task.add_done_callback(lambda t: in_flight.discard(t))

    except asyncio.CancelledError:
        logger.info("Worker получил сигнал отмены | queue=%s | worker_id=%s", queue, worker_id)
    finally:
        if in_flight:
            logger.info("Worker ждёт завершения in-flight задач | queue=%s | count=%s", queue, len(in_flight))
            done, pending = await asyncio.wait(in_flight, timeout=float(policy.graceful_shutdown_timeout_sec))
            if pending:
                logger.warning(
                    "Worker graceful shutdown timeout | queue=%s | unfinished=%s | timeout_sec=%s",
                    queue,
                    len(pending),
                    policy.graceful_shutdown_timeout_sec,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)


async def run_light_worker(repo, sender_service, worker_id: str = "light-worker-1", *, stop_event: asyncio.Event | None = None, policy: WorkerResourcePolicy | None = None) -> None:
    await _worker_loop(repo, sender_service, queue="light", worker_id=worker_id, stop_event=stop_event, policy=policy or POLICY)


async def run_heavy_worker(repo, sender_service, worker_id: str = "heavy-worker-1", *, stop_event: asyncio.Event | None = None, policy: WorkerResourcePolicy | None = None) -> None:
    await _worker_loop(repo, sender_service, queue="heavy", worker_id=worker_id, stop_event=stop_event, policy=policy or POLICY)
