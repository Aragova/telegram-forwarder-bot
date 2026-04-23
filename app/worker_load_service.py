from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.worker_resource_policy import WorkerResourcePolicy


@dataclass(slots=True, frozen=True)
class WorkerLoadSnapshot:
    mode: str
    light_pending: int
    heavy_pending: int
    light_processing: int
    heavy_processing: int
    heavy_retry_in_flight: int
    oldest_pending_age_light_sec: int
    oldest_pending_age_heavy_sec: int
    pending_video_download: int
    pending_video_process: int
    pending_video_send: int
    retry_storm_warning: bool
    heavy_soft_limit_exceeded: bool
    heavy_hard_limit_exceeded: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "light_pending": self.light_pending,
            "heavy_pending": self.heavy_pending,
            "light_processing": self.light_processing,
            "heavy_processing": self.heavy_processing,
            "heavy_retry_in_flight": self.heavy_retry_in_flight,
            "oldest_pending_age_light_sec": self.oldest_pending_age_light_sec,
            "oldest_pending_age_heavy_sec": self.oldest_pending_age_heavy_sec,
            "pending_video_download": self.pending_video_download,
            "pending_video_process": self.pending_video_process,
            "pending_video_send": self.pending_video_send,
            "retry_storm_warning": self.retry_storm_warning,
            "heavy_soft_limit_exceeded": self.heavy_soft_limit_exceeded,
            "heavy_hard_limit_exceeded": self.heavy_hard_limit_exceeded,
        }


def _safe_int(data: dict[str, Any] | None, key: str) -> int:
    if not data:
        return 0
    try:
        return int(data.get(key) or 0)
    except Exception:
        return 0


def _queue_counts_from_repo(repo) -> dict[str, int]:
    if hasattr(repo, "get_job_queue_counts"):
        raw = repo.get_job_queue_counts()
        if isinstance(raw, dict):
            return {k: int(v or 0) for k, v in raw.items()}
    return {}


def _oldest_pending_ages_from_repo(repo) -> tuple[int, int]:
    if not hasattr(repo, "get_oldest_pending_job_ages"):
        return 0, 0
    raw = repo.get_oldest_pending_job_ages()
    if not isinstance(raw, dict):
        return 0, 0
    return int(raw.get("light", 0) or 0), int(raw.get("heavy", 0) or 0)


def build_worker_load_snapshot(repo, policy: WorkerResourcePolicy) -> WorkerLoadSnapshot:
    job_counts = repo.get_job_status_counts() if hasattr(repo, "get_job_status_counts") else {}
    stage_counts = repo.get_video_stage_job_counts() if hasattr(repo, "get_video_stage_job_counts") else {}
    queue_counts = _queue_counts_from_repo(repo)

    heavy_pending = _safe_int(queue_counts, "heavy_pending")
    heavy_processing = _safe_int(queue_counts, "heavy_processing")
    heavy_retry = _safe_int(queue_counts, "heavy_retry")

    if heavy_pending <= 0 and isinstance(stage_counts, dict):
        heavy_pending = sum(int((stage_counts.get(stage, {}) or {}).get("pending", 0) or 0) for stage in ("video_download", "video_process", "video_send", "video_delivery"))
    if heavy_processing <= 0 and isinstance(stage_counts, dict):
        heavy_processing = sum(int((stage_counts.get(stage, {}) or {}).get("processing", 0) or 0) for stage in ("video_download", "video_process", "video_send", "video_delivery"))
    if heavy_retry <= 0 and isinstance(stage_counts, dict):
        heavy_retry = sum(int((stage_counts.get(stage, {}) or {}).get("retry", 0) or 0) for stage in ("video_download", "video_process", "video_send", "video_delivery"))

    total_pending = _safe_int(job_counts, "pending")
    total_processing = _safe_int(job_counts, "processing")
    light_pending = max(total_pending - heavy_pending, 0)
    light_processing = max(total_processing - heavy_processing, 0)

    age_light, age_heavy = _oldest_pending_ages_from_repo(repo)
    pending_video_download = int((stage_counts.get("video_download", {}) or {}).get("pending", 0) or 0)
    pending_video_process = int((stage_counts.get("video_process", {}) or {}).get("pending", 0) or 0)
    pending_video_send = int((stage_counts.get("video_send", {}) or {}).get("pending", 0) or 0)

    heavy_soft = heavy_pending >= int(policy.backlog_soft_limit_heavy)
    heavy_hard = heavy_pending >= int(policy.backlog_hard_limit_heavy)
    retry_storm = heavy_retry >= int(policy.max_heavy_retries_in_flight)

    mode = "normal"
    if heavy_hard:
        mode = "saturated"
    elif heavy_soft or retry_storm:
        mode = "degraded"

    return WorkerLoadSnapshot(
        mode=mode,
        light_pending=light_pending,
        heavy_pending=heavy_pending,
        light_processing=light_processing,
        heavy_processing=heavy_processing,
        heavy_retry_in_flight=heavy_retry,
        oldest_pending_age_light_sec=age_light,
        oldest_pending_age_heavy_sec=age_heavy,
        pending_video_download=pending_video_download,
        pending_video_process=pending_video_process,
        pending_video_send=pending_video_send,
        retry_storm_warning=retry_storm,
        heavy_soft_limit_exceeded=heavy_soft,
        heavy_hard_limit_exceeded=heavy_hard,
    )
