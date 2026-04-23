from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.scheduler_runtime import scheduler_tick
from app.worker_load_service import build_worker_load_snapshot
from app.worker_resource_policy import WorkerResourcePolicy
from app.worker_runtime import _InFlightState, _can_start_job, _run_one_job, compute_retry_delay_seconds, run_heavy_worker


class _Rule:
    def __init__(self, rule_id: int, mode: str) -> None:
        self.id = rule_id
        self.mode = mode
        self.is_active = True


class _Repo:
    def __init__(self) -> None:
        self.jobs: dict[int, dict] = {}
        self.next_job_id = 1
        self.rules = [_Rule(1, "repost"), _Rule(2, "video")]
        self.heavy_block = False
        self.lease_calls: list[tuple[str, int]] = []

    def get_all_rules(self):
        return self.rules

    def take_due_delivery(self, rule_id: int, due_iso: str):
        if int(rule_id) == 1:
            return {
                "delivery_id": 11,
                "message_id": 111,
                "source_channel": "src",
                "source_thread_id": None,
                "content_json": "{}",
                "media_group_id": None,
                "target_id": "dst",
                "target_thread_id": None,
                "interval": 30,
            }
        if self.heavy_block:
            return None
        return {
            "delivery_id": 22,
            "message_id": 222,
            "source_channel": "src",
            "source_thread_id": None,
            "content_json": "{}",
            "media_group_id": None,
            "target_id": "dst",
            "target_thread_id": None,
            "interval": 30,
        }

    def get_delivery(self, delivery_id: int):
        return {
            "id": int(delivery_id),
            "rule_id": 1 if int(delivery_id) == 11 else 2,
            "message_id": int(delivery_id) + 100,
            "source_channel": "src",
            "source_thread_id": None,
            "target_id": "dst",
            "target_thread_id": None,
            "media_group_id": None,
        }

    def get_rule(self, rule_id: int):
        for rule in self.rules:
            if int(rule.id) == int(rule_id):
                return rule
        return None

    def get_album_pending_for_rule(self, *args, **kwargs):
        return []

    def get_active_job_by_dedup_key(self, dedup_key: str):
        for job in self.jobs.values():
            if job.get("dedup_key") == dedup_key and job["status"] in {"pending", "leased", "processing", "retry"}:
                return job
        return None

    def create_job(self, job_type: str, payload: dict, queue: str, priority: int = 100, run_at: str | None = None, dedup_key: str | None = None):
        if dedup_key and self.get_active_job_by_dedup_key(dedup_key):
            return None
        job_id = self.next_job_id
        self.next_job_id += 1
        self.jobs[job_id] = {
            "id": job_id,
            "job_type": job_type,
            "payload_json": dict(payload),
            "queue": queue,
            "priority": priority,
            "status": "pending",
            "dedup_key": dedup_key,
            "created_at": datetime.now(timezone.utc),
            "run_at": datetime.now(timezone.utc),
            "attempts": 0,
            "max_attempts": 3,
        }
        return job_id

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        self.lease_calls.append((queue, int(limit)))
        out = []
        for job in self.jobs.values():
            if len(out) >= max(1, int(limit)):
                break
            if job["queue"] != queue or job["status"] not in {"pending", "retry"}:
                continue
            job["status"] = "leased"
            job["locked_by"] = worker_id
            out.append(dict(job))
        return out

    def mark_job_processing(self, job_id: int, worker_id: str) -> bool:
        job = self.jobs[int(job_id)]
        if job["status"] != "leased":
            return False
        job["status"] = "processing"
        return True

    def complete_job(self, job_id: int) -> bool:
        self.jobs[int(job_id)]["status"] = "done"
        return True

    def retry_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        job = self.jobs[int(job_id)]
        job["status"] = "retry"
        job["attempts"] += 1
        return True

    def fail_job(self, job_id: int, error_text: str) -> bool:
        self.jobs[int(job_id)]["status"] = "failed"
        return True

    def get_job_status_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "leased": 0, "processing": 0, "retry": 0, "failed": 0, "done": 0}
        for job in self.jobs.values():
            counts[job["status"]] += 1
        return counts

    def get_video_stage_job_counts(self) -> dict[str, dict[str, int]]:
        keys = ("pending", "leased", "processing", "retry", "failed", "done")
        result = {k: {s: 0 for s in keys} for k in ("video_download", "video_process", "video_send", "video_delivery")}
        for job in self.jobs.values():
            if job["job_type"] in result:
                result[job["job_type"]][job["status"]] += 1
        return result

    def get_job_queue_counts(self) -> dict[str, int]:
        out = {
            "light_pending": 0,
            "light_processing": 0,
            "light_retry": 0,
            "heavy_pending": 0,
            "heavy_processing": 0,
            "heavy_retry": 0,
        }
        for job in self.jobs.values():
            key = f"{job['queue']}_{job['status']}"
            if key in out:
                out[key] += 1
        return out

    def get_oldest_pending_job_ages(self):
        return {"light": 0, "heavy": 0}


class _Sender:
    async def execute_repost_single_from_job(self, **payload):
        await asyncio.sleep(0.01)
        return True

    async def execute_repost_album_from_job(self, **payload):
        return True

    async def execute_video_download_from_job(self, **payload):
        await asyncio.sleep(0.01)
        return {"ok": True, "fallback_to_legacy": False, "source_video_path": None}

    async def execute_video_process_from_job(self, **payload):
        await asyncio.sleep(0.01)
        return {"ok": True, "fallback_to_legacy": False, "processed_video_path": None, "thumbnail_path": None}

    async def execute_video_send_from_job(self, **payload):
        await asyncio.sleep(0.01)
        return {"ok": True, "fallback_to_legacy": False}

    async def execute_video_delivery_from_job(self, **payload):
        await asyncio.sleep(0.01)
        return True


def test_light_and_heavy_concurrency_limits_are_separate() -> None:
    policy = WorkerResourcePolicy(light_max_concurrency=2, heavy_max_concurrency=1)
    state = _InFlightState(light_active=2, heavy_active=0)

    can_light, _ = _can_start_job("light", "repost_single", state, policy)
    can_heavy, _ = _can_start_job("heavy", "video_download", state, policy)

    assert can_light is False
    assert can_heavy is True


def test_heavy_stage_limits_are_separate() -> None:
    policy = WorkerResourcePolicy(
        heavy_max_concurrency=3,
        heavy_download_max_concurrency=1,
        heavy_process_max_concurrency=1,
        heavy_send_max_concurrency=1,
    )
    state = _InFlightState(heavy_active=1, heavy_process_active=1)

    can_process, _ = _can_start_job("heavy", "video_process", state, policy)
    can_send, _ = _can_start_job("heavy", "video_send", state, policy)

    assert can_process is False
    assert can_send is True


def test_worker_does_not_lease_above_capacity() -> None:
    repo = _Repo()
    for idx in range(4):
        repo.create_job("video_download", {"delivery_id": idx + 1}, "heavy")
    sender = _Sender()
    policy = WorkerResourcePolicy(heavy_max_concurrency=1, lease_batch_size_heavy=5)

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy", policy=policy, state=_InFlightState()))

    assert repo.lease_calls
    assert max(limit for queue, limit in repo.lease_calls if queue == "heavy") == 1


def test_backlog_thresholds_and_modes() -> None:
    repo = _Repo()
    for idx in range(6):
        repo.create_job("video_process", {"delivery_id": idx + 1}, "heavy")
    policy = WorkerResourcePolicy(backlog_soft_limit_heavy=3, backlog_hard_limit_heavy=5)

    snapshot = build_worker_load_snapshot(repo, policy)

    assert snapshot.mode == "saturated"
    assert snapshot.heavy_soft_limit_exceeded is True
    assert snapshot.heavy_hard_limit_exceeded is True


def test_scheduler_throttles_heavy_under_hard_limit() -> None:
    repo = _Repo()
    repo.heavy_block = True

    # принудительно создаём heavy backlog выше hard limit
    for idx in range(260):
        repo.create_job("video_download", {"delivery_id": idx + 1000}, "heavy")

    tick = asyncio.run(scheduler_tick(repo, enabled=True))

    created_types = [job["job_type"] for job in repo.jobs.values() if job["id"] > 260]
    assert "repost_single" in created_types
    assert "video_download" not in created_types
    assert tick["created"] >= 1


def test_retry_jitter_for_heavy_jobs() -> None:
    policy = WorkerResourcePolicy(retry_jitter_min_sec=2, retry_jitter_max_sec=4)
    value = compute_retry_delay_seconds("heavy", 1, "video_process", policy)
    assert 32 <= value <= 34


def test_mode_degraded_on_retry_storm() -> None:
    repo = _Repo()
    for idx in range(5):
        job_id = repo.create_job("video_send", {"delivery_id": idx + 1}, "heavy")
        repo.jobs[job_id]["status"] = "retry"
    policy = WorkerResourcePolicy(max_heavy_retries_in_flight=3, backlog_soft_limit_heavy=100, backlog_hard_limit_heavy=200)

    snapshot = build_worker_load_snapshot(repo, policy)
    assert snapshot.mode == "degraded"
    assert snapshot.retry_storm_warning is True


def test_graceful_shutdown_stops_taking_new_jobs() -> None:
    repo = _Repo()
    for idx in range(3):
        repo.create_job("video_download", {"delivery_id": idx + 1}, "heavy")
    sender = _Sender()
    stop_event = asyncio.Event()
    policy = WorkerResourcePolicy(heavy_max_concurrency=1, lease_batch_size_heavy=1, graceful_shutdown_timeout_sec=2)

    async def _run() -> None:
        task = asyncio.create_task(run_heavy_worker(repo, sender, "heavy-1", stop_event=stop_event, policy=policy))
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.sleep(0.05)
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(_run())
    done_or_processing = [j for j in repo.jobs.values() if j["status"] in {"done", "processing", "leased", "retry"}]
    assert done_or_processing


def test_heavy_saturation_does_not_block_light_path() -> None:
    repo = _Repo()
    for idx in range(300):
        repo.create_job("video_download", {"delivery_id": idx + 1}, "heavy")
    repo.heavy_block = True

    tick = asyncio.run(scheduler_tick(repo, enabled=True))

    assert tick["created"] >= 1
    assert any(job["job_type"] == "repost_single" for job in repo.jobs.values())
