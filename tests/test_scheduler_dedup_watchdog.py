from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from app.job_service import (
    build_dedup_key_for_album,
    build_dedup_key_for_single,
    build_dedup_key_for_video,
    enqueue_repost_album,
    enqueue_repost_single,
    enqueue_video_delivery,
)
from app.scheduler_runtime import scheduler_tick
from app.job_watchdog import watchdog_tick
from app.worker_runtime import compute_retry_delay_seconds


class _Rule:
    def __init__(self, rule_id: int, mode: str = "repost", is_active: bool = True) -> None:
        self.id = rule_id
        self.mode = mode
        self.is_active = is_active


class _Repo:
    def __init__(self) -> None:
        self.rules = [_Rule(1, "repost", True), _Rule(2, "video", True)]
        self.jobs: dict[int, dict] = {}
        self.next_job_id = 1
        self.next_due_by_rule: dict[int, dict] = {
            1: {
                "delivery_id": 10,
                "source_channel": "src",
                "source_thread_id": None,
                "media_group_id": None,
            },
            2: {
                "delivery_id": 20,
                "source_channel": "src",
                "source_thread_id": None,
                "media_group_id": None,
            },
        }
        self.deliveries = {
            10: {"id": 10, "rule_id": 1, "message_id": 100, "source_channel": "src", "source_thread_id": None, "target_id": "dst", "target_thread_id": None, "media_group_id": None},
            11: {"id": 11, "rule_id": 1, "message_id": 101, "source_channel": "src", "source_thread_id": None, "target_id": "dst", "target_thread_id": None, "media_group_id": "alb"},
            12: {"id": 12, "rule_id": 1, "message_id": 102, "source_channel": "src", "source_thread_id": None, "target_id": "dst", "target_thread_id": None, "media_group_id": "alb"},
            20: {"id": 20, "rule_id": 2, "message_id": 200, "source_channel": "src", "source_thread_id": None, "target_id": "dst", "target_thread_id": None, "media_group_id": None},
        }

    def get_all_rules(self):
        return self.rules

    def take_due_delivery(self, rule_id: int, due_iso: str):
        return self.next_due_by_rule.get(int(rule_id))

    def get_album_pending_for_rule(self, rule_id: int, source_channel: str, source_thread_id, media_group_id: str):
        if int(rule_id) == 1 and media_group_id == "alb":
            return [{"delivery_id": 11}, {"delivery_id": 12}]
        return []

    def get_delivery(self, delivery_id: int):
        return self.deliveries.get(int(delivery_id))

    def get_rule(self, rule_id: int):
        for r in self.rules:
            if r.id == int(rule_id):
                return r
        return None

    def get_active_job_by_dedup_key(self, dedup_key: str):
        for job in self.jobs.values():
            if job.get("dedup_key") == dedup_key and job["status"] in {"pending", "leased", "processing", "retry"}:
                return job
        return None

    def create_job(self, job_type: str, payload: dict, queue: str, priority: int = 100, run_at: str | None = None, dedup_key: str | None = None):
        existing = self.get_active_job_by_dedup_key(dedup_key) if dedup_key else None
        if existing:
            return existing["id"]
        job_id = self.next_job_id
        self.next_job_id += 1
        self.jobs[job_id] = {
            "id": job_id,
            "job_type": job_type,
            "queue": queue,
            "priority": priority,
            "payload_json": payload,
            "dedup_key": dedup_key,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
            "run_at": datetime.now(timezone.utc),
            "lease_until": None,
            "updated_at": datetime.now(timezone.utc),
            "attempts": 0,
        }
        return job_id

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        eligible = [
            j
            for j in self.jobs.values()
            if j["queue"] == queue and j["status"] in {"pending", "retry"}
        ]
        eligible.sort(key=lambda row: (row["priority"], row["run_at"], row["created_at"]))
        out = []
        for job in eligible[: max(1, int(limit))]:
            job["status"] = "leased"
            job["locked_by"] = worker_id
            job["lease_until"] = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
            out.append(dict(job))
        return out

    def get_expired_leased_jobs(self, limit: int = 100):
        now = datetime.now(timezone.utc)
        return [
            dict(j)
            for j in self.jobs.values()
            if j["status"] == "leased" and j.get("lease_until") and j["lease_until"] < now
        ][:limit]

    def requeue_expired_leases(self, delay_seconds: int = 15):
        now = datetime.now(timezone.utc)
        count = 0
        for job in self.jobs.values():
            if job["status"] == "leased" and job.get("lease_until") and job["lease_until"] < now:
                job["status"] = "retry"
                job["locked_by"] = None
                job["lease_until"] = None
                job["run_at"] = now + timedelta(seconds=delay_seconds)
                job["updated_at"] = now
                job["error_text"] = "Истёк lease задачи, задача возвращена в очередь"
                job["attempts"] += 1
                count += 1
        return count

    def get_stuck_processing_jobs(self, stuck_seconds: int = 600, limit: int = 100):
        border = datetime.now(timezone.utc) - timedelta(seconds=stuck_seconds)
        return [
            dict(j)
            for j in self.jobs.values()
            if j["status"] == "processing" and j["updated_at"] < border
        ][:limit]

    def requeue_stuck_processing_jobs(self, stuck_seconds: int = 600, delay_seconds: int = 15):
        border = datetime.now(timezone.utc) - timedelta(seconds=stuck_seconds)
        now = datetime.now(timezone.utc)
        count = 0
        for job in self.jobs.values():
            if job["status"] == "processing" and job["updated_at"] < border:
                job["status"] = "retry"
                job["locked_by"] = None
                job["lease_until"] = None
                job["run_at"] = now + timedelta(seconds=delay_seconds)
                job["updated_at"] = now
                job["error_text"] = "Watchdog: задача зависла в processing и возвращена в retry"
                job["attempts"] += 1
                count += 1
        return count


def test_scheduler_no_duplicate_repost_single() -> None:
    repo = _Repo()
    asyncio.run(scheduler_tick(repo, enabled=True))
    asyncio.run(scheduler_tick(repo, enabled=True))
    single_jobs = [j for j in repo.jobs.values() if j["job_type"] == "repost_single"]
    assert len(single_jobs) == 1


def test_scheduler_no_duplicate_video_delivery() -> None:
    repo = _Repo()
    asyncio.run(scheduler_tick(repo, enabled=True))
    asyncio.run(scheduler_tick(repo, enabled=True))
    video_jobs = [j for j in repo.jobs.values() if j["job_type"] == "video_download"]
    assert len(video_jobs) == 1


def test_scheduler_no_duplicate_album_logical_item() -> None:
    repo = _Repo()
    repo.next_due_by_rule[1] = {
        "delivery_id": 11,
        "source_channel": "src",
        "source_thread_id": None,
        "media_group_id": "alb",
    }
    asyncio.run(scheduler_tick(repo, enabled=True))
    asyncio.run(scheduler_tick(repo, enabled=True))
    album_jobs = [j for j in repo.jobs.values() if j["job_type"] == "repost_album"]
    assert len(album_jobs) == 1


def test_dedup_key_is_built_correctly() -> None:
    assert build_dedup_key_for_single(10) == "repost_single:delivery:10"
    assert build_dedup_key_for_video(20) == "video_download:delivery:20"
    assert build_dedup_key_for_album(1, "alb", [11, 12]) == "repost_album:rule:1:media_group:alb"
    assert build_dedup_key_for_album(1, None, [12, 11]) == "repost_album:rule:1:deliveries:11,12"


def test_lease_jobs_respects_priority() -> None:
    repo = _Repo()
    enqueue_video_delivery(repo, 20)
    enqueue_repost_single(repo, 10)
    leased = repo.lease_jobs("heavy", "w1", 1)
    assert leased[0]["job_type"] == "video_download"

    repo.create_job("repost_album", {"delivery_id": 10}, "light", priority=90, dedup_key="custom-1")
    leased_light = repo.lease_jobs("light", "w2", 1)
    assert leased_light[0]["priority"] == 90


def test_watchdog_requeues_expired_lease() -> None:
    repo = _Repo()
    job_id = enqueue_repost_single(repo, 10)
    job = repo.jobs[job_id]
    job["status"] = "leased"
    job["lease_until"] = datetime.now(timezone.utc) - timedelta(seconds=5)

    result = asyncio.run(watchdog_tick(repo))

    assert result["requeued"] == 1
    assert repo.jobs[job_id]["status"] == "retry"


def test_watchdog_detects_stuck_processing() -> None:
    repo = _Repo()
    job_id = enqueue_repost_single(repo, 10)
    job = repo.jobs[job_id]
    job["status"] = "processing"
    job["updated_at"] = datetime.now(timezone.utc) - timedelta(minutes=11)

    result = asyncio.run(watchdog_tick(repo, stuck_processing_seconds=600))

    assert result["stuck_processing"] == 1
    assert result["requeued_stuck_processing"] == 1
    assert repo.jobs[job_id]["status"] == "retry"


def test_retry_delay_differs_for_light_and_heavy() -> None:
    assert compute_retry_delay_seconds("light", 1, "repost_single") == 10
    assert compute_retry_delay_seconds("heavy", 1, "video_download") == 30


def test_repeated_scheduler_tick_is_idempotent() -> None:
    repo = _Repo()
    first = asyncio.run(scheduler_tick(repo, enabled=True))
    second = asyncio.run(scheduler_tick(repo, enabled=True))

    assert first["created"] >= 1
    assert second["duplicates"] >= 1
    assert len(repo.jobs) == 2
