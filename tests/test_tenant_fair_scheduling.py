from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.health_service import get_system_health
from app.scheduler_runtime import scheduler_tick
from app.tenant_fairness_service import TenantFairnessService
from app.worker_resource_policy import WorkerResourcePolicy
from app.worker_runtime import _run_one_job, run_heavy_worker


class _Rule:
    def __init__(self, rule_id: int, mode: str = "repost") -> None:
        self.id = int(rule_id)
        self.mode = str(mode)
        self.is_active = True


class _Repo:
    def __init__(self) -> None:
        self.jobs: dict[int, dict] = {}
        self.next_job_id = 1
        self.rules = [_Rule(1, "video")]
        self.subscriptions = {
            1: {"plan_name": "FREE", "priority_level": 1, "status": "active"},
            2: {"plan_name": "BASIC", "priority_level": 2, "status": "active"},
            3: {"plan_name": "PRO", "priority_level": 3, "status": "active"},
            99: {"plan_name": "OWNER", "priority_level": 10, "status": "active"},
        }
        self.lease_jobs_calls = 0
        self.fail_fair_lease = False

    def get_active_subscription(self, tenant_id: int):
        return self.subscriptions.get(int(tenant_id), {"plan_name": "FREE", "priority_level": 1, "status": "active"})

    def create_job(self, job_type: str, payload: dict, queue: str, priority: int = 100, run_at: str | None = None, dedup_key: str | None = None):
        job_id = self.next_job_id
        self.next_job_id += 1
        self.jobs[job_id] = {
            "id": job_id,
            "job_type": job_type,
            "payload_json": dict(payload),
            "queue": queue,
            "status": "pending",
            "priority": int(priority),
            "run_at": datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "attempts": 0,
            "max_attempts": 3,
            "locked_by": None,
        }
        return job_id

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        self.lease_jobs_calls += 1
        out = []
        for job in sorted(self.jobs.values(), key=lambda row: (row["priority"], row["run_at"], row["created_at"])):
            if len(out) >= max(1, int(limit)):
                break
            if job["queue"] != queue or job["status"] not in {"pending", "retry"}:
                continue
            job["status"] = "leased"
            job["locked_by"] = worker_id
            out.append(dict(job))
        return out

    def lease_jobs_for_tenant(self, tenant_id: int, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        out = []
        for job in sorted(self.jobs.values(), key=lambda row: (row["priority"], row["run_at"], row["created_at"])):
            if len(out) >= max(1, int(limit)):
                break
            if job["queue"] != queue or job["status"] not in {"pending", "retry"}:
                continue
            if int(job.get("payload_json", {}).get("tenant_id") or 1) != int(tenant_id):
                continue
            job["status"] = "leased"
            job["locked_by"] = worker_id
            out.append(dict(job))
        return out

    def lease_fair_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        if self.fail_fair_lease:
            raise RuntimeError("fair lease broken")
        service = TenantFairnessService(self)
        result = []
        used_tenants: set[int] = set()
        for _ in range(max(1, int(limit))):
            pending = self.get_tenant_job_counts(queue=queue)
            processing = self.get_tenant_processing_counts(queue=queue)
            retry = self.get_tenant_retry_counts(queue=queue)
            oldest = self.get_tenant_oldest_pending_ages(queue=queue)
            if not pending:
                break
            scores = []
            for tenant_id, cnt in pending.items():
                score = service.compute_fairness_score(
                    tenant_id=int(tenant_id),
                    pending=int(cnt),
                    processing=int(processing.get(int(tenant_id), 0)),
                    retry=int(retry.get(int(tenant_id), 0)),
                    oldest_pending_age_sec=int(oldest.get(int(tenant_id), 0)),
                )
                if int(tenant_id) in used_tenants:
                    score -= 20
                scores.append((score, int(tenant_id)))
            scores.sort(key=lambda item: (-item[0], item[1]))
            if not scores:
                break
            tenant_id = scores[0][1]
            batch = self.lease_jobs_for_tenant(tenant_id, queue, worker_id, 1, lease_seconds)
            if not batch:
                break
            used_tenants.add(tenant_id)
            result.extend(batch)
        return result

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
        self.jobs[int(job_id)]["status"] = "retry"
        self.jobs[int(job_id)]["attempts"] += 1
        return True

    def fail_job(self, job_id: int, error_text: str) -> bool:
        self.jobs[int(job_id)]["status"] = "failed"
        return True

    def get_job_status_counts(self):
        out = {"pending": 0, "leased": 0, "processing": 0, "retry": 0, "failed": 0, "done": 0}
        for job in self.jobs.values():
            out[job["status"]] += 1
        return out

    def get_tenant_job_counts(self, queue: str | None = None):
        out: dict[int, int] = {}
        for job in self.jobs.values():
            if queue and job["queue"] != queue:
                continue
            if job["status"] not in {"pending", "retry"}:
                continue
            tenant_id = int(job.get("payload_json", {}).get("tenant_id") or 1)
            out[tenant_id] = out.get(tenant_id, 0) + 1
        return out

    def get_tenant_processing_counts(self, queue: str | None = None):
        out: dict[int, int] = {}
        for job in self.jobs.values():
            if queue and job["queue"] != queue:
                continue
            if job["status"] not in {"leased", "processing"}:
                continue
            tenant_id = int(job.get("payload_json", {}).get("tenant_id") or 1)
            out[tenant_id] = out.get(tenant_id, 0) + 1
        return out

    def get_tenant_retry_counts(self, queue: str | None = None):
        out: dict[int, int] = {}
        for job in self.jobs.values():
            if queue and job["queue"] != queue:
                continue
            if job["status"] != "retry":
                continue
            tenant_id = int(job.get("payload_json", {}).get("tenant_id") or 1)
            out[tenant_id] = out.get(tenant_id, 0) + 1
        return out

    def get_tenant_oldest_pending_ages(self, queue: str | None = None):
        now = datetime.now(timezone.utc)
        out: dict[int, int] = {}
        for job in self.jobs.values():
            if queue and job["queue"] != queue:
                continue
            if job["status"] not in {"pending", "retry"}:
                continue
            tenant_id = int(job.get("payload_json", {}).get("tenant_id") or 1)
            age = int((now - job["created_at"]).total_seconds())
            prev = out.get(tenant_id, 0)
            out[tenant_id] = max(prev, age)
        return out

    def get_tenant_throughput_snapshot(self, *, window_minutes: int = 15, queue: str | None = None):
        return {}

    def get_video_stage_job_counts(self):
        return {"video_download": {"pending": 0, "processing": 0, "retry": 0}}

    def get_job_queue_counts(self):
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

    def get_expired_leased_jobs(self, limit: int = 100):
        return []

    def get_stuck_processing_jobs(self, stuck_seconds: int = 600, limit: int = 100):
        return []

    def get_runtime_heartbeats(self):
        return [{"role": "worker", "last_seen_at": datetime.now(timezone.utc)}]

    def get_queue_stats(self):
        return {"pending": len([j for j in self.jobs.values() if j["status"] in {"pending", "retry"}]), "processing": len([j for j in self.jobs.values() if j["status"] == "processing"])}

    def count_recent_errors(self, minutes: int = 5):
        return 0

    def get_saas_health_snapshot(self):
        return {}

    def get_usage_for_date(self, tenant_id: int, day: str):
        return {}

    def get_all_rules(self):
        return self.rules

    def take_due_delivery(self, rule_id: int, due_iso: str):
        if int(rule_id) != 1:
            return None
        return {"delivery_id": 1, "media_group_id": None, "tenant_id": 1}

    def get_rule(self, rule_id: int):
        return self.rules[0]

    def get_delivery(self, delivery_id: int):
        return {
            "id": 1,
            "rule_id": 1,
            "message_id": 100,
            "source_channel": "src",
            "source_thread_id": None,
            "target_id": "dst",
            "target_thread_id": None,
            "media_group_id": None,
            "tenant_id": 1,
        }

    def get_active_job_by_dedup_key(self, dedup_key: str):
        return None

    def get_album_pending_for_rule(self, *args, **kwargs):
        return []


class _Sender:
    async def execute_repost_single_from_job(self, **payload):
        return True

    async def execute_repost_album_from_job(self, **payload):
        return True

    async def execute_video_download_from_job(self, **payload):
        return {"ok": True, "fallback_to_legacy": False, "source_video_path": None}

    async def execute_video_process_from_job(self, **payload):
        return {"ok": True, "fallback_to_legacy": False, "processed_video_path": None, "thumbnail_path": None}

    async def execute_video_send_from_job(self, **payload):
        return {"ok": True, "fallback_to_legacy": False}

    async def execute_video_delivery_from_job(self, **payload):
        return True


def test_tenant_weight_based_on_plan() -> None:
    repo = _Repo()
    service = TenantFairnessService(repo)
    assert service.get_tenant_weight(1) >= 1
    assert service.get_tenant_weight(3) > service.get_tenant_weight(2)
    assert service.get_tenant_weight(2) > service.get_tenant_weight(1)


def test_free_plan_has_non_zero_weight() -> None:
    repo = _Repo()
    service = TenantFairnessService(repo)
    assert service.get_tenant_weight(1) == 1


def test_fair_leasing_does_not_take_all_from_one_tenant() -> None:
    repo = _Repo()
    for _ in range(3):
        repo.create_job("repost_single", {"tenant_id": 3}, "light")
    repo.create_job("repost_single", {"tenant_id": 1}, "light")
    leased = repo.lease_fair_jobs("light", "w1", limit=2)
    tenants = [int(j["payload_json"]["tenant_id"]) for j in leased]
    assert 3 in tenants
    assert 1 in tenants


def test_tenant_specific_lease_only_for_target_tenant() -> None:
    repo = _Repo()
    repo.create_job("repost_single", {"tenant_id": 3}, "light")
    repo.create_job("repost_single", {"tenant_id": 2}, "light")
    leased = repo.lease_jobs_for_tenant(2, "light", "w1", limit=5)
    assert leased
    assert all(int(j["payload_json"]["tenant_id"]) == 2 for j in leased)


def test_processing_penalty_reduces_score() -> None:
    repo = _Repo()
    service = TenantFairnessService(repo)
    low_processing = service.compute_fairness_score(tenant_id=3, pending=10, processing=0, retry=0, oldest_pending_age_sec=20)
    high_processing = service.compute_fairness_score(tenant_id=3, pending=10, processing=5, retry=0, oldest_pending_age_sec=20)
    assert high_processing < low_processing


def test_heavy_throttling_for_noisy_tenant() -> None:
    repo = _Repo()
    service = TenantFairnessService(repo)
    throttled = service.should_throttle_tenant(
        tenant_id=1,
        system_mode="saturated",
        queue="heavy",
        tenant_pending=300,
        tenant_processing=10,
        tenant_retry=20,
        tenant_heavy_pending=250,
    )
    assert throttled is True


def test_scheduler_skips_heavy_enqueue_for_throttled_tenant() -> None:
    repo = _Repo()
    # создаём шумный heavy backlog для tenant #1
    for _ in range(220):
        job_id = repo.create_job("video_download", {"tenant_id": 1}, "heavy")
        repo.jobs[job_id]["created_at"] = datetime.now(timezone.utc) - timedelta(minutes=20)

    out = asyncio.run(scheduler_tick(repo, enabled=True))
    assert out["created"] == 0


def test_fallback_old_lease_path_does_not_break_worker() -> None:
    repo = _Repo()
    repo.create_job("video_download", {"tenant_id": 1, "delivery_id": 1}, "heavy")
    repo.fail_fair_lease = True
    sender = _Sender()

    async def _run_once() -> bool:
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_heavy_worker(
                repo,
                sender,
                "heavy-1",
                stop_event=stop_event,
                policy=WorkerResourcePolicy(heavy_max_concurrency=1, lease_batch_size_heavy=1),
            )
        )
        await asyncio.sleep(0.05)
        stop_event.set()
        await asyncio.sleep(0.05)
        await asyncio.wait_for(task, timeout=2)
        return True

    assert asyncio.run(_run_once()) is True
    assert repo.lease_jobs_calls > 0


def test_run_one_job_prefers_fair_lease_when_available() -> None:
    repo = _Repo()
    repo.create_job("repost_single", {"tenant_id": 1}, "light")
    sender = _Sender()
    ok = asyncio.run(_run_one_job(repo, sender, "light-1", "light", policy=WorkerResourcePolicy(light_max_concurrency=1)))
    assert ok is True


def test_health_snapshot_contains_tenant_fairness_data() -> None:
    repo = _Repo()
    repo.create_job("repost_single", {"tenant_id": 1}, "light")
    health = get_system_health(repo)
    fairness = health.get("tenant_fairness") or {}
    assert "tenants_with_pending_jobs" in fairness
    assert "top_tenants_by_backlog" in fairness
    assert "tenant_fairness_mode" in fairness
