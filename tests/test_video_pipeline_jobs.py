from __future__ import annotations

import asyncio

from app.job_service import (
    JOB_TYPE_VIDEO_DELIVERY,
    JOB_TYPE_VIDEO_DOWNLOAD,
    JOB_TYPE_VIDEO_PROCESS,
    JOB_TYPE_VIDEO_SEND,
    build_dedup_key_for_video_stage,
    enqueue_video_delivery,
    enqueue_video_process,
    enqueue_video_send,
)
from app.worker_runtime import _run_one_job


class _Rule:
    mode = "video"
    interval = 60
    schedule_mode = "interval"


class _Repo:
    def __init__(self) -> None:
        self.next_job_id = 1
        self.jobs: dict[int, dict] = {}
        self.deliveries = {
            20: {
                "id": 20,
                "rule_id": 2,
                "message_id": 200,
                "source_channel": "src",
                "source_thread_id": None,
                "target_id": "dst",
                "target_thread_id": None,
                "media_group_id": None,
            }
        }

    def get_delivery(self, delivery_id: int):
        return self.deliveries.get(int(delivery_id))

    def get_rule(self, rule_id: int):
        return _Rule() if int(rule_id) == 2 else None

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
            "payload_json": dict(payload),
            "queue": queue,
            "priority": priority,
            "status": "pending",
            "dedup_key": dedup_key,
            "attempts": 0,
            "max_attempts": 3,
            "locked_by": None,
        }
        return job_id

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        for job in self.jobs.values():
            if job["queue"] == queue and job["status"] in {"pending", "retry"}:
                job["status"] = "leased"
                job["locked_by"] = worker_id
                return [dict(job)]
        return []

    def mark_job_processing(self, job_id: int, worker_id: str) -> bool:
        job = self.jobs[job_id]
        if job["status"] != "leased":
            return False
        job["status"] = "processing"
        return True

    def complete_job(self, job_id: int) -> bool:
        self.jobs[job_id]["status"] = "done"
        return True

    def retry_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        job = self.jobs[job_id]
        job["status"] = "retry"
        job["attempts"] += 1
        job["error_text"] = error_text
        return True

    def fail_job(self, job_id: int, error_text: str) -> bool:
        job = self.jobs[job_id]
        job["status"] = "failed"
        job["attempts"] += 1
        job["error_text"] = error_text
        return True


class _Sender:
    def __init__(self) -> None:
        self.send_ok = True
        self.fallback_process = False

    async def execute_repost_single_from_job(self, **payload):
        return True

    async def execute_repost_album_from_job(self, **payload):
        return True

    async def execute_video_download_from_job(self, **payload):
        return {"ok": True, "video_file_path": "/tmp/input20.mp4", "fallback_to_legacy": False}

    async def execute_video_process_from_job(self, **payload):
        if self.fallback_process:
            return {"ok": False, "fallback_to_legacy": True}
        return {"ok": True, "processed_file_path": payload.get("video_file_path"), "fallback_to_legacy": False}

    async def execute_video_send_from_job(self, **payload):
        return {"ok": self.send_ok, "fallback_to_legacy": False}

    async def execute_video_delivery_from_job(self, **payload):
        return True


def test_video_download_creates_video_process() -> None:
    repo = _Repo()
    sender = _Sender()
    download_job_id = enqueue_video_delivery(repo, 20)

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[download_job_id]["job_type"] == JOB_TYPE_VIDEO_DOWNLOAD
    process_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_PROCESS]
    assert len(process_jobs) == 1
    assert process_jobs[0]["payload_json"]["video_file_path"] == "/tmp/input20.mp4"


def test_video_process_creates_video_send() -> None:
    repo = _Repo()
    sender = _Sender()
    enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": "/tmp/input20.mp4",
            "processed_file_path": None,
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    send_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_SEND]
    assert len(send_jobs) == 1
    assert send_jobs[0]["payload_json"]["processed_file_path"] == "/tmp/input20.mp4"


def test_video_send_completes_delivery_job() -> None:
    repo = _Repo()
    sender = _Sender()
    job_id = enqueue_video_send(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": "/tmp/input20.mp4",
            "processed_file_path": "/tmp/input20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[job_id]["status"] == "done"


def test_retry_is_stage_specific() -> None:
    repo = _Repo()
    sender = _Sender()
    sender.send_ok = False
    send_job_id = enqueue_video_send(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": "/tmp/input20.mp4",
            "processed_file_path": "/tmp/input20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[send_job_id]["status"] == "retry"
    assert all(j["job_type"] != JOB_TYPE_VIDEO_PROCESS for j in repo.jobs.values())


def test_dedup_works_for_video_stages() -> None:
    repo = _Repo()
    enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": "/tmp/input20.mp4",
            "processed_file_path": None,
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )
    second_id = enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": "/tmp/input20.mp4",
            "processed_file_path": None,
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    assert len([j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_PROCESS]) == 1
    assert repo.jobs[second_id]["dedup_key"] == build_dedup_key_for_video_stage(JOB_TYPE_VIDEO_PROCESS, 20)


def test_fallback_to_legacy_video_delivery() -> None:
    repo = _Repo()
    sender = _Sender()
    sender.fallback_process = True
    enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "video_file_path": None,
            "processed_file_path": None,
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    legacy_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_DELIVERY]
    assert len(legacy_jobs) == 1


def test_payload_is_transferred_between_stages() -> None:
    repo = _Repo()
    sender = _Sender()
    enqueue_video_delivery(repo, 20)

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    send_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_SEND]
    assert len(send_jobs) == 1
    payload = send_jobs[0]["payload_json"]
    assert payload["delivery_id"] == 20
    assert payload["video_file_path"] == "/tmp/input20.mp4"
    assert payload["processed_file_path"] == "/tmp/input20.mp4"
