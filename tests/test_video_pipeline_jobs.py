from __future__ import annotations

import asyncio
from pathlib import Path

from app.job_service import (
    JOB_TYPE_VIDEO_DELIVERY,
    JOB_TYPE_VIDEO_DOWNLOAD,
    JOB_TYPE_VIDEO_PROCESS,
    JOB_TYPE_VIDEO_SEND,
    VIDEO_ARTIFACT_VERSION,
    VIDEO_PIPELINE_VERSION,
    build_dedup_key_for_video_stage,
    enqueue_video_delivery,
    enqueue_video_process,
    enqueue_video_send,
)
from app.worker_runtime import _run_one_job, cleanup_video_artifacts


class _Rule:
    mode = "video"
    interval = 60
    schedule_mode = "interval"


class _Repo:
    def __init__(self) -> None:
        self.next_job_id = 1
        self.jobs: dict[int, dict] = {}
        self.usage = {"jobs_count": 0, "video_count": 0}
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

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0):
        self.usage["jobs_count"] += int(jobs_delta or 0)
        self.usage["video_count"] += int(video_delta or 0)

    def get_usage_for_date(self, tenant_id: int, day: str):
        return dict(self.usage)


class _Sender:
    def __init__(self) -> None:
        self.send_ok = True
        self.retryable = True
        self.fallback_process = False
        self.send_calls = 0
        self.legacy_calls = 0
        self.download_calls = 0
        self.process_calls = 0

    async def execute_repost_single_from_job(self, **payload):
        return True

    async def execute_repost_album_from_job(self, **payload):
        return True

    async def execute_video_download_from_job(self, **payload):
        self.download_calls += 1
        return {
            "ok": True,
            "source_video_path": "/tmp/input20.mp4",
            "fallback_to_legacy": False,
            "retryable": True,
        }

    async def execute_video_process_from_job(self, **payload):
        self.process_calls += 1
        if self.fallback_process:
            return {"ok": False, "fallback_to_legacy": True, "retryable": False}
        return {
            "ok": True,
            "processed_video_path": "/tmp/processed20.mp4",
            "thumbnail_path": "/tmp/thumb20.jpg",
            "cleanup_paths": ["/tmp/input20.mp4", "/tmp/processed20.mp4", "/tmp/thumb20.jpg"],
            "duration": 10.0,
            "width": 1280,
            "height": 720,
            "has_intro": False,
            "trim_applied": True,
            "processing_summary": {"ok": True},
            "fallback_to_legacy": False,
        }

    async def execute_video_send_from_job(self, **payload):
        self.send_calls += 1
        return {"ok": self.send_ok, "fallback_to_legacy": False, "retryable": self.retryable}

    async def execute_video_delivery_from_job(self, **payload):
        self.legacy_calls += 1
        return True


def test_video_download_creates_video_process() -> None:
    repo = _Repo()
    sender = _Sender()
    download_job_id = enqueue_video_delivery(repo, 20)

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[download_job_id]["job_type"] == JOB_TYPE_VIDEO_DOWNLOAD
    process_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_PROCESS]
    assert len(process_jobs) == 1
    payload = process_jobs[0]["payload_json"]
    assert payload["source_video_path"] == "/tmp/input20.mp4"
    assert payload["artifact_version"] == VIDEO_ARTIFACT_VERSION


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
            "source_video_path": "/tmp/input20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
            "artifact_version": 1,
            "pipeline_version": 1,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    send_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_SEND]
    assert len(send_jobs) == 1
    payload = send_jobs[0]["payload_json"]
    assert payload["processed_video_path"] == "/tmp/processed20.mp4"
    assert payload["thumbnail_path"] == "/tmp/thumb20.jpg"


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
            "source_video_path": "/tmp/input20.mp4",
            "processed_video_path": "/tmp/processed20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[job_id]["status"] == "done"
    assert sender.send_calls == 1
    assert sender.legacy_calls == 0


def test_retry_is_stage_specific_for_video_send() -> None:
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
            "source_video_path": "/tmp/input20.mp4",
            "processed_video_path": "/tmp/processed20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.jobs[send_job_id]["status"] == "retry"
    assert all(j["job_type"] != JOB_TYPE_VIDEO_PROCESS for j in repo.jobs.values())


def test_retry_is_stage_specific_for_video_download() -> None:
    repo = _Repo()

    class _DownloadFails(_Sender):
        async def execute_video_download_from_job(self, **payload):
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}

    sender = _DownloadFails()
    job_id = enqueue_video_delivery(repo, 20)
    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    assert repo.jobs[job_id]["status"] == "retry"
    assert all(j["job_type"] != JOB_TYPE_VIDEO_PROCESS for j in repo.jobs.values())


def test_video_usage_increments_once_on_final_stage() -> None:
    repo = _Repo()
    sender = _Sender()
    enqueue_video_delivery(repo, 20)

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))

    assert repo.usage["video_count"] == 1


def test_retry_is_stage_specific_for_video_process() -> None:
    repo = _Repo()

    class _ProcessFails(_Sender):
        async def execute_video_process_from_job(self, **payload):
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}

    sender = _ProcessFails()
    job_id = enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_video_path": "/tmp/in.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )
    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    assert repo.jobs[job_id]["status"] == "retry"
    assert all(j["job_type"] != JOB_TYPE_VIDEO_SEND for j in repo.jobs.values())


def test_dedup_works_for_video_stages() -> None:
    repo = _Repo()
    enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "source_video_path": "/tmp/input20.mp4",
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
            "source_video_path": "/tmp/input20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    assert len([j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_PROCESS]) == 1
    assert repo.jobs[second_id]["dedup_key"] == build_dedup_key_for_video_stage(JOB_TYPE_VIDEO_PROCESS, 20)


def test_fallback_to_legacy_video_delivery_only_on_emergency() -> None:
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
            "source_video_path": None,
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
    assert payload["source_video_path"] == "/tmp/input20.mp4"
    assert payload["processed_video_path"] == "/tmp/processed20.mp4"
    assert payload["pipeline_version"] == VIDEO_PIPELINE_VERSION


def test_cleanup_after_success_deletes_files(tmp_path: Path) -> None:
    source = tmp_path / "source.mp4"
    processed = tmp_path / "processed.mp4"
    thumb = tmp_path / "thumb.jpg"
    source.write_bytes(b"1")
    processed.write_bytes(b"2")
    thumb.write_bytes(b"3")

    cleanup_video_artifacts(
        {
            "source_video_path": str(source),
            "processed_video_path": str(processed),
            "thumbnail_path": str(thumb),
            "cleanup_paths": [],
        },
        mode="success",
    )

    assert not source.exists()
    assert not processed.exists()
    assert not thumb.exists()


def test_cleanup_not_run_on_retryable_failure(tmp_path: Path) -> None:
    repo = _Repo()

    class _SendRetry(_Sender):
        async def execute_video_send_from_job(self, **payload):
            return {"ok": False, "fallback_to_legacy": False, "retryable": True}

    sender = _SendRetry()
    processed = tmp_path / "processed.mp4"
    processed.write_bytes(b"ok")
    job_id = enqueue_video_send(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_video_path": "/tmp/input20.mp4",
            "processed_video_path": str(processed),
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    assert repo.jobs[job_id]["status"] == "retry"
    assert processed.exists()


def test_non_retryable_failure_marks_failed() -> None:
    repo = _Repo()

    class _ContractBroken(_Sender):
        async def execute_video_send_from_job(self, **payload):
            return {"ok": False, "fallback_to_legacy": False, "retryable": False}

    sender = _ContractBroken()
    job_id = enqueue_video_send(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_video_path": "/tmp/input20.mp4",
            "processed_video_path": "/tmp/processed20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    assert repo.jobs[job_id]["status"] == "failed"


def test_caption_contract_payload_survives_to_send_job() -> None:
    repo = _Repo()
    sender = _Sender()
    enqueue_video_process(
        repo,
        {
            "delivery_id": 20,
            "rule_id": 2,
            "source_channel": "src",
            "target_id": "dst",
            "source_video_path": "/tmp/input20.mp4",
            "job_type": JOB_TYPE_VIDEO_DELIVERY,
            "caption_delivery_mode": "builder_first",
            "caption_entities_json": "[{\"type\":\"bold\",\"offset\":0,\"length\":3}]",
        },
    )

    asyncio.run(_run_one_job(repo, sender, "heavy-1", "heavy"))
    send_jobs = [j for j in repo.jobs.values() if j["job_type"] == JOB_TYPE_VIDEO_SEND]
    assert len(send_jobs) == 1
    payload = send_jobs[0]["payload_json"]
    assert payload["caption_delivery_mode"] == "builder_first"
    assert payload["caption_entities_json"] is not None
