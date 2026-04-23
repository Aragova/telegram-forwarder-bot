from __future__ import annotations

from app.job_service import (
    JOB_QUEUE_BY_TYPE,
    JOB_TYPE_REPOST_ALBUM,
    JOB_TYPE_REPOST_SINGLE,
    JOB_TYPE_VIDEO_DELIVERY,
    enqueue_repost_album,
    enqueue_repost_single,
    enqueue_video_delivery,
)


class _FakeRepo:
    def __init__(self) -> None:
        self.jobs: dict[int, dict] = {}
        self.next_id = 1
        self.deliveries = {
            10: {
                "id": 10,
                "rule_id": 1,
                "message_id": 100,
                "source_channel": "src",
                "source_thread_id": None,
                "target_id": "dst",
                "target_thread_id": None,
                "media_group_id": None,
            },
            11: {
                "id": 11,
                "rule_id": 1,
                "message_id": 101,
                "source_channel": "src",
                "source_thread_id": None,
                "target_id": "dst",
                "target_thread_id": None,
                "media_group_id": "alb-1",
            },
            12: {
                "id": 12,
                "rule_id": 1,
                "message_id": 102,
                "source_channel": "src",
                "source_thread_id": None,
                "target_id": "dst",
                "target_thread_id": None,
                "media_group_id": "alb-1",
            },
        }

    def get_delivery(self, delivery_id: int):
        return self.deliveries.get(int(delivery_id))

    def get_rule(self, rule_id: int):
        class _Rule:
            mode = "repost"
            interval = 30
            schedule_mode = "interval"

        rule = _Rule()
        if int(rule_id) == 1:
            return rule
        return None

    def create_job(self, job_type: str, payload: dict, queue: str, priority: int = 100, run_at: str | None = None):
        for job in self.jobs.values():
            if job["job_type"] == job_type and job["payload_json"].get("delivery_id") == payload.get("delivery_id") and job["status"] in {"pending", "retry", "leased", "processing"}:
                return job["id"]

        job_id = self.next_id
        self.next_id += 1
        self.jobs[job_id] = {
            "id": job_id,
            "job_type": job_type,
            "payload_json": payload,
            "queue": queue,
            "priority": priority,
            "status": "pending",
            "attempts": 0,
            "max_attempts": 3,
            "locked_by": None,
        }
        return job_id

    def lease_jobs(self, queue: str, worker_id: str, limit: int = 1, lease_seconds: int = 30):
        out = []
        for job in self.jobs.values():
            if len(out) >= limit:
                break
            if job["queue"] == queue and job["status"] in {"pending", "retry"}:
                job["status"] = "leased"
                job["locked_by"] = worker_id
                out.append(job.copy())
        return out

    def mark_job_processing(self, job_id: int, worker_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job or job["status"] != "leased" or job["locked_by"] != worker_id:
            return False
        job["status"] = "processing"
        return True

    def complete_job(self, job_id: int) -> bool:
        job = self.jobs.get(job_id)
        if not job:
            return False
        job["status"] = "done"
        return True

    def retry_job(self, job_id: int, error_text: str, delay_seconds: int) -> bool:
        job = self.jobs.get(job_id)
        if not job:
            return False
        job["status"] = "retry"
        job["attempts"] += 1
        job["error_text"] = error_text
        return True

    def fail_job(self, job_id: int, error_text: str) -> bool:
        job = self.jobs.get(job_id)
        if not job:
            return False
        job["status"] = "failed"
        job["attempts"] += 1
        job["error_text"] = error_text
        return True

    def get_job(self, job_id: int):
        return self.jobs.get(job_id)


def test_create_and_get_job() -> None:
    repo = _FakeRepo()
    job_id = enqueue_repost_single(repo, 10)
    assert job_id is not None
    assert repo.get_job(job_id)["status"] == "pending"


def test_lease_and_complete_job() -> None:
    repo = _FakeRepo()
    job_id = enqueue_repost_single(repo, 10)
    leased = repo.lease_jobs("light", "light-worker-1", 1)
    assert leased and leased[0]["id"] == job_id
    assert repo.mark_job_processing(job_id, "light-worker-1") is True
    assert repo.complete_job(job_id) is True
    assert repo.get_job(job_id)["status"] == "done"


def test_retry_job() -> None:
    repo = _FakeRepo()
    job_id = enqueue_repost_single(repo, 10)
    repo.lease_jobs("light", "light-worker-1", 1)
    repo.mark_job_processing(job_id, "light-worker-1")
    assert repo.retry_job(job_id, "tmp", 30) is True
    job = repo.get_job(job_id)
    assert job["status"] == "retry"
    assert job["attempts"] == 1


def test_fail_job() -> None:
    repo = _FakeRepo()
    job_id = enqueue_repost_single(repo, 10)
    repo.lease_jobs("light", "light-worker-1", 1)
    repo.mark_job_processing(job_id, "light-worker-1")
    assert repo.fail_job(job_id, "fatal") is True
    assert repo.get_job(job_id)["status"] == "failed"


def test_light_heavy_queue_split() -> None:
    repo = _FakeRepo()
    repost_id = enqueue_repost_single(repo, 10)
    video_id = enqueue_video_delivery(repo, 10)
    assert repo.get_job(repost_id)["queue"] == "light"
    assert repo.get_job(video_id)["queue"] == "heavy"


def test_job_type_to_queue_mapping() -> None:
    assert JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_SINGLE] == "light"
    assert JOB_QUEUE_BY_TYPE[JOB_TYPE_REPOST_ALBUM] == "light"
    assert JOB_QUEUE_BY_TYPE[JOB_TYPE_VIDEO_DELIVERY] == "heavy"


def test_atomic_lease_baseline_no_double_take() -> None:
    repo = _FakeRepo()
    enqueue_repost_single(repo, 10)
    first = repo.lease_jobs("light", "w1", 1)
    second = repo.lease_jobs("light", "w2", 1)
    assert len(first) == 1
    assert len(second) == 0


def test_enqueue_album_payload() -> None:
    repo = _FakeRepo()
    job_id = enqueue_repost_album(repo, [11, 12], "alb-1")
    payload = repo.get_job(job_id)["payload_json"]
    assert payload["delivery_ids"] == [11, 12]
    assert payload["media_group_id"] == "alb-1"
