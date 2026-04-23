from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.scheduler_runtime import scheduler_tick
from app.sender import SenderService


@dataclass
class _Rule:
    id: int
    mode: str
    interval: int
    schedule_mode: str = "interval"
    is_active: bool = True
    next_run_at: str | None = None
    target_id: str = "dst"
    target_thread_id: int | None = None
    video_caption: str | None = None
    video_caption_entities_json: str | None = None
    video_caption_delivery_mode: str = "auto"


class _Repo:
    def __init__(self, *, base_dt: datetime, mode: str = "video") -> None:
        self.base_dt = base_dt
        self.rule = _Rule(
            id=1,
            mode=mode,
            interval=43200,
            next_run_at=base_dt.isoformat(),
        )
        self.deliveries: list[dict] = []
        self.jobs: dict[int, dict] = {}
        self.next_job_id = 1
        self.touched: list[tuple[int, int]] = []

    # scheduler API
    def get_all_rules(self):
        return [self.rule]

    def take_due_delivery(self, rule_id: int, due_iso: str):
        if self.rule.next_run_at and self.rule.next_run_at > due_iso:
            return None
        if any(d["status"] == "processing" for d in self.deliveries):
            return None
        for row in self.deliveries:
            if row["status"] == "pending":
                row["status"] = "processing"
                return {
                    "delivery_id": row["id"],
                    "message_id": row["message_id"],
                    "source_channel": row["source_channel"],
                    "source_thread_id": row.get("source_thread_id"),
                    "content_json": "{}",
                    "media_group_id": row.get("media_group_id"),
                    "target_id": "dst",
                    "target_thread_id": None,
                    "interval": self.rule.interval,
                }
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
        }
        return job_id

    def get_delivery(self, delivery_id: int):
        for row in self.deliveries:
            if int(row["id"]) == int(delivery_id):
                return {
                    "id": row["id"],
                    "rule_id": self.rule.id,
                    "message_id": row["message_id"],
                    "source_channel": row["source_channel"],
                    "source_thread_id": row.get("source_thread_id"),
                    "target_id": self.rule.target_id,
                    "target_thread_id": self.rule.target_thread_id,
                    "media_group_id": row.get("media_group_id"),
                }
        return None

    def get_rule(self, rule_id: int):
        return self.rule if int(rule_id) == self.rule.id else None

    # sender API
    def mark_delivery_sent(self, delivery_id: int):
        for row in self.deliveries:
            if int(row["id"]) == int(delivery_id):
                row["status"] = "sent"
                return

    def touch_rule_after_send(self, rule_id: int, interval: int) -> None:
        self.touched.append((int(rule_id), int(interval)))
        pending_count = sum(1 for row in self.deliveries if row["status"] == "pending")
        if pending_count <= 0:
            self.rule.next_run_at = None
            return
        self.rule.next_run_at = (self.base_dt + timedelta(seconds=max(int(interval), 1))).isoformat()


class _VideoProcessorStub:
    async def get_video_info(self, path: str, use_cache: bool = False):
        return {"duration": 10}

    async def send_with_retry(self, *args, **kwargs):
        class _Msg:
            message_id = 555

        return _Msg()


def _build_sender(repo: _Repo) -> SenderService:
    sender = SenderService(bot=object(), telethon_client=object(), reaction_clients=[], db=repo)
    sender.video_processor = _VideoProcessorStub()

    async def _skip_reactions(*args, **kwargs):
        return None

    sender._add_reaction_if_possible = _skip_reactions
    return sender


def test_video_interval_with_backlog_blocks_next_delivery_until_due(tmp_path: Path) -> None:
    base_dt = datetime(2026, 4, 23, 8, 0, tzinfo=timezone.utc)
    repo = _Repo(base_dt=base_dt, mode="video")
    repo.deliveries = [
        {"id": 101 + idx, "message_id": 500 + idx, "source_channel": "src", "status": "pending"}
        for idx in range(4)
    ]

    first_tick = asyncio.run(scheduler_tick(repo, now_iso=base_dt.isoformat(), enabled=True))
    assert first_tick["created"] == 1

    sender = _build_sender(repo)
    processed_video = tmp_path / "processed.mp4"
    processed_video.write_bytes(b"ok")

    first_delivery_id = repo.deliveries[0]["id"]
    result = asyncio.run(
        sender.execute_video_send_from_processed_job(
            delivery_id=first_delivery_id,
            rule_id=repo.rule.id,
            source_channel="src",
            target_id="dst",
            processed_video_path=str(processed_video),
            interval=repo.rule.interval,
            schedule_mode="interval",
        )
    )
    assert result["ok"] is True
    assert repo.touched == [(1, 43200)]
    assert repo.rule.next_run_at == (base_dt + timedelta(hours=12)).isoformat()

    one_hour_later = base_dt + timedelta(hours=1)
    second_tick = asyncio.run(scheduler_tick(repo, now_iso=one_hour_later.isoformat(), enabled=True))
    assert second_tick["created"] == 0

    after_due = base_dt + timedelta(hours=12, seconds=1)
    third_tick = asyncio.run(scheduler_tick(repo, now_iso=after_due.isoformat(), enabled=True))
    assert third_tick["created"] == 1


def test_legacy_video_delivery_touches_interval_schedule() -> None:
    repo = _Repo(base_dt=datetime(2026, 4, 23, 8, 0, tzinfo=timezone.utc), mode="video")
    sender = _build_sender(repo)

    async def _deliver_single_video(*args, **kwargs):
        return True

    sender._deliver_single_video = _deliver_single_video
    result = asyncio.run(
        sender.execute_video_delivery_from_job(
            rule_id=1,
            delivery_id=101,
            message_id=501,
            source_channel="src",
            target_id="dst",
            interval=43200,
            schedule_mode="interval",
            mode="video",
        )
    )

    assert result is True
    assert repo.touched == [(1, 43200)]


def test_repost_scheduler_behavior_not_affected() -> None:
    base_dt = datetime(2026, 4, 23, 8, 0, tzinfo=timezone.utc)
    repo = _Repo(base_dt=base_dt, mode="repost")
    repo.deliveries = [{"id": 201, "message_id": 601, "source_channel": "src", "status": "pending"}]

    tick = asyncio.run(scheduler_tick(repo, now_iso=base_dt.isoformat(), enabled=True))
    assert tick["created"] == 1
    assert list(repo.jobs.values())[0]["job_type"] == "repost_single"
