from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.health_service import get_system_health, update_heartbeat


class _FakeRepo:
    def __init__(self) -> None:
        self.heartbeats: dict[str, datetime] = {}

    def update_runtime_heartbeat(self, role: str) -> None:
        self.heartbeats[str(role)] = datetime.now(timezone.utc)

    def get_runtime_heartbeats(self) -> list[dict[str, object]]:
        return [
            {"role": role, "last_seen_at": seen_at}
            for role, seen_at in self.heartbeats.items()
        ]

    def get_queue_stats(self):
        return {
            "pending": 3,
            "processing": 1,
        }

    def count_recent_errors(self, minutes: int = 5) -> int:
        return 2

    def get_job_status_counts(self) -> dict[str, int]:
        return {
            "pending": 1,
            "leased": 0,
            "processing": 0,
            "retry": 0,
            "failed": 0,
        }

    def get_expired_leased_jobs(self, limit: int = 100):
        return []

    def get_stuck_processing_jobs(self, stuck_seconds: int = 600, limit: int = 100):
        return []


def test_update_heartbeat_updates_timestamp() -> None:
    repo = _FakeRepo()

    update_heartbeat(repo, "bot")

    assert "bot" in repo.heartbeats


def test_system_health_is_ok_when_heartbeat_is_fresh() -> None:
    repo = _FakeRepo()
    repo.heartbeats["worker"] = datetime.now(timezone.utc) - timedelta(seconds=10)

    health = get_system_health(repo)

    assert health["roles"]["worker"] == "ok"


def test_system_health_is_down_when_heartbeat_is_stale() -> None:
    repo = _FakeRepo()
    repo.heartbeats["scheduler"] = datetime.now(timezone.utc) - timedelta(seconds=20)

    health = get_system_health(repo)

    assert health["roles"]["scheduler"] == "down"
