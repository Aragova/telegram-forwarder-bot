from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.repository import RepositoryProtocol


def update_heartbeat(repo: RepositoryProtocol, role: str) -> None:
    repo.update_runtime_heartbeat(role)


def get_system_health(repo: RepositoryProtocol) -> dict[str, Any]:
    rows = repo.get_runtime_heartbeats()
    now = datetime.now(timezone.utc)

    status = {
        "bot": "down",
        "scheduler": "down",
        "worker": "down",
    }

    for row in rows:
        role = str(row.get("role") or "")
        if role not in status:
            continue

        last_seen_at = row.get("last_seen_at")
        if not isinstance(last_seen_at, datetime):
            continue

        if last_seen_at.tzinfo is None:
            last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)

        delta = (now - last_seen_at).total_seconds()
        if delta < 15:
            status[role] = "ok"

    queue = repo.get_queue_stats()
    errors = repo.count_recent_errors(minutes=5)

    return {
        "roles": status,
        "pending": int(queue.get("pending") or 0),
        "processing": int(queue.get("processing") or 0),
        "errors": int(errors),
    }
