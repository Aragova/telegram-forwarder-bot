"""Lease revocation across delivery stages; local cancellation is best effort."""
from __future__ import annotations

import asyncio
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any

DELIVERY_JOB_TYPES = frozenset({
    "repost_single", "repost_album", "video_download", "video_process",
    "video_send", "video_delivery",
})


class DeliveryJobRevoked(asyncio.CancelledError):
    """A revoked owner must not enter fallback/retry or send another RPC."""


@dataclass(frozen=True)
class DeliveryJob:
    repository: Any
    rule_id: int
    job_id: int
    worker_id: str
    task: asyncio.Task


_current: ContextVar[DeliveryJob | None] = ContextVar("delivery_job_owner", default=None)
_local_tasks: dict[tuple[int, int], asyncio.Task] = {}


def current_delivery_job() -> DeliveryJob | None:
    return _current.get()


def enter_delivery_job(repo, job_type: str, rule_id: int, job_id: int,
                       worker_id: str) -> Token | None:
    task = asyncio.current_task()
    if job_type not in DELIVERY_JOB_TYPES or rule_id <= 0 or task is None:
        return None
    scope = DeliveryJob(repo, rule_id, job_id, worker_id, task)
    _local_tasks[(rule_id, job_id)] = task
    return _current.set(scope)


def leave_delivery_job(token: Token | None) -> None:
    if token is None:
        return
    scope = _current.get()
    if scope is not None:
        key = (scope.rule_id, scope.job_id)
        if _local_tasks.get(key) is scope.task:
            _local_tasks.pop(key, None)
    _current.reset(token)


def cancel_local_rule_delivery_jobs(rule_id: int) -> int:
    """Called only after PostgreSQL has committed lease revocation."""
    tasks = [task for (rid, _), task in tuple(_local_tasks.items()) if rid == rule_id]
    return sum(task.cancel() for task in tasks if not task.done())


async def assert_delivery_job_current() -> None:
    scope = _current.get()
    if scope is None:
        return
    check = getattr(scope.repository, "assert_current_delivery_job", None)
    if callable(check):
        await asyncio.to_thread(check)
