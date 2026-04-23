from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int, *, min_value: int = 1) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(min_value, value)


@dataclass(slots=True, frozen=True)
class WorkerResourcePolicy:
    light_max_concurrency: int = 4
    heavy_max_concurrency: int = 2

    heavy_download_max_concurrency: int = 1
    heavy_process_max_concurrency: int = 1
    heavy_send_max_concurrency: int = 1

    lease_batch_size_light: int = 4
    lease_batch_size_heavy: int = 2

    backlog_soft_limit_light: int = 200
    backlog_soft_limit_heavy: int = 100
    backlog_hard_limit_heavy: int = 250

    max_heavy_retries_in_flight: int = 20

    graceful_shutdown_timeout_sec: int = 20
    throughput_window_sec: int = 300
    retry_jitter_min_sec: int = 0
    retry_jitter_max_sec: int = 0

    @classmethod
    def from_env(cls) -> "WorkerResourcePolicy":
        return cls(
            light_max_concurrency=_env_int("LIGHT_MAX_CONCURRENCY", 4),
            heavy_max_concurrency=_env_int("HEAVY_MAX_CONCURRENCY", 2),
            heavy_download_max_concurrency=_env_int("HEAVY_DOWNLOAD_MAX_CONCURRENCY", 1),
            heavy_process_max_concurrency=_env_int("HEAVY_PROCESS_MAX_CONCURRENCY", 1),
            heavy_send_max_concurrency=_env_int("HEAVY_SEND_MAX_CONCURRENCY", 1),
            lease_batch_size_light=_env_int("LEASE_BATCH_SIZE_LIGHT", 4),
            lease_batch_size_heavy=_env_int("LEASE_BATCH_SIZE_HEAVY", 2),
            backlog_soft_limit_light=_env_int("BACKLOG_SOFT_LIMIT_LIGHT", 200),
            backlog_soft_limit_heavy=_env_int("BACKLOG_SOFT_LIMIT_HEAVY", 100),
            backlog_hard_limit_heavy=_env_int("BACKLOG_HARD_LIMIT_HEAVY", 250),
            max_heavy_retries_in_flight=_env_int("MAX_HEAVY_RETRIES_IN_FLIGHT", 20),
            graceful_shutdown_timeout_sec=_env_int("GRACEFUL_SHUTDOWN_TIMEOUT_SEC", 20),
            throughput_window_sec=_env_int("THROUGHPUT_WINDOW_SEC", 300),
            retry_jitter_min_sec=_env_int("HEAVY_RETRY_JITTER_MIN_SEC", 0, min_value=0),
            retry_jitter_max_sec=_env_int("HEAVY_RETRY_JITTER_MAX_SEC", 0, min_value=0),
        )


POLICY = WorkerResourcePolicy.from_env()
