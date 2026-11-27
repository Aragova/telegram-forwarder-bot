from __future__ import annotations

from typing import Awaitable, Callable

RuntimeCallable = Callable[[], Awaitable[None]]

_ALLOWED_ROLES = {"bot", "ui", "scheduler", "worker", "all"}

PRODUCTION_SPLIT_ROLES: tuple[str, ...] = ("bot", "scheduler", "worker")
LEGACY_COMPAT_ROLE = "all"


def normalize_runtime_role(role: str | None) -> str:
    normalized = (role or "all").strip().lower()
    if normalized == "ui":
        return "bot"
    if normalized not in _ALLOWED_ROLES:
        return "all"
    return normalized


class RuntimeOwnership:
    """Bind a process to one role before any runtime resources are started."""

    def __init__(self) -> None:
        self._role: str | None = None
        self._closing = False

    def bind(self, role: str) -> None:
        if role not in _ALLOWED_ROLES:
            raise ValueError(f"Неизвестная runtime-роль: {role}")
        normalized = normalize_runtime_role(role)
        if self._closing or (self._role is not None and self._role != normalized):
            raise RuntimeError("Нельзя менять runtime-роль запущенного процесса")
        self._role = normalized

    @property
    def can_start_workers(self) -> bool:
        return not self._closing and self._role in {"worker", "all"}

    @property
    def can_poll(self) -> bool:
        return not self._closing and self._role in {"bot", "all"}

    def begin_shutdown(self) -> None:
        self._closing = True


async def run_role(
    role: str,
    *,
    run_bot: RuntimeCallable,
    run_scheduler: RuntimeCallable,
    run_worker: RuntimeCallable,
    run_all: RuntimeCallable,
) -> None:
    normalized = normalize_runtime_role(role)

    if normalized == "bot":
        await run_bot()
        return

    if normalized == "scheduler":
        await run_scheduler()
        return

    if normalized == "worker":
        await run_worker()
        return

    await run_all()
