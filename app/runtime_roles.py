from __future__ import annotations

from datetime import datetime, timezone
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


class ForwardingControlUnavailable(RuntimeError):
    """Global start/stop cannot control other processes in split mode."""


class RuntimeOwnership:
    """Bind a process to one role before any runtime resources are started."""

    def __init__(self) -> None:
        self._role: str | None = None
        self._closing = False
        self._started_at: datetime | None = None

    def bind(self, role: str) -> None:
        if role not in _ALLOWED_ROLES:
            raise ValueError(f"Неизвестная runtime-роль: {role}")
        normalized = normalize_runtime_role(role)
        if self._closing or (self._role is not None and self._role != normalized):
            raise RuntimeError("Нельзя менять runtime-роль запущенного процесса")
        if self._role is None:
            # Telegram message dates have second precision.
            self._started_at = datetime.now(timezone.utc).replace(microsecond=0)
        self._role = normalized

    @property
    def can_start_workers(self) -> bool:
        return not self._closing and self._role in {"worker", "all"}

    @property
    def can_poll(self) -> bool:
        return not self._closing and self._role in {"bot", "all"}

    @property
    def can_control_forwarding(self) -> bool:
        return not self._closing and self._role == "all"

    def validate_forwarding_command(self, sent_at: datetime) -> None:
        if not self.can_control_forwarding:
            raise ForwardingControlUnavailable("Глобальное управление пересылкой доступно только в режиме all.")
        if self._started_at is None or sent_at is None or sent_at.tzinfo is None or sent_at < self._started_at:
            raise ForwardingControlUnavailable("Команда устарела. Нажмите кнопку пересылки повторно.")

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
