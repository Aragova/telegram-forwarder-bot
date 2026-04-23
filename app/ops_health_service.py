from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.health_service import get_system_health
from app.repository import RepositoryProtocol


@dataclass(slots=True)
class OperationalSnapshot:
    overall_status: str
    system_mode: str
    roles: dict[str, str]
    role_problems: list[str]
    backlog: dict[str, int]
    restart_loop_symptoms: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "system_mode": self.system_mode,
            "roles": self.roles,
            "role_problems": self.role_problems,
            "backlog": self.backlog,
            "restart_loop_symptoms": self.restart_loop_symptoms,
        }


def build_operational_snapshot(repo: RepositoryProtocol) -> OperationalSnapshot:
    health = get_system_health(repo)
    roles = dict(health.get("roles") or {})
    system_mode = str(health.get("system_mode") or "unknown")
    jobs = health.get("jobs") or {}

    role_problems = [role for role, state in roles.items() if state != "ok"]

    backlog = {
        "pending": int(health.get("pending") or 0),
        "processing": int(health.get("processing") or 0),
        "retry": int(jobs.get("retry") or 0),
        "failed": int(jobs.get("failed") or 0),
    }

    restart_loop_symptoms: list[str] = []
    if role_problems and backlog["pending"] == 0 and backlog["processing"] == 0:
        restart_loop_symptoms.append("Есть down-роли без нагрузки: проверь restart loop в systemd")

    overall_status = "healthy"
    if system_mode == "saturated":
        overall_status = "saturated"
    elif system_mode == "degraded" or role_problems:
        overall_status = "degraded"

    return OperationalSnapshot(
        overall_status=overall_status,
        system_mode=system_mode,
        roles=roles,
        role_problems=role_problems,
        backlog=backlog,
        restart_loop_symptoms=restart_loop_symptoms,
    )
