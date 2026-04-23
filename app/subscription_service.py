from __future__ import annotations

from datetime import datetime, timezone


class SubscriptionService:
    def __init__(self, repo) -> None:
        self._repo = repo

    def get_active_subscription(self, tenant_id: int):
        if not hasattr(self._repo, "get_active_subscription"):
            return {
                "tenant_id": int(tenant_id),
                "status": "active",
                "plan_name": "LEGACY",
                "max_rules": 0,
                "max_jobs_per_day": 0,
                "max_video_per_day": 0,
            }
        return self._repo.get_active_subscription(int(tenant_id))

    def assign_plan(self, tenant_id: int, plan_name: str, *, status: str = "active", expires_at: str | None = None) -> int | None:
        if not hasattr(self._repo, "get_plan_by_name"):
            return None
        plan = self._repo.get_plan_by_name(plan_name)
        if not plan:
            return None
        return self._repo.assign_subscription(int(tenant_id), int(plan["id"]), status=status, expires_at=expires_at)

    def is_subscription_active(self, tenant_id: int) -> bool:
        sub = self.get_active_subscription(tenant_id)
        if not sub:
            return False
        if str(sub.get("status") or "") not in {"active", "trial"}:
            return False
        expires_at = sub.get("expires_at")
        if not expires_at:
            return True
        try:
            exp = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return exp >= datetime.now(timezone.utc)
        except Exception:
            return True

    def expire_subscription(self, tenant_id: int) -> bool:
        if not hasattr(self._repo, "expire_subscription"):
            return False
        return bool(self._repo.expire_subscription(int(tenant_id)))
