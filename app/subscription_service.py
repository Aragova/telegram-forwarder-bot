from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

SUBSCRIPTION_TRANSITIONS: dict[str, set[str]] = {
    "trial": {"active", "expired"},
    "active": {"grace", "canceled"},
    "grace": {"active", "expired"},
    "canceled": {"active"},
    "expired": {"active"},
}


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
        sub_id = self._repo.assign_subscription(int(tenant_id), int(plan["id"]), status=status, expires_at=expires_at)
        if sub_id and hasattr(self._repo, "create_billing_event"):
            event_type = "trial_started" if status == "trial" else "subscription_started"
            self._repo.create_billing_event(
                int(tenant_id),
                event_type,
                event_source="subscription_service",
                metadata={"subscription_id": int(sub_id), "plan_name": str(plan_name).upper()},
            )
        return sub_id

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

    def transition_status(
        self,
        tenant_id: int,
        new_status: str,
        *,
        changed_by: str = "system",
        reason: str = "",
    ) -> bool:
        sub = self.get_active_subscription(int(tenant_id))
        if not sub:
            return False
        old_status = str(sub.get("status") or "expired")
        target = str(new_status or "").strip().lower()
        if target not in SUBSCRIPTION_TRANSITIONS.get(old_status, set()):
            return False
        sub_id = int(sub.get("id") or 0)
        if not sub_id or not hasattr(self._repo, "set_subscription_status"):
            return False
        if not self._repo.set_subscription_status(sub_id, target):
            return False
        self._append_subscription_history(
            tenant_id=int(tenant_id),
            old_plan_id=sub.get("plan_id"),
            new_plan_id=sub.get("plan_id"),
            old_status=old_status,
            new_status=target,
            changed_by=changed_by,
            reason=reason or f"transition:{old_status}->{target}",
            effective_from=datetime.now(timezone.utc).isoformat(),
        )
        self._write_lifecycle_event(int(tenant_id), old_status, target, {"subscription_id": sub_id})
        return True

    def change_plan(
        self,
        tenant_id: int,
        new_plan_name: str,
        *,
        changed_by: str = "system",
        reason: str = "",
        effective_mode: str = "immediate",
    ) -> bool:
        sub = self.get_active_subscription(int(tenant_id))
        if not sub or not hasattr(self._repo, "get_plan_by_name"):
            return False
        current_plan_id = int(sub.get("plan_id") or 0)
        current_plan_name = str(sub.get("plan_name") or "")
        new_plan = self._repo.get_plan_by_name(new_plan_name)
        if not new_plan:
            return False
        new_plan_id = int(new_plan.get("id") or 0)
        if not new_plan_id or new_plan_id == current_plan_id:
            return True
        now_iso = datetime.now(timezone.utc).isoformat()
        if effective_mode == "period_end":
            if not hasattr(self._repo, "set_subscription_pending_plan"):
                return False
            if not self._repo.set_subscription_pending_plan(int(sub["id"]), new_plan_id):
                return False
            self._append_subscription_history(
                tenant_id=int(tenant_id),
                old_plan_id=current_plan_id,
                new_plan_id=new_plan_id,
                old_status=sub.get("status"),
                new_status=sub.get("status"),
                changed_by=changed_by,
                reason=reason or "downgrade_deferred",
                effective_from=str(sub.get("expires_at") or now_iso),
            )
            if hasattr(self._repo, "create_billing_event"):
                self._repo.create_billing_event(
                    int(tenant_id),
                    "plan_downgraded",
                    event_source="subscription_service",
                    metadata={"from": current_plan_name, "to": str(new_plan_name).upper(), "effective_mode": "period_end"},
                )
            return True

        if not hasattr(self._repo, "replace_subscription_plan"):
            return False
        if not self._repo.replace_subscription_plan(int(sub["id"]), new_plan_id):
            return False
        upgrade = self._is_upgrade(sub, new_plan)
        self._append_subscription_history(
            tenant_id=int(tenant_id),
            old_plan_id=current_plan_id,
            new_plan_id=new_plan_id,
            old_status=sub.get("status"),
            new_status=sub.get("status"),
            changed_by=changed_by,
            reason=reason or ("upgrade" if upgrade else "downgrade"),
            effective_from=now_iso,
        )
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "plan_upgraded" if upgrade else "plan_downgraded",
                event_source="subscription_service",
                metadata={"from": current_plan_name, "to": str(new_plan_name).upper(), "effective_mode": "immediate"},
            )
        return True

    def start_grace_period(
        self,
        tenant_id: int,
        *,
        days: int = 3,
        changed_by: str = "system",
        reason: str = "grace_started",
    ) -> bool:
        sub = self.get_active_subscription(int(tenant_id))
        if not sub or str(sub.get("status") or "") != "active":
            return False
        now = datetime.now(timezone.utc)
        grace_end = now + timedelta(days=max(int(days), 1))
        sub_id = int(sub.get("id") or 0)
        if not sub_id or not hasattr(self._repo, "set_subscription_grace_window"):
            return False
        if not self._repo.set_subscription_grace_window(sub_id, now.isoformat(), grace_end.isoformat()):
            return False
        return self.transition_status(int(tenant_id), "grace", changed_by=changed_by, reason=reason)

    def is_in_grace(self, tenant_id: int) -> bool:
        sub = self.get_active_subscription(int(tenant_id))
        if not sub or str(sub.get("status") or "") != "grace":
            return False
        grace_ends_at = sub.get("grace_ends_at")
        if not grace_ends_at:
            return True
        try:
            dt = datetime.fromisoformat(str(grace_ends_at).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt >= datetime.now(timezone.utc)
        except Exception:
            return True

    def end_grace_period(self, tenant_id: int, *, restore_active: bool, changed_by: str = "system") -> bool:
        return self.transition_status(
            int(tenant_id),
            "active" if restore_active else "expired",
            changed_by=changed_by,
            reason="grace_ended_restore" if restore_active else "grace_ended_expire",
        )

    def _append_subscription_history(
        self,
        *,
        tenant_id: int,
        old_plan_id: Any,
        new_plan_id: Any,
        old_status: Any,
        new_status: Any,
        changed_by: str,
        reason: str,
        effective_from: str,
        effective_to: str | None = None,
    ) -> None:
        if not hasattr(self._repo, "add_subscription_history"):
            return
        self._repo.add_subscription_history(
            tenant_id=int(tenant_id),
            old_plan_id=(int(old_plan_id) if old_plan_id is not None else None),
            new_plan_id=(int(new_plan_id) if new_plan_id is not None else None),
            old_status=str(old_status) if old_status is not None else None,
            new_status=str(new_status) if new_status is not None else None,
            changed_by=changed_by,
            reason=reason,
            effective_from=effective_from,
            effective_to=effective_to,
        )

    def _write_lifecycle_event(self, tenant_id: int, old_status: str, new_status: str, metadata: dict[str, Any]) -> None:
        if not hasattr(self._repo, "create_billing_event"):
            return
        event_map = {
            ("trial", "active"): "subscription_started",
            ("trial", "expired"): "subscription_expired",
            ("active", "grace"): "grace_started",
            ("grace", "active"): "grace_ended",
            ("grace", "expired"): "subscription_expired",
            ("active", "canceled"): "subscription_expired",
            ("canceled", "active"): "subscription_started",
            ("expired", "active"): "subscription_extended",
        }
        event_type = event_map.get((old_status, new_status))
        if event_type:
            self._repo.create_billing_event(
                int(tenant_id),
                event_type,
                event_source="subscription_service",
                metadata=metadata,
            )

    @staticmethod
    def _is_upgrade(current_sub: dict[str, Any], new_plan: dict[str, Any]) -> bool:
        current_priority = int(current_sub.get("priority_level") or 0)
        new_priority = int(new_plan.get("priority_level") or 0)
        if new_priority != current_priority:
            return new_priority > current_priority
        return float(new_plan.get("price") or 0) >= float(current_sub.get("price") or 0)
