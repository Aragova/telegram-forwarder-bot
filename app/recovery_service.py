from __future__ import annotations

import logging
from typing import Any

from app.subscription_service import SubscriptionService

logger = logging.getLogger("forwarder.recovery")


class RecoveryService:
    def __init__(self, repo, subscription_service: SubscriptionService | None = None) -> None:
        self._repo = repo
        self._subscription_service = subscription_service or SubscriptionService(repo)

    def build_recovery_summary(self, tenant_id: int) -> dict[str, Any]:
        if not hasattr(self._repo, "get_recoverable_summary_for_tenant"):
            return {
                "active_rules_count": 0,
                "pending_deliveries_count": 0,
                "failed_limit_jobs_count": 0,
                "failed_limit_video_jobs_count": 0,
                "last_blocked_events": [],
            }
        summary = self._repo.get_recoverable_summary_for_tenant(int(tenant_id)) or {}
        return {
            "active_rules_count": int(summary.get("active_rules_count") or 0),
            "pending_deliveries_count": int(summary.get("pending_deliveries_count") or 0),
            "failed_limit_jobs_count": int(summary.get("failed_limit_jobs_count") or 0),
            "failed_limit_video_jobs_count": int(summary.get("failed_limit_video_jobs_count") or 0),
            "last_blocked_events": list(summary.get("last_blocked_events") or []),
        }

    def can_recover(self, tenant_id: int) -> tuple[bool, str | None]:
        sub = self._subscription_service.get_active_subscription(int(tenant_id)) or {}
        status = str(sub.get("status") or "expired").strip().lower()
        plan_name = str(sub.get("plan_name") or "").strip().upper()
        if plan_name == "OWNER":
            return True, None
        if status in {"active", "trial", "grace"}:
            return True, None
        return False, "Подписка ещё не активна"

    def recover_after_payment(self, tenant_id: int, triggered_by_user_id: int) -> dict[str, Any]:
        can_recover, reason = self.can_recover(int(tenant_id))
        if not can_recover:
            logger.info("recovery запрещён из-за неактивной подписки tenant_id=%s", tenant_id)
            return {
                "ok": False,
                "reason": reason or "Подписка ещё не активна",
                "checked_rules": 0,
                "restored_jobs": 0,
                "pending_deliveries": 0,
                "limit_events_found": 0,
                "already_recovered": True,
            }

        logger.info("recovery запущен tenant_id=%s user_id=%s", tenant_id, triggered_by_user_id)
        summary = self.build_recovery_summary(int(tenant_id))
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "post_payment_recovery_started",
                event_source="recovery_service",
                metadata={"tenant_id": int(tenant_id), "triggered_by_user_id": int(triggered_by_user_id), "summary": summary},
            )

        restored_jobs = self._repo.recover_blocked_jobs_for_tenant(int(tenant_id)) if hasattr(self._repo, "recover_blocked_jobs_for_tenant") else 0
        checked_rules = self._repo.recover_pending_deliveries_for_tenant(int(tenant_id)) if hasattr(self._repo, "recover_pending_deliveries_for_tenant") else 0

        result = {
            "ok": True,
            "reason": None,
            "checked_rules": int(checked_rules or 0),
            "restored_jobs": int(restored_jobs or 0),
            "pending_deliveries": int(summary.get("pending_deliveries_count") or 0),
            "limit_events_found": len(summary.get("last_blocked_events") or []),
            "already_recovered": int(restored_jobs or 0) == 0,
        }
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "post_payment_recovery_completed",
                event_source="recovery_service",
                metadata={"tenant_id": int(tenant_id), "triggered_by_user_id": int(triggered_by_user_id), "result": result},
            )

        logger.info(
            "recovery завершён tenant_id=%s restored_jobs=%s pending_deliveries=%s",
            tenant_id,
            result["restored_jobs"],
            result["pending_deliveries"],
        )
        return result
