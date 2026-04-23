from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from app.invoice_service import InvoiceService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService


class BillingService:
    def __init__(self, repo) -> None:
        self._repo = repo
        self._subscription_service = SubscriptionService(repo)
        self._usage_service = UsageService(repo)
        self._invoice_service = InvoiceService(repo)

    def get_recent_billing_events(self, tenant_id: int, limit: int = 10) -> list[dict[str, Any]]:
        if not hasattr(self._repo, "get_billing_events"):
            return []
        return self._repo.get_billing_events(int(tenant_id), limit=int(limit))

    def build_billable_usage_snapshot(self, tenant_id: int, date_from: str, date_to: str) -> dict[str, Any]:
        snapshot = self._usage_service.build_usage_snapshot(int(tenant_id), str(date_from), str(date_to))
        overage_candidates = list(snapshot.get("overage_candidates") or [])
        if overage_candidates and hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "usage_threshold_reached",
                event_source="billing_service",
                metadata={
                    "date_from": str(date_from),
                    "date_to": str(date_to),
                    "overage_candidates": overage_candidates,
                },
            )
        return snapshot

    def get_tenant_financial_snapshot(self, tenant_id: int) -> dict[str, Any]:
        today = date.today().isoformat()
        now = datetime.now(timezone.utc)
        period_start = now.replace(day=1).date().isoformat()
        current_sub = self._subscription_service.get_active_subscription(int(tenant_id))
        billing_period_snapshot = self.build_billable_usage_snapshot(int(tenant_id), period_start, today)
        today_snapshot = self._usage_service.get_today_usage(int(tenant_id))
        events = self.get_recent_billing_events(int(tenant_id), limit=5)
        open_invoices = self._repo.count_open_invoices(int(tenant_id)) if hasattr(self._repo, "count_open_invoices") else 0
        last_invoice = self._repo.get_last_invoice(int(tenant_id)) if hasattr(self._repo, "get_last_invoice") else None
        return {
            "tenant_id": int(tenant_id),
            "tenant": self._repo.get_tenant_by_id(int(tenant_id)) if hasattr(self._repo, "get_tenant_by_id") else {"id": int(tenant_id)},
            "current_plan": (current_sub or {}).get("plan_name"),
            "subscription_status": (current_sub or {}).get("status"),
            "usage_today": today_snapshot,
            "usage_billing_period": billing_period_snapshot,
            "last_billing_events": events,
            "open_invoices_count": int(open_invoices or 0),
            "last_invoice_summary": last_invoice,
            "over_limit_flags": billing_period_snapshot.get("over_limit_indicators") or {},
            "overage_flags": billing_period_snapshot.get("overage_candidates") or [],
        }

    @property
    def subscription_service(self) -> SubscriptionService:
        return self._subscription_service

    @property
    def invoice_service(self) -> InvoiceService:
        return self._invoice_service
