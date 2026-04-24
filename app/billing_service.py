from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.invoice_service import InvoiceService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService

logger = logging.getLogger("forwarder.billing")

OVERAGE_PRICING: dict[str, float] = {
    "extra_jobs": 0.01,
    "extra_video": 0.10,
    "storage_overage": 0.001,
}


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

    def get_current_billing_period(self, tenant_id: int) -> dict[str, Any] | None:
        sub = self._subscription_service.get_active_subscription(int(tenant_id))
        if not sub:
            return None
        ensured = self.ensure_billing_period(sub)
        return {
            "tenant_id": int(tenant_id),
            "subscription_id": int(ensured.get("id") or 0),
            "period_start": str(ensured.get("current_period_start")),
            "period_end": str(ensured.get("current_period_end")),
        }

    def ensure_billing_period(self, subscription: dict[str, Any]) -> dict[str, Any]:
        period_start = subscription.get("current_period_start")
        period_end = subscription.get("current_period_end")
        if period_start and period_end:
            return subscription

        start, end = self._derive_default_period_bounds(subscription)
        sub_id = int(subscription.get("id") or 0)
        if sub_id and hasattr(self._repo, "update_billing_period"):
            self._repo.update_billing_period(sub_id, start, end)
        patched = dict(subscription)
        patched["current_period_start"] = start
        patched["current_period_end"] = end
        return patched

    def advance_billing_period(self, subscription_id: int) -> dict[str, str] | None:
        if not hasattr(self._repo, "get_subscription_by_id"):
            return None
        sub = self._repo.get_subscription_by_id(int(subscription_id))
        if not sub:
            return None
        ensured = self.ensure_billing_period(sub)
        current_end = self._parse_iso_date(ensured.get("current_period_end"))
        next_start = current_end + timedelta(days=1)
        next_end = self._end_of_month(next_start)
        if hasattr(self._repo, "update_billing_period"):
            self._repo.update_billing_period(int(subscription_id), next_start.isoformat(), next_end.isoformat())
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(sub.get("tenant_id") or 0),
                "billing_period_advanced",
                event_source="billing_service",
                metadata={
                    "tenant_id": int(sub.get("tenant_id") or 0),
                    "subscription_id": int(subscription_id),
                    "period_start": next_start.isoformat(),
                    "period_end": next_end.isoformat(),
                    "plan_name": str(sub.get("plan_name") or "UNKNOWN"),
                    "total": 0,
                    "invoice_id": None,
                },
            )
        return {"period_start": next_start.isoformat(), "period_end": next_end.isoformat()}

    def get_period_key(self, tenant_id: int, period_start: str, period_end: str) -> str:
        return f"{int(tenant_id)}:{period_start}:{period_end}"

    def build_billing_usage_snapshot(self, tenant_id: int, period_start: str, period_end: str) -> dict[str, Any]:
        if hasattr(self._repo, "build_billing_usage_data"):
            usage = self._repo.build_billing_usage_data(int(tenant_id), str(period_start), str(period_end))
        else:
            usage = self._usage_service.build_usage_snapshot(int(tenant_id), str(period_start), str(period_end))
        usage = dict(usage)
        usage["tenant_id"] = int(tenant_id)
        usage["period_start"] = str(period_start)
        usage["period_end"] = str(period_end)
        return usage

    def calculate_overage_items(self, plan: dict[str, Any], usage_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if self._is_owner_plan(plan):
            return items

        jobs = int(usage_snapshot.get("jobs_count") or 0)
        video = int(usage_snapshot.get("video_count") or 0)
        storage = int(usage_snapshot.get("storage_used_mb") or 0)
        jobs_limit = int(plan.get("max_jobs_per_day") or 0)
        video_limit = int(plan.get("max_video_per_day") or 0)
        storage_limit = int(plan.get("max_storage_mb") or 0)

        if jobs_limit > 0 and jobs > jobs_limit:
            qty = jobs - jobs_limit
            items.append(self._make_item("extra_jobs", "Превышение лимита задач", qty, OVERAGE_PRICING["extra_jobs"]))
        if video_limit > 0 and video > video_limit:
            qty = video - video_limit
            items.append(self._make_item("extra_video", "Превышение лимита видео", qty, OVERAGE_PRICING["extra_video"]))
        if storage_limit > 0 and storage > storage_limit:
            qty = storage - storage_limit
            items.append(self._make_item("storage_overage", "Превышение лимита хранилища (МБ)", qty, OVERAGE_PRICING["storage_overage"]))
        return items

    def build_billable_usage_snapshot(self, tenant_id: int, date_from: str, date_to: str) -> dict[str, Any]:
        snapshot = self._usage_service.build_usage_snapshot(int(tenant_id), str(date_from), str(date_to))
        overage_candidates = list(snapshot.get("overage_candidates") or [])
        if overage_candidates and hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "usage_threshold_reached",
                event_source="billing_service",
                metadata={
                    "tenant_id": int(tenant_id),
                    "date_from": str(date_from),
                    "date_to": str(date_to),
                    "overage_candidates": overage_candidates,
                },
            )
        return snapshot

    def generate_invoice_for_current_period(self, tenant_id: int, finalize: bool = False) -> dict[str, Any]:
        tenant_id = int(tenant_id)
        sub = self._subscription_service.get_active_subscription(tenant_id)
        if not sub:
            return {"tenant_id": tenant_id, "invoice": None, "items": [], "skipped": True, "reason": "no_active_subscription"}
        sub = self.ensure_billing_period(sub)
        period_start = str(sub.get("current_period_start"))
        period_end = str(sub.get("current_period_end"))
        existing = self._repo.get_invoice_for_period(tenant_id, period_start, period_end) if hasattr(self._repo, "get_invoice_for_period") else None
        if existing:
            if finalize and str(existing.get("status")) == "draft":
                self._invoice_service.finalize_invoice(int(existing["id"]))
                existing = self._repo.get_invoice(int(existing["id"])) if hasattr(self._repo, "get_invoice") else existing
            if hasattr(self._repo, "create_billing_event"):
                self._repo.create_billing_event(
                    tenant_id,
                    "invoice_generation_skipped_existing",
                    event_source="billing_service",
                    metadata={
                        "tenant_id": tenant_id,
                        "invoice_id": int(existing.get("id") or 0),
                        "period_start": period_start,
                        "period_end": period_end,
                        "total": float(existing.get("total") or 0),
                        "plan_name": str(sub.get("plan_name") or "UNKNOWN"),
                    },
                )
            logger.info("Пропускаем генерацию счёта: уже существует tenant=%s period=%s..%s", tenant_id, period_start, period_end)
            return {"tenant_id": tenant_id, "invoice": existing, "items": self._safe_list_items(existing), "skipped": True}

        if self._is_owner_plan(sub):
            return {
                "tenant_id": tenant_id,
                "invoice": None,
                "items": [],
                "skipped": True,
                "reason": "owner_plan_skip",
                "period_start": period_start,
                "period_end": period_end,
                "total": 0.0,
            }

        usage = self.build_billing_usage_snapshot(tenant_id, period_start, period_end)
        base_item = {
            "item_type": "base_plan",
            "description": f"Тариф {sub.get('plan_name')}",
            "quantity": 1,
            "unit_price": float(sub.get("price") or 0),
            "amount": round(float(sub.get("price") or 0), 2),
            "metadata": {"plan_name": str(sub.get("plan_name") or "UNKNOWN")},
        }
        overage_items = self.calculate_overage_items(sub, usage)
        invoice_id = self._repo.create_invoice(
            tenant_id=tenant_id,
            subscription_id=int(sub.get("id") or 0),
            period_start=period_start,
            period_end=period_end,
            status="draft",
            currency="USD",
            due_at=None,
        )
        if not invoice_id:
            return {"tenant_id": tenant_id, "invoice": None, "items": [], "skipped": True, "reason": "create_invoice_failed"}

        self._repo.add_invoice_item(
            int(invoice_id),
            item_type=base_item["item_type"],
            description=base_item["description"],
            quantity=int(base_item["quantity"]),
            unit_price=float(base_item["unit_price"]),
            amount=float(base_item["amount"]),
            metadata=base_item["metadata"],
        )
        for item in overage_items:
            self._repo.add_invoice_item(
                int(invoice_id),
                item_type=item["item_type"],
                description=item["description"],
                quantity=int(item["quantity"]),
                unit_price=float(item["unit_price"]),
                amount=float(item["amount"]),
                metadata=item.get("metadata") or {},
            )
        self._repo.recalculate_invoice_totals(int(invoice_id))
        invoice = self._repo.get_invoice(int(invoice_id)) if hasattr(self._repo, "get_invoice") else {"id": int(invoice_id), "status": "draft", "total": 0}
        items = self._repo.list_invoice_items(int(invoice_id)) if hasattr(self._repo, "list_invoice_items") else []
        event_meta = {
            "tenant_id": tenant_id,
            "invoice_id": int(invoice_id),
            "period_start": period_start,
            "period_end": period_end,
            "total": float(invoice.get("total") or 0),
            "plan_name": str(sub.get("plan_name") or "UNKNOWN"),
        }
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(tenant_id, "invoice_created", event_source="billing_service", metadata=event_meta)
            if overage_items:
                self._repo.create_billing_event(tenant_id, "overage_detected", event_source="billing_service", metadata=event_meta)
        if finalize:
            self._invoice_service.finalize_invoice(int(invoice_id))
            invoice = self._repo.get_invoice(int(invoice_id)) if hasattr(self._repo, "get_invoice") else invoice
            if hasattr(self._repo, "create_billing_event"):
                self._repo.create_billing_event(tenant_id, "invoice_finalized", event_source="billing_service", metadata=event_meta)
        return {"tenant_id": tenant_id, "invoice": invoice, "items": items, "usage": usage, "skipped": False}

    def close_current_billing_period(self, tenant_id: int) -> dict[str, Any]:
        summary = self.generate_invoice_for_current_period(int(tenant_id), finalize=True)
        sub = self._subscription_service.get_active_subscription(int(tenant_id))
        if not sub:
            return summary
        sub = self.ensure_billing_period(sub)
        next_period = self.advance_billing_period(int(sub.get("id") or 0))
        invoice = summary.get("invoice") or {}
        if hasattr(self._repo, "create_billing_event"):
            self._repo.create_billing_event(
                int(tenant_id),
                "billing_period_closed",
                event_source="billing_service",
                metadata={
                    "tenant_id": int(tenant_id),
                    "invoice_id": invoice.get("id"),
                    "period_start": str(sub.get("current_period_start")),
                    "period_end": str(sub.get("current_period_end")),
                    "total": float(invoice.get("total") or 0),
                    "plan_name": str(sub.get("plan_name") or "UNKNOWN"),
                },
            )
        summary["next_period"] = next_period
        return summary

    def billing_cycle_tick(self, *, now: datetime | None = None, limit: int = 100) -> dict[str, Any]:
        current = now or datetime.now(timezone.utc)
        if not hasattr(self._repo, "get_subscriptions_due_for_billing"):
            return {"closed": 0, "errors": 0}
        due_rows = self._repo.get_subscriptions_due_for_billing(current.isoformat(), limit=int(limit))
        closed = 0
        errors = 0
        for row in due_rows:
            tenant_id = int(row.get("tenant_id") or 0)
            if tenant_id <= 0:
                continue
            try:
                self.close_current_billing_period(tenant_id)
                closed += 1
            except Exception as exc:
                errors += 1
                logger.warning("Ошибка billing_cycle_tick tenant=%s: %s", tenant_id, exc)
                if hasattr(self._repo, "create_billing_event"):
                    self._repo.create_billing_event(
                        tenant_id,
                        "invoice_generation_error",
                        event_source="billing_service",
                        metadata={"tenant_id": tenant_id, "error": str(exc)},
                    )
        return {"closed": closed, "errors": errors}

    def run_billing_cycle_tick(self, *, now: datetime | None = None, limit: int = 100) -> dict[str, Any]:
        return self.billing_cycle_tick(now=now, limit=limit)

    def build_billing_summary(self, tenant_id: int) -> dict[str, Any]:
        period = self.get_current_billing_period(int(tenant_id))
        if not period:
            return {"tenant_id": int(tenant_id), "message": "Подписка не найдена"}
        sub = self._subscription_service.get_active_subscription(int(tenant_id)) or {}
        usage = self.build_billing_usage_snapshot(int(tenant_id), period["period_start"], period["period_end"])
        overage_items = self.calculate_overage_items(sub, usage)
        base_price = 0.0 if self._is_owner_plan(sub) else float(sub.get("price") or 0)
        forecast_total = round(base_price + sum(float(i.get("amount") or 0) for i in overage_items), 2)
        return {
            "tenant_id": int(tenant_id),
            "period_start": period["period_start"],
            "period_end": period["period_end"],
            "plan_name": str(sub.get("plan_name") or "UNKNOWN"),
            "base_price": base_price,
            "usage": usage,
            "overage_items": overage_items,
            "forecast_total": forecast_total,
        }

    def get_last_invoice_summary(self, tenant_id: int) -> dict[str, Any] | None:
        if not hasattr(self._repo, "get_last_invoice"):
            return None
        invoice = self._repo.get_last_invoice(int(tenant_id))
        if not invoice:
            return None
        items = self._repo.list_invoice_items(int(invoice["id"])) if hasattr(self._repo, "list_invoice_items") else []
        return {"invoice": invoice, "items": items}

    def get_tenant_financial_snapshot(self, tenant_id: int) -> dict[str, Any]:
        current_sub = self._subscription_service.get_active_subscription(int(tenant_id))
        period = self.get_current_billing_period(int(tenant_id))
        if period:
            billing_period_snapshot = self.build_billing_usage_snapshot(int(tenant_id), period["period_start"], period["period_end"])
        else:
            today = date.today().isoformat()
            billing_period_snapshot = self.build_billable_usage_snapshot(int(tenant_id), today, today)
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
            "over_limit_flags": {
                "jobs": bool(any(i.get("item_type") == "extra_jobs" for i in self.calculate_overage_items(current_sub or {}, billing_period_snapshot))),
                "video": bool(any(i.get("item_type") == "extra_video" for i in self.calculate_overage_items(current_sub or {}, billing_period_snapshot))),
                "storage": bool(any(i.get("item_type") == "storage_overage" for i in self.calculate_overage_items(current_sub or {}, billing_period_snapshot))),
            },
            "overage_flags": [item.get("item_type") for item in self.calculate_overage_items(current_sub or {}, billing_period_snapshot)],
        }

    @property
    def subscription_service(self) -> SubscriptionService:
        return self._subscription_service

    @property
    def invoice_service(self) -> InvoiceService:
        return self._invoice_service

    def _derive_default_period_bounds(self, subscription: dict[str, Any]) -> tuple[str, str]:
        started_at = subscription.get("started_at")
        try:
            anchor = self._parse_iso_date(started_at)
        except Exception:
            anchor = datetime.now(timezone.utc).date()
        period_start = anchor.replace(day=1)
        period_end = self._end_of_month(period_start)
        return period_start.isoformat(), period_end.isoformat()

    @staticmethod
    def _parse_iso_date(raw: Any) -> date:
        value = str(raw or "").strip()
        if not value:
            return datetime.now(timezone.utc).date()
        if len(value) >= 10:
            value = value[:10]
        return date.fromisoformat(value)

    @staticmethod
    def _end_of_month(day: date) -> date:
        next_month = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
        return next_month - timedelta(days=1)

    @staticmethod
    def _is_owner_plan(plan: dict[str, Any] | None) -> bool:
        return str((plan or {}).get("plan_name") or (plan or {}).get("name") or "").upper() == "OWNER"

    @staticmethod
    def _make_item(item_type: str, description: str, quantity: int, unit_price: float) -> dict[str, Any]:
        qty = max(int(quantity), 0)
        price = float(unit_price)
        return {
            "item_type": item_type,
            "description": description,
            "quantity": qty,
            "unit_price": price,
            "amount": round(qty * price, 2),
            "metadata": {"pricing_source": "default_overage_pricing"},
        }

    def _safe_list_items(self, invoice: dict[str, Any]) -> list[dict[str, Any]]:
        invoice_id = int(invoice.get("id") or 0)
        if invoice_id <= 0 or not hasattr(self._repo, "list_invoice_items"):
            return []
        return self._repo.list_invoice_items(invoice_id)
