from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.billing_service import BillingService
from app.invoice_service import InvoiceService
from app.payment_provider_protocol import NoopPaymentProvider
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService


class _FakeRepo:
    def __init__(self) -> None:
        self.tenants = {1: {"id": 1, "name": "tenant-1", "owner_admin_id": 101, "is_active": True}}
        self.plans = {
            "FREE": {"id": 1, "name": "FREE", "priority_level": 1, "price": 0, "max_jobs_per_day": 2, "max_video_per_day": 1, "max_storage_mb": 10},
            "PRO": {"id": 2, "name": "PRO", "priority_level": 2, "price": 10, "max_jobs_per_day": 100, "max_video_per_day": 50, "max_storage_mb": 1000},
        }
        self.subscriptions: dict[int, dict] = {}
        self.subscription_history: list[dict] = []
        self.billing_events: list[dict] = []
        self.usage_rows: dict[tuple[int, str], dict] = {}
        self.invoices: dict[int, dict] = {}
        self.invoice_items: dict[int, list[dict]] = {}
        self._sub_seq = 0
        self._invoice_seq = 0
        self._invoice_item_seq = 0

    def get_plan_by_name(self, name: str):
        return self.plans.get(str(name).upper())

    def assign_subscription(self, tenant_id: int, plan_id: int, *, status: str = "active", expires_at=None):
        self._sub_seq += 1
        self.subscriptions[tenant_id] = {
            "id": self._sub_seq,
            "tenant_id": tenant_id,
            "plan_id": plan_id,
            "status": status,
            "expires_at": expires_at,
            "started_at": "2026-01-01T00:00:00+00:00",
            "grace_ends_at": None,
            "pending_plan_id": None,
        }
        return self._sub_seq

    def get_active_subscription(self, tenant_id: int):
        sub = self.subscriptions.get(tenant_id)
        if not sub:
            return None
        plan = next(v for v in self.plans.values() if v["id"] == sub["plan_id"])
        plan_data = {k: v for k, v in plan.items() if k != "id"}
        return {**sub, **plan_data, "plan_name": plan["name"]}

    def set_subscription_status(self, subscription_id: int, new_status: str):
        for sub in self.subscriptions.values():
            if sub["id"] == subscription_id:
                sub["status"] = new_status
                return True
        return False

    def set_subscription_grace_window(self, subscription_id: int, grace_started_at: str, grace_ends_at: str):
        for sub in self.subscriptions.values():
            if sub["id"] == subscription_id:
                sub["grace_ends_at"] = grace_ends_at
                return True
        return False

    def replace_subscription_plan(self, subscription_id: int, plan_id: int):
        for sub in self.subscriptions.values():
            if sub["id"] == subscription_id:
                sub["plan_id"] = int(plan_id)
                return True
        return False

    def set_subscription_pending_plan(self, subscription_id: int, pending_plan_id: int):
        for sub in self.subscriptions.values():
            if sub["id"] == subscription_id:
                sub["pending_plan_id"] = pending_plan_id
                return True
        return False

    def add_subscription_history(self, **payload):
        self.subscription_history.append(payload)
        return len(self.subscription_history)

    def create_billing_event(self, tenant_id: int, event_type: str, *, event_source=None, amount=None, currency=None, metadata=None):
        self.billing_events.append({"tenant_id": tenant_id, "event_type": event_type, "metadata_json": metadata or {}})
        return len(self.billing_events)

    def get_billing_events(self, tenant_id: int, limit: int = 20):
        rows = [r for r in self.billing_events if r["tenant_id"] == tenant_id]
        return list(reversed(rows))[:limit]

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0):
        key = (tenant_id, date.today().isoformat())
        row = self.usage_rows.setdefault(key, {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0})
        row["jobs_count"] += jobs_delta
        row["video_count"] += video_delta
        row["storage_used_mb"] += storage_delta_mb
        row["api_calls"] += api_calls_delta

    def get_usage_for_date(self, tenant_id: int, day: str):
        return self.usage_rows.get((tenant_id, day), {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0})

    def get_usage_for_period(self, tenant_id: int, date_from: str, date_to: str):
        rows = [row for (tid, _d), row in self.usage_rows.items() if tid == tenant_id]
        return {
            "jobs_count": sum(r["jobs_count"] for r in rows),
            "video_count": sum(r["video_count"] for r in rows),
            "storage_used_mb": max([r["storage_used_mb"] for r in rows] or [0]),
            "api_calls": sum(r["api_calls"] for r in rows),
        }

    def create_invoice(self, **payload):
        self._invoice_seq += 1
        self.invoices[self._invoice_seq] = {"id": self._invoice_seq, **payload, "subtotal": 0, "total": 0, "status": payload["status"]}
        return self._invoice_seq

    def add_invoice_item(self, invoice_id: int, **payload):
        self._invoice_item_seq += 1
        self.invoice_items.setdefault(invoice_id, []).append({"id": self._invoice_item_seq, **payload})
        return self._invoice_item_seq

    def recalculate_invoice_totals(self, invoice_id: int):
        total = sum(float(item["amount"]) for item in self.invoice_items.get(invoice_id, []))
        self.invoices[invoice_id]["subtotal"] = total
        self.invoices[invoice_id]["total"] = total
        return True

    def set_invoice_status(self, invoice_id: int, status: str, **kwargs):
        if invoice_id not in self.invoices:
            return False
        self.invoices[invoice_id]["status"] = status
        self.invoices[invoice_id].update(kwargs)
        return True

    def get_invoice(self, invoice_id: int):
        return self.invoices.get(invoice_id)

    def get_last_invoice(self, tenant_id: int):
        rows = [i for i in self.invoices.values() if i["tenant_id"] == tenant_id]
        if not rows:
            return None
        return sorted(rows, key=lambda i: i["id"])[-1]

    def count_open_invoices(self, tenant_id: int):
        return sum(1 for i in self.invoices.values() if i["tenant_id"] == tenant_id and i["status"] in {"draft", "open", "uncollectible"})

    def get_tenant_by_id(self, tenant_id: int):
        return self.tenants.get(tenant_id)


def _setup():
    repo = _FakeRepo()
    subscription = SubscriptionService(repo)
    usage = UsageService(repo)
    invoice = InvoiceService(repo)
    billing = BillingService(repo)
    return repo, subscription, usage, invoice, billing


def test_free_subscription_foundation_created():
    repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "FREE", status="trial")
    sub = repo.get_active_subscription(1)
    assert sub["plan_name"] == "FREE"
    assert sub["status"] == "trial"


def test_upgrade_writes_subscription_history():
    repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "FREE")
    assert subscription.change_plan(1, "PRO", changed_by="admin") is True
    assert repo.subscription_history[-1]["old_plan_id"] == 1
    assert repo.subscription_history[-1]["new_plan_id"] == 2


def test_downgrade_writes_subscription_history_deferred():
    repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "PRO")
    assert subscription.change_plan(1, "FREE", effective_mode="period_end") is True
    assert repo.subscription_history[-1]["new_plan_id"] == 1


def test_invoice_draft_creation_and_item_addition_and_finalize():
    repo, subscription, _usage, invoice, _billing = _setup()
    sub_id = subscription.assign_plan(1, "PRO")
    invoice_id = invoice.create_draft_invoice(1, int(sub_id or 0), "2026-04-01", "2026-04-30")
    assert invoice_id is not None
    item_id = invoice.add_invoice_item(int(invoice_id), item_type="base_plan", description="План PRO", quantity=1, unit_price=10)
    assert item_id is not None
    assert repo.invoices[int(invoice_id)]["total"] == 10
    assert invoice.finalize_invoice(int(invoice_id)) is True
    assert repo.invoices[int(invoice_id)]["status"] == "open"


def test_mark_invoice_paid_works():
    _repo, subscription, _usage, invoice, _billing = _setup()
    sub_id = subscription.assign_plan(1, "PRO")
    invoice_id = invoice.create_draft_invoice(1, int(sub_id or 0), "2026-04-01", "2026-04-30")
    invoice.finalize_invoice(int(invoice_id))
    assert invoice.mark_invoice_paid(int(invoice_id), external_reference="pay_1") is True


def test_mark_invoice_void_works():
    _repo, subscription, _usage, invoice, _billing = _setup()
    sub_id = subscription.assign_plan(1, "PRO")
    invoice_id = invoice.create_draft_invoice(1, int(sub_id or 0), "2026-04-01", "2026-04-30")
    assert invoice.mark_invoice_void(int(invoice_id), reason="manual") is True


def test_billing_event_on_lifecycle():
    repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "FREE", status="trial")
    assert subscription.transition_status(1, "active") is True
    assert any(evt["event_type"] == "subscription_started" for evt in repo.billing_events)


def test_trial_can_expire_via_state_machine():
    _repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "FREE", status="trial")
    assert subscription.transition_status(1, "expired") is True


def test_grace_period_lifecycle():
    _repo, subscription, *_ = _setup()
    subscription.assign_plan(1, "PRO", status="active")
    assert subscription.start_grace_period(1, days=2) is True
    assert subscription.is_in_grace(1) is True
    assert subscription.end_grace_period(1, restore_active=False) is True


def test_usage_snapshot_for_period():
    _repo, subscription, usage, _invoice, _billing = _setup()
    subscription.assign_plan(1, "FREE")
    usage.increment_jobs(1, 3)
    usage.increment_video(1, 2)
    snap = usage.build_usage_snapshot(1, "2026-04-01", "2026-04-30")
    assert snap["jobs_count"] == 3
    assert snap["video_count"] == 2
    assert snap["overage_candidates"]


def test_financial_snapshot_builds():
    _repo, subscription, usage, invoice, billing = _setup()
    sub_id = subscription.assign_plan(1, "PRO")
    usage.increment_jobs(1, 1)
    inv_id = invoice.create_draft_invoice(1, int(sub_id or 0), "2026-04-01", "2026-04-30")
    invoice.add_invoice_item(int(inv_id), item_type="base_plan", description="План", quantity=1, unit_price=10)
    snap = billing.get_tenant_financial_snapshot(1)
    assert snap["current_plan"] == "PRO"
    assert snap["open_invoices_count"] >= 1


def test_payment_provider_noop_path_is_stable():
    provider = NoopPaymentProvider()
    checkout = provider.create_checkout_session(1, 10, return_url="https://example.com")
    assert checkout["provider"] == "noop"
    sync = provider.sync_payment_status(checkout["external_reference"])
    assert sync.status == "unknown"
