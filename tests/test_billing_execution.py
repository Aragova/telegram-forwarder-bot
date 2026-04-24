from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.billing_service import BillingService


class _FakeRepo:
    def __init__(self) -> None:
        self.plans = {
            1: {"id": 1, "name": "BASIC", "price": 10.0, "max_jobs_per_day": 3, "max_video_per_day": 1, "max_storage_mb": 10},
            2: {"id": 2, "name": "OWNER", "price": 0.0, "max_jobs_per_day": 0, "max_video_per_day": 0, "max_storage_mb": 0},
        }
        self.subscriptions = {
            1: {"id": 1, "tenant_id": 1, "plan_id": 1, "status": "active", "started_at": "2026-04-10T00:00:00+00:00", "current_period_start": None, "current_period_end": None},
            2: {"id": 2, "tenant_id": 2, "plan_id": 2, "status": "active", "started_at": "2026-04-10T00:00:00+00:00", "current_period_start": "2026-04-01", "current_period_end": "2026-04-30"},
            3: {"id": 3, "tenant_id": 3, "plan_id": 1, "status": "active", "started_at": "2026-04-01T00:00:00+00:00", "current_period_start": "2026-04-01", "current_period_end": "2026-04-01"},
        }
        self.billing_events: list[dict] = []
        self.usage_rows: dict[tuple[int, str], dict] = {}
        self.invoices: dict[int, dict] = {}
        self.invoice_items: dict[int, list[dict]] = {}
        self._invoice_seq = 0
        self._item_seq = 0

    def get_active_subscription(self, tenant_id: int):
        for sub in self.subscriptions.values():
            if sub["tenant_id"] == tenant_id and sub["status"] in {"active", "trial", "grace"}:
                plan = self.plans[sub["plan_id"]]
                return {
                    **sub,
                    "plan_name": plan["name"],
                    "name": plan["name"],
                    "price": plan["price"],
                    "max_jobs_per_day": plan["max_jobs_per_day"],
                    "max_video_per_day": plan["max_video_per_day"],
                    "max_storage_mb": plan["max_storage_mb"],
                }
        return None

    def get_subscription_by_id(self, subscription_id: int):
        sub = self.subscriptions.get(subscription_id)
        if not sub:
            return None
        return self.get_active_subscription(sub["tenant_id"])

    def update_billing_period(self, subscription_id: int, period_start: str, period_end: str):
        self.subscriptions[subscription_id]["current_period_start"] = period_start
        self.subscriptions[subscription_id]["current_period_end"] = period_end
        return True

    def get_subscriptions_due_for_billing(self, due_before: str, limit: int = 100):
        return [s for s in self.subscriptions.values() if s.get("current_period_end") and s["current_period_end"] <= due_before[:10]][:limit]

    def build_billing_usage_data(self, tenant_id: int, period_start: str, period_end: str):
        rows = [r for (tid, d), r in self.usage_rows.items() if tid == tenant_id and period_start <= d <= period_end]
        return {
            "tenant_id": tenant_id,
            "period_start": period_start,
            "period_end": period_end,
            "jobs_count": sum(r["jobs_count"] for r in rows),
            "video_count": sum(r["video_count"] for r in rows),
            "storage_used_mb": max([r["storage_used_mb"] for r in rows] or [0]),
            "api_calls": sum(r["api_calls"] for r in rows),
        }

    def get_usage_for_period(self, tenant_id: int, date_from: str, date_to: str):
        return self.build_billing_usage_data(tenant_id, date_from, date_to)

    def get_usage_for_date(self, tenant_id: int, day: str):
        return self.usage_rows.get((tenant_id, day), {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0})

    def create_invoice(self, **payload):
        self._invoice_seq += 1
        self.invoices[self._invoice_seq] = {"id": self._invoice_seq, **payload, "subtotal": 0, "total": 0}
        return self._invoice_seq

    def add_invoice_item(self, invoice_id: int, **payload):
        self._item_seq += 1
        self.invoice_items.setdefault(invoice_id, []).append({"id": self._item_seq, **payload})
        return self._item_seq

    def recalculate_invoice_totals(self, invoice_id: int):
        total = round(sum(float(i["amount"]) for i in self.invoice_items.get(invoice_id, [])), 2)
        self.invoices[invoice_id]["subtotal"] = total
        self.invoices[invoice_id]["total"] = total
        return True

    def get_invoice(self, invoice_id: int):
        return self.invoices.get(invoice_id)

    def set_invoice_status(self, invoice_id: int, status: str, **kwargs):
        self.invoices[invoice_id]["status"] = status
        return True

    def get_invoice_for_period(self, tenant_id: int, period_start: str, period_end: str):
        for inv in self.invoices.values():
            if inv["tenant_id"] == tenant_id and inv["period_start"] == period_start and inv["period_end"] == period_end and inv["status"] != "void":
                return inv
        return None

    def get_last_invoice(self, tenant_id: int):
        rows = [inv for inv in self.invoices.values() if inv["tenant_id"] == tenant_id]
        return rows[-1] if rows else None

    def list_invoice_items(self, invoice_id: int):
        return list(self.invoice_items.get(invoice_id, []))

    def count_open_invoices(self, tenant_id: int):
        return len([i for i in self.invoices.values() if i["tenant_id"] == tenant_id and i["status"] in {"draft", "open"}])

    def create_billing_event(self, tenant_id: int, event_type: str, *, event_source=None, amount=None, currency=None, metadata=None):
        self.billing_events.append({"tenant_id": tenant_id, "event_type": event_type, "metadata_json": metadata or {}})
        return len(self.billing_events)

    def get_billing_events(self, tenant_id: int, limit: int = 20):
        rows = [e for e in self.billing_events if e["tenant_id"] == tenant_id]
        return rows[-limit:]


def _setup():
    repo = _FakeRepo()
    service = BillingService(repo)
    return repo, service


def test_legacy_subscription_gets_billing_period():
    repo, service = _setup()
    sub = repo.get_active_subscription(1)
    ensured = service.ensure_billing_period(sub)
    assert ensured["current_period_start"] == "2026-04-01"
    assert ensured["current_period_end"] == "2026-04-30"


def test_base_plan_item_created():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 1, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    out = service.generate_invoice_for_current_period(1)
    items = out["items"]
    assert any(i["item_type"] == "base_plan" and float(i["amount"]) == 10.0 for i in items)


def test_overage_jobs_item_created():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 10, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    out = service.generate_invoice_for_current_period(1)
    assert any(i["item_type"] == "extra_jobs" for i in out["items"])


def test_overage_video_item_created():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 1, "video_count": 3, "storage_used_mb": 1, "api_calls": 1}
    out = service.generate_invoice_for_current_period(1)
    assert any(i["item_type"] == "extra_video" for i in out["items"])


def test_owner_plan_skips_invoice():
    _repo, service = _setup()
    out = service.generate_invoice_for_current_period(2)
    assert out["skipped"] is True
    assert out.get("reason") == "owner_plan_skip"


def test_generate_invoice_is_idempotent():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 1, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    first = service.generate_invoice_for_current_period(1)
    second = service.generate_invoice_for_current_period(1)
    assert first["invoice"]["id"] == second["invoice"]["id"]
    assert len(repo.invoices) == 1


def test_close_current_period_finalizes_invoice():
    repo, service = _setup()
    repo.usage_rows[(3, "2026-04-01")] = {"jobs_count": 1, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    closed = service.close_current_billing_period(3)
    assert closed["invoice"]["status"] == "open"


def test_close_current_period_advances_period():
    repo, service = _setup()
    repo.usage_rows[(3, "2026-04-01")] = {"jobs_count": 1, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    service.close_current_billing_period(3)
    sub = repo.subscriptions[3]
    assert sub["current_period_start"] == "2026-04-02"


def test_billing_cycle_tick_closes_due():
    repo, service = _setup()
    repo.usage_rows[(3, "2026-04-01")] = {"jobs_count": 1, "video_count": 0, "storage_used_mb": 1, "api_calls": 1}
    result = service.billing_cycle_tick(now=datetime(2026, 4, 2, tzinfo=timezone.utc))
    assert result["closed"] >= 1


def test_billing_summary_uses_real_period_and_usage():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 4, "video_count": 2, "storage_used_mb": 1, "api_calls": 1}
    summary = service.build_billing_summary(1)
    assert summary["period_start"] == "2026-04-01"
    assert summary["usage"]["jobs_count"] == 4


def test_invoice_summary_shows_items_and_total():
    repo, service = _setup()
    repo.usage_rows[(1, "2026-04-20")] = {"jobs_count": 4, "video_count": 2, "storage_used_mb": 1, "api_calls": 1}
    service.generate_invoice_for_current_period(1)
    summary = service.get_last_invoice_summary(1)
    assert summary is not None
    assert summary["invoice"]["total"] > 0
    assert len(summary["items"]) > 0


def test_invoice_events_written():
    repo, service = _setup()
    repo.usage_rows[(1, date.today().isoformat())] = {"jobs_count": 4, "video_count": 2, "storage_used_mb": 1, "api_calls": 1}
    service.generate_invoice_for_current_period(1, finalize=True)
    event_types = {e["event_type"] for e in repo.billing_events}
    assert "invoice_created" in event_types
    assert "invoice_finalized" in event_types
