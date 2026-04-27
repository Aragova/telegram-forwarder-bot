from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.postgres_repository import PostgresRepository


class _SpyRepo:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def _call(**kwargs):
            self.calls.append((name, kwargs))
            if name == "get_rule_tenant_id":
                return 1
            if name.startswith("get_") or name.startswith("list_") or name.startswith("build_"):
                return {"ok": True, "method": name}
            if name.startswith("count_"):
                return 1
            if name.startswith("reset_"):
                return 1
            if name.startswith("set_") or name.startswith("expire_") or name.startswith("replace_") or name.startswith("recalculate_") or name.startswith("update_"):
                return True
            if name == "create_tenant":
                return 10
            if name in {"assign_subscription", "add_subscription_history", "create_billing_event", "create_invoice", "add_invoice_item"}:
                return 11
            return None

        return _call


def test_postgres_repository_split_wires_domain_repositories():
    repo = PostgresRepository()
    assert repo.tenant_repo is not None
    assert repo.subscription_repo is not None
    assert repo.billing_repo is not None
    assert repo.usage_repo is not None


def test_tenant_facade_compatibility(monkeypatch):
    repo = PostgresRepository()
    spy = _SpyRepo()
    monkeypatch.setattr(repo, "tenant_repo", spy)

    assert repo.create_tenant(1, "acme") == 10
    assert repo.get_tenant_by_id(1)["method"] == "get_tenant_by_id"
    assert repo.get_tenant_by_admin(1)["method"] == "get_tenant_by_admin"
    assert repo.set_tenant_active(1, True) is True
    assert repo.add_tenant_user(1, 100, "admin") is None
    assert repo.get_tenant_user_role(1, 100)["method"] == "get_tenant_user_role"


def test_subscription_facade_compatibility(monkeypatch):
    repo = PostgresRepository()
    spy = _SpyRepo()
    monkeypatch.setattr(repo, "subscription_repo", spy)

    assert repo.get_plan_by_name("free")["method"] == "get_plan_by_name"
    assert repo.assign_subscription(1, 2) == 11
    assert repo.get_active_subscription(1)["method"] == "get_active_subscription"
    assert repo.get_latest_subscription(1)["method"] == "get_latest_subscription"
    assert repo.get_subscription_by_id(1)["method"] == "get_subscription_by_id"
    assert repo.expire_subscription(1) is True
    assert repo.set_subscription_status(1, "active") is True
    assert repo.set_subscription_grace_window(1, "a", "b") is True
    assert repo.set_subscription_pending_plan(1, 2) is True
    assert repo.replace_subscription_plan(1, 2) is True
    assert repo.add_subscription_history(
        tenant_id=1,
        old_plan_id=1,
        new_plan_id=2,
        old_status="trial",
        new_status="active",
        changed_by="system",
        reason="upgrade",
        effective_from="2026-01-01",
    ) == 11
    assert repo.get_subscription_history(1)["method"] == "get_subscription_history"
    assert repo.get_subscriptions_due_for_billing("2026-01-31T00:00:00+00:00")["method"] == "get_subscriptions_due_for_billing"
    assert repo.update_billing_period(1, "2026-01-01", "2026-01-31") is True


def test_billing_and_usage_facade_compatibility(monkeypatch):
    repo = PostgresRepository()
    billing_spy = _SpyRepo()
    usage_spy = _SpyRepo()
    monkeypatch.setattr(repo, "billing_repo", billing_spy)
    monkeypatch.setattr(repo, "usage_repo", usage_spy)

    assert repo.create_billing_event(1, "evt") == 11
    assert repo.get_billing_events(1)["method"] == "get_billing_events"
    assert repo.create_invoice(
        tenant_id=1,
        subscription_id=1,
        period_start="2026-01-01",
        period_end="2026-01-31",
        status="draft",
        currency="USD",
        due_at=None,
    ) == 11
    assert repo.add_invoice_item(1, item_type="base_plan", description="x", quantity=1, unit_price=1.0, amount=1.0) == 11
    assert repo.recalculate_invoice_totals(1) is True
    assert repo.set_invoice_status(1, "open") is True
    assert repo.get_invoice(1)["method"] == "get_invoice"
    assert repo.get_last_invoice(1)["method"] == "get_last_invoice"
    assert repo.list_invoices_for_tenant(1, limit=5)["method"] == "list_invoices_for_tenant"
    assert repo.count_open_invoices(1) == 1
    assert repo.get_invoice_for_period(1, "2026-01-01", "2026-01-31")["method"] == "get_invoice_for_period"
    assert repo.list_invoice_items(1)["method"] == "list_invoice_items"
    assert repo.count_invoices_by_status("draft") == 1
    assert repo.get_billing_periods_due("2026-01-31T00:00:00+00:00")["method"] == "get_billing_periods_due"
    assert repo.count_tenants_with_overage_current_period() == 1
    repo.bump_usage(1, jobs_delta=2)
    assert repo.get_usage_for_date(1, "2026-01-01")["method"] == "get_usage_for_date"
    assert repo.get_usage_for_period(1, "2026-01-01", "2026-01-31")["method"] == "get_usage_for_period"
    assert repo.build_billing_usage_data(1, "2026-01-01", "2026-01-31")["method"] == "build_billing_usage_data"
    assert repo.reset_usage_for_day("2026-01-01") == 1
    assert repo.count_rules_for_tenant(1) == 1
    assert repo.get_rule_tenant_id(1) == 1
    assert repo.get_saas_health_snapshot()["method"] == "get_saas_health_snapshot"


def test_init_still_calls_default_plan_seed(monkeypatch):
    repo = PostgresRepository()
    called = {"seed": False}

    def _seed():
        called["seed"] = True

    monkeypatch.setattr(repo.subscription_repo, "ensure_default_plans", _seed)
    repo._ensure_default_plans()
    assert called["seed"] is True
