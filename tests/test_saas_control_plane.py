from __future__ import annotations

from app.limit_service import LimitService
from app.subscription_service import SubscriptionService
from app.tenant_service import TenantService
from app.usage_service import UsageService


class _FakeRepo:
    def __init__(self) -> None:
        self.tenants = {}
        self.default_tenant = {"id": 1, "name": "default", "owner_admin_id": 0, "is_active": True}
        self.plans = {
            "FREE": {"id": 1, "name": "FREE", "max_rules": 1, "max_jobs_per_day": 2, "max_video_per_day": 1},
            "PRO": {"id": 2, "name": "PRO", "max_rules": 100, "max_jobs_per_day": 1000, "max_video_per_day": 100},
        }
        self.subscriptions = {}
        self.rules_count = {}
        self.usage = {}

    def create_tenant(self, admin_id: int, name: str):
        tenant_id = admin_id
        self.tenants[admin_id] = {"id": tenant_id, "name": name, "owner_admin_id": admin_id, "is_active": True}
        return tenant_id

    def get_tenant_by_admin(self, admin_id: int):
        return self.tenants.get(admin_id)

    def get_default_tenant(self):
        return self.default_tenant

    def set_tenant_active(self, tenant_id: int, is_active: bool):
        return True

    def get_plan_by_name(self, name: str):
        return self.plans.get(name)

    def assign_subscription(self, tenant_id: int, plan_id: int, *, status: str = "active", expires_at=None):
        self.subscriptions[tenant_id] = {"id": 10, "tenant_id": tenant_id, "plan_id": plan_id, "status": status}
        return 10

    def get_active_subscription(self, tenant_id: int):
        sub = self.subscriptions.get(tenant_id)
        if not sub:
            return None
        plan = next(v for v in self.plans.values() if v["id"] == sub["plan_id"])
        return {**sub, "plan_name": plan["name"], **plan}

    def expire_subscription(self, tenant_id: int):
        if tenant_id not in self.subscriptions:
            return False
        self.subscriptions[tenant_id]["status"] = "expired"
        return True

    def count_rules_for_tenant(self, tenant_id: int):
        return self.rules_count.get(tenant_id, 0)

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0):
        row = self.usage.setdefault(tenant_id, {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0})
        row["jobs_count"] += jobs_delta
        row["video_count"] += video_delta

    def get_usage_for_date(self, tenant_id: int, day: str):
        return self.usage.get(tenant_id, {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0})

    def reset_usage_for_day(self, day: str):
        for row in self.usage.values():
            row["jobs_count"] = 0
            row["video_count"] = 0
        return len(self.usage)


def test_tenant_creation_and_fallback():
    repo = _FakeRepo()
    tenant_service = TenantService(repo)

    created = tenant_service.ensure_tenant_exists(100)
    assert created["owner_admin_id"] == 100

    fallback = tenant_service.ensure_tenant_exists(0)
    assert fallback["id"] in {0, 1}


def test_plan_assignment_and_subscription_activity():
    repo = _FakeRepo()
    subscription_service = SubscriptionService(repo)

    subscription_service.assign_plan(1, "FREE")
    assert subscription_service.is_subscription_active(1) is True

    subscription_service.expire_subscription(1)
    assert subscription_service.is_subscription_active(1) is False


def test_limits_block_when_exceeded():
    repo = _FakeRepo()
    sub = SubscriptionService(repo)
    usage = UsageService(repo)
    limit = LimitService(repo, sub, usage)

    sub.assign_plan(1, "FREE")
    repo.rules_count[1] = 1
    usage.increment_jobs(1, 2)
    usage.increment_video(1, 1)

    assert limit.can_create_rule(1)[0] is False
    assert limit.can_enqueue_job(1)[0] is False
    assert limit.can_process_video(1)[0] is False


def test_usage_increment_and_reset():
    repo = _FakeRepo()
    usage = UsageService(repo)

    usage.increment_jobs(7)
    usage.increment_video(7)
    today = usage.get_today_usage(7)
    assert today["jobs_count"] == 1
    assert today["video_count"] == 1

    usage.reset_daily_usage()
    today_after = usage.get_today_usage(7)
    assert today_after["jobs_count"] == 0
    assert today_after["video_count"] == 0
