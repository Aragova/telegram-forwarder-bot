from __future__ import annotations

from app.limit_service import LimitService
from app.saas_bootstrap import ensure_owner_and_default_tenant_bootstrap
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService


class _FakeRepo:
    def __init__(self) -> None:
        self.tenants = {}
        self.default_tenant = {"id": 1, "name": "default", "owner_admin_id": 0, "is_active": False}
        self.plans = {
            "FREE": {"id": 1, "name": "FREE", "max_rules": 1, "max_jobs_per_day": 2, "max_video_per_day": 1},
            "PRO": {"id": 2, "name": "PRO", "max_rules": 100, "max_jobs_per_day": 1000, "max_video_per_day": 100},
            "OWNER": {"id": 3, "name": "OWNER", "max_rules": 0, "max_jobs_per_day": 0, "max_video_per_day": 0},
        }
        self.subscriptions = {}
        self.rules_count = {}
        self.usage = {}
        self.sub_seq = 0

    def create_tenant(self, admin_id: int, name: str):
        tenant_id = admin_id
        self.tenants[admin_id] = {"id": tenant_id, "name": name, "owner_admin_id": admin_id, "is_active": True}
        return tenant_id

    def get_tenant_by_admin(self, admin_id: int):
        return self.tenants.get(admin_id)

    def get_default_tenant(self):
        return self.default_tenant

    def set_tenant_active(self, tenant_id: int, is_active: bool):
        if int(tenant_id) == 1:
            self.default_tenant["is_active"] = bool(is_active)
            return True
        for tenant in self.tenants.values():
            if int(tenant.get("id") or 0) == int(tenant_id):
                tenant["is_active"] = bool(is_active)
                return True
        return True

    def get_plan_by_name(self, name: str):
        return self.plans.get(str(name).upper())

    def assign_subscription(self, tenant_id: int, plan_id: int, *, status: str = "active", expires_at=None):
        self.sub_seq += 1
        self.subscriptions[tenant_id] = {"id": self.sub_seq, "tenant_id": tenant_id, "plan_id": plan_id, "status": status, "expires_at": expires_at}
        return self.sub_seq

    def get_active_subscription(self, tenant_id: int):
        sub = self.subscriptions.get(tenant_id)
        if not sub:
            return None
        if str(sub.get("status") or "") not in {"active", "trial", "grace"}:
            return None
        plan = next(v for v in self.plans.values() if v["id"] == sub["plan_id"])
        return {**sub, "plan_name": plan["name"], **plan}

    def get_tenant_by_id(self, tenant_id: int):
        if int(tenant_id) == 1:
            return dict(self.default_tenant)
        for tenant in self.tenants.values():
            if int(tenant.get("id") or 0) == int(tenant_id):
                return dict(tenant)
        return None

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


def test_owner_tenant_is_created_automatically():
    repo = _FakeRepo()
    ensure_owner_and_default_tenant_bootstrap(repo, 100)
    owner = repo.get_tenant_by_admin(100)
    assert owner is not None
    assert owner["owner_admin_id"] == 100


def test_owner_tenant_gets_active_subscription():
    repo = _FakeRepo()
    ensure_owner_and_default_tenant_bootstrap(repo, 500)
    owner_tenant = repo.get_tenant_by_admin(500)
    subscription_service = SubscriptionService(repo)
    assert subscription_service.is_subscription_active(int(owner_tenant["id"])) is True


def test_default_tenant_id_1_not_blocked_after_bootstrap():
    repo = _FakeRepo()
    ensure_owner_and_default_tenant_bootstrap(repo, 700)
    sub = SubscriptionService(repo)
    usage = UsageService(repo)
    limit = LimitService(repo, sub, usage)
    assert repo.default_tenant["is_active"] is True
    assert limit.can_enqueue_job(1)[0] is True


def test_owner_plan_is_unlimited():
    repo = _FakeRepo()
    repo.create_tenant(200, "tenant-200")
    SubscriptionService(repo).assign_plan(200, "OWNER")
    repo.rules_count[200] = 99999
    usage = UsageService(repo)
    usage.increment_jobs(200, 99999)
    usage.increment_video(200, 99999)
    limit = LimitService(repo, SubscriptionService(repo), usage)

    assert limit.can_create_rule(200)[0] is True
    assert limit.can_enqueue_job(200)[0] is True
    assert limit.can_process_video(200)[0] is True


def test_regular_tenant_without_active_subscription_is_still_blocked():
    repo = _FakeRepo()
    repo.create_tenant(300, "tenant-300")
    SubscriptionService(repo).assign_plan(300, "FREE")
    SubscriptionService(repo).expire_subscription(300)
    usage = UsageService(repo)
    limit = LimitService(repo, SubscriptionService(repo), usage)

    assert limit.can_enqueue_job(300)[0] is False
    assert limit.can_process_video(300)[0] is False


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
