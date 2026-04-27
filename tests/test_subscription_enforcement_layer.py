from __future__ import annotations

from app.limit_service import LimitService
from app.subscription_service import SubscriptionService
from app.usage_service import UsageService
from app import user_ui


class _Repo:
    def __init__(self) -> None:
        self.rules_count = 0
        self.usage = {"jobs_count": 0, "video_count": 0, "storage_used_mb": 0, "api_calls": 0}
        self.sub = {
            "tenant_id": 1,
            "status": "active",
            "plan_name": "FREE",
            "max_rules": 3,
            "max_jobs_per_day": 5,
            "max_video_per_day": 2,
        }

    def get_active_subscription(self, tenant_id: int):
        return dict(self.sub)

    def count_rules_for_tenant(self, tenant_id: int):
        return self.rules_count

    def get_usage_for_date(self, tenant_id: int, day: str):
        return dict(self.usage)

    def bump_usage(self, tenant_id: int, *, jobs_delta: int = 0, video_delta: int = 0, storage_delta_mb: int = 0, api_calls_delta: int = 0):
        self.usage["jobs_count"] += int(jobs_delta or 0)
        self.usage["video_count"] += int(video_delta or 0)


def test_can_create_rule_blocks_at_limit() -> None:
    repo = _Repo()
    repo.rules_count = 3
    limit = LimitService(repo, SubscriptionService(repo), UsageService(repo))

    ok, reason = limit.can_create_rule(1)

    assert ok is False
    assert "лимит правил" in str(reason or "").lower()


def test_inactive_subscription_blocks_product_actions() -> None:
    repo = _Repo()
    repo.sub["status"] = "expired"
    limit = LimitService(repo, SubscriptionService(repo), UsageService(repo))

    assert limit.can_create_rule(1)[0] is False
    assert limit.can_enqueue_job(1)[0] is False
    assert limit.can_process_video(1)[0] is False


def test_owner_bypasses_limits() -> None:
    repo = _Repo()
    repo.sub.update({"plan_name": "OWNER", "status": "expired", "max_rules": 1, "max_jobs_per_day": 1, "max_video_per_day": 1})
    repo.rules_count = 999
    repo.usage["jobs_count"] = 999
    repo.usage["video_count"] = 999
    limit = LimitService(repo, SubscriptionService(repo), UsageService(repo))

    assert limit.can_create_rule(1)[0] is True
    assert limit.can_enqueue_job(1)[0] is True
    assert limit.can_process_video(1)[0] is True


def test_user_limit_text_avoids_technical_words() -> None:
    text = user_ui.build_user_usage_text(
        {"plan_name": "FREE", "status": "active", "max_rules": 3, "max_video_per_day": 5, "max_jobs_per_day": 100},
        {"video_count": 1, "jobs_count": 10},
        2,
    )

    lowered = text.lower()
    for bad in ("worker", "tenant_id", "dedup", "lease", "job_id"):
        assert bad not in lowered


def test_subscription_blocked_text_contains_actionable_buttons() -> None:
    text = user_ui.build_user_subscription_blocked_text({"plan_name": "FREE", "status": "expired"})
    kb = user_ui.build_user_usage_keyboard()
    labels = [btn.text for row in kb.inline_keyboard for btn in row]

    assert "Подписка неактивна" in text
    assert "💎 Сменить тариф" in labels
    assert "🧾 Мои счета" in labels
