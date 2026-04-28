from __future__ import annotations

from app.recovery_service import RecoveryService
from app.subscription_service import SubscriptionService
from app import user_ui


class _Repo:
    def __init__(self) -> None:
        self.sub = {"tenant_id": 1, "plan_name": "BASIC", "status": "active"}
        self.rules = [
            {"id": 1, "tenant_id": 1, "is_active": True, "next_run_at": "old"},
            {"id": 2, "tenant_id": 2, "is_active": True, "next_run_at": "old"},
        ]
        self.deliveries = [
            {"id": 1, "rule_id": 1, "status": "pending"},
            {"id": 2, "rule_id": 2, "status": "pending"},
        ]
        self.jobs = [
            {"id": 1, "tenant_id": 1, "job_type": "repost_single", "status": "failed", "error_text": "Достигнут дневной лимит задач"},
            {"id": 2, "tenant_id": 1, "job_type": "video_download", "status": "failed", "error_text": "Лимит видео"},
            {"id": 3, "tenant_id": 1, "job_type": "video_download", "status": "failed", "error_text": "ffmpeg decode error"},
            {"id": 4, "tenant_id": 2, "job_type": "repost_single", "status": "failed", "error_text": "subscription blocked"},
        ]
        self.events = [
            {"tenant_id": 1, "event_type": "limit_job_blocked"},
            {"tenant_id": 2, "event_type": "limit_video_blocked"},
        ]
        self.created_events: list[dict] = []

    def get_active_subscription(self, tenant_id: int):
        return dict(self.sub)

    def get_recoverable_summary_for_tenant(self, tenant_id: int):
        active_rules = [r for r in self.rules if r["tenant_id"] == tenant_id and r["is_active"]]
        tenant_rule_ids = {r["id"] for r in active_rules}
        pending = [d for d in self.deliveries if d["rule_id"] in tenant_rule_ids and d["status"] == "pending"]
        failed_jobs = [j for j in self.jobs if j["tenant_id"] == tenant_id and j["status"] == "failed"]
        limit_jobs = [j for j in failed_jobs if "лимит" in j["error_text"].lower() or "subscription" in j["error_text"].lower()]
        return {
            "active_rules_count": len(active_rules),
            "pending_deliveries_count": len(pending),
            "failed_limit_jobs_count": sum(1 for j in limit_jobs if not j["job_type"].startswith("video_")),
            "failed_limit_video_jobs_count": sum(1 for j in limit_jobs if j["job_type"].startswith("video_")),
            "last_blocked_events": [e for e in self.events if e["tenant_id"] == tenant_id],
        }

    def recover_blocked_jobs_for_tenant(self, tenant_id: int):
        restored = 0
        for job in self.jobs:
            if job["tenant_id"] != tenant_id or job["status"] != "failed":
                continue
            text = str(job.get("error_text") or "").lower()
            allowed = any(marker in text for marker in ("лимит", "subscription", "подписка неактивна", "limit"))
            blocked = any(marker in text for marker in ("ffmpeg", "telegram", "invalid source", "access"))
            if allowed and not blocked:
                job["status"] = "retry"
                job["error_text"] = None
                restored += 1
        return restored

    def recover_pending_deliveries_for_tenant(self, tenant_id: int):
        updated = 0
        for rule in self.rules:
            if rule["tenant_id"] == tenant_id and rule["is_active"]:
                rule["next_run_at"] = "now"
                updated += 1
        return updated

    def create_billing_event(self, tenant_id: int, event_type: str, **kwargs):
        self.created_events.append({"tenant_id": tenant_id, "event_type": event_type, **kwargs})


def test_recovery_summary_scoped_by_tenant() -> None:
    repo = _Repo()
    service = RecoveryService(repo, SubscriptionService(repo))

    summary = service.build_recovery_summary(1)

    assert summary["active_rules_count"] == 1
    assert summary["pending_deliveries_count"] == 1
    assert summary["failed_limit_jobs_count"] == 1
    assert summary["failed_limit_video_jobs_count"] == 1
    assert len(summary["last_blocked_events"]) == 1


def test_recovery_restores_only_limit_jobs_and_not_foreign_or_ffmpeg() -> None:
    repo = _Repo()
    service = RecoveryService(repo, SubscriptionService(repo))

    result = service.recover_after_payment(1, triggered_by_user_id=100)

    assert result["ok"] is True
    assert result["restored_jobs"] == 2
    assert repo.jobs[0]["status"] == "retry"
    assert repo.jobs[1]["status"] == "retry"
    assert repo.jobs[2]["status"] == "failed"
    assert repo.jobs[3]["status"] == "failed"


def test_recovery_updates_only_active_rules_for_tenant() -> None:
    repo = _Repo()
    service = RecoveryService(repo, SubscriptionService(repo))

    service.recover_after_payment(1, triggered_by_user_id=100)

    assert repo.rules[0]["next_run_at"] == "now"
    assert repo.rules[1]["next_run_at"] == "old"


def test_recovery_forbidden_for_inactive_subscription() -> None:
    repo = _Repo()
    repo.sub["status"] = "expired"
    service = RecoveryService(repo, SubscriptionService(repo))

    ok, reason = service.can_recover(1)
    result = service.recover_after_payment(1, triggered_by_user_id=100)

    assert ok is False
    assert reason == "Подписка ещё не активна"
    assert result["ok"] is False


def test_recovery_idempotent_on_repeat() -> None:
    repo = _Repo()
    service = RecoveryService(repo, SubscriptionService(repo))

    first = service.recover_after_payment(1, triggered_by_user_id=100)
    second = service.recover_after_payment(1, triggered_by_user_id=100)

    assert first["restored_jobs"] == 2
    assert second["restored_jobs"] == 0
    assert second["already_recovered"] is True


def test_user_recovery_text_has_no_technical_words() -> None:
    text = user_ui.build_user_recovery_summary_text(
        {
            "active_rules_count": 2,
            "pending_deliveries_count": 3,
            "failed_limit_jobs_count": 1,
            "failed_limit_video_jobs_count": 1,
            "last_blocked_events": [{"event_type": "limit_job_blocked"}],
        }
    )

    lowered = text.lower()
    for bad in ("worker", "tenant_id", "dedup", "lease"):
        assert bad not in lowered
