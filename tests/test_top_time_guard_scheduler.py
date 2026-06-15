import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from app.repository_models import GLOBAL_INTERVAL_GAP_SECONDS
from app.scheduler_runtime import scheduler_tick


class FakeRepo:
    def __init__(self, *, mode="repost", pause=None):
        self.rule = SimpleNamespace(id=7, is_active=True, mode=mode, target_id="-1001", target_thread_id=None)
        self.pause = pause
        self.next_run_at = None
        self.jobs = []
        self.audit = []
        self.delivery_status = "pending"
        self.faulty_count = 0

    def get_all_rules(self):
        return [self.rule]

    def get_due_delivery(self, rule_id, due_iso):
        if self.next_run_at and self.next_run_at > due_iso:
            return None
        return {"delivery_id": 101, "tenant_id": 1, "media_group_id": None}

    def get_active_campaign_top_time_pause_for_target(self, **kwargs):
        return self.pause

    def update_rule_next_run_at(self, rule_id, next_run_iso):
        self.next_run_at = next_run_iso
        return True

    def log_event(self, **kwargs):
        self.audit.append(kwargs)

    def get_tenant_job_counts(self, queue="light"):
        return {}

    def get_tenant_processing_counts(self, queue="light"):
        return {}

    def get_tenant_retry_counts(self, queue="light"):
        return {}

    def take_due_delivery_and_create_job(self, rule_id, due_iso):
        self.delivery_status = "processing"
        self.jobs.append((rule_id, due_iso))
        return {"status": "created", "job_id": 55, "delivery_id": 101, "tenant_id": 1}


def pause_until(hours=1):
    ends_at = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=hours)).isoformat()
    return {"id": 10 + hours, "target_id": "-1001", "target_thread_id": None, "ends_at": ends_at}


def expected_next(ends_at):
    return (datetime.fromisoformat(ends_at) + timedelta(seconds=GLOBAL_INTERVAL_GAP_SECONDS)).isoformat()


def test_scheduler_postpones_repost_rule_when_top_time_pause_active():
    pause = pause_until(1)
    repo = FakeRepo(mode="repost", pause=pause)

    result = asyncio.run(scheduler_tick(repo, now_iso="2026-01-01T00:00:00+00:00"))

    assert result["created"] == 0
    assert repo.jobs == []
    assert repo.delivery_status == "pending"
    assert repo.next_run_at == expected_next(pause["ends_at"])
    assert repo.audit[0]["event_type"] == "top_time_auto_postponed"


def test_scheduler_postpones_video_rule_when_top_time_pause_active():
    pause = pause_until(2)
    repo = FakeRepo(mode="video", pause=pause)

    result = asyncio.run(scheduler_tick(repo, now_iso="2026-01-01T00:00:00+00:00"))

    assert result["created"] == 0
    assert repo.jobs == []
    assert repo.next_run_at == expected_next(pause["ends_at"])


def test_scheduler_allows_when_no_pause():
    repo = FakeRepo(mode="repost", pause=None)

    result = asyncio.run(scheduler_tick(repo, now_iso="2026-01-01T00:00:00+00:00"))

    assert result["created"] == 1
    assert repo.jobs


def test_scheduler_does_not_mark_delivery_failed_on_pause():
    repo = FakeRepo(mode="repost", pause=pause_until(1))

    asyncio.run(scheduler_tick(repo, now_iso="2026-01-01T00:00:00+00:00"))

    assert repo.delivery_status == "pending"
    assert repo.faulty_count == 0


def test_scheduler_uses_latest_pause_end():
    pause = pause_until(3)
    repo = FakeRepo(mode="repost", pause=pause)

    asyncio.run(scheduler_tick(repo, now_iso="2026-01-01T00:00:00+00:00"))

    assert repo.next_run_at == expected_next(pause["ends_at"])


def test_send_now_does_not_call_top_time_guard():
    source = Path("app/user_status_handlers.py").read_text()
    assert "TopTimeGuardService" not in source
    assert "get_active_campaign_top_time_pause_for_target" not in source


def test_vip_scheduled_posts_do_not_call_top_time_guard():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text()
    assert "TopTimeGuardService" not in source
    assert "get_active_campaign_top_time_pause_for_target" not in source


def test_campaign_runtime_does_not_lookup_active_top_time_pause():
    source = Path("app/repost_campaign_runtime_service.py").read_text()
    assert "get_active_campaign_top_time_pause_for_target" not in source
    assert "TopTimeGuardService" not in source


def test_top_time_postpone_markers_are_only_in_scheduler_area():
    allowed = {"app/scheduler_runtime.py", "app/sender.py"}
    offenders = []
    for path in Path("app").rglob("*.py"):
        text = path.read_text()
        if "top_time_auto_postponed" in text or "TOP_TIME_GUARD_BLOCKED_AUTO_POST" in text:
            rel = path.as_posix()
            if rel not in allowed:
                offenders.append(rel)
    assert offenders == []
