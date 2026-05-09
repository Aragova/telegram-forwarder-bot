from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.repost_campaign_scheduled_post_service import RepostCampaignScheduledPostService


@dataclass
class Rule:
    id: int
    mode: str = "repost"
    target_id: str | None = "-1001"
    target_thread_id: int | None = None
    target_title: str | None = "Main"


class FakeRepo:
    def __init__(self):
        self.rules = {}
        self.saved_posts = {}
        self.current_targets = []
        self.posts = {}
        self.post_targets = {}
        self.checks = {}
        self.events = {}
        self._id = 1

    def get_rule(self, rule_id): return self.rules.get(rule_id)
    def get_saved_post(self, saved_post_id): return self.saved_posts.get(saved_post_id)
    def create_campaign_scheduled_post_draft(self, *, rule_id, tenant_id=1, created_by=None, title=None, metadata=None):
        if getattr(self, "force_create_fail", False):
            return None
        sid = self._id; self._id += 1
        self.posts[sid] = {"id": sid, "rule_id": rule_id, "status": "draft", "saved_post_id": None, "show_seconds": 0, "scheduled_at": None}
        return sid
    def update_campaign_scheduled_post(self, scheduled_post_id, **kwargs): self.posts[scheduled_post_id].update(kwargs); return True
    def get_campaign_scheduled_post(self, scheduled_post_id): return self.posts.get(scheduled_post_id)
    def list_campaign_scheduled_posts(self, *, rule_id, statuses=None, limit=20): return [p for p in self.posts.values() if p["rule_id"]==rule_id][:limit]
    def list_rule_repost_campaign_targets(self, rule_id, active_only=True): return [dict(x) for x in self.current_targets]
    def replace_campaign_scheduled_post_targets(self, *, scheduled_post_id, rule_id, targets): self.post_targets[scheduled_post_id]=[dict(x) for x in targets]; return len(targets)
    def list_campaign_scheduled_post_targets(self, scheduled_post_id, *, active_only=True): return [dict(x) for x in self.post_targets.get(scheduled_post_id, [])]
    def update_campaign_scheduled_post_target_check_result(self, target_row_id, **kwargs):
        for sid, rows in self.post_targets.items():
            for r in rows:
                if int(r.get("id") or 0) == int(target_row_id): r.update(kwargs); return True
        return True
    def log_campaign_scheduled_post_check(self, **kwargs):
        assert kwargs["check_type"] in {"publish", "delete", "full"}
        assert kwargs["status"] in {"confirmed", "denied", "unknown", "failed"}
        self.checks.setdefault(kwargs["scheduled_post_id"], []).append(kwargs); return 1
    def list_campaign_scheduled_post_checks(self, scheduled_post_id, *, limit=50): return self.checks.get(scheduled_post_id, [])[:limit]
    def log_campaign_scheduled_post_event(self, **kwargs): self.events.setdefault(kwargs["scheduled_post_id"], []).append(kwargs); return 1
    def list_campaign_scheduled_post_events(self, scheduled_post_id, *, limit=50): return self.events.get(scheduled_post_id, [])[:limit]
    def schedule_campaign_scheduled_post(self, scheduled_post_id, *, scheduled_by=None):
        if self.posts[scheduled_post_id]["status"] in {"draft", "ready"}: self.posts[scheduled_post_id]["status"] = "scheduled"; return True
        return False
    def cancel_campaign_scheduled_post(self, scheduled_post_id, *, cancelled_by=None, reason=None):
        if self.posts[scheduled_post_id]["status"] in {"draft", "ready", "scheduled"}: self.posts[scheduled_post_id]["status"] = "cancelled"; return True
        return False


class FakeCampaignRuntime:
    def __init__(self, active=False): self.active = active
    def build_campaign_launch_readiness(self, *, rule_id: int) -> dict:
        return {"active_placement": self.active, "delete_failed": 0}


class CheckResult:
    def __init__(self, can_publish=True, publish_status="confirmed", delete_status="unknown"):
        self.can_publish = can_publish; self.can_delete = True; self.publish_status = publish_status; self.delete_status = delete_status
        self.source = "fake"; self.details = {}


class FakeTargetChecker:
    def __init__(self, denied=False): self.called = 0; self.denied = denied
    async def check_target(self, **kwargs):
        self.called += 1
        return CheckResult(can_publish=not self.denied, publish_status="denied" if self.denied else "confirmed")


def make_service(active=False, checker=None):
    repo = FakeRepo(); repo.rules[1] = Rule(1)
    return RepostCampaignScheduledPostService(repo=repo, campaign_runtime=FakeCampaignRuntime(active=active), target_checker=checker), repo


def test_create_draft_requires_existing_repost_rule():
    service, repo = make_service()
    out = service.create_draft(rule_id=100)
    assert not out.ok
    repo.rules[2] = Rule(2, mode="video")
    out2 = service.create_draft(rule_id=2)
    assert not out2.ok
    out3 = service.create_draft(rule_id=1)
    assert out3.ok
    assert repo.events[out3.extra["scheduled_post_id"]][-1]["event_type"] == "draft_created"


def test_update_saved_post_stores_snapshot():
    service, repo = make_service()
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.saved_posts[7] = {"id": 7, "title": "ad", "content_json": {"kind": "album", "media_items": [1, 2]}}
    out = service.update_draft_saved_post(scheduled_post_id=sid, saved_post_id=7)
    assert out.ok
    snap = repo.posts[sid]["post_snapshot"]
    assert snap["id"] == 7 and snap["kind"] == "album" and snap["media_items_count"] == 2


def test_update_targets_from_current_campaign_creates_independent_snapshot():
    service, repo = make_service()
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.current_targets = [{"id": 10, "target_id": "-200", "target_thread_id": 1, "title": "Extra"}]
    out = service.update_draft_targets_from_current_campaign(scheduled_post_id=sid)
    assert out.ok and out.extra["targets_total"] == 2
    repo.current_targets[0]["target_id"] = "-999"
    assert repo.post_targets[sid][1]["target_id"] == "-200"


def test_update_show_seconds_validates_positive_value():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    assert not service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=0).ok
    assert service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=60).ok


def test_update_scheduled_at_requires_future_time():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    now = datetime.now(timezone.utc)
    assert not service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=now - timedelta(minutes=1)).ok
    assert not service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=now + timedelta(seconds=30)).ok
    assert service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=now + timedelta(minutes=2)).ok


def test_build_readiness_blocks_missing_post_targets_time():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    rd = service.build_readiness(scheduled_post_id=sid)
    assert "Рекламный пост не выбран" in rd["block_reasons"]
    assert "Каналы/группы не выбраны" in rd["block_reasons"]
    assert "Срок показа не задан" in rd["block_reasons"]
    assert "Время запуска не задано" in rd["block_reasons"]


def test_build_readiness_ready_with_warnings():
    service, repo = make_service(active=True); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.saved_posts[1] = {"id": 1, "title": "t", "content_json": {"kind": "text", "media_items": []}}
    service.update_draft_saved_post(scheduled_post_id=sid, saved_post_id=1)
    service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=60)
    service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=datetime.now(timezone.utc)+timedelta(minutes=2))
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "publish_status": "unknown", "delete_status": "unknown"}]
    rd = service.build_readiness(scheduled_post_id=sid)
    assert rd["can_schedule"] and rd["targets_warning_count"] > 0 and rd["targets_blocked_count"] == 0 and rd["warnings"]


def test_build_readiness_blocks_denied_publish_target():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.saved_posts[1] = {"id": 1, "title": "t", "content_json": {"kind": "text", "media_items": []}}
    service.update_draft_saved_post(scheduled_post_id=sid, saved_post_id=1)
    service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=60)
    service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=datetime.now(timezone.utc)+timedelta(minutes=2))
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "publish_status": "denied", "delete_status": "confirmed", "can_publish": False}]
    rd = service.build_readiness(scheduled_post_id=sid)
    assert rd["targets_blocked_count"] == 1 and not rd["can_schedule"]


def test_schedule_post_requires_readiness():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    assert not service.schedule_post(scheduled_post_id=sid).ok
    repo.saved_posts[1] = {"id": 1, "title": "t", "content_json": {"kind": "text", "media_items": []}}
    service.update_draft_saved_post(scheduled_post_id=sid, saved_post_id=1)
    service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=60)
    service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=datetime.now(timezone.utc)+timedelta(minutes=2))
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "publish_status": "confirmed", "delete_status": "confirmed", "can_publish": True}]
    ok = service.schedule_post(scheduled_post_id=sid)
    assert ok.ok and repo.posts[sid]["status"] == "scheduled"


def test_cancel_post_policy():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    assert service.cancel_post(scheduled_post_id=sid).ok
    sid2 = service.create_draft(rule_id=1).extra["scheduled_post_id"]; repo.posts[sid2]["status"] = "scheduled"
    assert service.cancel_post(scheduled_post_id=sid2).ok
    sid3 = service.create_draft(rule_id=1).extra["scheduled_post_id"]; repo.posts[sid3]["status"] = "processing"
    assert not service.cancel_post(scheduled_post_id=sid3).ok


def test_check_targets_updates_snapshot_and_history():
    checker = FakeTargetChecker()
    service, repo = make_service(checker=checker); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "target_thread_id": None, "publish_status": "unknown", "delete_status": "unknown"}]
    import asyncio
    out = asyncio.run(service.check_targets(scheduled_post_id=sid))
    assert out.ok and checker.called == 1
    assert repo.list_campaign_scheduled_post_checks(sid)
    assert any(x["event_type"] == "targets_checked" for x in repo.events[sid])


def test_get_post_details_returns_post_targets_checks_events_readiness():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1"}]
    repo.log_campaign_scheduled_post_check(scheduled_post_id=sid, rule_id=1, target_id="-1", target_thread_id=None, check_type="full", status="confirmed")
    repo.log_campaign_scheduled_post_event(scheduled_post_id=sid, rule_id=1, event_type="draft_created")
    d = service.get_post_details(scheduled_post_id=sid)
    assert d and {"post", "targets", "checks", "events", "readiness"}.issubset(set(d.keys()))


def test_create_draft_returns_error_on_repo_create_failure():
    service, repo = make_service()
    repo.force_create_fail = True
    out = service.create_draft(rule_id=1)
    assert not out.ok
    assert out.error_text == "Не удалось создать черновик запланированного поста"


class BoomChecker:
    async def check_target(self, **kwargs):
        raise RuntimeError("boom")


def test_check_targets_logs_failed_status_on_checker_exception():
    import asyncio
    service, repo = make_service(checker=BoomChecker())
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "target_thread_id": None}]
    out = asyncio.run(service.check_targets(scheduled_post_id=sid))
    assert out.ok
    assert repo.checks[sid][-1]["check_type"] == "full"
    assert repo.checks[sid][-1]["status"] == "failed"
