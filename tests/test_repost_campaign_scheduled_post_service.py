from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
        self.launched_calls = []
        self.failed_calls = []
        self.delay_calls = []

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
        if getattr(self, "force_schedule_fail", False):
            return False
        if self.posts[scheduled_post_id]["status"] in {"draft", "ready"}: self.posts[scheduled_post_id]["status"] = "scheduled"; return True
        return False
    def cancel_campaign_scheduled_post(self, scheduled_post_id, *, cancelled_by=None, reason=None):
        if getattr(self, "force_cancel_fail", False):
            return False
        if self.posts[scheduled_post_id]["status"] in {"draft", "ready", "scheduled"}: self.posts[scheduled_post_id]["status"] = "cancelled"; return True
        return False
    def reset_stuck_campaign_scheduled_posts(self, *, stuck_seconds=300): return 0
    def claim_due_campaign_scheduled_posts(self, **kwargs): return []
    def mark_campaign_scheduled_post_launched(self, scheduled_post_id, *, campaign_run_id):
        self.launched_calls.append((scheduled_post_id, campaign_run_id)); return True
    def mark_campaign_scheduled_post_failed(self, scheduled_post_id, *, error_text, campaign_run_id=None):
        self.failed_calls.append((scheduled_post_id, error_text, campaign_run_id)); return True
    def delay_campaign_scheduled_post_retry(self, scheduled_post_id, *, next_retry_at, error_text=None):
        self.delay_calls.append((scheduled_post_id, next_retry_at, error_text)); return True
    def delay_campaign_scheduled_post_until(self, scheduled_post_id, *, next_retry_at, error_text=None):
        self.delay_calls.append((scheduled_post_id, next_retry_at, error_text, "until")); return True


class FakeCampaignRuntime:
    def __init__(self, active=False): self.active = active
    def build_campaign_launch_readiness(self, *, rule_id: int) -> dict:
        return {"active_placement": self.active, "delete_failed": 0, "next_available_at": "2026-05-09T20:49:30+00:00", "active_delete_after_text": "09.05 23:49 UTC+3", "active_run_id": 22}


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


def test_schedule_post_does_not_log_scheduled_event_when_repo_transition_fails():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.saved_posts[1] = {"id": 1, "title": "t", "content_json": {"kind": "text", "media_items": []}}
    service.update_draft_saved_post(scheduled_post_id=sid, saved_post_id=1)
    service.update_draft_show_seconds(scheduled_post_id=sid, show_seconds=60)
    service.update_draft_scheduled_at(scheduled_post_id=sid, scheduled_at_utc=datetime.now(timezone.utc)+timedelta(minutes=2))
    repo.post_targets[sid] = [{"id": 1, "target_id": "-1", "publish_status": "confirmed", "delete_status": "confirmed", "can_publish": True}]
    repo.force_schedule_fail = True
    out = service.schedule_post(scheduled_post_id=sid)
    assert not out.ok
    assert not any(x["event_type"] == "scheduled" for x in repo.events.get(sid, []))


def test_cancel_post_does_not_log_cancelled_event_when_repo_transition_fails():
    service, repo = make_service(); sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.force_cancel_fail = True
    out = service.cancel_post(scheduled_post_id=sid)
    assert not out.ok

def test_add_manual_target_preserves_existing_targets():
    service, repo = make_service()
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.post_targets[sid] = [{"target_kind": "extra", "target_id": "-1001", "target_thread_id": None, "target_title": "A", "is_active": True}]
    out = service.add_manual_target(scheduled_post_id=sid, target_id="-1002", target_title="B")
    assert out.ok
    assert len(repo.post_targets[sid]) == 2
    assert any(x["target_id"] == "-1001" for x in repo.post_targets[sid])
    assert any(x["target_id"] == "-1002" for x in repo.post_targets[sid])

def test_add_manual_target_deduplicates_same_target():
    service, repo = make_service()
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.post_targets[sid] = [{"target_kind": "extra", "target_id": "-1001", "target_thread_id": None, "target_title": "A", "is_active": True}]
    out = service.add_manual_target(scheduled_post_id=sid, target_id="-1001", target_title="A")
    assert out.ok
    assert out.extra.get("already_exists") is True
    assert len(repo.post_targets[sid]) == 1

def test_add_manual_target_rejects_non_editable_status():
    service, repo = make_service()
    sid = service.create_draft(rule_id=1).extra["scheduled_post_id"]
    repo.posts[sid]["status"] = "scheduled"
    out = service.add_manual_target(scheduled_post_id=sid, target_id="-1001")
    assert not out.ok
    assert not any(x["event_type"] == "cancelled" for x in repo.events.get(sid, []))


class FakeRuntimeLaunch(FakeCampaignRuntime):
    def __init__(self, result, active=False):
        super().__init__(active=active)
        self.result = result
        self.calls = []
    async def launch_campaign_from_snapshot(self, **kwargs):
        self.calls.append(kwargs)
        return self.result

def test_process_due_posts_claims_and_launches_snapshot():
    import asyncio
    repo = FakeRepo(); repo.rules[1] = Rule(1); repo.saved_posts[7] = {"id": 7}
    repo.posts[1] = {"id": 1, "rule_id": 1, "saved_post_id": 7, "show_seconds": 60, "status": "processing", "campaign_run_id": None}
    repo.post_targets[1] = [{"target_id": "-1001", "target_kind": "main"}]
    repo.reset_stuck_campaign_scheduled_posts = lambda stuck_seconds=300: 0
    repo.claim_due_campaign_scheduled_posts = lambda **kwargs: [repo.posts[1]]
    marks = []
    repo.mark_campaign_scheduled_post_launched = lambda sid, campaign_run_id: marks.append((sid, campaign_run_id)) or True
    repo.mark_campaign_scheduled_post_failed = lambda *a, **k: False
    repo.delay_campaign_scheduled_post_retry = lambda *a, **k: False
    rt = FakeRuntimeLaunch(type('R',(),{'ok':True,'extra':{'campaign_run_id': 77},'error_text':None})())
    service = RepostCampaignScheduledPostService(repo=repo, campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w1'))
    assert rt.calls
    assert marks == [(1,77)]


def _mk_runtime_result(ok, run_id=None, error_text=None):
    return type("R", (), {"ok": ok, "extra": {"campaign_run_id": run_id} if run_id else {}, "error_text": error_text})()


def test_process_due_posts_delays_when_active_placement():
    import asyncio
    repo = FakeRepo(); repo.rules[1]=Rule(1); repo.saved_posts[7]={"id":7}
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":0}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(True,77), active=True)
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert not rt.calls and repo.delay_calls
    assert any(len(call) == 4 and call[3] == "until" for call in repo.delay_calls)
    assert not repo.failed_calls


def test_process_due_posts_marks_failed_for_missing_saved_post():
    import asyncio
    repo = FakeRepo(); repo.rules[1]=Rule(1)
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":0}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(True,77))
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert repo.failed_calls and not rt.calls


def test_process_due_posts_never_retries_when_campaign_run_id_exists():
    import asyncio
    repo = FakeRepo(); row={"id":1,"rule_id":1,"campaign_run_id":123,"attempt_count":0}
    repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(True,77))
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert not rt.calls and not repo.delay_calls


def test_process_due_posts_failed_launch_with_campaign_run_id_marks_failed_without_retry():
    import asyncio
    repo=FakeRepo(); repo.rules[1]=Rule(1); repo.saved_posts[7]={"id":7}
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":0}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(False,777,'boom'))
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert repo.failed_calls and not repo.delay_calls


def test_process_due_posts_failed_launch_without_run_id_delays_retry():
    import asyncio
    repo=FakeRepo(); repo.rules[1]=Rule(1); repo.saved_posts[7]={"id":7}
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":0}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(False,None,'boom'))
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert repo.delay_calls


def test_process_due_posts_runtime_exception_delays_retry():
    import asyncio
    class X(FakeCampaignRuntime):
        async def launch_campaign_from_snapshot(self, **kwargs):
            raise RuntimeError('boom')
    repo=FakeRepo(); repo.rules[1]=Rule(1); repo.saved_posts[7]={"id":7}
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":0}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=X())
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert repo.delay_calls


def test_process_due_posts_max_attempts_marks_failed():
    import asyncio
    repo=FakeRepo(); repo.rules[1]=Rule(1); repo.saved_posts[7]={"id":7}
    row={"id":1,"rule_id":1,"saved_post_id":7,"show_seconds":60,"status":"processing","campaign_run_id":None,"attempt_count":5}
    repo.post_targets[1]=[{"target_id":"-1"}]; repo.claim_due_campaign_scheduled_posts=lambda **k:[row]
    rt=FakeRuntimeLaunch(_mk_runtime_result(False,None,'boom'))
    service=RepostCampaignScheduledPostService(repo=repo,campaign_runtime=rt)
    asyncio.run(service.process_due_posts(worker_id='w'))
    assert repo.failed_calls and not repo.delay_calls


def test_scheduled_post_loop_runs_process_due_posts_and_stops():
    import asyncio
    from app.repost_campaign_scheduled_post_service import run_repost_campaign_scheduled_post_loop
    class R:
        def __init__(self):
            import logging
            self.calls=0
            self.logger=logging.getLogger('t')
        async def process_due_posts(self, worker_id):
            self.calls += 1
    runtime=R(); stop=asyncio.Event()
    async def _run():
        task=asyncio.create_task(run_repost_campaign_scheduled_post_loop(runtime=runtime, stop_event=stop, interval_seconds=0.01, worker_id='w'))
        await asyncio.sleep(0.03); stop.set(); await task
    asyncio.run(_run())
    assert runtime.calls > 0


def test_vip_scheduled_post_loop_has_startup_log():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    assert "VIP_SCHEDULED_POST_LOOP_STARTED" in source
    assert "worker_id" in source
    assert "interval_seconds" in source


def test_vip_scheduled_post_process_due_posts_has_runtime_logs():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    assert "VIP_SCHEDULED_POST_DUE_CLAIMED" in source
    assert "VIP_SCHEDULED_POST_LAUNCH_STARTED" in source
    assert "VIP_SCHEDULED_POST_LAUNCH_FINISHED" in source
