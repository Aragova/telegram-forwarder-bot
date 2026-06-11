from datetime import datetime, timedelta, timezone
import asyncio
from pathlib import Path

from app.repost_campaign_runtime_service import RepostCampaignActionResult
from app.repost_campaign_schedule_service import (
    RepostCampaignScheduleService,
    format_campaign_schedule_datetime,
    parse_campaign_schedule_input_to_utc,
)


class FR:
    def __init__(self):
        self.rows=[]
        self.claim=[]
        self.launches={}
        self.events=[]
        self.reset_result={"requeued": 0, "needs_review": 0}

    def create_campaign_scheduled_launch(self, **kw):
        self.rows.append(kw)
        return 1

    def reset_stuck_campaign_scheduled_launches(self, **kw):
        now=datetime.now(timezone.utc)
        summary={"requeued": 0, "needs_review": 0}
        for row in self.launches.values():
            if row.get("status") != "processing":
                continue
            locked_at = row.get("locked_at") or now
            if locked_at >= now - timedelta(seconds=int(kw.get("stuck_seconds") or 300)):
                continue
            if row.get("campaign_run_id"):
                row.update(status="needs_review", error_text=row.get("error_text") or "Запуск завис после создания campaign_run", locked_at=None, locked_by=None)
                summary["needs_review"] += 1
            else:
                row.update(status="scheduled", locked_at=None, locked_by=None)
                summary["requeued"] += 1
        if summary != {"requeued": 0, "needs_review": 0}:
            return summary
        return self.reset_result

    def claim_due_campaign_scheduled_launches(self, **kw):
        return self.claim

    def get_campaign_scheduled_launch(self, scheduled_launch_id):
        return self.launches.get(int(scheduled_launch_id), {})

    def set_campaign_scheduled_launch_campaign_run_id(self, scheduled_launch_id, campaign_run_id):
        self.events.append(("set_run_id", int(scheduled_launch_id), int(campaign_run_id)))
        row = self.launches.setdefault(int(scheduled_launch_id), {"id": int(scheduled_launch_id), "status": "processing"})
        row["campaign_run_id"] = int(campaign_run_id)
        return True

    def mark_campaign_scheduled_launch_failed(self, scheduled_launch_id, *, error_text):
        self.events.append(("failed", int(scheduled_launch_id), error_text))
        row = self.launches.setdefault(int(scheduled_launch_id), {"id": int(scheduled_launch_id)})
        row.update(status="failed", error_text=error_text)
        return True

    def mark_campaign_scheduled_launch_launched(self, scheduled_launch_id, *, campaign_run_id):
        self.events.append(("launched", int(scheduled_launch_id), int(campaign_run_id)))
        row = self.launches.setdefault(int(scheduled_launch_id), {"id": int(scheduled_launch_id)})
        row.update(status="launched", campaign_run_id=int(campaign_run_id))
        return True

    def mark_campaign_scheduled_launch_needs_review(self, scheduled_launch_id, *, error_text, campaign_run_id=None):
        self.events.append(("needs_review", int(scheduled_launch_id), error_text, campaign_run_id))
        row = self.launches.setdefault(int(scheduled_launch_id), {"id": int(scheduled_launch_id)})
        row.update(status="needs_review", error_text=error_text)
        if campaign_run_id is not None:
            row["campaign_run_id"] = int(campaign_run_id)
        return True


class RT:
    def __init__(self, *, result=None, exc=None, callback_run_id=None, readiness=None):
        self.result = result
        self.exc = exc
        self.callback_run_id = callback_run_id
        self.readiness = readiness
        self.launch_kwargs = []
        self.readiness_kwargs = []

    def build_campaign_launch_readiness(self, **kw):
        self.readiness_kwargs.append(kw)
        if self.readiness is not None:
            return dict(self.readiness)
        return {"can_launch": True, "saved_post_id": 10, "show_seconds": 3600}

    async def launch_campaign_now(self, **kw):
        self.launch_kwargs.append(kw)
        if self.callback_run_id is not None:
            kw["on_campaign_run_created"](self.callback_run_id)
        if self.exc:
            raise self.exc
        return self.result or RepostCampaignActionResult(ok=True, action="launch_campaign", rule_id=kw["rule_id"], extra={"campaign_run_id": self.callback_run_id or 55})


def _repo_with_claim():
    repo=FR()
    row={"id": 1, "rule_id": 7, "created_by": 99, "status": "processing"}
    repo.claim=[row]
    repo.launches[1]=dict(row)
    return repo


def test_parse_campaign_schedule_input_to_utc():
    now=datetime(2026,5,8,10,0,tzinfo=timezone.utc)
    dt=parse_campaign_schedule_input_to_utc('09.05 18:00', now_utc=now)
    assert dt is not None


def test_format_campaign_schedule_datetime():
    dt=datetime(2026,5,9,15,0,tzinfo=timezone.utc)
    assert 'UTC+3' in format_campaign_schedule_datetime(dt)


def test_schedule_campaign_launch_saves_row_not_launch_now():
    repo=FR(); rt=RT(); svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    res=svc.schedule_campaign_launch(rule_id=1, scheduled_at_utc=datetime(2026,5,9,15,0,tzinfo=timezone.utc), created_by=7)
    assert res.ok and repo.rows


def test_schedule_campaign_launch_old_behavior_rejects_not_ready_without_policy():
    repo=FR(); rt=RT(readiness={"can_launch": False, "saved_post_id": 10, "show_seconds": 3600})
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)

    res=svc.schedule_campaign_launch(rule_id=1, scheduled_at_utc=datetime(2026,5,9,15,0,tzinfo=timezone.utc), created_by=7)

    assert not res.ok
    assert res.error_text == "Кампания не готова к запуску"
    assert len(rt.readiness_kwargs) == 1
    assert repo.rows == []


def test_schedule_campaign_launch_accepts_can_schedule_policy():
    repo=FR(); rt=RT(readiness={"can_launch": False, "saved_post_id": 999, "show_seconds": 0})
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    policy = {
        "ok": True,
        "action": "schedule_with_clean_channel_wait",
        "can_schedule": True,
        "base_readiness": {"can_launch": False, "saved_post_id": 100, "show_seconds": 3600},
    }

    res=svc.schedule_campaign_launch(
        rule_id=1,
        scheduled_at_utc=datetime(2026,5,9,15,0,tzinfo=timezone.utc),
        created_by=7,
        scheduled_policy=policy,
    )

    assert res.ok and repo.rows
    assert repo.rows[0]["saved_post_id"] == 100
    assert repo.rows[0]["preview"]["scheduled_policy"] is policy
    assert res.extra["scheduled_policy"]["action"] == "schedule_with_clean_channel_wait"


def test_schedule_campaign_launch_rejects_policy_error():
    repo=FR(); rt=RT()
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    policy = {
        "ok": False,
        "action": "policy_error",
        "can_schedule": False,
        "blocking_text": "Не удалось проверить Чистый канал",
        "base_readiness": {"can_launch": True, "saved_post_id": 100, "show_seconds": 3600},
    }

    res=svc.schedule_campaign_launch(
        rule_id=1,
        scheduled_at_utc=datetime(2026,5,9,15,0,tzinfo=timezone.utc),
        created_by=7,
        scheduled_policy=policy,
    )

    assert not res.ok
    assert repo.rows == []
    assert res.extra["scheduled_policy"] is policy
    assert res.extra["launch_readiness"] == policy["base_readiness"]


def test_campaign_run_id_saved_before_mark_launched():
    repo=_repo_with_claim(); rt=RT(callback_run_id=123)
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    asyncio.run(svc.process_due_scheduled_launches(worker_id="w"))
    assert repo.launches[1]["campaign_run_id"] == 123
    assert repo.events[:2] == [("set_run_id", 1, 123), ("launched", 1, 123)]
    assert rt.launch_kwargs[0]["on_campaign_run_created"]


def test_exception_after_campaign_run_moves_to_needs_review():
    repo=_repo_with_claim(); rt=RT(callback_run_id=123, exc=RuntimeError("boom"))
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    asyncio.run(svc.process_due_scheduled_launches(worker_id="w"))
    assert repo.launches[1]["status"] == "needs_review"
    assert repo.launches[1]["campaign_run_id"] == 123
    assert not [event for event in repo.events if event[0] == "failed"]


def test_not_ok_with_campaign_run_moves_to_needs_review():
    result=RepostCampaignActionResult(ok=False, action="launch_campaign", rule_id=7, error_text="bad", extra={"campaign_run_id": 123})
    repo=_repo_with_claim(); rt=RT(result=result, callback_run_id=123)
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    asyncio.run(svc.process_due_scheduled_launches(worker_id="w"))
    assert repo.launches[1]["status"] == "needs_review"
    assert repo.launches[1]["campaign_run_id"] == 123
    assert not [event for event in repo.events if event[0] == "failed"]


def test_not_ok_without_campaign_run_moves_to_failed():
    result=RepostCampaignActionResult(ok=False, action="launch_campaign", rule_id=7, error_text="bad", extra={})
    repo=_repo_with_claim(); rt=RT(result=result)
    svc=RepostCampaignScheduleService(repo=repo,campaign_runtime=rt)
    asyncio.run(svc.process_due_scheduled_launches(worker_id="w"))
    assert repo.launches[1]["status"] == "failed"
    assert repo.launches[1]["error_text"] == "bad"
    assert not [event for event in repo.events if event[0] == "needs_review"]


def test_recovery_stale_processing_with_campaign_run_moves_to_needs_review():
    repo=FR()
    repo.launches[1]={"id": 1, "status": "processing", "campaign_run_id": 123, "locked_at": datetime.now(timezone.utc)-timedelta(seconds=400)}
    summary=repo.reset_stuck_campaign_scheduled_launches(stuck_seconds=300)
    assert summary == {"requeued": 0, "needs_review": 1}
    assert repo.launches[1]["status"] == "needs_review"


def test_recovery_stale_processing_without_campaign_run_returns_to_scheduled():
    repo=FR()
    repo.launches[1]={"id": 1, "status": "processing", "campaign_run_id": None, "locked_at": datetime.now(timezone.utc)-timedelta(seconds=400)}
    summary=repo.reset_stuck_campaign_scheduled_launches(stuck_seconds=300)
    assert summary == {"requeued": 1, "needs_review": 0}
    assert repo.launches[1]["status"] == "scheduled"


def test_source_guard_for_scheduled_launch_idempotency():
    source = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    assert "on_campaign_run_created" in source
    assert "set_campaign_scheduled_launch_campaign_run_id" in source
    assert "mark_campaign_scheduled_launch_needs_review" in source
