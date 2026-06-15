from datetime import datetime, timedelta, timezone

from app.repost_campaign_top_time_view_service import RepostCampaignTopTimeViewService


class FakeRepo:
    def __init__(self, active=None, by_run=None, fail_active=False):
        self.active = active or []
        self.by_run = by_run or []
        self.fail_active = fail_active

    def list_active_campaign_top_time_pauses_for_rule(self, rule_id, *, limit=50):
        if self.fail_active:
            raise RuntimeError("boom")
        return self.active[:limit]

    def list_campaign_top_time_pauses_for_run(self, campaign_run_id, *, limit=100):
        return self.by_run[:limit]


def test_build_active_pauses_for_rule_empty():
    state = RepostCampaignTopTimeViewService(FakeRepo()).build_active_pauses_for_rule(10)
    assert state["ok"] is True
    assert state["total"] == 0
    assert state["pauses"] == []


def test_build_active_pauses_for_rule_formats_items():
    ends_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    state = RepostCampaignTopTimeViewService(FakeRepo(active=[{"campaign_run_id": 154, "target_title": "Wikiboy’s", "ends_at": ends_at}])).build_active_pauses_for_rule(10)
    item = state["pauses"][0]
    assert item["title_text"] == "Wikiboy’s"
    assert item["remaining_text"]
    assert item["ends_at_text"]
    assert item["open_run_callback"] == "rule_repost_campaign_history_detail:10:154"


def test_build_active_pauses_for_rule_fail_safe():
    state = RepostCampaignTopTimeViewService(FakeRepo(fail_active=True)).build_active_pauses_for_rule(10)
    assert state["ok"] is False
    assert state["error_text"]


def test_build_run_top_time_summary_active():
    earlier = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    later = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    state = RepostCampaignTopTimeViewService(FakeRepo(by_run=[
        {"campaign_run_id": 154, "status": "active", "ends_at": earlier},
        {"campaign_run_id": 154, "status": "active", "ends_at": later},
    ])).build_run_top_time_summary(154, top_time_enabled_snapshot=True, top_time_seconds_snapshot=7200)
    assert state["active_count"] == 2
    assert state["latest_ends_at"]
    assert state["latest_ends_at_text"]


def test_build_run_top_time_summary_disabled_snapshot():
    state = RepostCampaignTopTimeViewService(FakeRepo()).build_run_top_time_summary(154, top_time_enabled_snapshot=False)
    assert state["status_text"] == "🔴 выключено для этого запуска"
    assert state["active_count"] == 0
