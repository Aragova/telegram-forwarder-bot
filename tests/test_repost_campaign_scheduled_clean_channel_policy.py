from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.repost_campaign_schedule_service import RepostCampaignScheduleService


SCHEDULED_AT = datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc)


class FakeRuntime:
    def __init__(self, *, can_launch: bool = True):
        self.can_launch = can_launch
        self.readiness_calls: list[dict[str, Any]] = []

    def build_campaign_launch_readiness(self, **kwargs) -> dict[str, Any]:
        self.readiness_calls.append(kwargs)
        return {
            "ok": True,
            "rule_id": kwargs.get("rule_id"),
            "can_launch": self.can_launch,
            "saved_post_id": 100,
            "show_seconds": 3600,
            "block_reasons": [] if self.can_launch else ["Пост не выбран"],
            "warnings": [],
        }


class FakeRepo:
    def __init__(
        self,
        *,
        placements: list[dict[str, Any]] | None = None,
        summary: dict[str, Any] | None = None,
        clean_channel_settings: dict[str, Any] | None = None,
        fail_active_placements: bool = False,
    ):
        self.placements = placements or []
        self.summary = summary if summary is not None else _summary_from_placements(self.placements)
        self.clean_channel_settings = clean_channel_settings if clean_channel_settings is not None else {
            "ok": True,
            "rule_id": 10,
            "enabled": True,
        }
        self.fail_active_placements = fail_active_placements
        self.clean_channel_settings_calls: list[int] = []
        self.list_active_calls: list[dict[str, Any]] = []
        self.summary_calls: list[dict[str, Any]] = []
        self.write_calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def get_rule_repost_campaign_clean_channel_settings(self, rule_id: int) -> dict[str, Any] | None:
        self.clean_channel_settings_calls.append(rule_id)
        return self.clean_channel_settings

    def list_active_campaign_placements_for_rule(self, rule_id: int, *, limit: int = 20, basic_only: bool = True) -> list[dict[str, Any]]:
        if self.fail_active_placements:
            raise RuntimeError("db is unavailable")
        self.list_active_calls.append({"rule_id": rule_id, "limit": limit, "basic_only": basic_only})
        return self.placements[:limit]

    def get_active_campaign_placements_summary_for_rule(self, rule_id: int, *, basic_only: bool = True) -> dict[str, Any]:
        if self.fail_active_placements:
            raise RuntimeError("db is unavailable")
        self.summary_calls.append({"rule_id": rule_id, "basic_only": basic_only})
        return dict(self.summary)

    def create_campaign_scheduled_launch(self, *args, **kwargs):
        self.write_calls.append(("create_campaign_scheduled_launch", args, kwargs))
        return 1

    def mark_campaign_scheduled_launch_failed(self, *args, **kwargs):
        self.write_calls.append(("mark_campaign_scheduled_launch_failed", args, kwargs))
        return True

    def mark_campaign_scheduled_launch_launched(self, *args, **kwargs):
        self.write_calls.append(("mark_campaign_scheduled_launch_launched", args, kwargs))
        return True

    def mark_campaign_scheduled_launch_needs_review(self, *args, **kwargs):
        self.write_calls.append(("mark_campaign_scheduled_launch_needs_review", args, kwargs))
        return True

    def create_campaign_run(self, *args, **kwargs):
        self.write_calls.append(("create_campaign_run", args, kwargs))
        return 1


def _placement(
    run_id: int,
    *,
    placement_status: str = "active",
    delete_pending: int = 0,
    delete_processing: int = 0,
    delete_failed: int = 0,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "rule_id": 10,
        "saved_post_id": 100 + run_id,
        "run_type": "manual",
        "run_status": "sent",
        "scheduled_post_id": None,
        "created_at": "2026-06-07T12:00:00+00:00",
        "started_at": "2026-06-07T12:01:00+00:00",
        "finished_at": "2026-06-07T12:02:00+00:00",
        "delete_after_at_min": "2026-06-07T15:30:00+00:00",
        "delete_after_at_max": "2026-06-07T15:45:00+00:00",
        "last_sent_at": "2026-06-07T12:01:30+00:00",
        "targets_total": 3,
        "targets_success": 3,
        "targets_failed": 0,
        "active_messages_total": delete_pending + delete_processing + delete_failed,
        "delete_pending": delete_pending,
        "delete_processing": delete_processing,
        "delete_failed": delete_failed,
        "placement_status": placement_status,
    }


def _summary_from_placements(placements: list[dict[str, Any]]) -> dict[str, int]:
    active_total = 0
    delete_problem_total = 0
    mixed_total = 0
    delete_pending_total = 0
    delete_processing_total = 0
    delete_failed_total = 0
    for placement in placements:
        delete_pending = int(placement.get("delete_pending") or 0)
        delete_processing = int(placement.get("delete_processing") or 0)
        delete_failed = int(placement.get("delete_failed") or 0)
        has_active = delete_pending + delete_processing > 0
        has_problem = delete_failed > 0
        if has_active:
            active_total += 1
        if has_problem:
            delete_problem_total += 1
        if has_active and has_problem:
            mixed_total += 1
        delete_pending_total += delete_pending
        delete_processing_total += delete_processing
        delete_failed_total += delete_failed
    return {
        "placements_total": len(placements),
        "active_total": active_total,
        "delete_problem_total": delete_problem_total,
        "mixed_total": mixed_total,
        "delete_pending_total": delete_pending_total,
        "delete_processing_total": delete_processing_total,
        "delete_failed_total": delete_failed_total,
    }


def _service(repo: FakeRepo, runtime: FakeRuntime | None = None) -> RepostCampaignScheduleService:
    return RepostCampaignScheduleService(repo=repo, campaign_runtime=runtime or FakeRuntime())


def test_base_block_does_not_call_placement_service():
    repo = FakeRepo(placements=[_placement(1, delete_pending=1)])
    runtime = FakeRuntime(can_launch=False)

    result = _service(repo, runtime).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["action"] == "base_block"
    assert result["can_schedule"] is False
    assert result["clean_channel_policy"] is None
    assert repo.clean_channel_settings_calls == []
    assert repo.list_active_calls == []
    assert repo.summary_calls == []
    assert runtime.readiness_calls == [{"rule_id": 10, "include_active_placement_block": False}]


def test_clean_state_allows_schedule():
    repo = FakeRepo()

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["action"] == "allow"
    assert result["can_schedule"] is True
    assert result["can_launch_if_due_now"] is True
    assert result["will_wait_if_busy"] is False
    assert result["will_launch_over_active"] is False
    assert result["clean_channel_policy"]["launch_mode"] == "scheduled"
    assert repo.list_active_calls == [{"rule_id": 10, "limit": 20, "basic_only": True}]
    assert repo.summary_calls == [{"rule_id": 10, "basic_only": True}]


def test_clean_channel_on_active_placements_schedules_with_wait():
    repo = FakeRepo(placements=[_placement(1, delete_pending=1)])

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["action"] == "schedule_with_clean_channel_wait"
    assert result["can_schedule"] is True
    assert result["can_launch_if_due_now"] is False
    assert result["will_wait_if_busy"] is True
    assert result["will_launch_over_active"] is False
    assert "подождёт" in result["warning_text"]


def test_clean_channel_on_delete_problem_schedules_with_wait():
    repo = FakeRepo(placements=[_placement(1, placement_status="delete_problem", delete_failed=1)])

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["action"] == "schedule_with_clean_channel_wait"
    assert result["can_schedule"] is True
    assert result["will_wait_if_busy"] is True
    assert result["delete_problem_total"] > 0


def test_clean_channel_off_active_placements_schedules_with_warning():
    repo = FakeRepo(
        placements=[_placement(1, delete_pending=1)],
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
    )

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["action"] == "schedule_with_overlap_warning"
    assert result["can_schedule"] is True
    assert result["can_launch_if_due_now"] is True
    assert result["requires_confirmation"] is True
    assert result["will_launch_over_active"] is True
    assert "Чистый канал выключен" in result["warning_text"]


def test_settings_missing_fallback_enabled_true():
    repo = FakeRepo(
        placements=[_placement(1, delete_pending=1)],
        clean_channel_settings={"ok": False, "rule_id": 10},
    )

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["clean_channel_enabled"] is True
    assert result["action"] == "schedule_with_clean_channel_wait"


def test_placement_service_exception_returns_safe_policy_error():
    repo = FakeRepo(fail_active_placements=True)

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["ok"] is False
    assert result["action"] == "policy_error"
    assert result["can_schedule"] is False
    assert "Не удалось проверить активные размещения" in result["blocking_text"]
    assert "Traceback" not in str(result)
    assert "db is unavailable" not in str(result)


def test_method_is_read_only():
    repo = FakeRepo(placements=[_placement(1, delete_pending=1)])

    result = _service(repo).build_scheduled_launch_policy_state(rule_id=10, scheduled_at_utc=SCHEDULED_AT)

    assert result["can_schedule"] is True
    assert repo.write_calls == []


def test_source_guards_keep_scheduled_policy_disconnected_from_ui_worker_and_vip_posts():
    schedule_handlers = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    scheduled_post_service = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    runtime_service = Path("app/repost_campaign_runtime_service.py").read_text(encoding="utf-8")

    assert "build_scheduled_launch_policy_state" not in schedule_handlers
    assert "schedule_with_clean_channel_wait" not in schedule_handlers
    assert "schedule_with_overlap_warning" not in schedule_handlers

    assert "build_scheduled_launch_policy_state" not in scheduled_post_service
    assert "RepostCampaignPlacementService" not in scheduled_post_service
    assert "clean_channel_wait" not in scheduled_post_service
    assert "force_ignore_clean_channel" not in scheduled_post_service

    assert "build_scheduled_launch_policy_state" not in runtime_service
