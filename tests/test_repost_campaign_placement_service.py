from __future__ import annotations

from pathlib import Path
from typing import Any

from app.repost_campaign_placement_service import RepostCampaignPlacementService


class FakeRepo:
    def __init__(self, placements: list[dict[str, Any]] | None = None, summary: dict[str, Any] | None = None, *, fail: bool = False):
        self.placements = placements or []
        self.summary = summary if summary is not None else _summary_from_placements(self.placements)
        self.fail = fail
        self.list_calls: list[dict[str, Any]] = []
        self.summary_calls: list[dict[str, Any]] = []

    def list_active_campaign_placements_for_rule(self, rule_id: int, *, limit: int = 20, basic_only: bool = True) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError("db is unavailable")
        self.list_calls.append({"rule_id": rule_id, "limit": limit, "basic_only": basic_only})
        return self.placements[:limit]

    def get_active_campaign_placements_summary_for_rule(self, rule_id: int, *, basic_only: bool = True) -> dict[str, Any]:
        if self.fail:
            raise RuntimeError("db is unavailable")
        self.summary_calls.append({"rule_id": rule_id, "basic_only": basic_only})
        return dict(self.summary)


def _placement(
    run_id: int,
    *,
    run_type: str = "manual",
    placement_status: str = "active",
    delete_pending: int = 0,
    delete_processing: int = 0,
    delete_failed: int = 0,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "rule_id": 10,
        "saved_post_id": 100 + run_id,
        "run_type": run_type,
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


def test_clean_state():
    service = RepostCampaignPlacementService(FakeRepo())

    active = service.build_active_placements(rule_id=10)
    clean = service.build_clean_channel_state(rule_id=10)

    assert active["state"] == "clean"
    assert active["is_clean"] is True
    assert active["placements"] == []
    assert "Канал чист" in clean["status_text"]


def test_active_placement():
    repo = FakeRepo([_placement(123, placement_status="active", delete_pending=2)])
    service = RepostCampaignPlacementService(repo)

    result = service.build_active_placements(rule_id=10)
    placement = result["placements"][0]

    assert result["state"] == "active"
    assert result["has_active"] is True
    assert placement["placement_status_text"] == "Активно"
    assert placement["can_delete_now"] is True
    assert placement["details_callback_data"] == "rule_repost_campaign_history_detail:10:123"
    assert placement["delete_callback_data"] == "rule_repost_campaign_run_delete_confirm:10:123"
    assert placement["report_callback_data"] == "rule_repost_campaign_views_report:10:123"


def test_delete_problem_placement():
    repo = FakeRepo([_placement(124, placement_status="delete_problem", delete_failed=1)])
    service = RepostCampaignPlacementService(repo)

    result = service.build_active_placements(rule_id=10)
    placement = result["placements"][0]

    assert result["state"] == "delete_problem"
    assert result["has_delete_problem"] is True
    assert placement["placement_status_text"] == "Ошибка удаления"
    assert "Ошибки удаления" in placement["summary_text"]


def test_mixed_placement():
    repo = FakeRepo([_placement(125, placement_status="mixed", delete_pending=2, delete_failed=1)])
    service = RepostCampaignPlacementService(repo)

    result = service.build_active_placements(rule_id=10)
    placement = result["placements"][0]

    assert result["state"] == "mixed"
    assert result["has_mixed"] is True
    assert "Ожидают удаления" in placement["summary_text"]
    assert "Ошибки удаления" in placement["summary_text"]


def test_multiple_placements_keep_repository_order_and_callbacks():
    placements = [
        _placement(3, placement_status="delete_problem", delete_failed=1),
        _placement(2, placement_status="active", delete_pending=1),
        _placement(1, placement_status="active", delete_processing=1),
    ]
    service = RepostCampaignPlacementService(FakeRepo(placements))

    result = service.build_active_placements(rule_id=10)

    assert result["placements_total"] == 3
    assert [placement["run_id"] for placement in result["placements"]] == [3, 2, 1]
    for placement in result["placements"]:
        run_id = placement["run_id"]
        assert placement["details_callback_data"] == f"rule_repost_campaign_history_detail:10:{run_id}"
        assert placement["delete_callback_data"] == f"rule_repost_campaign_run_delete_confirm:10:{run_id}"
        assert placement["report_callback_data"] == f"rule_repost_campaign_views_report:10:{run_id}"


def test_run_type_text():
    service = RepostCampaignPlacementService(
        FakeRepo(
            [
                _placement(1, run_type="manual", delete_pending=1),
                _placement(2, run_type="scheduled", delete_pending=1),
                _placement(3, run_type="retry", delete_pending=1),
                _placement(4, run_type="unknown", delete_pending=1),
            ]
        )
    )

    result = service.build_active_placements(rule_id=10, basic_only=False)

    assert [placement["run_type_text"] for placement in result["placements"]] == [
        "Запуск сейчас",
        "Запланированный запуск",
        "Повторный запуск",
        "Запуск кампании",
    ]


def test_build_launch_policy_preview_clean():
    service = RepostCampaignPlacementService(FakeRepo())

    result = service.build_launch_policy_preview(rule_id=10, clean_channel_enabled=True, launch_mode="manual")

    assert result["action"] == "allow"
    assert result["can_launch"] is True
    assert result["requires_confirmation"] is False


def test_build_launch_policy_preview_on_active():
    service = RepostCampaignPlacementService(FakeRepo([_placement(123, delete_pending=1)]))

    result = service.build_launch_policy_preview(rule_id=10, clean_channel_enabled=True, launch_mode="manual")

    assert result["action"] == "block"
    assert result["can_launch"] is False
    assert result["requires_confirmation"] is False
    assert result["blocking_text"]


def test_build_launch_policy_preview_off_active():
    service = RepostCampaignPlacementService(FakeRepo([_placement(123, delete_pending=1)]))

    result = service.build_launch_policy_preview(rule_id=10, clean_channel_enabled=False, launch_mode="manual")

    assert result["action"] == "allow_with_warning"
    assert result["can_launch"] is True
    assert result["requires_confirmation"] is True
    assert "Чистый канал выключен" in result["warning_text"]


def test_repository_exception():
    service = RepostCampaignPlacementService(FakeRepo(fail=True))

    result = service.build_active_placements(rule_id=10)

    assert result["ok"] is False
    assert result["state"] == "unknown"
    assert result["placements"] == []
    assert result["error_text"] == "Не удалось получить активные размещения"


def test_no_scheduled_post_service_coupling():
    source = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")

    assert "RepostCampaignPlacementService" not in source
    assert "build_active_placements" not in source
    assert "build_clean_channel_state" not in source
    assert "build_launch_policy_preview" not in source
