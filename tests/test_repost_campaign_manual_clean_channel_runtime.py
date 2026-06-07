from __future__ import annotations

import asyncio
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from app.repost_campaign_runtime_service import RepostCampaignRuntimeService
from app.saved_post_renderer import SavedPostRenderResult


class FakeRepo:
    def __init__(
        self,
        *,
        rule=None,
        saved_post: dict[str, Any] | None = None,
        targets: list[dict[str, Any]] | None = None,
        clean_channel_settings: dict[str, Any] | None = None,
        placements: list[dict[str, Any]] | None = None,
        placement_summary: dict[str, Any] | None = None,
        fail_active_placements: bool = False,
        campaign_runs: list[dict[str, Any]] | None = None,
    ):
        self._rule = rule
        self._saved_post = saved_post
        self._targets = list(targets or [])
        self.clean_channel_settings = clean_channel_settings if clean_channel_settings is not None else {"ok": True, "rule_id": 10, "enabled": True}
        self.placements = list(placements or [])
        self.placement_summary = placement_summary if placement_summary is not None else _summary_from_placements(self.placements)
        self.fail_active_placements = fail_active_placements
        self.clean_channel_settings_calls: list[int] = []
        self.list_active_campaign_placements_calls: list[dict[str, Any]] = []
        self.active_campaign_placements_summary_calls: list[dict[str, Any]] = []
        self.create_campaign_run_calls: list[dict[str, Any]] = []
        self.create_campaign_run_message_calls: list[dict[str, Any]] = []
        self.update_campaign_run_status_calls: list[dict[str, Any]] = []
        self.mark_campaign_run_message_sending_calls: list[dict[str, Any]] = []
        self.mark_campaign_run_message_sent_calls: list[dict[str, Any]] = []
        self.mark_campaign_run_message_failed_calls: list[dict[str, Any]] = []
        self.mark_campaign_run_message_deleted_calls: list[int] = []
        self.mark_campaign_run_message_delete_failed_calls: list[dict[str, Any]] = []
        self.enqueue_campaign_launch_job_calls: list[dict[str, Any]] = []
        self._campaign_runs = list(campaign_runs or [])

    def get_rule(self, rule_id: int):
        return self._rule

    def get_saved_post(self, saved_post_id: int):
        return self._saved_post

    def list_rule_repost_campaign_targets(self, rule_id: int, active_only: bool = True) -> list[dict[str, Any]]:
        return list(self._targets)

    def list_campaign_runs_for_rule(self, rule_id: int, limit: int = 10) -> list[dict[str, Any]]:
        return self._campaign_runs[:limit]

    def get_rule_repost_campaign_clean_channel_settings(self, rule_id: int) -> dict[str, Any] | None:
        self.clean_channel_settings_calls.append(rule_id)
        return self.clean_channel_settings

    def list_active_campaign_placements_for_rule(
        self,
        rule_id: int,
        *,
        limit: int = 20,
        basic_only: bool = True,
    ) -> list[dict[str, Any]]:
        if self.fail_active_placements:
            raise RuntimeError("database traceback must not reach UI")
        self.list_active_campaign_placements_calls.append({"rule_id": rule_id, "limit": limit, "basic_only": basic_only})
        return self.placements[:limit]

    def get_active_campaign_placements_summary_for_rule(self, rule_id: int, *, basic_only: bool = True) -> dict[str, Any]:
        if self.fail_active_placements:
            raise RuntimeError("database traceback must not reach UI")
        self.active_campaign_placements_summary_calls.append({"rule_id": rule_id, "basic_only": basic_only})
        return dict(self.placement_summary)

    def create_campaign_run(self, **kwargs):
        self.create_campaign_run_calls.append(kwargs)
        return 1

    def create_campaign_run_message(self, **kwargs):
        self.create_campaign_run_message_calls.append(kwargs)
        return 1

    def update_campaign_run_status(self, *args, **kwargs):
        self.update_campaign_run_status_calls.append({"args": args, "kwargs": kwargs})
        return True

    def mark_campaign_run_message_sending(self, *args, **kwargs):
        self.mark_campaign_run_message_sending_calls.append({"args": args, "kwargs": kwargs})
        return True

    def mark_campaign_run_message_sent(self, *args, **kwargs):
        self.mark_campaign_run_message_sent_calls.append({"args": args, "kwargs": kwargs})
        return True

    def mark_campaign_run_message_failed(self, *args, **kwargs):
        self.mark_campaign_run_message_failed_calls.append({"args": args, "kwargs": kwargs})
        return True

    def mark_campaign_run_message_deleted(self, message_id: int):
        self.mark_campaign_run_message_deleted_calls.append(message_id)
        return True

    def mark_campaign_run_message_delete_failed(self, *args, **kwargs):
        self.mark_campaign_run_message_delete_failed_calls.append({"args": args, "kwargs": kwargs})
        return True

    def enqueue_campaign_launch_job(self, **kwargs):
        self.enqueue_campaign_launch_job_calls.append(kwargs)
        return True

    @property
    def write_calls_total(self) -> int:
        return sum(
            len(calls)
            for calls in (
                self.create_campaign_run_calls,
                self.create_campaign_run_message_calls,
                self.update_campaign_run_status_calls,
                self.mark_campaign_run_message_sending_calls,
                self.mark_campaign_run_message_sent_calls,
                self.mark_campaign_run_message_failed_calls,
                self.mark_campaign_run_message_deleted_calls,
                self.mark_campaign_run_message_delete_failed_calls,
                self.enqueue_campaign_launch_job_calls,
            )
        )


def _ready_rule():
    return SimpleNamespace(
        mode="repost",
        repost_campaign_saved_post_id=55,
        repost_campaign_show_seconds=300,
        target_id="-1001",
        target_thread_id=None,
    )


class FakeRenderer:
    def __init__(self, result=None):
        self.result = result if result is not None else SavedPostRenderResult(
            ok=True,
            method="bot_api",
            kind="text",
            chat_id="-1001",
            message_id=777,
        )
        self.calls: list[dict[str, Any]] = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


def _runtime(repo: FakeRepo, renderer=None) -> RepostCampaignRuntimeService:
    return RepostCampaignRuntimeService(repo=repo, renderer=renderer or FakeRenderer())


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
        "delete_after_at_min": "2026-06-07T15:30:00+00:00",
        "delete_after_at_max": "2026-06-07T15:45:00+00:00",
        "targets_total": 1,
        "targets_success": 1,
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
    for row in placements:
        status = str(row.get("placement_status") or "").strip().lower()
        delete_pending = int(row.get("delete_pending") or 0)
        delete_processing = int(row.get("delete_processing") or 0)
        delete_failed = int(row.get("delete_failed") or 0)
        delete_pending_total += delete_pending
        delete_processing_total += delete_processing
        delete_failed_total += delete_failed
        active_count = delete_pending + delete_processing
        if status == "active" or active_count > 0:
            active_total += 1
        if status in {"delete_problem", "mixed"} or delete_failed > 0:
            delete_problem_total += 1
        if status == "mixed" or (delete_failed > 0 and active_count > 0):
            mixed_total += 1
    return {
        "placements_total": len(placements),
        "active_total": active_total,
        "delete_problem_total": delete_problem_total,
        "mixed_total": mixed_total,
        "delete_pending_total": delete_pending_total,
        "delete_processing_total": delete_processing_total,
        "delete_failed_total": delete_failed_total,
    }


def test_manual_policy_base_block_does_not_call_placement_service():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=None, repost_campaign_show_seconds=0, target_id="-1001")
    repo = FakeRepo(rule=rule, saved_post=None, placements=[_placement(1, delete_pending=1)])

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["action"] == "base_block"
    assert result["can_launch"] is False
    assert result["clean_channel_policy"] is None
    assert result["clean_channel_settings"] is None
    assert result["base_readiness"]["block_reasons"]
    assert repo.clean_channel_settings_calls == []
    assert repo.list_active_campaign_placements_calls == []
    assert repo.active_campaign_placements_summary_calls == []


def test_manual_policy_clean_state_allows_launch():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {}}, placements=[])

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["action"] == "allow"
    assert result["can_launch"] is True
    assert result["requires_confirmation"] is False
    assert result["active_placements_total"] == 0
    assert repo.list_active_campaign_placements_calls[-1]["basic_only"] is True
    assert result["clean_channel_policy"]["launch_mode"] == "manual"


def test_manual_policy_clean_channel_on_active_placements_blocks():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {}}, placements=[_placement(1, delete_pending=1)])

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["action"] == "block"
    assert result["can_launch"] is False
    assert result["requires_confirmation"] is False
    assert result["blocking_text"]
    assert result["active_placements_total"] > 0


def test_manual_policy_clean_channel_on_delete_problem_blocks():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {}},
        placements=[_placement(2, placement_status="delete_problem", delete_failed=1)],
    )

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["action"] == "block"
    assert result["can_launch"] is False
    assert result["delete_problem_total"] > 0


def test_manual_policy_clean_channel_off_active_requires_confirmation():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(3, delete_pending=1)],
    )

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10, force_ignore_clean_channel=False)

    assert result["action"] == "confirm_required"
    assert result["can_launch"] is False
    assert result["requires_confirmation"] is True
    assert "Чистый канал выключен" in result["warning_text"]


def test_manual_policy_clean_channel_off_active_force_allows():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(4, delete_pending=1)],
    )

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10, force_ignore_clean_channel=True)

    assert result["action"] == "allow_forced"
    assert result["can_launch"] is True
    assert result["requires_confirmation"] is False
    assert result["warning_text"]
    assert result["force_ignore_clean_channel"] is True


def test_manual_policy_settings_missing_falls_back_to_clean_channel_enabled_true():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {}},
        clean_channel_settings={"ok": False, "rule_id": 10},
        placements=[_placement(5, delete_pending=1)],
    )

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["clean_channel_enabled"] is True
    assert result["action"] == "block"


def test_manual_policy_placement_exception_blocks_safely_without_traceback_text():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {}}, fail_active_placements=True)

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10)

    assert result["ok"] is False
    assert result["action"] == "block"
    assert result["can_launch"] is False
    assert "Не удалось проверить активные размещения" in result["blocking_text"]
    assert "Traceback" not in result["blocking_text"]
    assert "database traceback" not in result["blocking_text"]


def test_manual_policy_is_read_only():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(6, delete_pending=1)],
    )

    result = _runtime(repo).build_manual_launch_policy_state(rule_id=10, force_ignore_clean_channel=True)

    assert result["action"] == "allow_forced"
    assert repo.write_calls_total == 0


def test_launch_campaign_now_placement_exception_blocks_before_campaign_run_creation():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {"kind": "text"}},
        fail_active_placements=True,
    )

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10))

    assert result.ok is False
    assert "Не удалось проверить активные размещения" in (result.error_text or "")
    assert result.extra["clean_channel_blocked"] is True
    assert result.extra["manual_launch_policy"]["ok"] is False
    assert repo.create_campaign_run_calls == []
    assert "Traceback" not in (result.error_text or "")
    assert "database traceback" not in (result.error_text or "")


def test_launch_campaign_now_clean_channel_on_active_placements_blocks():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {"kind": "text"}}, placements=[_placement(11, delete_pending=1)])

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10, run_type="manual"))

    assert result.ok is False
    assert "Чистый канал" in (result.error_text or "")
    assert result.extra["clean_channel_blocked"] is True
    assert result.extra["manual_launch_policy"]["action"] == "block"
    assert repo.create_campaign_run_calls == []
    assert repo.write_calls_total == 0


def test_launch_campaign_now_clean_channel_on_delete_problem_blocks():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {"kind": "text"}},
        placements=[_placement(12, placement_status="delete_problem", delete_failed=1)],
    )

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10, run_type="manual"))

    assert result.ok is False
    assert result.extra["clean_channel_blocked"] is True
    assert result.extra["manual_launch_policy"]["action"] == "block"
    assert repo.create_campaign_run_calls == []


def test_launch_campaign_now_clean_channel_off_active_requires_confirmation_without_force():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {"kind": "text"}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(13, delete_pending=1)],
    )

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10, force_ignore_clean_channel=False))

    assert result.ok is False
    assert result.error_text == "Нужно подтвердить запуск поверх активной рекламы"
    assert result.extra["requires_clean_channel_confirmation"] is True
    assert result.extra["manual_launch_policy"]["action"] == "confirm_required"
    assert repo.create_campaign_run_calls == []


def test_launch_campaign_now_clean_channel_off_active_force_allows_campaign_run_creation():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {"kind": "text"}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(14, delete_pending=1)],
    )

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10, force_ignore_clean_channel=True))

    assert result.ok is True
    assert result.extra["campaign_run_id"] == 1
    assert repo.create_campaign_run_calls
    assert result.extra["manual_launch_policy"]["action"] == "allow_forced"
    assert "clean_channel_blocked" not in result.extra


def test_launch_campaign_now_clean_state_launches_normally():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {"kind": "text"}}, placements=[])

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10))

    assert result.ok is True
    assert repo.create_campaign_run_calls
    assert result.extra["manual_launch_policy"]["action"] == "allow"


def test_launch_campaign_now_base_readiness_blocks_before_placement_service():
    rule = SimpleNamespace(mode="repost", repost_campaign_saved_post_id=None, repost_campaign_show_seconds=0, target_id="-1001")
    repo = FakeRepo(rule=rule, saved_post=None, placements=[_placement(15, delete_pending=1)])

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10))

    assert result.ok is False
    assert result.extra["manual_launch_policy"]["action"] == "base_block"
    assert repo.clean_channel_settings_calls == []
    assert repo.list_active_campaign_placements_calls == []
    assert repo.active_campaign_placements_summary_calls == []
    assert repo.create_campaign_run_calls == []


def test_launch_campaign_now_non_manual_ignores_force_and_keeps_legacy_active_block():
    repo = FakeRepo(
        rule=_ready_rule(),
        saved_post={"content_json": {"kind": "text"}},
        clean_channel_settings={"ok": True, "rule_id": 10, "enabled": False},
        placements=[_placement(16, delete_pending=1)],
        campaign_runs=[{"id": 99}],
    )
    runtime = _runtime(repo)
    runtime.get_campaign_run_details = lambda **kwargs: {
        "summary": {"delete_pending": 1, "delete_processing": 0, "delete_failed": 0},
        "messages": [{"send_status": "sent", "delete_status": "pending", "delete_after_at": "2026-06-07T15:30:00+00:00"}],
    }

    result = asyncio.run(runtime.launch_campaign_now(rule_id=10, run_type="scheduled", force_ignore_clean_channel=True))

    assert result.ok is False
    assert result.error_text == "Кампания уже активна"
    assert result.extra["active_placement"] is True
    assert repo.clean_channel_settings_calls == []
    assert repo.list_active_campaign_placements_calls == []
    assert repo.active_campaign_placements_summary_calls == []
    assert repo.create_campaign_run_calls == []


def test_launch_campaign_now_signature_backward_compatible_without_force_flag():
    repo = FakeRepo(rule=_ready_rule(), saved_post={"content_json": {"kind": "text"}}, placements=[])

    result = asyncio.run(_runtime(repo).launch_campaign_now(rule_id=10, admin_id=123))

    assert result.ok is True
    assert repo.create_campaign_run_calls[0]["started_by"] == 123


def test_manual_policy_source_guards_keep_user_flow_unwired():
    guards = {
        "app/repost_campaign_handlers.py": [],
        "app/repost_campaign_launch_job_service.py": [
            "build_manual_launch_policy_state",
        ],
        "app/repost_campaign_schedule_service.py": [
            "force_ignore_clean_channel",
            "build_manual_launch_policy_state",
        ],
        "app/repost_campaign_scheduled_post_service.py": [
            "force_ignore_clean_channel",
            "build_manual_launch_policy_state",
            "RepostCampaignPlacementService",
        ],
    }
    for file_name, forbidden_strings in guards.items():
        source = Path(file_name).read_text()
        for forbidden in forbidden_strings:
            assert forbidden not in source

    runtime_source = Path("app/repost_campaign_runtime_service.py").read_text()
    match = re.search(
        r"\n    async def launch_campaign_now\(.*?(?=\n    def |\n    async def |\Z)",
        runtime_source,
        flags=re.S,
    )
    assert match is not None
    launch_source = match.group(0)
    assert "force_ignore_clean_channel: bool = False" in launch_source
    assert "build_manual_launch_policy_state" in launch_source
