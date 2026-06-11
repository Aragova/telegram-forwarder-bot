from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.repost_campaign_runtime_service import RepostCampaignActionResult
from app.repost_campaign_schedule_service import (
    CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS,
    RepostCampaignScheduleService,
)


SCHEDULED_AT = datetime(2026, 6, 10, 9, 0, tzinfo=timezone.utc)


class FakeRepo:
    def __init__(
        self,
        *,
        row: dict[str, Any] | None = None,
        clean_channel_enabled: bool = True,
        placements: list[dict[str, Any]] | None = None,
        fail_placements: bool = False,
    ):
        self.row = row or _row()
        self.clean_channel_enabled = clean_channel_enabled
        self.placements = placements or []
        self.fail_placements = fail_placements
        self.events: list[tuple[Any, ...]] = []
        self.launches = {int(self.row["id"]): dict(self.row)}

    def reset_stuck_campaign_scheduled_launches(self, *, stuck_seconds: int):
        self.events.append(("reset_stuck", stuck_seconds))
        return {"requeued": 0, "needs_review": 0}

    def claim_due_campaign_scheduled_launches(self, *, now_iso: str, worker_id: str, limit: int = 5):
        self.events.append(("claim", worker_id, limit))
        return [dict(self.row)]

    def mark_campaign_scheduled_launch_failed(self, scheduled_launch_id: int, *, error_text: str):
        self.events.append(("failed", int(scheduled_launch_id), error_text))
        self.launches[int(scheduled_launch_id)].update(status="failed", error_text=error_text)
        return True

    def mark_campaign_scheduled_launch_needs_review(self, scheduled_launch_id: int, *, error_text: str, campaign_run_id: int | None = None):
        self.events.append(("needs_review", int(scheduled_launch_id), error_text, campaign_run_id))
        self.launches[int(scheduled_launch_id)].update(status="needs_review", error_text=error_text)
        if campaign_run_id is not None:
            self.launches[int(scheduled_launch_id)]["campaign_run_id"] = int(campaign_run_id)
        return True

    def mark_campaign_scheduled_launch_waiting_clean_channel(
        self,
        scheduled_launch_id: int,
        *,
        next_retry_at: str,
        reason: str | None = None,
        policy_snapshot: dict[str, Any] | None = None,
    ):
        self.events.append(("waiting_clean_channel", int(scheduled_launch_id), next_retry_at, reason, policy_snapshot))
        self.launches[int(scheduled_launch_id)].update(
            status="waiting_clean_channel",
            clean_channel_next_retry_at=next_retry_at,
            clean_channel_last_reason=reason,
            clean_channel_policy_json=policy_snapshot,
        )
        return True

    def set_campaign_scheduled_launch_campaign_run_id(self, scheduled_launch_id: int, campaign_run_id: int):
        self.events.append(("set_run_id", int(scheduled_launch_id), int(campaign_run_id)))
        self.launches[int(scheduled_launch_id)]["campaign_run_id"] = int(campaign_run_id)
        return True

    def get_campaign_scheduled_launch(self, scheduled_launch_id: int):
        return self.launches.get(int(scheduled_launch_id), {})

    def mark_campaign_scheduled_launch_launched(self, scheduled_launch_id: int, *, campaign_run_id: int):
        self.events.append(("launched", int(scheduled_launch_id), int(campaign_run_id)))
        self.launches[int(scheduled_launch_id)].update(status="launched", campaign_run_id=int(campaign_run_id))
        return True

    def get_rule_repost_campaign_clean_channel_settings(self, rule_id: int):
        return {"ok": True, "rule_id": int(rule_id), "enabled": self.clean_channel_enabled}

    def list_active_campaign_placements_for_rule(self, rule_id: int, *, limit: int = 20, basic_only: bool = True):
        if self.fail_placements:
            raise RuntimeError("db is unavailable")
        return self.placements[:limit]

    def get_active_campaign_placements_summary_for_rule(self, rule_id: int, *, basic_only: bool = True):
        if self.fail_placements:
            raise RuntimeError("db is unavailable")
        active_total = 0
        delete_problem_total = 0
        delete_pending_total = 0
        delete_processing_total = 0
        delete_failed_total = 0
        for placement in self.placements:
            delete_pending = int(placement.get("delete_pending") or 0)
            delete_processing = int(placement.get("delete_processing") or 0)
            delete_failed = int(placement.get("delete_failed") or 0)
            if delete_pending + delete_processing > 0:
                active_total += 1
            if delete_failed > 0:
                delete_problem_total += 1
            delete_pending_total += delete_pending
            delete_processing_total += delete_processing
            delete_failed_total += delete_failed
        return {
            "placements_total": len(self.placements),
            "active_total": active_total,
            "delete_problem_total": delete_problem_total,
            "mixed_total": 0,
            "delete_pending_total": delete_pending_total,
            "delete_processing_total": delete_processing_total,
            "delete_failed_total": delete_failed_total,
        }


class FakeRuntime:
    def __init__(
        self,
        *,
        can_launch: bool = True,
        result: RepostCampaignActionResult | None = None,
        exc: Exception | None = None,
        callback_run_id: int | None = None,
    ):
        self.can_launch = can_launch
        self.result = result
        self.exc = exc
        self.callback_run_id = callback_run_id
        self.readiness_calls: list[dict[str, Any]] = []
        self.launch_calls: list[dict[str, Any]] = []

    def build_campaign_launch_readiness(self, **kwargs):
        self.readiness_calls.append(kwargs)
        return {
            "ok": True,
            "rule_id": kwargs.get("rule_id"),
            "can_launch": self.can_launch,
            "saved_post_id": 100,
            "show_seconds": 3600,
            "block_reasons": [] if self.can_launch else ["Пост не выбран"],
        }

    async def launch_campaign_now(self, **kwargs):
        self.launch_calls.append(kwargs)
        if self.callback_run_id is not None:
            kwargs["on_campaign_run_created"](self.callback_run_id)
        if self.exc is not None:
            raise self.exc
        return self.result or RepostCampaignActionResult(
            ok=True,
            action="launch_campaign",
            rule_id=int(kwargs["rule_id"]),
            extra={"campaign_run_id": self.callback_run_id or 777},
        )


def _row(**overrides: Any) -> dict[str, Any]:
    row = {
        "id": 1,
        "rule_id": 10,
        "created_by": 99,
        "status": "processing",
        "scheduled_at": SCHEDULED_AT.isoformat(),
        "clean_channel_wait_attempt_count": 0,
    }
    row.update(overrides)
    return row


def _placement(*, delete_pending: int = 1, delete_processing: int = 0, delete_failed: int = 0) -> dict[str, Any]:
    return {
        "run_id": 5,
        "rule_id": 10,
        "run_type": "manual",
        "placement_status": "active" if delete_failed == 0 else "delete_problem",
        "delete_pending": delete_pending,
        "delete_processing": delete_processing,
        "delete_failed": delete_failed,
    }


def _service(repo: FakeRepo, runtime: FakeRuntime | None = None) -> RepostCampaignScheduleService:
    return RepostCampaignScheduleService(repo=repo, campaign_runtime=runtime or FakeRuntime())


def test_clean_channel_on_active_placement_waits():
    repo = FakeRepo(clean_channel_enabled=True, placements=[_placement()])
    runtime = FakeRuntime()

    result = asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    waiting_events = [event for event in repo.events if event[0] == "waiting_clean_channel"]
    assert len(waiting_events) == 1
    assert waiting_events[0][2]
    assert waiting_events[0][3] == "Чистый канал занят активной рекламой"
    assert waiting_events[0][4]["action"] == "schedule_with_clean_channel_wait"
    assert runtime.launch_calls == []
    assert not [event for event in repo.events if event[0] == "failed"]
    assert result["claimed"] == 1


def test_waiting_clean_channel_due_row_waits_again():
    repo = FakeRepo(
        row=_row(status="waiting_clean_channel", clean_channel_wait_attempt_count=2),
        clean_channel_enabled=True,
        placements=[_placement()],
    )
    runtime = FakeRuntime()

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    waiting_events = [event for event in repo.events if event[0] == "waiting_clean_channel"]
    assert len(waiting_events) == 1
    assert waiting_events[0][2]
    assert runtime.launch_calls == []


def test_waiting_clean_channel_row_becomes_clean_and_launches():
    repo = FakeRepo(row=_row(status="waiting_clean_channel", clean_channel_wait_attempt_count=2), clean_channel_enabled=True, placements=[])
    runtime = FakeRuntime(callback_run_id=777)

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert len(runtime.launch_calls) == 1
    assert runtime.launch_calls[0]["run_type"] == "scheduled"
    assert repo.launches[1]["status"] == "launched"
    assert not [event for event in repo.events if event[0] == "waiting_clean_channel"]


def test_clean_channel_off_active_placement_launches_over_active():
    repo = FakeRepo(clean_channel_enabled=False, placements=[_placement()])
    runtime = FakeRuntime(callback_run_id=777)

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert len(runtime.launch_calls) == 1
    assert runtime.launch_calls[0]["ignore_active_placement_block"] is True
    assert not [event for event in repo.events if event[0] == "waiting_clean_channel"]
    assert not [event for event in repo.events if event[0] == "failed"]


def test_base_block_fails():
    repo = FakeRepo(clean_channel_enabled=True, placements=[])
    runtime = FakeRuntime(can_launch=False)

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert ("failed", 1, "Кампания не готова к запуску в момент старта") in repo.events
    assert runtime.launch_calls == []
    assert not [event for event in repo.events if event[0] == "waiting_clean_channel"]


def test_policy_error_needs_review_with_safe_error_text():
    repo = FakeRepo(clean_channel_enabled=True, placements=[_placement()], fail_placements=True)
    runtime = FakeRuntime()

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    needs_review = [event for event in repo.events if event[0] == "needs_review"]
    assert len(needs_review) == 1
    assert needs_review[0][2] == "Не удалось проверить Чистый канал перед запуском"
    assert "db is unavailable" not in needs_review[0][2]
    assert runtime.launch_calls == []


def test_wait_limit_needs_review():
    repo = FakeRepo(
        row=_row(clean_channel_wait_attempt_count=CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS),
        clean_channel_enabled=True,
        placements=[_placement()],
    )
    runtime = FakeRuntime()

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert ("needs_review", 1, "Запуск слишком долго ждёт чистый канал", None) in repo.events
    assert not [event for event in repo.events if event[0] == "waiting_clean_channel"]
    assert runtime.launch_calls == []


def test_launch_exception_after_campaign_run_created_remains_needs_review():
    repo = FakeRepo(clean_channel_enabled=True, placements=[])
    runtime = FakeRuntime(callback_run_id=777, exc=RuntimeError("boom"))

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert ("set_run_id", 1, 777) in repo.events
    assert [event for event in repo.events if event[0] == "needs_review" and event[3] == 777]
    assert not [event for event in repo.events if event[0] == "failed"]


def test_launch_result_failed_with_run_id_remains_needs_review():
    result = RepostCampaignActionResult(ok=False, action="launch_campaign", rule_id=10, error_text="bad", extra={"campaign_run_id": 777})
    repo = FakeRepo(clean_channel_enabled=True, placements=[])
    runtime = FakeRuntime(result=result)

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert [event for event in repo.events if event[0] == "needs_review" and event[3] == 777]


def test_launch_result_failed_without_run_id_remains_failed():
    result = RepostCampaignActionResult(ok=False, action="launch_campaign", rule_id=10, error_text="bad", extra={})
    repo = FakeRepo(clean_channel_enabled=True, placements=[])
    runtime = FakeRuntime(result=result)

    asyncio.run(_service(repo, runtime).process_due_scheduled_launches(worker_id="worker"))

    assert ("failed", 1, "bad") in repo.events


def test_source_guards_keep_clean_channel_worker_enforcement_scoped():
    schedule_service = Path("app/repost_campaign_schedule_service.py").read_text(encoding="utf-8")
    process_due_source = schedule_service.split("    async def process_due_scheduled_launches", 1)[1].split("\n\nasync def run_repost_campaign_scheduled_launch_loop", 1)[0]

    assert "build_scheduled_launch_policy_state" in process_due_source
    assert "mark_campaign_scheduled_launch_waiting_clean_channel" in process_due_source
    assert "schedule_with_clean_channel_wait" in process_due_source
    assert "schedule_with_overlap_warning" in process_due_source
    assert "CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_RETRY_SECONDS" in schedule_service
    assert "CAMPAIGN_SCHEDULE_CLEAN_CHANNEL_MAX_ATTEMPTS" in process_due_source
    assert 'readiness.get("active_placement")' not in process_due_source
    assert "readiness.get('active_placement')" not in process_due_source
    assert 'readiness.get("delete_failed")' not in process_due_source
    assert "readiness.get('delete_failed')" not in process_due_source

    schedule_handlers = Path("app/repost_campaign_schedule_handlers.py").read_text(encoding="utf-8")
    message_handlers = Path("app/repost_campaign_message_handlers.py").read_text(encoding="utf-8")
    scheduled_post_service = Path("app/repost_campaign_scheduled_post_service.py").read_text(encoding="utf-8")
    ui = Path("app/repost_campaign_ui.py").read_text(encoding="utf-8")

    assert "mark_campaign_scheduled_launch_waiting_clean_channel" not in schedule_handlers
    assert "waiting_clean_channel" not in schedule_handlers
    assert "mark_campaign_scheduled_launch_waiting_clean_channel" not in message_handlers
    assert "waiting_clean_channel" not in message_handlers
    assert "mark_campaign_scheduled_launch_waiting_clean_channel" not in scheduled_post_service
    assert "waiting_clean_channel" not in scheduled_post_service
    assert "clean_channel_next_retry_at" not in scheduled_post_service
    assert "mark_campaign_scheduled_launch_waiting_clean_channel" not in ui
