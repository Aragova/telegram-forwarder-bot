from __future__ import annotations

from types import SimpleNamespace

from app.scheduler_service import SchedulerService


class _FakeRepo:
    def __init__(self):
        self.calls: list[tuple] = []
        self.rule = None

    def get_rule(self, rule_id: int):
        self.calls.append(("get_rule", rule_id))
        return self.rule

    def update_rule_next_run_at(self, rule_id: int, next_run_iso: str) -> bool:
        self.calls.append(("update_rule_next_run_at", rule_id, next_run_iso))
        return True

    def update_rule_fixed_times(self, rule_id: int, times: list[str]) -> bool:
        self.calls.append(("update_rule_fixed_times", rule_id, list(times)))
        return True

    def update_rule_interval(self, rule_id: int, new_interval: int) -> bool:
        self.calls.append(("update_rule_interval", rule_id, int(new_interval)))
        return True

    def set_rule_interval_mode(self, rule_id: int, interval: int) -> bool:
        self.calls.append(("set_rule_interval_mode", rule_id, int(interval)))
        return True

    def trigger_rule_now(self, rule_id: int) -> bool:
        self.calls.append(("trigger_rule_now", rule_id))
        return True

    def activate_rule_with_backfill(self, rule_id: int) -> bool:
        self.calls.append(("activate_rule_with_backfill", rule_id))
        return True

    def touch_rule_after_send(self, rule_id: int, interval: int) -> None:
        self.calls.append(("touch_rule_after_send", rule_id, int(interval)))


def test_recompute_next_run_for_fixed_rule_uses_fixed_times_update():
    repo = _FakeRepo()
    repo.rule = SimpleNamespace(
        schedule_mode="fixed",
        fixed_times=lambda: ["09:00", "18:30"],
        interval=3600,
    )
    service = SchedulerService(repo)

    ok = service.recompute_next_run(10)

    assert ok is True
    assert repo.calls == [
        ("get_rule", 10),
        ("update_rule_fixed_times", 10, ["09:00", "18:30"]),
    ]


def test_recompute_next_run_for_interval_rule_uses_interval_update():
    repo = _FakeRepo()
    repo.rule = SimpleNamespace(schedule_mode="interval", interval=900)
    service = SchedulerService(repo)

    ok = service.recompute_next_run(11)

    assert ok is True
    assert repo.calls == [
        ("get_rule", 11),
        ("update_rule_interval", 11, 900),
    ]


def test_trigger_now_routes_to_repository_trigger_now():
    repo = _FakeRepo()
    service = SchedulerService(repo)

    ok = service.trigger_now(12)

    assert ok is True
    assert repo.calls == [
        ("trigger_rule_now", 12),
    ]


def test_touch_after_send_routes_to_repository_touch():
    repo = _FakeRepo()
    service = SchedulerService(repo)

    service.touch_after_send(13, 120)

    assert repo.calls == [
        ("touch_rule_after_send", 13, 120),
    ]
