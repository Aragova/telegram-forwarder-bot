from __future__ import annotations

from typing import Protocol


class SchedulerRepositoryProtocol(Protocol):
    def get_rule(self, rule_id: int): ...
    def update_rule_next_run_at(self, rule_id: int, next_run_iso: str) -> bool: ...
    def update_rule_fixed_times(self, rule_id: int, times: list[str]) -> bool: ...
    def update_rule_interval(self, rule_id: int, new_interval: int) -> bool: ...
    def set_rule_interval_mode(self, rule_id: int, interval: int) -> bool: ...
    def trigger_rule_now(self, rule_id: int) -> bool: ...
    def activate_rule_with_backfill(self, rule_id: int) -> bool: ...
    def touch_rule_after_send(self, rule_id: int, interval: int) -> None: ...


class SchedulerService:
    def __init__(self, repository: SchedulerRepositoryProtocol) -> None:
        self._repo = repository

    def set_next_run(self, rule_id: int, next_run_iso: str) -> bool:
        return bool(self._repo.update_rule_next_run_at(rule_id, next_run_iso))

    def update_fixed_times(self, rule_id: int, normalized_times: list[str]) -> bool:
        return bool(self._repo.update_rule_fixed_times(rule_id, normalized_times))

    def update_interval(self, rule_id: int, interval: int, *, set_interval_mode: bool = False) -> bool:
        if set_interval_mode:
            return bool(self._repo.set_rule_interval_mode(rule_id, interval))
        return bool(self._repo.update_rule_interval(rule_id, interval))

    def trigger_now(self, rule_id: int) -> bool:
        return bool(self._repo.trigger_rule_now(rule_id))

    def touch_after_send(self, rule_id: int, interval: int) -> None:
        self._repo.touch_rule_after_send(rule_id, interval)

    def activate_with_backfill(self, rule_id: int) -> bool:
        return bool(self._repo.activate_rule_with_backfill(rule_id))

    def recompute_next_run(self, rule_id: int) -> bool:
        rule = self._repo.get_rule(rule_id)
        if not rule:
            return False

        schedule_mode = (getattr(rule, "schedule_mode", "interval") or "interval").strip().lower()

        if schedule_mode == "fixed":
            fixed_times = rule.fixed_times() if hasattr(rule, "fixed_times") else []
            return bool(self._repo.update_rule_fixed_times(rule_id, fixed_times))

        interval = int(getattr(rule, "interval", 0) or 0)
        return bool(self._repo.update_rule_interval(rule_id, interval))
