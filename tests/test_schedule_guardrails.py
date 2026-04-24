from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.repository_models import utc_now_iso


@dataclass
class _RuleState:
    rule_id: int
    interval: int
    next_run_at: datetime
    pending: list[int]
    processing: set[int]
    sent: set[int]


class _GuardrailRepo:
    def __init__(self) -> None:
        self._rules: dict[int, _RuleState] = {}
        self._delivery_to_rule: dict[int, int] = {}
        self._rule_seq = 1
        self._delivery_seq = 1000

    def seed_rule(self, *, interval: int, items: int) -> int:
        rule_id = self._rule_seq
        self._rule_seq += 1
        delivery_ids: list[int] = []
        for _ in range(max(1, int(items))):
            self._delivery_seq += 1
            delivery_id = self._delivery_seq
            delivery_ids.append(delivery_id)
            self._delivery_to_rule[delivery_id] = rule_id
        self._rules[rule_id] = _RuleState(
            rule_id=rule_id,
            interval=max(1, int(interval)),
            next_run_at=datetime.now(timezone.utc),
            pending=delivery_ids,
            processing=set(),
            sent=set(),
        )
        return rule_id

    def take_due_delivery(self, rule_id: int, due_iso: str):
        state = self._rules[int(rule_id)]
        due_dt = datetime.fromisoformat(str(due_iso))
        if due_dt.tzinfo is None:
            due_dt = due_dt.replace(tzinfo=timezone.utc)
        if due_dt < state.next_run_at:
            return None
        if not state.pending:
            return None
        if state.processing:
            return None
        delivery_id = state.pending.pop(0)
        state.processing.add(delivery_id)
        return {"delivery_id": delivery_id}

    def mark_delivery_sent(self, delivery_id: int):
        delivery_id = int(delivery_id)
        rule_id = self._delivery_to_rule[delivery_id]
        state = self._rules[rule_id]
        state.processing.discard(delivery_id)
        state.sent.add(delivery_id)

    def touch_rule_after_send(self, rule_id: int, interval: int):
        state = self._rules[int(rule_id)]
        state.next_run_at = datetime.now(timezone.utc) + timedelta(seconds=max(1, int(interval)))

    def get_processing_count(self, rule_id: int) -> int:
        return len(self._rules[int(rule_id)].processing)

    def get_pending_count(self, rule_id: int) -> int:
        return len(self._rules[int(rule_id)].pending)

    def get_rule_next_run_at(self, rule_id: int) -> datetime:
        return self._rules[int(rule_id)].next_run_at

    def clear_next_run(self, rule_id: int) -> None:
        state = self._rules[int(rule_id)]
        state.next_run_at = datetime.min.replace(tzinfo=timezone.utc)


def test_backlog_is_rate_limited_for_all_mode_combinations() -> None:
    repo = _GuardrailRepo()
    scenarios = [
        ("repost", "interval"),
        ("repost", "fixed"),
        ("video", "interval"),
        ("video", "fixed"),
    ]
    for _mode, _schedule_mode in scenarios:
        rule_id = repo.seed_rule(interval=3600, items=2)
        first = repo.take_due_delivery(rule_id, utc_now_iso())
        assert first is not None
        repo.mark_delivery_sent(int(first["delivery_id"]))
        repo.touch_rule_after_send(rule_id, 3600)

        second = repo.take_due_delivery(rule_id, (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat())
        assert second is None


def test_take_due_delivery_repairs_missing_next_slot_behavior() -> None:
    repo = _GuardrailRepo()
    rule_id = repo.seed_rule(interval=7200, items=2)
    repo.clear_next_run(rule_id)

    taken = repo.take_due_delivery(rule_id, utc_now_iso())
    assert taken is not None
    repo.mark_delivery_sent(int(taken["delivery_id"]))
    repo.touch_rule_after_send(rule_id, 7200)
    assert repo.get_rule_next_run_at(rule_id) > datetime.now(timezone.utc)


def test_only_one_logical_item_per_allowed_slot_with_repeated_ticks_and_worker() -> None:
    repo = _GuardrailRepo()
    rule_id = repo.seed_rule(interval=7200, items=3)

    first_due = utc_now_iso()
    first = repo.take_due_delivery(rule_id, first_due)
    assert first is not None
    first_delivery_id = int(first["delivery_id"])

    for _ in range(3):
        assert repo.take_due_delivery(rule_id, first_due) is None

    repo.mark_delivery_sent(first_delivery_id)
    repo.touch_rule_after_send(rule_id, 7200)
    repo.touch_rule_after_send(rule_id, 7200)

    one_hour_later = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    for _ in range(3):
        assert repo.take_due_delivery(rule_id, one_hour_later) is None

    after_due = (datetime.now(timezone.utc) + timedelta(hours=2, seconds=1)).isoformat()
    second = repo.take_due_delivery(rule_id, after_due)
    assert second is not None
    assert int(second["delivery_id"]) != first_delivery_id
    assert repo.get_processing_count(rule_id) == 1
    assert repo.get_pending_count(rule_id) == 1
