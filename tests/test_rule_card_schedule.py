from datetime import datetime, timezone, timedelta
from app.rule_card_schedule import rule_schedule_label, rule_fixed_times_label, rule_next_send_label


def test_schedule_formats_exact_interval_and_fixed_times():
    assert rule_schedule_label({"interval": 7200}) == "Интервал [2 ч.]"
    assert rule_schedule_label({"interval": 5431}) == "Интервал [1 ч. 30 мин. 31 сек.]"
    assert rule_schedule_label({"schedule_mode": "fixed"}) == "Фикс. график"
    assert rule_fixed_times_label({"schedule_mode": "fixed", "fixed_times_json": '["17:30","21:00"]'}) == "17:30, 21:00"
    assert rule_fixed_times_label({"schedule_mode": "fixed", "fixed_times_json": '["<bad>"]'}) == "не задан"
    assert rule_fixed_times_label({"interval": 7200}) is None


def test_next_send_handles_timezone_date_and_disabled_queue():
    tz = timezone(timedelta(hours=3))
    now = datetime(2026, 11, 28, 13, tzinfo=timezone.utc)
    row = {"is_active": True, "logical_pending": 26, "next_run_at": "2026-11-28T14:30:00+00:00"}
    assert rule_next_send_label(row, tz, now=now) == "17:30"
    assert rule_next_send_label({**row, "next_run_at": "2026-11-29T14:30:00+00:00"}, tz, now=now) == "29.11 17:30"
    assert rule_next_send_label({**row, "is_active": False}, tz, now=now) == "правило выключено"
    assert rule_next_send_label({**row, "logical_pending": 0, "logical_processing": 1}, tz, now=now) == "в обработке"
