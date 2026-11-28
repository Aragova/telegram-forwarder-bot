"""Shared presentation of rule schedules; never changes scheduling state."""
from datetime import datetime, timezone
import json
import re


def _interval_label(value):
    try:
        seconds = int(value)
    except (TypeError, ValueError, OverflowError):
        return "не задан"
    if seconds <= 0:
        return "не задан"
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return " ".join(
        f"{number} {unit}" for number, unit in (
            (hours, "ч."), (minutes, "мин."), (seconds, "сек."),
        ) if number
    )


def rule_schedule_label(row):
    if row.get("schedule_mode") == "fixed":
        return "Фикс. график"
    return f"Интервал [{_interval_label(row.get('interval'))}]"


def rule_fixed_times_label(row):
    if row.get("schedule_mode") != "fixed":
        return None
    raw = row.get("fixed_times_json")
    try:
        times = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        times = None
    if not isinstance(times, list) or not times or any(
        not isinstance(t, str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", t)
        for t in times
    ):
        return "не задан"
    return ", ".join(times)


def rule_next_send_label(row, user_tz, *, now=None):
    if not row.get("is_active"):
        return "правило выключено"
    pending = row.get("logical_pending", row.get("pending", 0))
    processing = row.get("logical_processing", row.get("processing", 0))
    if not pending:
        return "в обработке" if processing else "очередь закончена"
    value = row.get("next_run_at")
    if not value:
        return "готово к отправке"
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = now or datetime.now(timezone.utc)
        if dt <= now:
            return "готово к отправке"
        local = dt.astimezone(user_tz)
        return local.strftime("%H:%M" if local.date() == now.astimezone(user_tz).date()
                              else "%d.%m %H:%M")
    except (TypeError, ValueError, OverflowError):
        return "ошибка времени"
