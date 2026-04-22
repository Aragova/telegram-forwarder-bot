from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


USER_TZ = timezone(timedelta(hours=3))
GLOBAL_INTERVAL_GAP_SECONDS = 180  # 3 минуты между плавающими правилами


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_fixed_times(times: list[str]) -> list[str]:
    normalized: list[str] = []

    for raw in times:
        value = str(raw or "").strip()
        if not value:
            continue

        parts = value.split(":")
        if len(parts) != 2:
            continue

        try:
            hour = int(parts[0])
            minute = int(parts[1])
        except Exception:
            continue

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            continue

        normalized.append(f"{hour:02d}:{minute:02d}")

    return sorted(set(normalized))


def get_next_fixed_run_utc(
    fixed_times: list[str],
    now_utc: datetime | None = None,
) -> str | None:
    normalized = normalize_fixed_times(fixed_times)
    if not normalized:
        return None

    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    now_local = now_utc.astimezone(USER_TZ)

    # Смотрим сегодня + ближайшие 2 дня, как и раньше
    for day_offset in range(0, 3):
        candidate_date = (now_local + timedelta(days=day_offset)).date()

        for value in normalized:
            try:
                hour_str, minute_str = value.split(":")
                hour = int(hour_str)
                minute = int(minute_str)
            except Exception:
                continue

            candidate_local = datetime(
                year=candidate_date.year,
                month=candidate_date.month,
                day=candidate_date.day,
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
                tzinfo=USER_TZ,
            )

            if candidate_local > now_local:
                return candidate_local.astimezone(timezone.utc).isoformat()

    # Теоретически сюда не должны попадать, но оставим надёжный fallback
    first_value = normalized[0]
    hour_str, minute_str = first_value.split(":")
    candidate_local = (
        now_local + timedelta(days=1)
    ).replace(
        hour=int(hour_str),
        minute=int(minute_str),
        second=0,
        microsecond=0,
    )
    return candidate_local.astimezone(timezone.utc).isoformat()


@dataclass(slots=True)
class IntroItem:
    id: int
    display_name: str
    file_name: str
    file_path: str
    duration: int
    created_at: str | None = None


@dataclass(slots=True)
class Rule:
    id: int
    source_id: str
    source_thread_id: int | None
    target_id: str
    target_thread_id: int | None
    interval: int

    created_by: int | None = None
    created_date: str | None = None
    is_active: bool = False
    next_run_at: str | None = None
    last_sent_at: str | None = None

    source_title: str | None = None
    target_title: str | None = None

    mode: str = "repost"
    video_trim_seconds: int = 120
    video_add_intro: bool = False
    video_intro_horizontal: str | None = None
    video_intro_vertical: str | None = None
    video_caption: str | None = None
    video_caption_entities_json: str | None = None

    caption_delivery_mode: str = "auto"
    video_caption_delivery_mode: str = "auto"

    schedule_mode: str = "interval"
    fixed_times_json: str | None = None

    video_intro_horizontal_id: int | None = None
    video_intro_vertical_id: int | None = None

    def fixed_times(self) -> list[str]:
        raw = self.fixed_times_json

        if not raw:
            return []

        try:
            if isinstance(raw, str):
                parsed = json.loads(raw)
            else:
                parsed = raw
        except Exception:
            return []

        if not isinstance(parsed, list):
            return []

        return normalize_fixed_times([str(x) for x in parsed])

    @property
    def is_fixed(self) -> bool:
        return (self.schedule_mode or "interval") == "fixed"

    @property
    def is_interval(self) -> bool:
        return (self.schedule_mode or "interval") == "interval"
