from __future__ import annotations

from dataclasses import dataclass


def parse_video_clip_duration_input(text: str) -> int | None:
    raw = (text or "").strip()
    if not raw:
        return None
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            return None
        return int(parts[0]) * 60 + int(parts[1])
    if raw.isdigit():
        return int(raw)
    return None


def is_video_clip_duration_in_bounds(seconds: int) -> bool:
    return 10 <= int(seconds) <= 600


def format_duration_ru(seconds: int) -> str:
    total = max(0, int(seconds))
    minutes = total // 60
    sec = total % 60
    if minutes <= 0:
        return f"{sec} сек"
    return f"{minutes} мин {sec:02d} сек"


@dataclass(slots=True)
class ClipWindow:
    should_cut: bool
    start_time: float
    duration: float


def calculate_center_clip_window(source_duration_seconds: float, requested_clip_seconds: int) -> ClipWindow:
    source = float(source_duration_seconds)
    requested = float(int(requested_clip_seconds))
    if source <= requested:
        return ClipWindow(should_cut=False, start_time=0.0, duration=source)
    start = max(0.0, (source / 2.0) - (requested / 2.0))
    return ClipWindow(should_cut=True, start_time=start, duration=requested)
