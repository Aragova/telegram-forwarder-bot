from __future__ import annotations

REPOST_CAMPAIGN_TOP_TIME_PRESET_SECONDS = {
    900,
    1800,
    3600,
    7200,
    10800,
    21600,
    43200,
}

DEFAULT_REPOST_CAMPAIGN_TOP_TIME_SECONDS = 7200


def normalize_repost_campaign_top_time_settings(
    *,
    enabled: bool,
    seconds: int | None,
) -> dict[str, int | bool]:
    if not enabled:
        return {"enabled": False, "seconds": 0}
    value = int(seconds or 0)
    if value not in REPOST_CAMPAIGN_TOP_TIME_PRESET_SECONDS:
        value = DEFAULT_REPOST_CAMPAIGN_TOP_TIME_SECONDS
    return {"enabled": True, "seconds": value}
