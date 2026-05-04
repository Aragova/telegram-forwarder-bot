from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.job_service import enqueue_repost_campaign_send_copy

REPOST_CAMPAIGN_MAX_SHOW_SECONDS = 48 * 60 * 60
REPOST_CAMPAIGN_ALLOWED_SHOW_SECONDS = {60, 15 * 60, 60 * 60, 2 * 60 * 60, 6 * 60 * 60, 12 * 60 * 60, 24 * 60 * 60, 48 * 60 * 60}


def normalize_campaign_show_seconds(seconds: int) -> int:
    value = int(seconds or 0)
    if value <= 0:
        return 0
    if value > REPOST_CAMPAIGN_MAX_SHOW_SECONDS:
        raise ValueError("Срок показа не может быть больше 48 часов")
    if value not in REPOST_CAMPAIGN_ALLOWED_SHOW_SECONDS:
        raise ValueError("Разрешены только пресеты срока показа")
    return value


def format_campaign_show_seconds_ru(seconds: int) -> str:
    mapping = {0: "выключено", 60: "1 минута", 900: "15 минут", 3600: "1 час", 7200: "2 часа", 21600: "6 часов", 43200: "12 часов", 86400: "24 часа", 172800: "48 часов"}
    return mapping.get(int(seconds or 0), f"{int(seconds or 0)} сек")


def build_campaign_delete_after_iso(show_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=int(show_seconds))).isoformat()


def schedule_campaign_copies_for_delivery(repo, *, rule_id: int, delivery_id: int) -> dict:
    rule = repo.get_rule(int(rule_id))
    if not rule or str(getattr(rule, "mode", "")) != "repost":
        return {"ok": True, "created_copies": 0, "created_jobs": 0, "skipped_reason": "rule_mode_not_repost"}
    if not bool(getattr(rule, "repost_campaign_enabled", False)):
        return {"ok": True, "created_copies": 0, "created_jobs": 0, "skipped_reason": "campaign_disabled"}
    show_seconds = int(getattr(rule, "repost_campaign_show_seconds", 0) or 0)
    if show_seconds <= 0:
        return {"ok": True, "created_copies": 0, "created_jobs": 0, "skipped_reason": "show_seconds_zero"}
    targets = repo.list_rule_repost_campaign_targets(int(rule_id), active_only=True) or []
    created_copies = 0
    created_jobs = 0
    now = datetime.now(timezone.utc)
    for index, target in enumerate(targets):
        copy_id = repo.create_delivery_campaign_copy(delivery_id=int(delivery_id), rule_id=int(rule_id), target_id=str(target.get("target_id")), target_thread_id=target.get("target_thread_id"), target_title=target.get("title"))
        if not copy_id:
            continue
        created_copies += 1
        run_at = (now + timedelta(seconds=index)).isoformat()
        if enqueue_repost_campaign_send_copy(repo, int(copy_id), run_at=run_at):
            created_jobs += 1
    return {"ok": True, "created_copies": created_copies, "created_jobs": created_jobs, "skipped_reason": None}
