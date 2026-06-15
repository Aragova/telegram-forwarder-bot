from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.repost_campaign_ui import format_repost_campaign_top_time_seconds_text
from app.repost_campaign_view_model import format_campaign_datetime_text


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_end_text(value) -> str:
    text = format_campaign_datetime_text(value)
    return "—" if text == "не указано" else text


def _format_remaining(value) -> str:
    dt = _parse_dt(value)
    if dt is None:
        return "—"
    seconds = max(0, int((dt - datetime.now(timezone.utc)).total_seconds()))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class RepostCampaignTopTimeViewService:
    def __init__(self, repo, *, logger=None):
        self.repo = repo
        self.logger = logger or logging.getLogger(__name__)

    def build_active_pauses_for_rule(self, rule_id: int, *, limit: int = 50) -> dict[str, Any]:
        try:
            rows = self.repo.list_active_campaign_top_time_pauses_for_rule(rule_id, limit=limit)
            pauses = [self._build_pause(row, rule_id=int(rule_id)) for row in (rows or [])]
            return {"ok": True, "rule_id": int(rule_id), "pauses": pauses, "total": len(pauses)}
        except Exception:
            self.logger.exception("Не удалось получить активные паузы Времени в топе для правила %s", rule_id)
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "pauses": [],
                "total": 0,
                "error_text": "Не удалось получить активные паузы",
            }

    def build_run_top_time_summary(
        self,
        campaign_run_id: int,
        *,
        top_time_enabled_snapshot: bool | None = None,
        top_time_seconds_snapshot: int | None = None,
    ) -> dict[str, Any]:
        enabled = bool(top_time_enabled_snapshot)
        seconds = int(top_time_seconds_snapshot or 0)
        if not enabled:
            return {
                "ok": True,
                "campaign_run_id": int(campaign_run_id),
                "enabled_snapshot": False,
                "seconds_snapshot": 0,
                "seconds_text": format_repost_campaign_top_time_seconds_text(0),
                "pauses_total": 0,
                "active_count": 0,
                "completed_count": 0,
                "cancelled_count": 0,
                "latest_ends_at": None,
                "latest_ends_at_text": None,
                "pauses": [],
                "status_text": "🔴 выключено для этого запуска",
            }
        try:
            rows = self.repo.list_campaign_top_time_pauses_for_run(campaign_run_id, limit=100)
            pauses = [self._build_pause(row, rule_id=int(row.get("rule_id") or 0)) for row in (rows or [])]
            active = [p for p in pauses if p.get("status") == "active"]
            completed_count = sum(1 for p in pauses if p.get("status") == "completed")
            cancelled_count = sum(1 for p in pauses if p.get("status") == "cancelled")
            latest = max((_parse_dt(p.get("ends_at")) for p in active), default=None)
            if active:
                status_text = "🟢 активно"
            elif completed_count > 0:
                status_text = "✅ завершено"
            else:
                status_text = "⚪ паузы не создавались"
            return {
                "ok": True,
                "campaign_run_id": int(campaign_run_id),
                "enabled_snapshot": True,
                "seconds_snapshot": seconds,
                "seconds_text": format_repost_campaign_top_time_seconds_text(seconds),
                "pauses_total": len(pauses),
                "active_count": len(active),
                "completed_count": completed_count,
                "cancelled_count": cancelled_count,
                "latest_ends_at": latest.isoformat() if latest else None,
                "latest_ends_at_text": _format_end_text(latest) if latest else None,
                "pauses": pauses,
                "status_text": status_text,
            }
        except Exception:
            self.logger.exception("Не удалось получить паузы Времени в топе для запуска %s", campaign_run_id)
            return {
                "ok": False,
                "campaign_run_id": int(campaign_run_id),
                "enabled_snapshot": True,
                "seconds_snapshot": seconds,
                "seconds_text": format_repost_campaign_top_time_seconds_text(seconds),
                "pauses_total": 0,
                "active_count": 0,
                "completed_count": 0,
                "cancelled_count": 0,
                "latest_ends_at": None,
                "latest_ends_at_text": None,
                "pauses": [],
                "error_text": "Не удалось получить данные Времени в топе",
            }

    def build_pause_detail(self, pause_id: int) -> dict[str, Any]:
        try:
            row = self.repo.get_campaign_top_time_pause(pause_id)
        except Exception:
            self.logger.exception("Не удалось получить паузу Времени в топе pause_id=%s", pause_id)
            return {"ok": False, "error_text": "Не удалось получить паузу"}
        if not row:
            return {"ok": False, "error_text": "Пауза не найдена"}
        pause = self._build_pause(row, rule_id=int(row.get("rule_id") or 0))
        return {"ok": True, "pause": pause}

    def _build_pause(self, row: dict[str, Any], *, rule_id: int) -> dict[str, Any]:
        pause = dict(row or {})
        run_id = int(pause.get("campaign_run_id") or 0)
        title = str(pause.get("target_title") or pause.get("target_id") or "Канал/Группа").strip()
        status = str(pause.get("status") or "").strip().lower()
        status_texts = {
            "active": "🟢 Активна",
            "completed": "✅ Завершена",
            "cancelled": "🚫 Завершена вручную",
        }
        pause["status"] = status
        pause["title_text"] = title or "Канал/Группа"
        pause["target_text"] = pause["title_text"]
        pause["status_text"] = status_texts.get(status, "⚪ Неизвестный статус")
        pause["starts_at_text"] = _format_end_text(pause.get("starts_at"))
        pause["ends_at_text"] = _format_end_text(pause.get("ends_at"))
        pause["completed_at_text"] = _format_end_text(pause.get("completed_at"))
        pause["cancelled_at_text"] = _format_end_text(pause.get("cancelled_at"))
        pause["remaining_text"] = _format_remaining(pause.get("ends_at"))
        pause["can_cancel"] = status == "active"
        pause["open_run_callback"] = f"rule_repost_campaign_history_detail:{rule_id}:{run_id}"
        return pause
