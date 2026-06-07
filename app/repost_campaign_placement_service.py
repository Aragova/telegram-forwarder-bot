from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from app.repository_models import USER_TZ
from app.repost_campaign_view_model import format_campaign_datetime_text


PLACEMENT_STATUS_META: dict[str, dict[str, str]] = {
    "active": {"icon": "🟢", "text": "Активно"},
    "delete_problem": {"icon": "⚠️", "text": "Ошибка удаления"},
    "mixed": {"icon": "🟡", "text": "Активно, есть ошибки удаления"},
    "clean": {"icon": "✅", "text": "Очищено"},
}

RUN_TYPE_TEXTS: dict[str, str] = {
    "manual": "Запуск сейчас",
    "scheduled": "Запланированный запуск",
    "retry": "Повторный запуск",
    "test": "Тестовый запуск",
}


class RepostCampaignPlacementService:
    def __init__(self, repo, *, user_tz=None, logger=None):
        self.repo = repo
        self.user_tz = _normalize_user_tz(user_tz)
        self.logger = logger or logging.getLogger(__name__)

    def build_active_placements(
        self,
        *,
        rule_id: int,
        basic_only: bool = True,
        limit: int = 20,
    ) -> dict[str, Any]:
        try:
            raw_placements = self.repo.list_active_campaign_placements_for_rule(
                rule_id,
                limit=limit,
                basic_only=basic_only,
            )
            summary = self.repo.get_active_campaign_placements_summary_for_rule(
                rule_id,
                basic_only=basic_only,
            )
        except Exception:
            self.logger.exception(
                "Не удалось получить активные размещения для правила %s",
                rule_id,
            )
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "basic_only": bool(basic_only),
                "state": "unknown",
                "is_clean": False,
                "has_active": False,
                "has_delete_problem": False,
                "has_mixed": False,
                "placements": [],
                "error_text": "Не удалось получить активные размещения",
            }

        placements = [self._build_placement_view(dict(row or {}), rule_id=int(rule_id)) for row in (raw_placements or [])]

        placements_total = _to_int(summary.get("placements_total"), default=len(placements))
        active_total = _to_int(summary.get("active_total"))
        delete_problem_total = _to_int(summary.get("delete_problem_total"))
        mixed_total = _to_int(summary.get("mixed_total"))
        delete_pending_total = _to_int(summary.get("delete_pending_total"))
        delete_processing_total = _to_int(summary.get("delete_processing_total"))
        delete_failed_total = _to_int(summary.get("delete_failed_total"))

        state = _build_list_state(
            active_total=active_total,
            delete_problem_total=delete_problem_total,
            mixed_total=mixed_total,
        )

        return {
            "ok": True,
            "rule_id": int(rule_id),
            "basic_only": bool(basic_only),
            "limit": int(limit),
            "placements_total": placements_total,
            "active_total": active_total,
            "delete_problem_total": delete_problem_total,
            "mixed_total": mixed_total,
            "delete_pending_total": delete_pending_total,
            "delete_processing_total": delete_processing_total,
            "delete_failed_total": delete_failed_total,
            "has_active": active_total > 0,
            "has_delete_problem": delete_problem_total > 0,
            "has_mixed": mixed_total > 0 or (delete_problem_total > 0 and active_total > 0),
            "is_clean": state == "clean",
            "state": state,
            "placements": placements,
        }

    def build_clean_channel_state(
        self,
        *,
        rule_id: int,
        basic_only: bool = True,
        limit: int = 20,
    ) -> dict[str, Any]:
        active = self.build_active_placements(rule_id=rule_id, basic_only=basic_only, limit=limit)
        if not active.get("ok"):
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "basic_only": bool(basic_only),
                "state": "unknown",
                "is_clean": False,
                "has_active": False,
                "has_delete_problem": False,
                "has_mixed": False,
                "placements_total": 0,
                "active_total": 0,
                "delete_problem_total": 0,
                "mixed_total": 0,
                "title": "🧹 Чистый канал",
                "status_text": "⚠️ Не удалось проверить канал",
                "description_text": active.get("error_text") or "Не удалось получить активные размещения",
                "placements": [],
                "error_text": active.get("error_text"),
            }

        state = str(active.get("state") or "clean")
        status_text, description_text = _clean_channel_texts(state)
        return {
            "ok": True,
            "rule_id": int(rule_id),
            "basic_only": bool(basic_only),
            "state": state,
            "is_clean": bool(active.get("is_clean")),
            "has_active": bool(active.get("has_active")),
            "has_delete_problem": bool(active.get("has_delete_problem")),
            "has_mixed": bool(active.get("has_mixed")),
            "placements_total": _to_int(active.get("placements_total")),
            "active_total": _to_int(active.get("active_total")),
            "delete_problem_total": _to_int(active.get("delete_problem_total")),
            "mixed_total": _to_int(active.get("mixed_total")),
            "title": "🧹 Чистый канал",
            "status_text": status_text,
            "description_text": description_text,
            "placements": active.get("placements") or [],
        }

    def build_launch_policy_preview(
        self,
        *,
        rule_id: int,
        clean_channel_enabled: bool,
        launch_mode: str,
        basic_only: bool = True,
    ) -> dict[str, Any]:
        normalized_launch_mode = str(launch_mode or "").strip().lower()
        if normalized_launch_mode not in {"manual", "scheduled"}:
            normalized_launch_mode = "manual"

        active = self.build_active_placements(rule_id=rule_id, basic_only=basic_only)
        if not active.get("ok"):
            return {
                "ok": False,
                "rule_id": int(rule_id),
                "launch_mode": normalized_launch_mode,
                "clean_channel_enabled": bool(clean_channel_enabled),
                "state": "unknown",
                "can_launch": False,
                "requires_confirmation": False,
                "action": "block",
                "blocking_text": active.get("error_text") or "Не удалось получить активные размещения",
                "warning_text": None,
                "active_placements_total": 0,
                "delete_problem_total": 0,
                "placements": [],
                "error_text": active.get("error_text"),
            }

        state = str(active.get("state") or "clean")
        if state == "clean":
            action = "allow"
            can_launch = True
            requires_confirmation = False
            blocking_text = None
            warning_text = None
        elif clean_channel_enabled:
            action = "block"
            can_launch = False
            requires_confirmation = False
            blocking_text = _launch_blocking_text(normalized_launch_mode)
            warning_text = None
        else:
            action = "allow_with_warning"
            can_launch = True
            requires_confirmation = True
            blocking_text = None
            warning_text = (
                "Чистый канал выключен. ViMi разрешит запуск поверх активной рекламы, "
                "но каждое размещение нужно контролировать отдельно."
            )

        return {
            "ok": True,
            "rule_id": int(rule_id),
            "launch_mode": normalized_launch_mode,
            "clean_channel_enabled": bool(clean_channel_enabled),
            "state": state,
            "can_launch": can_launch,
            "requires_confirmation": requires_confirmation,
            "action": action,
            "blocking_text": blocking_text,
            "warning_text": warning_text,
            "active_placements_total": _to_int(active.get("active_total")),
            "delete_problem_total": _to_int(active.get("delete_problem_total")),
            "placements": active.get("placements") or [],
        }

    def _build_placement_view(self, row: dict[str, Any], *, rule_id: int) -> dict[str, Any]:
        run_id = _to_int(row.get("run_id") or row.get("id"))
        row_rule_id = _to_int(row.get("rule_id"), default=rule_id)
        run_type = str(row.get("run_type") or "").strip().lower()
        run_type_text = _run_type_text(run_type)

        delete_pending = _to_int(row.get("delete_pending"))
        delete_processing = _to_int(row.get("delete_processing"))
        delete_failed = _to_int(row.get("delete_failed"))
        active_messages_total = _to_int(row.get("active_messages_total"), default=_to_int(row.get("sent")))
        placement_status = _normalize_placement_status(
            row.get("placement_status"),
            delete_pending=delete_pending,
            delete_processing=delete_processing,
            delete_failed=delete_failed,
        )
        status_meta = PLACEMENT_STATUS_META[placement_status]

        delete_after_value = row.get("delete_after_at_min") or row.get("delete_after_at_max")
        delete_after_text = _format_user_dt(delete_after_value, user_tz=self.user_tz)
        short_title = f"#{run_id} · {run_type_text}"

        return {
            "run_id": run_id,
            "rule_id": row_rule_id,
            "saved_post_id": row.get("saved_post_id"),
            "run_type": run_type,
            "run_type_text": run_type_text,
            "run_status": row.get("run_status"),
            "placement_status": placement_status,
            "placement_status_text": status_meta["text"],
            "placement_icon": status_meta["icon"],
            "scheduled_post_id": row.get("scheduled_post_id"),
            "created_at": row.get("created_at"),
            "created_text": _format_user_dt(row.get("created_at"), user_tz=self.user_tz),
            "started_at": row.get("started_at"),
            "started_text": _format_user_dt(row.get("started_at"), user_tz=self.user_tz),
            "finished_at": row.get("finished_at"),
            "finished_text": _format_user_dt(row.get("finished_at"), user_tz=self.user_tz),
            "delete_after_at_min": row.get("delete_after_at_min"),
            "delete_after_at_max": row.get("delete_after_at_max"),
            "delete_after_text": delete_after_text,
            "last_sent_at": row.get("last_sent_at"),
            "last_sent_text": _format_user_dt(row.get("last_sent_at"), user_tz=self.user_tz),
            "targets_total": _to_int(row.get("targets_total")),
            "targets_success": _to_int(row.get("targets_success")),
            "targets_failed": _to_int(row.get("targets_failed")),
            "active_messages_total": active_messages_total,
            "delete_pending": delete_pending,
            "delete_processing": delete_processing,
            "delete_failed": delete_failed,
            "can_open_details": run_id > 0,
            "can_delete_now": placement_status != "clean" and active_messages_total > 0,
            "can_open_report": run_id > 0,
            "details_callback_data": f"rule_repost_campaign_history_detail:{row_rule_id}:{run_id}",
            "delete_callback_data": f"rule_repost_campaign_run_delete_confirm:{row_rule_id}:{run_id}",
            "report_callback_data": f"rule_repost_campaign_views_report:{row_rule_id}:{run_id}",
            "short_title": short_title,
            "summary_text": _build_summary_text(
                short_title=short_title,
                active_messages_total=active_messages_total,
                delete_pending=delete_pending,
                delete_processing=delete_processing,
                delete_failed=delete_failed,
                delete_after_text=delete_after_text,
            ),
        }


def _normalize_user_tz(user_tz):
    if user_tz is None:
        return USER_TZ
    if isinstance(user_tz, timezone):
        return user_tz
    if isinstance(user_tz, int):
        return timezone(timedelta(minutes=user_tz))
    return user_tz


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_user_dt(value, *, user_tz=USER_TZ) -> str | None:
    dt = _parse_dt(value)
    if dt is None:
        return None
    offset = user_tz.utcoffset(dt) if hasattr(user_tz, "utcoffset") else None
    if offset is not None:
        offset_seconds = int(offset.total_seconds())
        if offset_seconds % 3600 == 0:
            text = format_campaign_datetime_text(dt, timezone_offset_hours=offset_seconds // 3600)
            return None if text == "не указано" else text
    return dt.astimezone(user_tz).strftime("%d.%m %H:%M")


def _to_int(value, *, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _run_type_text(run_type: str | None) -> str:
    value = str(run_type or "").strip().lower()
    return RUN_TYPE_TEXTS.get(value, "Запуск кампании")


def _normalize_placement_status(
    value,
    *,
    delete_pending: int,
    delete_processing: int,
    delete_failed: int,
) -> str:
    status = str(value or "").strip().lower()
    if status in PLACEMENT_STATUS_META:
        return status
    active_count = int(delete_pending) + int(delete_processing)
    if delete_failed > 0 and active_count > 0:
        return "mixed"
    if delete_failed > 0:
        return "delete_problem"
    if active_count > 0:
        return "active"
    return "clean"


def _build_list_state(*, active_total: int, delete_problem_total: int, mixed_total: int) -> str:
    if mixed_total > 0:
        return "mixed"
    if delete_problem_total > 0 and active_total > 0:
        return "mixed"
    if delete_problem_total > 0:
        return "delete_problem"
    if active_total > 0:
        return "active"
    return "clean"


def _build_summary_text(
    *,
    short_title: str,
    active_messages_total: int,
    delete_pending: int,
    delete_processing: int,
    delete_failed: int,
    delete_after_text: str | None,
) -> str:
    waiting_delete = int(delete_pending) + int(delete_processing)
    lines = [
        short_title,
        f"✅ Опубликовано: {int(active_messages_total)}",
    ]
    if waiting_delete > 0:
        lines.append(f"🧹 Ожидают удаления: {waiting_delete}")
    if delete_failed > 0:
        lines.append(f"⚠️ Ошибки удаления: {int(delete_failed)}")
    if delete_after_text:
        lines.append(f"⏳ Автоудаление: {delete_after_text}")
    return "\n".join(lines)


def _clean_channel_texts(state: str) -> tuple[str, str]:
    if state == "active":
        return (
            "🟢 Есть активные размещения",
            "Сейчас в канале есть активные рекламные посты. Их можно открыть, удалить или дождаться автоудаления.",
        )
    if state == "delete_problem":
        return (
            "⚠️ Есть ошибки удаления",
            "Некоторые рекламные посты не удалось удалить автоматически. Проверьте размещения и повторите удаление.",
        )
    if state == "mixed":
        return (
            "🟡 Есть активные размещения и ошибки удаления",
            "Часть рекламных постов ещё активна, а по некоторым есть ошибки удаления.",
        )
    return (
        "✅ Канал чист",
        "Активных рекламных размещений по этому правилу нет.",
    )


def _launch_blocking_text(launch_mode: str) -> str:
    if launch_mode == "scheduled":
        return (
            "Сейчас уже есть активное рекламное размещение. Запланированный запуск не стартует поверх него, "
            "пока «Чистый канал» включён."
        )
    return (
        "Сейчас уже есть активное рекламное размещение. Новый запуск не стартует поверх него, "
        "пока «Чистый канал» включён."
    )
