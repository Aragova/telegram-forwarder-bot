from __future__ import annotations

from datetime import datetime

from app.repost_campaign_service import format_campaign_show_seconds_ru


def format_campaign_show_seconds_text(seconds: int | str | None) -> str:
    try:
        value = int(seconds or 0)
    except (TypeError, ValueError):
        return "не задан"
    if value <= 0:
        return "не задан"
    return format_campaign_show_seconds_ru(value)


def format_campaign_run_status_text(status: str | None) -> str:
    mapping = {
        "created": "⏳ В процессе",
        "sending": "⏳ В процессе",
        "sent": "✅ Отправлено",
        "partial": "🟡 Частично",
        "failed": "❌ Ошибка",
        "cancelled": "⛔ Отменено",
    }
    return mapping.get((status or "").strip().lower(), "⚪ Неизвестно")


def format_campaign_run_type_text(run_type: str | None) -> str:
    mapping = {
        "test": "📤 Проверочная публикация",
        "manual": "🚀 Кампания",
        "scheduled": "🕒 Запланированная кампания",
        "retry": "🔁 Повторный запуск",
    }
    return mapping.get((run_type or "").strip().lower(), "⚪ Неизвестный тип")


def format_campaign_render_mode_text(render_mode: str | None) -> str:
    mapping = {
        "telethon_builder": "Premium-отправка",
        "bot_api": "Обычная отправка через бота",
        "copy_message": "Копирование сообщения",
        "telethon_origin": "Оригинал через аккаунт",
        "mixed": "Смешанный способ отправки",
    }
    if render_mode is None:
        return "не указан"
    return mapping.get(str(render_mode).strip().lower(), str(render_mode))


def format_campaign_target_kind_text(target_kind: str | None) -> str:
    mapping = {
        "main": "Основной канал",
        "extra": "Дополнительный канал",
    }
    return mapping.get((target_kind or "").strip().lower(), "Канал кампании")


def format_campaign_send_status_text(send_status: str | None) -> str:
    mapping = {
        "pending": "⏳ Ожидает отправки",
        "sending": "⏳ Отправляется",
        "sent": "✅ Отправлено",
        "failed": "❌ Ошибка отправки",
    }
    return mapping.get((send_status or "").strip().lower(), "⚪ Неизвестно")


def format_campaign_datetime_text(value) -> str:
    if not value:
        return "не указано"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return "не указано"
    else:
        return "не указано"
    return dt.strftime("%d.%m %H:%M")


def format_campaign_error_text(value, *, limit: int = 120) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def format_campaign_delete_status_text(message: dict) -> str:
    status = (message.get("delete_status") or "").strip().lower()
    if status == "pending":
        return f"Удаление: запланировано на {format_campaign_datetime_text(message.get('delete_after_at'))}"
    if status == "processing":
        return "Удаление: выполняется"
    if status == "deleted":
        return f"Удаление: ✅ удалено {format_campaign_datetime_text(message.get('deleted_at'))}"
    if status == "failed":
        reason = format_campaign_error_text(message.get("delete_error_text")) or "не указано"
        attempts = int(message.get("delete_attempt_count") or 0)
        return (
            "Удаление: ❌ ошибка удаления\n"
            f"Причина удаления: {reason}\n"
            f"Попыток удаления: {attempts}"
        )
    return "Удаление: не запланировано"


def build_campaign_run_item_view(run: dict, *, index: int | None = None) -> dict:
    run_id = run.get("id")
    order = f"#{run_id}" if run_id is not None else (f"#{index}" if index is not None else "#—")
    return {
        "id": run_id,
        "title": f"{order} · {format_campaign_run_type_text(run.get('run_type'))} · {format_campaign_run_status_text(run.get('status'))}",
        "saved_post_text": f"Публикация: #{run.get('saved_post_id') or '—'}",
        "method_text": f"Метод: {format_campaign_render_mode_text(run.get('render_mode'))}",
        "targets_text": f"Каналы: {int(run.get('targets_success') or 0)}/{int(run.get('targets_total') or 0)}",
        "time_text": f"Время: {format_campaign_datetime_text(run.get('started_at'))}",
        "error_text": (
            f"Ошибка: {format_campaign_error_text(run.get('error_text'))}"
            if format_campaign_error_text(run.get("error_text"))
            else None
        ),
    }


def build_campaign_run_message_view(message: dict, *, index: int | None = None) -> dict:
    status_text = format_campaign_send_status_text(message.get("send_status"))
    kind_text = format_campaign_target_kind_text(message.get("target_kind"))
    send_error = format_campaign_error_text(message.get("send_error_text"))
    sent_id = message.get("sent_message_id")
    sent_at = message.get("sent_at")
    delete_status = (message.get("delete_status") or "").strip().lower()
    can_delete_now = (
        (message.get("send_status") or "").strip().lower() == "sent"
        and sent_id is not None
        and delete_status in {"pending", "failed", "processing"}
    )
    delete_action_text = None
    if can_delete_now:
        delete_action_text = "🔁 Повторить удаление" if delete_status == "failed" else "🧹 Удалить сейчас"
    return {
        "title": f"{index or 1}. {'✅' if message.get('send_status') == 'sent' else '❌'} {kind_text}",
        "channel_text": f"Канал: {message.get('target_title') or 'не указано'}",
        "target_text": f"Target: {message.get('target_id') or 'не указано'}",
        "send_status_text": f"Статус: {status_text}",
        "message_id_text": f"Message ID: {sent_id}" if sent_id is not None else None,
        "sent_at_text": f"Отправлено: {format_campaign_datetime_text(sent_at)}" if sent_at else None,
        "send_error_text": f"Ошибка отправки: {send_error}" if send_error else None,
        "delete_text": format_campaign_delete_status_text(message),
        "can_delete_now": can_delete_now,
        "delete_action_text": delete_action_text,
    }


def build_campaign_control_center_view_model(
    *,
    summary: dict,
    saved_post_line: str,
    control_center: dict | None,
) -> dict:
    readiness = (control_center or {}).get("readiness") or {}
    has_data = bool(control_center and control_center.get("ok"))
    if not has_data:
        status_title = "❌ Данные кампании временно недоступны"
    elif readiness.get("ready") is True:
        status_title = "✅ Кампания готова к запуску"
    else:
        status_title = "⚠️ Кампания требует настройки"

    post_text = str(saved_post_line or "").strip()
    if post_text.startswith("📝 Рекламный пост:"):
        post_value = post_text.replace("📝 Рекламный пост:", "", 1).strip()
    elif post_text.startswith("📝 Креатив:"):
        post_value = post_text.replace("📝 Креатив:", "", 1).strip()
    else:
        post_value = post_text
    post_line = f"📝 Креатив: {post_value or 'не выбран'}"

    targets_active = int((summary or {}).get("targets_active") or 0)
    targets_line = f"📣 Площадки: {targets_active} активных" if targets_active > 0 else "📣 Площадки: не подключены"

    show_seconds_value = (summary or {}).get("show_seconds")
    if not show_seconds_value:
        show_seconds_value = readiness.get("show_seconds")
    show_seconds_text = format_campaign_show_seconds_text(show_seconds_value)
    show_seconds_line = f"⏳ Срок показа: {show_seconds_text}"
    delete_line = "🧹 Auto-delete: включён после срока показа" if show_seconds_text != "не задан" else "🧹 Auto-delete: ожидает настройки срока"

    last_run = (control_center or {}).get("last_run")
    last_details = (control_center or {}).get("last_run_details") or {}
    if not last_run:
        last_run_block = "📊 Последний запуск\n\nПока запусков не было."
    else:
        run_view = build_campaign_run_item_view(last_run)
        lines = [
            "📊 Последний запуск",
            "",
            run_view["title"],
            run_view["saved_post_text"],
            run_view["targets_text"],
            run_view["method_text"],
            run_view["time_text"],
        ]
        if last_details.get("ok"):
            delete_summary = (last_details.get("summary") or {})
            lines.append(
                f"🧹 Удаление: {int(delete_summary.get('deleted') or 0)} удалено · "
                f"{int(delete_summary.get('delete_failed') or 0)} ошибок · "
                f"{int(delete_summary.get('delete_pending') or 0)} ожидает"
            )
        last_run_block = "\n".join(lines)

    issues = list((control_center or {}).get("issues") or [])
    if not issues:
        issues_block = "⚠️ Требует внимания\n\n✅ Критичных проблем нет"
    else:
        issues_lines = issues[:5]
        block_lines = ["⚠️ Требует внимания", ""] + [f"• {line}" for line in issues_lines]
        if len(issues) > 5:
            block_lines.append(f"• ...и ещё {len(issues) - 5}")
        issues_block = "\n".join(block_lines)

    can_check_publication = bool((summary or {}).get("saved_post_id"))
    can_launch = bool(readiness and readiness.get("ready"))
    return {
        "status_title": status_title,
        "post_line": post_line,
        "targets_line": targets_line,
        "show_seconds_line": show_seconds_line,
        "delete_line": delete_line,
        "last_run_block": last_run_block,
        "issues_block": issues_block,
        "can_launch": can_launch,
        "can_check_publication": can_check_publication,
        "last_run_id": (last_run or {}).get("id") if last_run else None,
    }
