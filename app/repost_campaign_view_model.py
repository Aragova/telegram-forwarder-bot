from __future__ import annotations

from datetime import datetime, timedelta

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

def normalize_campaign_target_error_text(error_text: str | None) -> str:
    text = str(error_text or "").strip()
    if not text:
        return ""
    if "Аккаунт-парсер не имеет права публиковать" in text:
        return "ViMi пока не видит право публикации в этом канале/группе. Проверьте роль администратора и разрешение на отправку сообщений."
    if "Аккаунт-парсер не видит" in text:
        return "ViMi пока не видит этот канал/группу. Проверьте, что канал добавлен правильно и доступен для аккаунта ViMi."
    if "Не удалось подтвердить право публикации" in text:
        return "Не удалось проверить доступ к публикации. Проверьте, что аккаунт ViMi добавлен в администраторы канала/группы."
    return format_campaign_error_text(text) or ""



def format_campaign_target_display_title(target: dict, *, index: int | None = None) -> str:
    target_id = str(target.get("target_id") or "")
    title = str(target.get("title") or "").strip()
    username = str(target.get("username") or "").strip().lstrip("@")

    if title and title != target_id:
        display_title = title
    elif username:
        display_title = f"@{username}"
    elif index is not None:
        display_title = f"Канал/Группа #{index}"
    else:
        display_title = "Канал/Группа"
    return display_title

def build_campaign_target_item_view(target: dict, *, index: int | None = None) -> dict:
    row_id = int(target.get("id") or 0)
    target_id = str(target.get("target_id") or "")
    title_raw = format_campaign_target_display_title(target, index=index)
    is_active = bool(target.get("is_active"))
    check_error = normalize_campaign_target_error_text(target.get("last_check_error"))
    requires_attention = bool(check_error)
    if requires_attention:
        status_icon = "⚠️"
        status_line = "Статус: ⚠️ требует внимания"
        check_line = "Проверка: ⚠️ нужна проверка"
        can_pause = False
        can_enable = False
    elif is_active:
        status_icon = "🟢"
        status_line = "Статус: 🟢 активен"
        check_line = "Проверка: ✅ готово"
        can_pause = True
        can_enable = False
    else:
        status_icon = "⏸"
        status_line = "Статус: ⏸ на паузе"
        check_line = "Проверка: ✅ готово"
        can_pause = False
        can_enable = True
    order = f"{index}. " if index is not None else ""
    thread_id = target.get("target_thread_id")
    return {
        "row_id": row_id,
        "title": f"{order}{status_icon} {title_raw}",
        "status_line": status_line,
        "target_line": None,
        "technical_line": f"Технический ID: {target_id}",
        "thread_line": f"Тема: {thread_id}" if thread_id is not None else "Тема: не задана",
        "check_line": check_line,
        "error_line": f"Ошибка: {check_error}" if check_error else None,
        "requires_attention": requires_attention,
        "can_pause": can_pause,
        "can_enable": can_enable,
        "can_resume": can_enable,
        "can_check": True,
        "can_delete": True,
        "can_remove": True,
        "status_icon": status_icon,
        "check_action_text": "🔎 Проверить",
    }


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


def _format_active_channels_count(value: int) -> str:
    if value <= 0:
        return "не подключены"
    if value == 1:
        return "1 активный"
    return f"{value} активных"


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


def build_campaign_views_report_view_model(*, report: dict) -> dict:
    status = str(report.get("status") or "unavailable")
    status_map = {
        "ready": "✅ Просмотры собраны",
        "partial": "🟡 Просмотры собраны частично",
        "unavailable": "⚠️ Просмотры недоступны",
        "not_found": "❌ Запуск не найден",
    }
    items = report.get("items") or []
    channel_lines = []
    for item in items:
        title = str(item.get("target_title") or item.get("target_id") or "Канал/группа")
        if item.get("views_status") == "ok":
            line = f"👁 {int(item.get('views') or 0):,}".replace(",", " ") + f" — {title}"
            if item.get("is_album"):
                line += f" · альбом {int(item.get('album_items') or 0)} медиа"
        else:
            line = f"⚠️ нет данных — {title}"
        channel_lines.append(line)
    top_lines = []
    for idx, item in enumerate(report.get("top_items") or [], 1):
        top_lines.append(f"🏆 {idx}. {item.get('target_title') or item.get('target_id')} — {int(item.get('views') or 0):,}".replace(",", " "))
    problem_lines = []
    for item in report.get("problem_items") or []:
        reason = format_campaign_error_text(item.get("error_text"), limit=160) or "не удалось получить просмотры"
        problem_lines.append(f"⚠️ {item.get('target_title') or item.get('target_id')} — {reason}")
    delete_statuses = {str((x or {}).get("delete_status") or "").strip().lower() for x in items}
    if delete_statuses and delete_statuses.issubset({"deleted"}):
        delete_note = "🧹 Публикации уже удалены. Просмотры показаны по данным, доступным Telegram на момент отчёта."
    else:
        delete_note = "⏳ Часть публикаций ещё ожидает автоудаления. Просмотры могут увеличиться."
    return {
        "title": "📊 Отчёт просмотров",
        "status_line": status_map.get(status, status_map["unavailable"]),
        "total_views_line": f"👁 Всего просмотров: {int(report.get('views_total') or 0):,}".replace(",", " "),
        "coverage_line": f"📣 Каналов с данными: {int(report.get('views_available') or 0)} / {int(report.get('sent_total') or 0)}",
        "post_line": f"📝 Пост: #{report.get('saved_post_id') or '—'}",
        "run_line": f"🧾 Запуск: #{report.get('run_id') or '—'}",
        "show_seconds_line": f"⏳ Время показа: {format_campaign_show_seconds_text(report.get('show_seconds'))}",
        "delete_note_line": delete_note,
        "summary_line": report.get("summary_text") or "",
        "channel_lines": channel_lines,
        "problem_lines": problem_lines,
        "top_lines": top_lines,
        "can_refresh": True,
        "can_open_details": bool(report.get("run_id")),
    }


def build_campaign_posts_library_view_model(*, library: dict) -> dict:
    summary = library.get("summary") or {}
    items_vm = []
    for item in library.get("items") or []:
        kind = item.get("kind") or "unknown"
        kind_text = {"album": "альбом", "photo": "фото", "video": "видео", "text": "текст"}.get(kind, kind)
        media_tail = f" · {int(item.get('media_count') or 0)} медиа" if item.get("is_album") else ""
        if item.get("views_total") is None:
            stats_line = f"👁 Просмотры: пока недоступны · {int(item.get('runs_count') or 0)} запуска"
        else:
            stats_line = f"👁 {int(item.get('views_total') or 0):,} просмотров · {int(item.get('runs_count') or 0)} запуска".replace(",", " ")
        top = (item.get("top_channels") or [{}])[0]
        top_line = None
        if top and top.get("target_title"):
            top_line = f"🏆 Лучший канал: {top.get('target_title')} · {int(top.get('views_total') or 0):,}".replace(",", " ")
        items_vm.append({
            "saved_post_id": int(item.get("saved_post_id") or 0),
            "title_line": f"📝 #{int(item.get('saved_post_id') or 0)} · {kind_text}{media_tail}",
            "current_line": "✅ Сейчас выбран" if item.get("is_current") else None,
            "stats_line": stats_line,
            "last_run_line": f"Последний запуск: {format_campaign_datetime_text(item.get('last_started_at'))}",
            "channels_line": f"📣 Размещений: {int(item.get('placements_sent') or 0)} · ошибок: {int(item.get('placements_failed') or 0)}",
            "coverage_line": f"Данные просмотров: {int(item.get('views_available') or 0)}/{int(item.get('views_available') or 0) + int(item.get('views_unavailable') or 0)}",
            "top_line": top_line,
            "has_views": item.get("views_total") is not None,
            "has_stats": True,
        })
    return {
        "title": "📚 Библиотека постов",
        "intro_line": "Здесь хранятся рекламные посты этой кампании и статистика по ним.",
        "summary_line": f"Постов: {int(summary.get('posts_total') or 0)} · запусков: {int(summary.get('runs_total') or 0)}",
        "views_line": ("👁 Всего просмотров: " + f"{int(summary.get('views_total') or 0):,}".replace(",", " ")) if summary.get("views_total") is not None else "👁 Просмотры: пока недоступны",
        "coverage_line": f"📊 Данные просмотров: {int(summary.get('views_available') or 0)} / {int(summary.get('views_available') or 0) + int(summary.get('views_unavailable') or 0)}",
        "items": items_vm,
        "empty_text": None if items_vm else "Пока в библиотеке нет постов.",
    }


def build_campaign_post_stats_view_model(*, stats: dict) -> dict:
    kind = stats.get("kind") or "unknown"
    kind_text = {"album": "Альбом", "photo": "Фото", "video": "Видео", "text": "Текст"}.get(kind, kind)
    channels = []
    for ch in stats.get("top_channels") or []:
        channels.append(f"👁 {int(ch.get('views_total') or 0):,} — {ch.get('target_title') or ch.get('target_id')} · запусков: {int(stats.get('runs_count') or 0)}".replace(",", " "))
    for ch in stats.get("problem_channels") or []:
        channels.append(f"⚠️ нет данных — {ch.get('target_title') or ch.get('target_id')}")
    runs = []
    return {
        "title": f"📄 Статистика поста #{int(stats.get('saved_post_id') or 0)}",
        "kind_line": f"📝 {kind_text}" + (f" · {int(stats.get('media_count') or 0)} медиа" if stats.get("is_album") else ""),
        "current_line": "✅ Сейчас выбран" if stats.get("is_current") else None,
        "views_line": ("👁 Всего просмотров: " + f"{int(stats.get('views_total') or 0):,}".replace(",", " ")) if stats.get("views_total") is not None else "👁 Просмотры: пока недоступны",
        "runs_line": f"🔁 Запусков: {int(stats.get('runs_count') or 0)}",
        "placements_line": f"📣 Размещений: {int(stats.get('placements_sent') or 0)}",
        "coverage_line": f"📊 Данные просмотров: {int(stats.get('views_available') or 0)} / {int(stats.get('views_available') or 0) + int(stats.get('views_unavailable') or 0)}",
        "channels_lines": channels,
        "runs_lines": runs,
    }


def build_campaign_control_center_view_model(
    *,
    summary: dict,
    saved_post_line: str,
    control_center: dict | None,
) -> dict:
    readiness = (control_center or {}).get("readiness") or {}
    has_data = bool(control_center and control_center.get("ok"))

    post_text = str(saved_post_line or "").strip()
    if post_text.startswith("📝 Рекламный пост:"):
        post_value = post_text.replace("📝 Рекламный пост:", "", 1).strip()
    elif post_text.startswith("📝 Креатив:"):
        post_value = post_text.replace("📝 Креатив:", "", 1).strip()
    else:
        post_value = post_text
    creative_line = f"📝 Рекламный пост: {post_value or 'не выбран'}"

    targets_active = int((summary or {}).get("targets_active") or 0)
    targets_ready = (summary or {}).get("targets_ready")
    targets_with_errors = (summary or {}).get("targets_with_errors")
    targets_line = f"📣 Каналы/Группы: {_format_active_channels_count(targets_active)}"
    if targets_ready is not None and targets_with_errors is not None and targets_active > 0:
        ready_count = int(targets_ready or 0)
        errors_count = int(targets_with_errors or 0)
        targets_line = f"📣 Каналы/Группы: {_format_active_channels_count(targets_active)} · {ready_count} готовы"
        if errors_count > 0:
            targets_line += f" · {errors_count} требуют проверки"

    show_seconds_value = (summary or {}).get("show_seconds")
    if not show_seconds_value:
        show_seconds_value = readiness.get("show_seconds")
    show_seconds_text = format_campaign_show_seconds_text(show_seconds_value)
    show_seconds_line = f"⏳ Время показа: {show_seconds_text}"

    last_run = (control_center or {}).get("last_run")
    last_details = (control_center or {}).get("last_run_details") or {}
    if not last_run:
        last_run_line = "📊 Последний запуск: ещё не было"
    else:
        run_view = build_campaign_run_item_view(last_run)
        last_run_line = f"📊 Последний запуск: {run_view['title']} · {int((last_run or {}).get('targets_success') or 0)}/{int((last_run or {}).get('targets_total') or 0)}"

    delete_summary = (last_details.get("summary") or {}) if last_details.get("ok") else {}
    delete_pending = int(delete_summary.get("delete_pending") or 0)
    delete_failed = int(delete_summary.get("delete_failed") or 0)
    deleted = int(delete_summary.get("deleted") or 0)
    auto_delete_line = "🧹 Автоудаление: включено" if show_seconds_text != "не задан" else "🧹 Автоудаление: не задано"
    if delete_failed > 0:
        last_run_delete_line = f"🧹 Удаление последнего запуска: {delete_failed} ошибка"
    elif delete_pending > 0:
        last_run_delete_line = f"🧹 Удаление последнего запуска: ожидает {delete_pending}"
    elif deleted > 0:
        last_run_delete_line = "🧹 Удаление последнего запуска: всё удалено"
    else:
        last_run_delete_line = "🧹 Удаление последнего запуска: не применялось"

    has_post = bool((summary or {}).get("saved_post_id"))
    has_show_seconds = show_seconds_text != "не задан"
    has_targets = targets_active > 0
    if delete_failed > 0:
        title_status = "⚠️ Есть проблемы удаления"
        next_step_line = "Следующий шаг: проверьте проблемы в последнем запуске."
        primary_action = "open_last_run" if last_run else None
    elif readiness.get("ready") is True:
        title_status = "✅ Готова к запуску"
        next_step_line = "Можно запускать кампанию."
        primary_action = "launch"
    elif not has_post:
        title_status = "⚠️ Нужно выбрать рекламный пост"
        next_step_line = "Следующий шаг: выберите рекламный пост."
        primary_action = "creative"
    elif not has_show_seconds:
        title_status = "⚠️ Нужно настроить время показа"
        next_step_line = "Следующий шаг: задайте время показа."
        primary_action = "show_seconds"
    elif not has_targets:
        title_status = "⚠️ Нужно добавить каналы/группы"
        next_step_line = "Следующий шаг: добавьте каналы/группы."
        primary_action = "targets"
    else:
        title_status = "⚠️ Кампания требует настройки"
        next_step_line = "Следующий шаг: выберите рекламный пост."
        primary_action = "creative"

    can_check_publication = bool((summary or {}).get("saved_post_id"))
    can_launch = bool(readiness and readiness.get("ready"))
    return {
        "title_status": title_status,
        "creative_line": creative_line,
        "targets_line": targets_line,
        "show_seconds_line": show_seconds_line,
        "auto_delete_line": auto_delete_line,
        "last_run_line": last_run_line,
        "last_run_delete_line": last_run_delete_line,
        "delete_line": last_run_delete_line,
        "next_step_line": next_step_line,
        "primary_action": primary_action,
        "can_launch": can_launch,
        "can_check_publication": can_check_publication,
        "last_run_id": (last_run or {}).get("id") if last_run else None,
        "has_delete_issues": delete_failed > 0,
    }


def build_campaign_scenario_preview_view_model(
    *,
    rule_id: int,
    summary: dict,
    saved_post_id: int | None,
    saved_post_description: str | None,
    saved_post_line: str | None = None,
    readiness: dict | None = None,
    control_center: dict | None = None,
    targets_preview_text: str = "",
    warnings: list[str] | None = None,
    now: datetime | None = None,
) -> dict:
    del rule_id
    summary = dict(summary or {})
    summary.setdefault("saved_post_id", saved_post_id)
    if saved_post_line:
        post_line = saved_post_line
    elif saved_post_id:
        post_line = f"📝 Рекламный пост: #{saved_post_id} · {saved_post_description or 'пост'}"
    else:
        post_line = "📝 Рекламный пост: не выбран"

    cc_payload = control_center
    if cc_payload is None:
        cc_payload = {"ok": bool(readiness), "readiness": readiness or {}, "last_run": None, "last_run_details": None, "issues": []}
    vm = build_campaign_control_center_view_model(summary=summary, saved_post_line=post_line, control_center=cc_payload)

    show_seconds = int((summary or {}).get("show_seconds") or 0)
    expected_delete_line = "🕒 Ожидаемое удаление: не рассчитано"
    if show_seconds > 0:
        now_dt = now or datetime.now()
        delete_at = now_dt + timedelta(seconds=show_seconds)
        if delete_at.date() == now_dt.date():
            expected_delete_line = f"🕒 Ожидаемое удаление: сегодня в {delete_at.strftime('%H:%M')}"
        else:
            expected_delete_line = f"🕒 Ожидаемое удаление: {delete_at.strftime('%d.%m в %H:%M')}"

    checks_status_text = str((readiness or {}).get("checks_status_text") or "").lower()
    targets_active = int((summary or {}).get("targets_active") or 0)
    targets_with_errors = int((summary or {}).get("targets_with_errors") or 0)
    has_checks_problem = (
        targets_with_errors > 0
        or "ошиб" in checks_status_text
        or "треб" in checks_status_text
        or "⚠" in checks_status_text
        or "❌" in checks_status_text
    )
    can_check_rights = bool(
        targets_active > 0
        and (
            readiness is None
            or readiness.get("ready") is not True
            or has_checks_problem
        )
    )

    if not saved_post_id:
        vm["title_status"] = "⚠️ Нужно выбрать рекламный пост"
        vm["next_step_line"] = "Следующий шаг: выберите рекламный пост."
    elif show_seconds <= 0:
        vm["title_status"] = "⚠️ Нужно настроить время показа"
        vm["next_step_line"] = "Следующий шаг: задайте время показа."
    elif targets_active <= 0:
        vm["title_status"] = "⚠️ Нужно добавить каналы/группы"
        vm["next_step_line"] = "Следующий шаг: добавьте каналы/группы."
    elif has_checks_problem and not (readiness and readiness.get("ready") is True):
        vm["title_status"] = "⚠️ Нужно проверить каналы/группы"
        vm["next_step_line"] = "Следующий шаг: проверьте права каналов/групп."
    elif readiness and readiness.get("ready") is True:
        vm["title_status"] = "✅ Готова к запуску"
        vm["next_step_line"] = "Можно запускать кампанию."

    issues_lines = [f"⚠️ {format_campaign_error_text(i, limit=180)}" for i in ((cc_payload or {}).get("issues") or []) if format_campaign_error_text(i, limit=180)]
    for w in (warnings or []):
        text = str(w or "").strip()
        if text:
            issues_lines.append(text if text.startswith("⚠") else f"⚠️ {text}")

    return {
        "title": "👁 Предпросмотр сценария",
        "status_line": vm["title_status"],
        "creative_line": vm["creative_line"],
        "targets_line": vm["targets_line"],
        "show_seconds_line": vm["show_seconds_line"],
        "auto_delete_line": vm["auto_delete_line"],
        "expected_delete_line": expected_delete_line,
        "readiness_line": (readiness or {}).get("summary_text") or "",
        "issues_lines": issues_lines,
        "targets_preview_text": targets_preview_text or "пока нет активных каналов",
        "scenario_steps": [
            "• бот опубликует рекламный пост в основной канал правила;",
            "• отправит копии в выбранные каналы/группы;",
            "• сохранит результат по каждому получателю;",
            "• автоматически удалит публикации после времени показа;",
            "• история размещения останется в отчёте.",
        ],
        "next_step_line": vm["next_step_line"],
        "can_launch": bool(readiness and readiness.get("ready") is True),
        "can_check_rights": can_check_rights,
        "can_edit_post": True,
        "can_edit_targets": True,
        "can_edit_show_seconds": True,
        "saved_post_id": saved_post_id,
        "show_seconds": show_seconds,
        "targets_active": targets_active,
        "targets_ready": int((summary or {}).get("targets_ready") or 0),
        "targets_with_errors": targets_with_errors,
    }


def build_campaign_launch_readiness_view_model(*, readiness: dict, now: datetime | None = None) -> dict:
    readiness = dict(readiness or {})
    can_launch = bool(readiness.get("can_launch"))
    saved_post_exists = bool(readiness.get("saved_post_exists"))
    show_seconds = int(readiness.get("show_seconds") or 0)
    main_target_ready = bool(readiness.get("main_target_ready"))
    extra_active_problem = int(readiness.get("extra_active_problem") or 0)
    if can_launch:
        status_line = "✅ Кампания готова к запуску"
        next_step_line = "Можно запускать кампанию."
    elif not main_target_ready:
        status_line = "❌ Не задан основной канал"
        next_step_line = "Вернитесь к карточке правила и проверьте основной канал."
    elif not saved_post_exists:
        status_line = "⚠️ Нужно выбрать рекламный пост"
        next_step_line = "Выберите рекламный пост перед запуском."
    elif show_seconds <= 0:
        status_line = "⚠️ Нужно задать время показа"
        next_step_line = "Задайте время показа перед запуском."
    elif extra_active_problem > 0:
        status_line = "⚠️ Нужно проверить каналы/группы"
        next_step_line = "Проверьте права или поставьте проблемные каналы на паузу."
    else:
        status_line = "⚠️ Кампания требует настройки"
        next_step_line = "Проверьте параметры кампании перед запуском."
    expected_delete_line = "🕒 Ожидаемое удаление: не рассчитано"
    if show_seconds > 0:
        now_dt = now or datetime.now()
        delete_at = now_dt + timedelta(seconds=show_seconds)
        expected_delete_line = f"🕒 Ожидаемое удаление: сегодня в {delete_at.strftime('%H:%M')}" if delete_at.date() == now_dt.date() else f"🕒 Ожидаемое удаление: {delete_at.strftime('%d.%m в %H:%M')}"
    return {
        "title": "🚦 Проверка перед запуском",
        "status_line": status_line,
        "will_send_line": f"📣 Будет опубликовано: {int(readiness.get('will_send_total') or 0)} получателей",
        "will_skip_line": (f"⏸ Будет пропущено: {int(readiness.get('will_skip_total') or 0)} (на паузе: {int(readiness.get('extra_paused') or 0)} · требуют настройки: {int(readiness.get('extra_problem') or 0)})" if int(readiness.get("will_skip_total") or 0) > 0 else "⏸ Будет пропущено: 0"),
        "show_seconds_line": f"⏳ Время показа: {format_campaign_show_seconds_text(show_seconds)}",
        "auto_delete_line": "🧹 Автоудаление: включено" if show_seconds > 0 else "🧹 Автоудаление: не задано",
        "expected_delete_line": expected_delete_line,
        "block_reason_lines": list(readiness.get("block_reasons") or []),
        "warning_lines": list(readiness.get("warnings") or []),
        "ready_targets_line": f"✅ Готовы: {int(readiness.get('extra_ready') or 0)}",
        "problem_targets_line": f"⚠️ Требуют настройки: {int(readiness.get('extra_problem') or 0)}",
        "paused_targets_line": f"⏸ На паузе: {int(readiness.get('extra_paused') or 0)}",
        "next_step_line": next_step_line,
        "can_launch": can_launch,
        "can_check_rights": int(readiness.get("extra_problem") or 0) > 0 or extra_active_problem > 0,
        "can_open_targets": True,
        "can_edit_post": True,
        "can_edit_show_seconds": True,
    }
