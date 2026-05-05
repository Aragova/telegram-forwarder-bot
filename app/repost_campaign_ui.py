from __future__ import annotations

from datetime import datetime

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_service import format_campaign_show_seconds_ru


def format_repost_campaign_readiness_block(readiness: dict | None) -> str:
    if not readiness:
        return "🚦 Готовность кампании\n\n⚠️ Данные готовности временно недоступны"
    return (
        "🚦 Готовность кампании\n\n"
        f"📝 Пост: {readiness.get('post_status_text') or '⚠️ недоступно'}\n"
        f"⏳ Срок: {readiness.get('show_seconds_status_text') or '⚠️ недоступно'}\n"
        f"📣 Каналы: {readiness.get('targets_status_text') or '⚠️ недоступно'}\n"
        f"🔐 Проверка: {readiness.get('checks_status_text') or '⚠️ недоступно'}\n\n"
        f"{readiness.get('summary_text') or '⚠️ Кампания не готова: исправьте пункты выше'}"
    )


def build_repost_campaign_menu_view(
    *, rule_id: int, summary: dict, saved_post_line: str, readiness: dict | None = None
) -> tuple[str, InlineKeyboardMarkup]:
    show_seconds_text = str((summary or {}).get("show_seconds_text") or "не задан")
    targets_active = int((summary or {}).get("targets_active") or 0)
    saved_post_id = (summary or {}).get("saved_post_id")
    readiness_block = format_repost_campaign_readiness_block(readiness)
    text = (
        "💰 Рекламная кампания\n\n"
        "Тестовый режим для админа.\n\n"
        "Новый репост из этого правила будет опубликован в основной канал и каналы кампании.\n"
        "После окончания срока показа бот автоматически удалит рекламные публикации.\n\n"
        f"⏳ Срок показа: {show_seconds_text}\n"
        f"📣 Каналы кампании: {targets_active}\n"
        f"{saved_post_line}\n\n"
        f"{readiness_block}"
    )
    rows = [
        [InlineKeyboardButton(text="⏳ Срок показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="📣 Каналы кампании", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="👁 Предпросмотр кампании", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
        [InlineKeyboardButton(text="📊 История кампаний", callback_data=f"rule_repost_campaign_history:{rule_id}")],
    ]
    if saved_post_id:
        test_button_text = "🚀 Тестовый запуск"
        if readiness and readiness.get("ready") is False:
            test_button_text = "🧪 Тестовый запуск"
        rows.append([InlineKeyboardButton(text=test_button_text, callback_data=f"rule_repost_campaign_test_send:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="❌ Отключить кампанию", callback_data=f"rule_repost_campaign_disable:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к правилу", callback_data=f"rule_card:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_post_menu_view(
    *, rule_id: int, saved_post_id: int | None, saved_post_description: str | None
) -> tuple[str, InlineKeyboardMarkup]:
    status = "не выбран"
    if saved_post_id:
        status = f"#{saved_post_id} · {saved_post_description or 'пост'}"
    buttons = []
    if saved_post_id:
        buttons.extend([
            [InlineKeyboardButton(text="👁 Предпросмотр поста", callback_data=f"rule_repost_campaign_post_preview:{rule_id}")],
            [InlineKeyboardButton(text="🔁 Заменить пост", callback_data=f"rule_repost_campaign_post_add:{rule_id}")],
            [InlineKeyboardButton(text="🗑 Убрать из кампании", callback_data=f"rule_repost_campaign_post_unlink:{rule_id}")],
        ])
    else:
        buttons.append([InlineKeyboardButton(text="➕ Добавить пост", callback_data=f"rule_repost_campaign_post_add:{rule_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")])
    text = (
        "📝 Рекламный пост кампании\n\n"
        f"Текущий пост: {status}\n\n"
        "Пост хранится отдельно от очереди канала-источника.\n"
        "Его можно повторно использовать в будущих кампаниях."
    )
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


def build_repost_campaign_preview_view(
    *,
    rule_id: int,
    saved_post_id: int | None,
    saved_post_description: str | None,
    show_seconds_text: str,
    targets_active: int,
    targets_ready: int,
    targets_with_errors: int,
    targets_preview_text: str,
    warnings: list[str],
) -> tuple[str, InlineKeyboardMarkup]:
    warnings_block = ("\n" + "\n".join(warnings) + "\n") if warnings else ""
    text = (
        "👁 Предпросмотр кампании\n\n"
        f"Правило #{rule_id}\n"
        "Режим: репост\n\n"
        f"📝 Рекламный пост: #{saved_post_id}\n"
        f"Тип: {saved_post_description or 'пост'}\n\n"
        f"⏳ Срок показа: {show_seconds_text}\n"
        f"📣 Каналы кампании: {targets_active} активных\n"
        f"🧪 Готовность: {targets_ready} / {targets_active}"
        f"{warnings_block}\n"
        "Что произойдёт при запуске:\n"
        "1. Рекламный пост будет опубликован в основной канал правила.\n"
        "2. Затем бот создаст копии для активных каналов кампании.\n"
        f"3. Каждая копия будет удалена через {show_seconds_text}.\n\n"
        "Каналы кампании:\n"
        f"{targets_preview_text}\n\n"
        "Это только предпросмотр. Публикация не запускается."
    )
    return text, InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="⏳ Срок показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="📣 Каналы кампании", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="🔄 Обновить предпросмотр", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])


def build_repost_campaign_show_menu_view(*, rule_id: int, current_show_seconds_text: str) -> tuple[str, InlineKeyboardMarkup]:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 минута 🧪", callback_data=f"rule_repost_campaign_show_set:{rule_id}:60")],
        [InlineKeyboardButton(text="15 минут", callback_data=f"rule_repost_campaign_show_set:{rule_id}:900")],
        [InlineKeyboardButton(text="1 час", callback_data=f"rule_repost_campaign_show_set:{rule_id}:3600")],
        [InlineKeyboardButton(text="2 часа", callback_data=f"rule_repost_campaign_show_set:{rule_id}:7200")],
        [InlineKeyboardButton(text="6 часов", callback_data=f"rule_repost_campaign_show_set:{rule_id}:21600")],
        [InlineKeyboardButton(text="12 часов", callback_data=f"rule_repost_campaign_show_set:{rule_id}:43200")],
        [InlineKeyboardButton(text="24 часа", callback_data=f"rule_repost_campaign_show_set:{rule_id}:86400")],
        [InlineKeyboardButton(text="48 часов", callback_data=f"rule_repost_campaign_show_set:{rule_id}:172800")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    text = (
        "⏳ Срок показа рекламы\n\n"
        "Выберите, сколько рекламный пост должен оставаться в каналах кампании.\n"
        "После этого бот автоматически удалит опубликованные копии.\n\n"
        f"Текущий срок: {current_show_seconds_text}"
    )
    return text, kb


def build_repost_campaign_targets_menu_view(*, rule_id: int, summary: dict) -> tuple[str, InlineKeyboardMarkup]:
    active = int((summary or {}).get("targets_active") or 0)
    ready = int((summary or {}).get("targets_ready") or 0)
    with_errors = int((summary or {}).get("targets_with_errors") or 0)
    text = (
        "📣 Каналы кампании\n\n"
        "Каналы, куда рекламный пост будет дополнительно отправляться после публикации в основной канал.\n\n"
        f"Активных: {active}\n"
        f"Готовы к работе: {ready}\n"
        f"Требуют проверки: {with_errors}\n\n"
        "Срок показа задаётся в меню рекламной кампании."
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Добавить списком", callback_data=f"rule_repost_campaign_add_list:{rule_id}")],
        [InlineKeyboardButton(text="📋 Список каналов", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
        [InlineKeyboardButton(text="🧪 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, keyboard


def build_repost_campaign_targets_list_view(*, rule_id: int, targets: list[dict]) -> tuple[str, InlineKeyboardMarkup]:
    if not targets:
        text = "📋 Список каналов кампании\n\nПока не добавлено ни одного канала."
    else:
        lines: list[str] = ["📋 Список каналов кампании", ""]
        for idx, row in enumerate(targets[:30], 1):
            has_error = bool(row.get("last_check_error"))
            is_active = bool(row.get("is_active"))
            icon = "⚠️" if has_error else ("🟢" if is_active else "⏸")
            title = str(row.get("title") or row.get("target_id") or "")
            if has_error:
                lines.append(f"{idx}. {icon} {title} — {row.get('last_check_error')}")
            else:
                lines.append(f"{idx}. {icon} {title}")
        if len(targets) > 30:
            lines.extend(["", f"Показаны первые 30 из {len(targets)}."])
        text = "\n".join(lines)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏸ Выключить по ID", callback_data=f"rule_repost_campaign_target_disable_prompt:{rule_id}")],
        [InlineKeyboardButton(text="▶️ Включить по ID", callback_data=f"rule_repost_campaign_target_enable_prompt:{rule_id}")],
        [InlineKeyboardButton(text="🗑 Удалить по ID", callback_data=f"rule_repost_campaign_target_remove_prompt:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
    ])
    return text, keyboard


def format_campaign_run_status_ru(status: str | None) -> str:
    mapping = {
        "created": "⏳ В процессе",
        "sending": "⏳ В процессе",
        "sent": "✅ Отправлено",
        "partial": "🟡 Частично",
        "failed": "❌ Ошибка",
        "cancelled": "⛔ Отменено",
    }
    return mapping.get((status or "").strip().lower(), "⚪ Неизвестно")


def format_campaign_run_type_ru(run_type: str | None) -> str:
    mapping = {
        "test": "🧪 Тестовый запуск",
        "manual": "🚀 Ручной запуск",
        "scheduled": "🕒 Запланированный запуск",
        "retry": "🔁 Повторный запуск",
    }
    return mapping.get((run_type or "").strip().lower(), "⚪ Неизвестный тип")


def format_campaign_render_mode_ru(render_mode: str | None) -> str:
    mapping = {
        "telethon_builder": "Premium-отправка через аккаунт",
        "bot_api": "Обычная отправка через бота",
        "copy_message": "Копирование сообщения",
        "telethon_origin": "Оригинал через аккаунт",
    }
    if render_mode is None:
        return "не указан"
    return mapping.get(str(render_mode).strip().lower(), str(render_mode))


def format_campaign_datetime_ru(value) -> str:
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


def build_repost_campaign_history_view(*, rule_id: int, history: dict) -> tuple[str, InlineKeyboardMarkup]:
    if not history.get("ok"):
        text = (
            "📊 История кампаний\n\n"
            "❌ Не удалось загрузить историю\n\n"
            f"{history.get('error_text') or 'Неизвестная ошибка'}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_repost_campaign_history:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        ])
        return text, kb
    runs = list((history.get("runs") or [])[:10])
    if not runs:
        text = (
            "📊 История кампаний\n\n"
            "Пока запусков нет.\n\n"
            "Когда вы сделаете тестовый или полноценный запуск кампании, здесь появится история:\n"
            "• какой рекламный пост отправлялся\n"
            "• в какие каналы\n"
            "• каким методом\n"
            "• какие публикации были успешны\n"
            "• где были ошибки\n\n"
            "Начните с “🚀 Тестовый запуск” в меню рекламной кампании."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 К рекламной кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_repost_campaign_history:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        ])
        return text, kb
    summary = history.get("summary") or {}
    lines = [
        "📊 История кампаний",
        "",
        "Сводка:",
        f"Всего запусков: {int(summary.get('total') or 0)}",
        f"✅ Успешных: {int(summary.get('sent') or 0)}",
        f"🟡 Частичных: {int(summary.get('partial') or 0)}",
        f"❌ Ошибок: {int(summary.get('failed') or 0)}",
        f"⏳ В процессе: {int(summary.get('sending') or 0)}",
        "",
        "Последние запуски:",
    ]
    for idx, run in enumerate(runs, 1):
        err = str(run.get("error_text") or "").strip()
        if len(err) > 120:
            err = err[:120] + "..."
        lines.extend([
            f"#{idx} · {format_campaign_run_type_ru(run.get('run_type'))} · {format_campaign_run_status_ru(run.get('status'))}",
            f"Пост: #{run.get('saved_post_id') or '—'}",
            f"Метод: {format_campaign_render_mode_ru(run.get('render_mode'))}",
            f"Каналы: {int(run.get('targets_success') or 0)}/{int(run.get('targets_total') or 0)}",
        ])
        if err:
            lines.append(f"Ошибка: {err}")
        lines.append(f"Время: {format_campaign_datetime_ru(run.get('started_at'))}")
        lines.append("")
    buttons = []
    for idx, run in enumerate(runs[:3], 1):
        buttons.append([InlineKeyboardButton(text=f"📄 #{idx}", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{int(run.get('id') or 0)}")])
    buttons.extend([
        [InlineKeyboardButton(text=f"📄 Детали #{int((summary.get('last_run') or {}).get('id') or runs[0].get('id') or 0)}", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{int((summary.get('last_run') or runs[0]).get('id') or 0)}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(inline_keyboard=buttons)


def build_repost_campaign_run_details_view(*, rule_id: int, details: dict) -> tuple[str, InlineKeyboardMarkup]:
    run_id = int(details.get("run_id") or 0)
    if not details.get("ok"):
        text = f"📄 Детали запуска\n\n❌ Не удалось загрузить запуск\n\n{details.get('error_text') or 'Неизвестная ошибка'}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 К истории", callback_data=f"rule_repost_campaign_history:{rule_id}")],
            [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        ])
        return text, kb
    run = details.get("run") or {}
    summary = details.get("summary") or {}
    lines = [
        f"📄 Запуск #{run.get('id') or run_id}",
        "",
        f"Тип: {format_campaign_run_type_ru(run.get('run_type'))}",
        f"Статус: {format_campaign_run_status_ru(run.get('status'))}",
        f"Пост: #{run.get('saved_post_id') or '—'}",
        f"Метод: {format_campaign_render_mode_ru(run.get('render_mode'))}",
        f"Срок показа: {format_campaign_show_seconds_ru(int(run.get('show_seconds') or 0))}",
        "",
        "📣 Публикации:",
        f"Всего: {int(summary.get('total') or 0)}",
        f"✅ Отправлено: {int(summary.get('sent') or 0)}",
        f"❌ Ошибок: {int(summary.get('failed') or 0)}",
        f"⏳ В процессе: {int(summary.get('pending') or 0)}",
        "",
    ]
    for idx, msg in enumerate(details.get("messages") or [], 1):
        sent_ok = msg.get("send_status") == "sent"
        lines.append(f"{idx}. {'✅' if sent_ok else '❌'} Основной канал")
        lines.append(f"   Канал: {msg.get('target_title') or 'не указано'}")
        lines.append(f"   Target: {msg.get('target_id') or 'не указано'}")
        if sent_ok:
            if msg.get("sent_message_id") is not None:
                lines.append(f"   Message ID: {msg.get('sent_message_id')}")
            lines.append(f"   Отправлено: {format_campaign_datetime_ru(msg.get('sent_at'))}")
        else:
            lines.append(f"   Ошибка: {msg.get('error_text') or 'не указано'}")
        delete_status = msg.get("delete_status")
        if delete_status == "pending":
            lines.append(f"   Удаление: запланировано на {format_campaign_datetime_ru(msg.get('delete_after_at'))}")
        elif delete_status == "deleted":
            lines.append(f"   Удаление: ✅ удалено {format_campaign_datetime_ru(msg.get('deleted_at'))}")
        elif delete_status == "failed":
            lines.append("   Удаление: ❌ ошибка удаления")
        else:
            lines.append("   Удаление: не запланировано")
        lines.append("")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить детали", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run.get('id') or run_id}")],
        [InlineKeyboardButton(text="📊 К истории", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines).rstrip(), kb
