from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_view_model import (
    build_campaign_control_center_view_model,
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    format_campaign_datetime_text,
    format_campaign_error_text,
    format_campaign_render_mode_text,
    format_campaign_run_status_text,
    format_campaign_run_type_text,
    format_campaign_show_seconds_text,
)


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
    *,
    rule_id: int,
    summary: dict,
    saved_post_line: str,
    readiness: dict | None = None,
    control_center: dict | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    cc_payload = control_center
    if cc_payload is None:
        cc_payload = {"ok": bool(readiness), "readiness": readiness, "last_run": None, "last_run_details": None, "issues": []}
    vm = build_campaign_control_center_view_model(summary=summary, saved_post_line=saved_post_line, control_center=cc_payload)
    readiness_block = format_repost_campaign_readiness_block(readiness)
    text = (
        "💰 Рекламная кампания\n\n"
        "🧭 Центр управления\n"
        f"Правило #{rule_id}\n\n"
        f"{vm['status_title']}\n\n"
        f"{vm['post_line']}\n"
        f"{vm['targets_line']}\n"
        f"{vm['show_seconds_line']}\n"
        f"{vm['delete_line']}\n\n"
        f"{readiness_block}\n\n"
        f"{vm['last_run_block']}\n\n"
        f"{vm['issues_block']}"
    )
    rows = []
    if vm["can_launch"]:
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    if vm["can_check_publication"]:
        rows.append([InlineKeyboardButton(text="📤 Проверить публикацию", callback_data=f"rule_repost_campaign_test_send:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="📝 Креатив", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="⏳ Срок", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="📣 Площадки", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="📊 История", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="👁 Предпросмотр", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
    ])
    if vm["last_run_id"]:
        rows.append([InlineKeyboardButton(text="📄 Последний запуск", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{vm['last_run_id']}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="❌ Отключить кампанию", callback_data=f"rule_repost_campaign_disable:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к правилу", callback_data=f"rule_card:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_launch_result_view(*, rule_id: int, result) -> tuple[str, InlineKeyboardMarkup]:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    extra = payload.get("extra") or {}
    run_id = extra.get("campaign_run_id")
    success = int(extra.get("targets_success") or 0)
    failed = int(extra.get("targets_failed") or 0)
    total = int(extra.get("targets_total") or 0)
    saved_post_id = payload.get("saved_post_id")
    status = str(extra.get("final_status") or "")
    if payload.get("ok"):
        title = "🚀 Кампания запущена"
        if success <= 0:
            title = "❌ Кампания не отправлена"
        elif failed > 0 or status == "partial":
            title = "🟡 Кампания запущена частично"
        text = (
            f"{title}\n\n"
            "Рекламный пост отправлен в каналы кампании.\n\n"
            "📊 Итог:\n"
            f"✅ Успешно: {success}\n"
            f"❌ Ошибок: {failed}\n"
            f"📣 Всего каналов: {total}\n\n"
            f"📝 Пост: #{saved_post_id}\n"
            f"🧾 Запуск: #{run_id}\n"
            f"⏳ Срок показа: {format_campaign_show_seconds_text(extra.get('show_seconds'))}\n\n"
            "История уже обновлена. Детальный отчёт доступен в разделе “📊 История кампаний”."
        )
    else:
        text = (
            "❌ Не удалось запустить кампанию\n\n"
            f"{payload.get('error_text') or 'Неизвестная ошибка'}\n\n"
            "Проверьте готовность кампании:\n"
            "• рекламный пост\n"
            "• срок показа\n"
            "• активные каналы\n"
            "• права аккаунта-парсера"
        )
        if payload.get("premium_required"):
            text += (
                "\n\nPremium-оформление требует отправки через аккаунт-парсер.\n"
                "Проверьте, что аккаунт-парсер добавлен в каналы и имеет право публикации."
            )
    rows = [
        [InlineKeyboardButton(text="📊 История кампаний", callback_data=f"rule_repost_campaign_history:{rule_id}")],
    ]
    if run_id:
        rows.append([InlineKeyboardButton(text="📄 Детали запуска", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить кампанию", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
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
    readiness: dict | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    readiness_block = format_repost_campaign_readiness_block(readiness) if readiness else f"🧪 Готовность: {targets_ready} / {targets_active}"
    warnings_block = ("\n" + "\n".join(warnings)) if warnings else ""
    if warnings_block:
        warnings_block = f"\n{warnings_block}"
    text = (
        "👁 Предпросмотр кампании\n\n"
        f"Правило #{rule_id}\n"
        "Режим: репост\n\n"
        f"📝 Рекламный пост: #{saved_post_id}\n"
        f"Тип: {saved_post_description or 'пост'}\n\n"
        f"⏳ Срок показа: {show_seconds_text}\n"
        f"📣 Каналы кампании: {targets_active} активных\n"
        f"{readiness_block}"
        f"{warnings_block}\n\n"
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
        [InlineKeyboardButton(text="1 минута", callback_data=f"rule_repost_campaign_show_set:{rule_id}:60")],
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
    runs_raw = list((history.get("runs") or [])[:10])
    runs = list(reversed(runs_raw))
    if not runs:
        text = (
            "📊 История кампаний\n\n"
            "Пока запусков нет.\n\n"
            "Когда вы выполните проверочную публикацию или запустите кампанию, здесь появится история:\n"
            "• какой рекламный пост отправлялся\n"
            "• в какие каналы\n"
            "• каким методом\n"
            "• какие публикации были успешны\n"
            "• где были ошибки\n\n"
            "Начните с проверки публикации или запуска кампании в меню рекламной кампании."
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
        "Обзор:",
        f"📦 Всего запусков: {int(summary.get('total') or 0)}",
        f"✅ Успешно: {int(summary.get('sent') or 0)}",
        f"🟡 Частично: {int(summary.get('partial') or 0)}",
        f"❌ Ошибки: {int(summary.get('failed') or 0)}",
        f"⏳ В процессе: {int(summary.get('sending') or 0)}",
        "",
        "Хронология запусков:",
    ]
    for idx, run in enumerate(runs, 1):
        view = build_campaign_run_item_view(run, index=idx)
        lines.extend([
            view["title"],
            view["saved_post_text"],
            view["method_text"],
            view["targets_text"],
        ])
        if view["error_text"]:
            lines.append(view["error_text"])
        lines.append(view["time_text"])
        lines.append("")
    buttons = []
    last_run = summary.get("last_run") or {}
    last_run_id = int(last_run.get("id") or runs_raw[0].get("id") or 0)
    if last_run_id:
        buttons.append([InlineKeyboardButton(text="📄 Детали последнего запуска", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{last_run_id}")])
    recent_desc = list(runs_raw)
    for run in recent_desc:
        run_id = int(run.get("id") or 0)
        if run_id and run_id != last_run_id:
            buttons.append([InlineKeyboardButton(text=f"📄 Детали #{run_id}", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")])
        if len(buttons) >= (4 if last_run_id else 3):
            break
    buttons.extend([
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
        f"Тип: {format_campaign_run_type_text(run.get('run_type'))}",
        f"Статус: {format_campaign_run_status_text(run.get('status'))}",
        f"Пост: #{run.get('saved_post_id') or '—'}",
        f"Метод: {format_campaign_render_mode_text(run.get('render_mode'))}",
        f"Срок показа: {format_campaign_show_seconds_text(run.get('show_seconds'))}",
        "",
        "📣 Публикации:",
        f"Всего: {int(summary.get('total') or 0)}",
        f"✅ Отправлено: {int(summary.get('sent') or 0)}",
        f"❌ Ошибок: {int(summary.get('failed') or 0)}",
        f"⏳ В процессе: {int(summary.get('pending') or 0)}",
        "",
    ]
    delete_action_buttons: list[list[InlineKeyboardButton]] = []
    for idx, msg in enumerate(details.get("messages") or [], 1):
        view = build_campaign_run_message_view(msg, index=idx)
        lines.append(view["title"])
        lines.append(f"   {view['channel_text']}")
        lines.append(f"   {view['target_text']}")
        lines.append(f"   {view['send_status_text']}")
        if view["message_id_text"]:
            lines.append(f"   {view['message_id_text']}")
        if view["sent_at_text"]:
            lines.append(f"   {view['sent_at_text']}")
        if view["send_error_text"]:
            lines.append(f"   {view['send_error_text']}")
        for delete_line in str(view["delete_text"]).splitlines():
            lines.append(f"   {delete_line}")
        if view.get("can_delete_now") and len(delete_action_buttons) < 10:
            delete_action_buttons.append([
                InlineKeyboardButton(
                    text=f"{view.get('delete_action_text')} #{idx}",
                    callback_data=f"rule_repost_campaign_delete_message:{rule_id}:{run.get('id') or run_id}:{int(msg.get('id') or 0)}",
                )
            ])
        lines.append("")
    total_actionable = sum(1 for msg in (details.get("messages") or []) if build_campaign_run_message_view(msg).get("can_delete_now"))
    if total_actionable > 10:
        lines.extend([
            "Показаны первые 10 действий удаления. Остальные доступны после обновления или через будущий массовый режим.",
            "",
        ])
    kb_rows = delete_action_buttons + [
        [InlineKeyboardButton(text="🔄 Обновить детали", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run.get('id') or run_id}")],
        [InlineKeyboardButton(text="📊 К истории", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    return "\n".join(lines).rstrip(), kb


def build_repost_campaign_delete_result_view(*, rule_id: int, result) -> tuple[str, InlineKeyboardMarkup]:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    extra = payload.get("extra") or {}
    run_id = extra.get("campaign_run_id")
    if payload.get("ok"):
        if extra.get("already_deleted"):
            text = (
                "✅ Публикация уже была удалена\n\n"
                "Повторное удаление не требуется."
            )
        else:
            text = (
                "🧹 Публикация удалена\n\n"
                "Сообщение рекламной кампании удалено из канала.\n\n"
                f"Target: {payload.get('target_id')}\n"
                f"Message ID: {payload.get('message_id')}\n"
                f"Метод удаления: {payload.get('method') or 'не указан'}\n\n"
                "История запуска обновлена."
            )
    else:
        text = (
            "❌ Не удалось удалить публикацию\n\n"
            f"{payload.get('error_text') or 'Неизвестная ошибка'}\n\n"
            "Статус и ошибка сохранены в истории запуска."
        )
    rows = []
    if run_id:
        rows.append([InlineKeyboardButton(text="📄 К деталям запуска", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="📊 К истории", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)
