from __future__ import annotations

from datetime import datetime

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_view_model import (
    build_campaign_target_item_view,
    build_campaign_control_center_view_model,
    build_campaign_run_item_view,
    build_campaign_run_message_view,
    build_campaign_scenario_preview_view_model,
    build_campaign_launch_readiness_view_model,
    normalize_campaign_target_error_text,
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
        f"⏳ Время показа: {readiness.get('show_seconds_status_text') or '⚠️ недоступно'}\n"
        f"📣 Каналы/Группы: {readiness.get('targets_status_text') or '⚠️ недоступно'}\n"
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
    text = (
        "💰 Рекламная кампания\n\n"
        f"{vm['title_status']}\n\n"
        f"{vm['creative_line']}\n"
        f"{vm['targets_line']}\n"
        f"{vm['show_seconds_line']}\n"
        f"{(vm.get('auto_delete_line') or '🧹 Автоудаление: не задано')}\n"
        "\n"
        f"{vm['last_run_line']}\n"
        f"{(vm.get('last_run_delete_line') or vm.get('delete_line'))}\n\n"
        f"{vm['next_step_line']}"
    )
    rows = []
    primary_action = vm.get("primary_action")
    if primary_action == "show_seconds":
        rows.append([InlineKeyboardButton(text="⏳ Задать время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")])
    elif primary_action == "creative":
        rows.append([InlineKeyboardButton(text="📝 Выбрать рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")])
    elif primary_action == "targets":
        rows.append([InlineKeyboardButton(text="📣 Добавить каналы/группы", callback_data=f"rule_repost_campaign_targets:{rule_id}")])
    elif primary_action == "launch":
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    elif primary_action == "open_last_run" and vm["last_run_id"]:
        rows.append([InlineKeyboardButton(text="📄 Открыть последний запуск", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{vm['last_run_id']}")])

    rows.extend([
        [
            InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}"),
            InlineKeyboardButton(text="⏳ Время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}"),
        ],
        [
            InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}"),
            InlineKeyboardButton(text="📊 История", callback_data=f"rule_repost_campaign_history:{rule_id}"),
        ],
        [
            InlineKeyboardButton(text="👁 Предпросмотр", callback_data=f"rule_repost_campaign_preview:{rule_id}"),
            InlineKeyboardButton(text="⚙️ Ещё", callback_data=f"rule_repost_campaign_more:{rule_id}"),
        ],
    ])
    if vm["can_launch"] and primary_action != "launch":
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="⬅️ Назад к правилу", callback_data=f"rule_card:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_more_view(*, rule_id: int, saved_post_id: int | None, last_run_id: int | None) -> tuple[str, InlineKeyboardMarkup]:
    rows = []
    if saved_post_id:
        rows.append([InlineKeyboardButton(text="📤 Проверить публикацию", callback_data=f"rule_repost_campaign_test_send:{rule_id}")])
    if last_run_id:
        rows.append([InlineKeyboardButton(text="📄 Последний запуск", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{last_run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить кампанию", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="❌ Отключить кампанию", callback_data=f"rule_repost_campaign_disable:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    text = "⚙️ Ещё\n\nДополнительные действия для проверки и обслуживания рекламной кампании."
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
            "Рекламный пост опубликован в выбранные каналы/группы.\n\n"
            "📊 Размещение:\n"
            f"✅ Опубликовано: {success}\n"
            f"⚠️ Ошибки: {failed}\n"
            f"📣 Всего получателей: {total}\n\n"
            f"📝 Пост: #{saved_post_id}\n"
            f"🧾 Запуск: #{run_id}\n"
            f"🧹 Автоудаление: через {format_campaign_show_seconds_text(extra.get('show_seconds'))}\n\n"
            "История обновлена. Детали доступны в отчёте запуска."
        )
    else:
        launch_readiness = extra.get("launch_readiness")
        if launch_readiness:
            vm = build_campaign_launch_readiness_view_model(readiness=launch_readiness)
            reasons = "\n".join([f"• {x}" for x in vm["block_reason_lines"]]) or "• Проверьте параметры кампании"
            text = (
                "🚦 Кампания не готова к запуску\n\n"
                f"Причины:\n{reasons}\n\n"
                f"{vm['will_send_line']}\n"
                f"{vm['will_skip_line']}\n\n"
                f"Следующий шаг:\n{vm['next_step_line']}"
            )
            rows = []
            if vm["can_check_rights"]:
                rows.append([InlineKeyboardButton(text="🔎 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")])
            rows.extend([
                [InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
                [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
                [InlineKeyboardButton(text="⏳ Время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
                [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            ])
            return text, InlineKeyboardMarkup(inline_keyboard=rows)
        text = (
            "❌ Кампания не запущена\n\n"
            f"{payload.get('error_text') or 'Неизвестная ошибка'}\n\n"
            "Проверьте готовность кампании:\n"
            "• рекламный пост\n"
            "• время показа\n"
            "• активные каналы\n"
            "• права аккаунта-парсера"
        )
        if payload.get("premium_required"):
            text += (
                "\n\nPremium-оформление требует отправки через аккаунт-парсер.\n"
                "Проверьте, что аккаунт-парсер добавлен в каналы и имеет право публикации."
            )
    rows = [
        [InlineKeyboardButton(text="📊 История размещений", callback_data=f"rule_repost_campaign_history:{rule_id}")],
    ]
    if run_id:
        rows.append([InlineKeyboardButton(text="📄 Детали запуска", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить кампанию", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_launch_readiness_view(*, rule_id: int, readiness: dict, now: datetime | None = None) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_launch_readiness_view_model(readiness=readiness, now=now)
    saved_post_id = readiness.get("saved_post_id")
    reasons_block = ""
    if vm["block_reason_lines"]:
        reasons_block = "\n\nПричины:\n" + "\n".join([f"• {line}" for line in vm["block_reason_lines"]])
    text = (
        "🚦 Проверка перед запуском\n\n"
        f"{vm['status_line']}\n\n"
        f"📝 Рекламный пост: {'#' + str(saved_post_id) if saved_post_id else 'не выбран'}\n"
        f"{vm['will_send_line']}\n"
        f"{vm['will_skip_line']}\n"
        f"{vm['show_seconds_line']}\n"
        f"{vm['auto_delete_line']}\n"
        f"{vm['expected_delete_line']}{reasons_block}\n\n"
        "Это финальная проверка перед публикацией.\n"
        "После подтверждения рекламный пост будет сразу отправлен в готовые каналы/группы.\n\n"
        "После запуска бот:\n"
        "• опубликует рекламный пост только в готовые каналы/группы;\n"
        "• сохранит результат по каждому получателю;\n"
        "• автоматически удалит публикации после времени показа;\n"
        "• оставит историю размещения.\n\n"
        f"{vm['next_step_line']}"
    )
    rows = []
    if vm["can_launch"]:
        rows.append([InlineKeyboardButton(text="🚀 Подтвердить запуск", callback_data=f"rule_repost_campaign_launch_confirm:{rule_id}")])
    if vm["can_check_rights"]:
        rows.append([InlineKeyboardButton(text="🔎 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="⏳ Время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="👁 Предпросмотр сценария", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
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
    summary: dict | None = None,
    control_center: dict | None = None,
    saved_post_line: str | None = None,
    now: datetime | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_scenario_preview_view_model(
        rule_id=rule_id,
        summary=summary or {
            "saved_post_id": saved_post_id,
            "show_seconds": 0 if show_seconds_text == "не задан" else None,
            "targets_active": targets_active,
            "targets_ready": targets_ready,
            "targets_with_errors": targets_with_errors,
        },
        saved_post_id=saved_post_id,
        saved_post_description=saved_post_description,
        saved_post_line=saved_post_line,
        readiness=readiness,
        control_center=control_center,
        targets_preview_text=targets_preview_text,
        warnings=warnings,
        now=now,
    )
    issues_block = "\n" + "\n".join(vm["issues_lines"]) if vm["issues_lines"] else ""
    text = (
        f"{vm['title']}\n\n"
        f"{vm['status_line']}\n\n"
        f"{vm['creative_line']}\n"
        f"{vm['targets_line']}\n"
        f"{vm['show_seconds_line']}\n"
        f"{vm['auto_delete_line']}\n"
        f"{vm['expected_delete_line']}\n"
        f"{issues_block}\n\n"
        "После запуска:\n"
        + "\n".join(vm["scenario_steps"])
        + "\n\nКаналы/Группы:\n"
        + vm["targets_preview_text"]
        + "\n\n"
        + vm["next_step_line"]
        + "\n\nЭто только предпросмотр сценария. Публикация не запускается."
    )
    rows = []
    if vm["can_launch"]:
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    if vm["can_check_rights"]:
        rows.append([InlineKeyboardButton(text="🧪 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="⏳ Время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="🔄 Обновить предпросмотр", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


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
        "⏳ Время показа рекламы\n\n"
        "Выберите, сколько рекламный пост будет находиться в каналах и группах.\n"
        "После этого бот автоматически удалит рекламные публикации.\n\n"
        f"Текущее время показа: {current_show_seconds_text}"
    )
    return text, kb


def build_repost_campaign_targets_menu_view(*, rule_id: int, summary: dict) -> tuple[str, InlineKeyboardMarkup]:
    active = int((summary or {}).get("targets_active") or 0)
    ready = int((summary or {}).get("targets_ready") or 0)
    with_errors = int((summary or {}).get("targets_with_errors") or 0)
    text = (
        "📣 Каналы/Группы\n\n"
        "Каналы и группы, куда рекламный пост будет дополнительно отправляться после публикации в основной канал.\n\n"
        f"Активных: {active}\n"
        f"Готовы к работе: {ready}\n"
        f"Требуют проверки: {with_errors}\n\n"
        "Время показа задаётся в меню рекламной кампании."
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Добавить списком", callback_data=f"rule_repost_campaign_add_list:{rule_id}")],
        [InlineKeyboardButton(text="📋 Список каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
        [InlineKeyboardButton(text="🔎 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, keyboard


def build_repost_campaign_targets_list_view(*, rule_id: int, targets: list[dict]) -> tuple[str, InlineKeyboardMarkup]:
    if not targets:
        text = (
            "📋 Каналы/Группы\n\n"
            "Пока не добавлено ни одного канала или группы.\n\n"
            "Добавьте получателей списком — кампания сможет отправлять один рекламный пост сразу в несколько каналов/групп."
        )
    else:
        total = len(targets)
        active = sum(1 for t in targets if bool(t.get("is_active")) and not t.get("last_check_error"))
        errors = sum(1 for t in targets if t.get("last_check_error"))
        paused = total - active - errors
        lines = [
            "📋 Каналы/Группы",
            "",
            f"Подключено: {total}",
            f"🟢 Активных: {active}",
            f"⏸ На паузе: {paused}",
            f"⚠️ Требуют проверки: {errors}",
            "",
        ]
        for idx, row in enumerate(targets[:10], 1):
            view = build_campaign_target_item_view(row, index=idx)
            lines.append(view["title"])
            lines.append(f"   {view['status_line']}")
            lines.append(f"   {view['check_line']}")
            if view.get("error_line"):
                lines.append(f"   {view['error_line']}")
            lines.append("")
        if total > 10:
            lines.append(f"Показаны первые 10 из {total}.")
        if errors > 0:
            lines.append("Нажмите “🔎 Проверить права”, чтобы обновить названия и готовность каналов/групп.")
        text = "\n".join(lines)
    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for idx, row in enumerate(targets[:10], 1):
        row_id = int(row.get("id") or 0)
        view = build_campaign_target_item_view(row, index=idx)
        if view.get("requires_attention"):
            keyboard_rows.append([InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}")])
        elif row.get("is_active"):
            keyboard_rows.append([InlineKeyboardButton(text="⏸ Пауза", callback_data=f"rule_repost_campaign_target_pause:{rule_id}:{row_id}"), InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}")])
        else:
            keyboard_rows.append([InlineKeyboardButton(text="▶️ Включить", callback_data=f"rule_repost_campaign_target_resume:{rule_id}:{row_id}"), InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}")])
        keyboard_rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"rule_repost_campaign_target_delete_confirm:{rule_id}:{row_id}")])
    keyboard_rows.extend([
        [InlineKeyboardButton(text="📥 Добавить списком", callback_data=f"rule_repost_campaign_add_list:{rule_id}")],
        [InlineKeyboardButton(text="🔎 Проверить права", callback_data=f"rule_repost_campaign_check:{rule_id}")],
        [InlineKeyboardButton(text="⚙️ Управление вручную", callback_data=f"rule_repost_campaign_targets_id_actions:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
    ])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_rows)
    return text, keyboard


def build_repost_campaign_targets_id_actions_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "⚙️ Управление вручную\n\n"
        "Этот раздел нужен для редких случаев: например, если канал/группа не отображается в списке или нужно выполнить действие вручную.\n\n"
        "Обычно удобнее управлять каналами через кнопки в списке."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏸ Поставить на паузу вручную", callback_data=f"rule_repost_campaign_target_disable_prompt:{rule_id}")],
        [InlineKeyboardButton(text="▶️ Включить вручную", callback_data=f"rule_repost_campaign_target_enable_prompt:{rule_id}")],
        [InlineKeyboardButton(text="🗑 Удалить вручную", callback_data=f"rule_repost_campaign_target_remove_prompt:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_target_action_result_view(*, rule_id: int, result: dict, action: str) -> tuple[str, InlineKeyboardMarkup]:
    if result.get("ok"):
        title = str(result.get("target_title") or "Канал/группа")
        if action == "pause":
            text = f"⏸ Канал/группа поставлены на паузу\n\n{title} больше не участвует в новых запусках кампании."
        elif action == "resume":
            text = f"▶️ Канал/группа включены\n\n{title} снова участвует в новых запусках кампании."
        else:
            text = f"🗑 Канал/группа удалены\n\n{title} больше не подключены к этой рекламной кампании."
    else:
        reason = result.get("error_text") or "Неизвестная ошибка"
        text = f"❌ Не удалось обновить канал/группу\n\nПричина: {reason}"
        last_check_error = normalize_campaign_target_error_text(((result.get("extra") or {}).get("last_check_error")))
        if last_check_error:
            text += f"\n\nТекущая проблема: {last_check_error}"
        text += "\n\nОбновите список и повторите действие. Если канал требует внимания, сначала нажмите “🔎 Проверить”."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
        [InlineKeyboardButton(text="📣 К каналам/группам", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_target_delete_confirm_view(*, rule_id: int, target: dict | None) -> tuple[str, InlineKeyboardMarkup]:
    if not target:
        text = "❌ Канал/группа не найдены в кампании."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")]
        ])
        return text, kb
    row_id = int(target.get("id") or 0)
    title = str(target.get("title") or target.get("target_id") or "Канал/группа")
    text = (
        "🗑 Удалить канал/группу?\n\n"
        f"{title}\n\n"
        "Канал/группа больше не будут участвовать в новых запусках кампании.\n"
        "История уже выполненных запусков сохранится."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Да, удалить", callback_data=f"rule_repost_campaign_target_delete:{rule_id}:{row_id}")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_history_view(*, rule_id: int, history: dict) -> tuple[str, InlineKeyboardMarkup]:
    if not history.get("ok"):
        text = (
            "📊 История размещений\n\n"
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
            "📊 История размещений\n\n"
            "Пока размещений нет.\n\n"
            "После запуска кампании здесь появятся:\n"
            "• дата и время публикации;\n"
            "• список каналов/групп;\n"
            "• статус публикации;\n"
            "• статус автоудаления;\n"
            "• ошибки по каждому получателю."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 К рекламной кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_repost_campaign_history:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        ])
        return text, kb
    summary = history.get("summary") or {}
    lines = [
        "📊 История размещений",
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
        f"Время показа: {format_campaign_show_seconds_text(run.get('show_seconds'))}",
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


def build_repost_campaign_target_preview_result_view(*, rule_id: int, result: dict) -> tuple[str, InlineKeyboardMarkup]:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    extra = payload.get("extra") or {}
    target_title = extra.get("target_title") or extra.get("target_id") or "—"
    kind = extra.get("kind") or payload.get("kind") or "post"
    method = extra.get("method") or payload.get("method") or "unknown"
    message_ids = extra.get("message_ids") or []
    preview_url = extra.get("preview_url")
    text = (
        "✅ Предпросмотр отправлен\n\n"
        "Рекламный пост опубликован в основном канале правила.\n"
        "Проверьте внешний вид, подпись, premium emoji и альбом.\n\n"
        f"Канал/Группа: {target_title}\n"
        f"Тип: {kind}\n"
        f"Метод: {method}\n"
    )
    if isinstance(message_ids, list) and len(message_ids) > 1:
        text += f"\nМедиа: {len(message_ids)}\n"
    if not preview_url:
        text += "\nОткройте основной канал правила и проверьте последний опубликованный рекламный пост."
    rows = []
    if preview_url:
        rows.append([InlineKeyboardButton(text="👁 Открыть предпросмотр", url=preview_url)])
    rows.extend([
        [InlineKeyboardButton(text="🗑 Удалить предпросмотр", callback_data=f"rule_repost_campaign_preview_delete:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ К рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_preview_delete_result_view(*, rule_id: int, result: dict) -> tuple[str, InlineKeyboardMarkup]:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    extra = payload.get("extra") or {}
    if payload.get("ok"):
        text = "🗑 Предпросмотр удалён\n\nСообщения предпросмотра удалены из основного канала."
        ids = extra.get("message_ids") or []
        if isinstance(ids, list) and len(ids) > 1:
            text += f"\n\nУдалено сообщений: {len(ids)}"
    else:
        text = f"❌ Не удалось удалить предпросмотр\n\n{payload.get('error_text') or 'Неизвестная ошибка'}"
    rows = [
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="📝 К рекламному посту", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_target_check_result_view(*, rule_id: int, result: dict) -> tuple[str, InlineKeyboardMarkup]:
    payload = result or {}
    ok = bool(payload.get("ok"))
    check_ok = bool(payload.get("check_ok", ok))
    saved = bool(payload.get("saved", True))
    title = (result or {}).get("target_title") or (result or {}).get("target_id") or "—"
    target_id = (result or {}).get("target_id") or "—"
    can_publish = (result or {}).get("can_publish")
    can_delete = (result or {}).get("can_delete")
    publish_line = "Публикация: ⚠️ не подтверждена"
    if can_publish is True:
        publish_line = "Публикация: ✅ разрешена"
    elif can_publish is False:
        publish_line = "Публикация: ❌ нет"
    delete_line = "Удаление: ⚠️ не подтверждено"
    if can_delete is True:
        delete_line = "Удаление: ✅ разрешено"
    elif can_delete is False:
        delete_line = "Удаление: ❌ нет"
    if check_ok and not saved:
        text = (
            "❌ Проверка выполнена, но результат не сохранён\n\n"
            f"Название: {title}\n"
            f"ID: {target_id}\n\n"
            "Проверка получила данные, но не удалось обновить карточку канала/группы.\n"
            "Повторите проверку или обновите список.\n\n"
            f"{publish_line}\n{delete_line}"
        )
    elif ok:
        text = (
            "✅ Канал/группа проверены\n\n"
            f"Название: {title}\n"
            f"ID: {target_id}\n\n"
            f"{publish_line}\n{delete_line}\n\n"
            "Теперь канал готов к размещению."
        )
    else:
        reason = normalize_campaign_target_error_text((result or {}).get("error_text")) or "Неизвестная ошибка"
        text = (
            "⚠️ Канал/группа требует внимания\n\n"
            f"Название: {title}\n"
            f"ID: {target_id}\n"
            f"Что нужно сделать: {reason}\n\n"
            "Публикация: ❌ недоступна\n"
            f"{delete_line}"
        )
    row_id = int((result or {}).get("target_row_id") or 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
        [InlineKeyboardButton(text="🔄 Проверить ещё раз", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}")],
        [InlineKeyboardButton(text="📣 К каналам/группам", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_targets_check_result_view(*, rule_id: int, result: dict) -> tuple[str, InlineKeyboardMarkup]:
    items = (result or {}).get("items") or []
    lines = [
        "🧪 Проверка прав завершена",
        "",
        f"✅ Готово к размещению: {int((result or {}).get('passed') or 0)}",
        f"⚠️ Требуют внимания: {int((result or {}).get('failed') or 0)}",
        f"📣 Проверено: {int((result or {}).get('checked') or 0)}",
        "",
    ]
    problem_lines: list[str] = []
    for idx, item in enumerate(items[:10], 1):
        mark = "✅" if item.get("ok") else "⚠️"
        line = f"{idx}. {mark} {item.get('target_title') or item.get('target_id') or '—'}"
        if not item.get("ok") and item.get("error_text"):
            line += f" — {normalize_campaign_target_error_text(item.get('error_text'))}"
        if item.get("ok"):
            lines.append(line)
        else:
            problem_lines.append(line)
    if problem_lines:
        lines.extend(["Проблемные каналы/группы:"])
        lines.extend(problem_lines)
    lines.extend(["", "Карточки каналов/групп обновлены."])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")],
        [InlineKeyboardButton(text="🔄 Проверить ещё раз", callback_data=f"rule_repost_campaign_check:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines), kb
