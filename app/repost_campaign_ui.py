from __future__ import annotations

from datetime import datetime, timezone, timedelta
import re

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.repost_campaign_view_model import (
    build_campaign_target_item_view,
    build_campaign_control_center_view_model,
    build_campaign_run_message_view,
    build_campaign_launch_readiness_view_model,
    build_campaign_views_report_view_model,
    build_campaign_posts_library_view_model,
    build_campaign_post_stats_view_model,
    normalize_campaign_target_error_text,
    format_campaign_datetime_text,
    format_campaign_error_text,
    format_campaign_render_mode_text,
    format_campaign_run_type_text,
    format_campaign_show_seconds_text,
    format_vip_scheduled_post_status_text,
    format_vip_scheduled_post_short_line,
    build_vip_scheduled_post_detail_view_model,
)

TG_TEXT_SAFE_LIMIT = 3800
RUN_DETAILS_VISIBLE_MESSAGES_LIMIT = 10
CAMPAIGN_TARGETS_PAGE_SIZE = 10


def trim_campaign_text_for_telegram(text: str, *, limit: int = TG_TEXT_SAFE_LIMIT) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n...\nТекст сокращён, чтобы Telegram принял сообщение.\nОткройте журнал запусков или отчёт просмотров для деталей."
    allowed = max(0, limit - len(suffix))
    lines: list[str] = []
    total = 0
    for line in text.splitlines():
        add = len(line) + (1 if lines else 0)
        if total + add > allowed:
            break
        lines.append(line)
        total += add
    return ("\n".join(lines)).rstrip() + suffix


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
    next_step_line = vm.get("next_step_line")
    last_run_lines = [
        vm.get("last_run_title_line"),
        vm.get("last_run_status_line"),
        vm.get("last_run_time_line"),
        vm.get("last_run_delete_line"),
    ]
    rendered_last_run_block = "\n".join([line for line in last_run_lines if line])
    blocks = [
        "💰 Рекламная кампания",
        vm["title_status"],
        f"{vm['creative_line']}\n{vm['creative_value_line']}",
        vm["targets_line"],
        vm["show_seconds_line"],
        rendered_last_run_block,
        next_step_line,
    ]
    text = "\n\n".join([block for block in blocks if block])
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
    elif primary_action in {"open_last_run", "open_active_run", "open_problem_run"} and vm["last_run_id"]:
        button_text = "📄 Открыть последний запуск"
        if vm.get("screen_state") == "active_placement":
            button_text = "📄 Открыть активное размещение"
        elif vm.get("screen_state") == "delete_problem":
            button_text = "📄 Открыть проблемный запуск"
        rows.append([InlineKeyboardButton(text=button_text, callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{vm['last_run_id']}")])

    rows.extend([
        [
            InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}"),
            InlineKeyboardButton(text="⏳ Время показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}"),
        ],
        [
            InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}"),
            InlineKeyboardButton(text="📚 Библиотека", callback_data=f"rule_repost_campaign_history:{rule_id}"),
        ],
        [
            InlineKeyboardButton(text="💎 VIP функции", callback_data=f"rule_repost_campaign_vip_features:{rule_id}"),
        ],
    ])
    blocked_launch_states = {"active_placement", "delete_problem"}
    if vm["can_launch"] and primary_action != "launch" and vm.get("screen_state") not in blocked_launch_states:
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    rows.extend([
        [InlineKeyboardButton(text="⬅️ Назад к правилу", callback_data=f"rule_card:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)



def build_repost_campaign_launch_mode_view(*, rule_id: int, readiness: dict) -> tuple[str, InlineKeyboardMarkup]:
    show_seconds = int(readiness.get("show_seconds") or 0)
    total_targets = int(readiness.get("will_send_total") or 0) + int(readiness.get("will_skip_total") or 0)
    ready_targets = int(readiness.get("will_send_total") or 0)
    problem_targets = int(readiness.get("will_skip_total") or 0)
    saved_post_ready = bool(readiness.get("saved_post_id")) and readiness.get("saved_post_exists") is not False
    text = (
        "🚀 Запуск кампании\n\n"
        "Текущая кампания:\n\n"
        "Рекламный пост:\n"
        f"{'✅ Готов к публикации' if saved_post_ready else '❌ Не выбран'}\n\n"
        "Публикация:\n"
        f"📣 Каналов/групп: {total_targets}\n"
        f"✅ Готовы: {ready_targets}\n"
        f"⚠️ Требуют внимания: {problem_targets}\n\n"
        "Срок показа:\n"
        f"⏳ {format_campaign_show_seconds_text(show_seconds)}\n\n"
        "Как запустить?"
    )
    rows = [
        [InlineKeyboardButton(text="⚡ Запустить сейчас", callback_data=f"rule_repost_campaign_launch_now_preview:{rule_id}")],
        [InlineKeyboardButton(text="🕒 Запланировать запуск", callback_data=f"rule_repost_campaign_schedule_current:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_schedule_current_view(*, rule_id: int, readiness: dict) -> tuple[str, InlineKeyboardMarkup]:
    show_seconds = int(readiness.get("show_seconds") or 0)
    total_targets = int(readiness.get("will_send_total") or 0) + int(readiness.get("will_skip_total") or 0)
    saved_post_ready = bool(readiness.get("saved_post_id")) and readiness.get("saved_post_exists") is not False
    text = (
        "🕒 Запланировать запуск текущей кампании\n\n"
        "Текущая кампания уже настроена:\n"
        f"📝 Рекламный пост: {'✅ выбран' if saved_post_ready else '❌ не выбран'}\n"
        f"📣 Каналы/группы: {total_targets}\n"
        f"⏳ Срок показа: {format_campaign_show_seconds_text(show_seconds)}\n\n"
        "Когда запустить?\n\n"
        "Часовой пояс: UTC+3"
    )
    rows = [
        [InlineKeyboardButton(text="Сегодня в 20:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:today_20")],
        [InlineKeyboardButton(text="Завтра в 12:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_12")],
        [InlineKeyboardButton(text="Завтра в 18:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_18")],
        [InlineKeyboardButton(text="✍️ Ввести дату и время", callback_data=f"rule_repost_campaign_schedule_input:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_launch:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_vip_features_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "💎 VIP функции\n\n"
        "Продвинутые сценарии публикации для рекламных постов.\n\n"
        "🕒 Запланированные посты\n"
        "Создавайте несколько будущих рекламных постов с разными материалами, сроками показа и временем запуска.\n"
        "⚠️ Сейчас доступна черновая версия сценария.\n\n"
        "🧹 Чистый канал\n"
        "Перед новой рекламой ViMi удалит предыдущий активный рекламный пост из этого правила.\n\n"
        "📌 Время в топе\n"
        "ViMi не будет публиковать обычные посты поверх рекламы в первые часы показа.\n\n"
        "✨ A/B-тесты\n"
        "Сравнивайте два варианта рекламного поста по просмотрам.\n\n"
        "Выберите функцию:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🕒 Запланированные посты", callback_data=f"rule_repost_campaign_scheduled_posts:{rule_id}")],[InlineKeyboardButton(text="🧹 Чистый канал", callback_data=f"rule_repost_campaign_vip_coming_soon:{rule_id}:clean_channel")],[InlineKeyboardButton(text="📌 Время в топе", callback_data=f"rule_repost_campaign_vip_coming_soon:{rule_id}:top_time")],[InlineKeyboardButton(text="✨ A/B-тесты", callback_data=f"rule_repost_campaign_vip_coming_soon:{rule_id}:ab_test")],[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")]])
    return text, kb
def build_repost_campaign_vip_coming_soon_view(*, rule_id: int, feature: str | None = None) -> tuple[str, InlineKeyboardMarkup]:
    return (
        "💎 Скоро в VIP функциях\n\nЭта функция появится в следующих обновлениях ViMi.",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к VIP функциям", callback_data=f"rule_repost_campaign_vip_features:{rule_id}")]]),
    )

def build_repost_campaign_schedule_menu_view(*, rule_id: int, scheduled_launches: list[dict] | None = None, now: datetime | None = None) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🕒 Запуск по расписанию\n\n"
        "Запланируйте рекламную кампанию заранее.\n"
        "ViMi сам запустит размещение в нужное время, удалит публикации после срока размещения и сохранит отчёт.\n\n"
        "Часовой пояс: UTC+3\n\n"
        "Выберите время запуска:"
    )
    rows=[[InlineKeyboardButton(text='Сегодня в 20:00', callback_data=f'rule_repost_campaign_schedule_quick:{rule_id}:today_20')],[InlineKeyboardButton(text='Завтра в 12:00', callback_data=f'rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_12')],[InlineKeyboardButton(text='Завтра в 18:00', callback_data=f'rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_18')],[InlineKeyboardButton(text='✍️ Ввести дату и время', callback_data=f'rule_repost_campaign_schedule_input:{rule_id}')]]
    launches = scheduled_launches or []
    pending_lines: list[str] = []
    for row in launches:
        if str(row.get("status") or "") != "scheduled":
            continue
        pending_lines.append(
            f"🕒 {format_campaign_datetime_text(row.get('scheduled_at'), timezone_offset_hours=3)} UTC+3 · ожидает запуска"
        )
        rows.append([InlineKeyboardButton(text=f"📄 Открыть запуск #{int(row.get('id') or 0)}", callback_data=f"rule_repost_campaign_scheduled_detail:{rule_id}:{int(row.get('id') or 0)}")])
    if pending_lines:
        text = f"{text}\n\nБлижайшие запланированные запуски:\n" + "\n".join(pending_lines[:5])
    rows.append([InlineKeyboardButton(text='⬅️ Назад к VIP функциям', callback_data=f'rule_repost_campaign_vip_features:{rule_id}')])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_schedule_wizard_step1_view(*, rule_id: int, readiness: dict) -> tuple[str, InlineKeyboardMarkup]:
    saved_post_ready = bool(readiness.get("saved_post_id")) and readiness.get("saved_post_exists") is not False
    text = (
        "🧙 VIP-запуск по расписанию · Шаг 1/4\n\n"
        "1) Рекламный пост\n\n"
        f"{'✅ Готов к публикации' if saved_post_ready else '❌ Не выбран'}"
    )
    rows = [[InlineKeyboardButton(text="✅ Далее", callback_data=f"rule_repost_campaign_schedule_step2:{rule_id}")]] if saved_post_ready else [[InlineKeyboardButton(text="➕ Добавить рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")]]
    rows.append([InlineKeyboardButton(text="⬅️ Назад к VIP функциям", callback_data=f"rule_repost_campaign_vip_features:{rule_id}")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_schedule_wizard_step2_view(*, rule_id: int, readiness: dict) -> tuple[str, InlineKeyboardMarkup]:
    extra_count = int(readiness.get("extra_total") or 0)
    text = (
        "🧙 VIP-запуск по расписанию · Шаг 2/4\n\n"
        "2) Каналы/группы\n\n"
        "Основной канал: ✅ Подключён по умолчанию\n"
        f"Дополнительные каналы/группы: {extra_count}"
    )
    rows = [
        [InlineKeyboardButton(text="📣 Добавить канал/группу", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить шаг", callback_data=f"rule_repost_campaign_schedule_step3:{rule_id}")],
    ]
    if extra_count > 0:
        rows.append([InlineKeyboardButton(text="✅ Далее", callback_data=f"rule_repost_campaign_schedule_step3:{rule_id}")])
        rows.append([InlineKeyboardButton(text="📋 Список каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_schedule_step1:{rule_id}")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_schedule_wizard_step3_view(*, rule_id: int, readiness: dict) -> tuple[str, InlineKeyboardMarkup]:
    show_seconds = int(readiness.get("show_seconds") or 0)
    text = (
        "🧙 VIP-запуск по расписанию · Шаг 3/4\n\n"
        "3) Время показа\n\n"
        f"Текущий срок размещения: {format_campaign_show_seconds_text(show_seconds)}\n\n"
        "Выберите срок размещения:"
    )
    rows = [
        [InlineKeyboardButton(text="1 час", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:3600"), InlineKeyboardButton(text="2 часа", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:7200")],
        [InlineKeyboardButton(text="6 часов", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:21600"), InlineKeyboardButton(text="12 часов", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:43200")],
        [InlineKeyboardButton(text="24 часа", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:86400"), InlineKeyboardButton(text="48 часов", callback_data=f"rule_repost_campaign_schedule_show_pick:{rule_id}:172800")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_schedule_step2:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_schedule_wizard_step4_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🧙 VIP-запуск по расписанию · Шаг 4/4\n\n"
        "4) Время запуска\n\n"
        "Когда запустить кампанию?\n\n"
        "Часовой пояс: UTC+3"
    )
    rows = [
        [InlineKeyboardButton(text="Сегодня в 20:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:today_20")],
        [InlineKeyboardButton(text="Завтра в 12:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_12")],
        [InlineKeyboardButton(text="Завтра в 18:00", callback_data=f"rule_repost_campaign_schedule_quick:{rule_id}:tomorrow_18")],
        [InlineKeyboardButton(text="✍️ Ввести дату и время", callback_data=f"rule_repost_campaign_schedule_input:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_schedule_step3:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_launch_progress_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🚀 Кампания запущена\n\n"
        "Идёт отправка..."
    )
    rows = [
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
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
            "📝 Рекламный пост опубликован\n"
            "🧾 Размещение создано\n"
            f"🧹 Удаление: автоматически через {format_campaign_show_seconds_text(extra.get('show_seconds'))}\n\n"
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
        rows.append([InlineKeyboardButton(text="📊 Отчёт просмотров", callback_data=f"rule_repost_campaign_views_report:{rule_id}:{run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="🔄 Обновить кампанию", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def _repost_campaign_launch_job_keyboard(rule_id: int, job_id: int | None) -> InlineKeyboardMarkup:
    rows = []
    if job_id:
        rows.append([InlineKeyboardButton(text="🔄 Обновить статус", callback_data=f"rule_repost_campaign_launch_job_status:{rule_id}:{job_id}")])
    rows.append([InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_launch_queued_view(*, rule_id: int, job_id: int | None) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🚀 Кампания поставлена в очередь\n\n"
        "Статус: ожидает отправки.\n"
        "Бот скоро начнёт рассылку.\n\n"
        "Можно закрыть это окно — процесс продолжится в фоне."
    )
    return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)


def build_repost_campaign_launch_needs_review_view(*, rule_id: int, job: dict) -> tuple[str, InlineKeyboardMarkup]:
    job_id = int(job.get("id") or 0) if job else None
    text = (
        "⚠️ Требуется проверка\n\n"
        "Сервер был перезапущен или состояние запуска неизвестно.\n"
        "Автоматический повтор остановлен, чтобы не отправить рекламу дважды."
    )
    last_error = (job or {}).get("last_error")
    if last_error:
        text += f"\n\nПричина: {last_error}"
    return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)


def build_repost_campaign_launch_job_status_view(*, rule_id: int, job: dict) -> tuple[str, InlineKeyboardMarkup]:
    payload = dict(job or {})
    status = str(payload.get("status") or "pending").strip().lower()
    job_id = int(payload.get("id") or 0) if payload.get("id") is not None else None
    if status == "needs_review":
        return build_repost_campaign_launch_needs_review_view(rule_id=rule_id, job=payload)
    if status == "processing":
        text = (
            "🚀 Кампания отправляется\n\n"
            "Статус: идёт рассылка.\n"
            "Можно закрыть это окно — процесс продолжится в фоне."
        )
        return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)
    if status == "sent":
        result_json = payload.get("result_json") or {}
        if result_json:
            return build_repost_campaign_launch_result_view(rule_id=rule_id, result=result_json)
        text = "✅ Кампания завершена\n\nСтатус: рассылка завершена."
        return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)
    if status == "failed":
        text = (
            "❌ Кампания не запущена\n\n"
            f"Причина: {payload.get('last_error') or 'неизвестная ошибка'}"
        )
        return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)
    if status == "cancelled":
        text = (
            "🚫 Запуск отменён\n\n"
            f"Причина: {payload.get('last_error') or 'запуск отменён'}"
        )
        return text, _repost_campaign_launch_job_keyboard(rule_id, job_id)
    return build_repost_campaign_launch_queued_view(rule_id=rule_id, job_id=job_id)


def build_repost_campaign_launch_readiness_view(*, rule_id: int, readiness: dict, now: datetime | None = None) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_launch_readiness_view_model(readiness=readiness, now=now)
    saved_post_ready = bool(readiness.get("saved_post_id")) and readiness.get("saved_post_exists") is not False
    saved_post_line = "✅ Готов к публикации" if saved_post_ready else "❌ Не готов к публикации"
    will_send_total = int(readiness.get("will_send_total") or 0)
    will_skip_total = int(readiness.get("will_skip_total") or 0)
    targets_total = int(readiness.get("targets_total") or 0)
    if targets_total <= 0:
        targets_total = will_send_total + will_skip_total
    show_seconds = int(readiness.get("show_seconds") or 0)
    expected_delete_line = "🕒 Ожидаемое удаление: не рассчитывается"
    if show_seconds > 0:
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        else:
            now_dt = now_dt.astimezone(timezone.utc)
        expected_delete_at = now_dt + timedelta(seconds=show_seconds)
        expected_delete_line = f"🕒 Ожидаемое удаление: {format_campaign_datetime_text(expected_delete_at, timezone_offset_hours=3)} UTC+3"

    lines = [
        "👁 Предпросмотр запуска",
        "",
        "Рекламный пост:",
        saved_post_line,
        "",
        "Публикация:",
        f"📣 Каналов/групп: {targets_total}",
        f"✅ Готовы: {will_send_total}",
        f"⚠️ Требуют внимания: {will_skip_total}",
        "",
        "Срок размещения:",
        f"⏳ {format_campaign_show_seconds_text(show_seconds)}",
        expected_delete_line,
        "",
        "После запуска ViMi:",
        "• опубликует пост в готовые каналы;",
        "• сохранит результат по каждому получателю;",
        "• соберёт просмотры перед удалением;",
        "• удалит копии после срока размещения;",
        "• подготовит отчёт XLSX/CSV/TXT.",
        "",
    ]
    if vm["can_launch"]:
        lines.append("Если всё верно — подтвердите запуск.")
    else:
        lines.extend(["Кампания не готова к запуску.", ""])
        if vm["block_reason_lines"]:
            lines.append("Причины:")
            lines.extend([f"• {line}" for line in vm["block_reason_lines"]])
            lines.append("")
        lines.append("Вернитесь назад и исправьте настройки.")
    text = "\n".join(lines)

    rows = []
    if vm["can_launch"]:
        rows.append([InlineKeyboardButton(text="✅ Подтвердить запуск", callback_data=f"rule_repost_campaign_launch_confirm:{rule_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")])
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
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, keyboard


def build_repost_campaign_targets_list_view(*, rule_id: int, targets: list[dict], page: int = 0, page_size: int = CAMPAIGN_TARGETS_PAGE_SIZE) -> tuple[str, InlineKeyboardMarkup]:
    safe_page_size = max(1, int(page_size or CAMPAIGN_TARGETS_PAGE_SIZE))
    total = len(targets)
    total_pages = max(1, (total + safe_page_size - 1) // safe_page_size)
    current_page = min(max(int(page or 0), 0), total_pages - 1)
    start = current_page * safe_page_size
    end = start + safe_page_size
    page_targets = targets[start:end]

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
        f"Страница {current_page + 1} из {total_pages}",
    ]
    if total == 0:
        lines.extend(["", "Пока не добавлено ни одного канала или группы."])
    text = "\n".join(lines)

    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for idx, row in enumerate(page_targets, start + 1):
        row_id = int(row.get("id") or 0)
        view = build_campaign_target_item_view(row, index=idx)
        keyboard_rows.append([InlineKeyboardButton(text=view.get("title") or f"Канал/Группа #{idx}", callback_data=f"rule_repost_campaign_target_card:{rule_id}:{row_id}:{current_page}")])

    if total_pages > 1:
        nav_row = []
        if current_page > 0:
            nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{current_page - 1}"))
        if current_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{current_page + 1}"))
        if nav_row:
            keyboard_rows.append(nav_row)

    keyboard_rows.extend([
        [InlineKeyboardButton(text="🔎 Проверить все права", callback_data=f"rule_repost_campaign_check:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=keyboard_rows)



def build_repost_campaign_target_card_view(*, rule_id: int, target: dict | None, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    safe_page = max(0, int(page or 0))
    if not target:
        text = "❌ Канал/группа не найдены\n\nВозможно, он уже удалён из кампании."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 К списку", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{safe_page}")]])
        return text, kb
    view = build_campaign_target_item_view(target)
    row_id = int(view.get("row_id") or target.get("id") or 0)
    lines = [
        "📣 Канал/Группа",
        "",
        view.get("title") or "Канал/группа",
        "",
        view.get("status_line") or "Статус: —",
        view.get("publish_line") or "Публикация: 🟡 не удалось подтвердить",
        view.get("delete_line") or "Удаление: 🟡 не удалось подтвердить",
        view.get("check_line") or "Проверка: —",
        f"Технический ID: {target.get('target_id') or '—'}",
    ]

    if view.get("thread_line"):
        lines.append(view["thread_line"])

    lines.append("")
    if view.get("error_line"):
        lines.extend(["Ошибка:", str(view.get("error_line")).replace("⚠️ ", "")])
    else:
        lines.append("Участвует в новых запусках кампании.")
    rows=[]
    if view.get("requires_attention"):
        rows.append([InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}:{safe_page}")])
    elif view.get("can_pause"):
        rows.append([InlineKeyboardButton(text="⏸ Пауза", callback_data=f"rule_repost_campaign_target_pause:{rule_id}:{row_id}:{safe_page}")])
        rows.append([InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}:{safe_page}")])
    elif view.get("can_enable"):
        rows.append([InlineKeyboardButton(text="▶️ Включить", callback_data=f"rule_repost_campaign_target_resume:{rule_id}:{row_id}:{safe_page}")])
        rows.append([InlineKeyboardButton(text="🔎 Проверить", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}:{safe_page}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"rule_repost_campaign_target_delete_confirm:{rule_id}:{row_id}:{safe_page}")])
    rows.extend([[InlineKeyboardButton(text="📋 К списку", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{safe_page}")],[InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")]])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_targets_check_loading_view(
    *,
    rule_id: int,
    targets_count: int,
) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🔎 Проверяем все права\n\n"
        "ViMi проверяет доступ к каналам и группам кампании.\n\n"
        f"Каналов/групп: {max(0, int(targets_count or 0))}\n\n"
        "Это может занять несколько секунд."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_target_action_result_view(*, rule_id: int, result: dict, action: str, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
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
    safe_page = max(0, int(page or 0))
    row_id = int(result.get("target_row_id") or 0)
    rows = []
    if action != "remove" and row_id > 0:
        rows.append([InlineKeyboardButton(text="📣 К каналу/группе", callback_data=f"rule_repost_campaign_target_card:{rule_id}:{row_id}:{safe_page}")])
    rows.append([InlineKeyboardButton(text="📋 К списку", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{safe_page}")])
    rows.append([InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    return text, kb


def build_repost_campaign_target_delete_confirm_view(*, rule_id: int, target: dict | None, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    safe_page = max(0, int(page or 0))
    if not target:
        text = "❌ Канал/группа не найдены в кампании."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{safe_page}")]
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
        [InlineKeyboardButton(text="🗑 Да, удалить", callback_data=f"rule_repost_campaign_target_delete:{rule_id}:{row_id}:{safe_page}")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"rule_repost_campaign_target_card:{rule_id}:{row_id}:{safe_page}")],
    ])
    return text, kb


def build_repost_campaign_posts_library_view(*, rule_id: int, library: dict) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_posts_library_view_model(library=library or {})
    lines = [vm["title"], "", vm["intro_line"], "", vm["posts_line"], vm["runs_line"], vm["placements_line"]]
    rows = []
    for item in (vm.get("items") or [])[:10]:
        sid = int(item.get("saved_post_id") or 0)
        if sid <= 0:
            continue
        title_line = str(item.get("title_line") or "").strip()
        button_text = "📄 Открыть пост"
        if title_line.startswith("✅"):
            button_text = "📄 Открыть текущий пост"
        else:
            dt_match = re.search(r"\d{2}\.\d{2}\s+\d{2}:\d{2}", title_line)
            if dt_match:
                button_text = f"📄 Открыть пост от {dt_match.group(0)}"
        rows.append([InlineKeyboardButton(text=button_text, callback_data=f"rule_repost_campaign_post_stats:{rule_id}:{sid}")])
    rows.extend([
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines).strip(), InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_post_stats_view(*, rule_id: int, saved_post_id: int, stats: dict) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_post_stats_view_model(stats=stats or {})
    lines = [vm["title"], ""]
    if vm.get("current_line"):
        lines.append(vm["current_line"])
    lines.extend([vm["kind_line"], "", vm["views_line"], vm["runs_line"], vm["placements_line"]])
    if vm.get("coverage_line"):
        lines.append(vm["coverage_line"])
    rows = [
        [InlineKeyboardButton(text="📊 Статистика по каналам", callback_data=f"rule_repost_campaign_post_channels_stats:{rule_id}:{saved_post_id}:0")],
        [InlineKeyboardButton(text="📊 Excel XLSX", callback_data=f"rule_repost_campaign_post_export_xlsx:{rule_id}:{saved_post_id}")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data=f"rule_repost_campaign_post_export_csv:{rule_id}:{saved_post_id}"), InlineKeyboardButton(text="📄 Экспорт TXT", callback_data=f"rule_repost_campaign_post_export_txt:{rule_id}:{saved_post_id}")],
    ]
    if vm.get("current_line"):
        rows.append([InlineKeyboardButton(text="🚀 Запустить кампанию", callback_data=f"rule_repost_campaign_launch:{rule_id}")])
    else:
        rows.append([InlineKeyboardButton(text="🚀 Использовать снова", callback_data=f"rule_repost_campaign_post_use:{rule_id}:{saved_post_id}")])
    rows.extend([
        [InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_post_channels_stats_view(
    *,
    rule_id: int,
    saved_post_id: int,
    stats: dict,
    offset: int = 0,
    page_size: int = 10,
) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_post_stats_view_model(stats=stats or {})
    items = list(vm.get("channels_items") or [])
    total_channels = len(items)
    if page_size <= 0:
        page_size = 10
    safe_offset = max(0, int(offset or 0))
    if total_channels > 0 and safe_offset >= total_channels:
        safe_offset = ((total_channels - 1) // page_size) * page_size
    page_items = items[safe_offset:safe_offset + page_size]
    total_pages = max(1, (total_channels + page_size - 1) // page_size)
    current_page = (safe_offset // page_size) + 1

    lines = [
        "📊 Статистика по каналам",
        "",
        "📄 Рекламный пост",
        vm.get("views_line") or "👁 Всего просмотров: 0",
        f"📣 Каналов/групп: {total_channels}",
        "",
        f"Страница {current_page} из {total_pages}",
        "",
    ]
    if not page_items:
        lines.append("—")
    else:
        for item in page_items:
            title = str(item.get("title") or "Без названия")
            status = str(item.get("views_status") or "ok")
            if status != "ok":
                lines.append(f"⚠️ нет данных — {title}")
            else:
                views_total = int(item.get("views_total") or 0)
                lines.append(f"👁 {views_total:,} — {title}".replace(",", " "))

    rows = []
    nav = []
    if safe_offset > 0:
        prev_offset = max(0, safe_offset - page_size)
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_post_channels_stats:{rule_id}:{saved_post_id}:{prev_offset}"))
    if safe_offset + page_size < total_channels:
        next_offset = safe_offset + page_size
        nav.append(InlineKeyboardButton(text="➡️ Вперёд", callback_data=f"rule_repost_campaign_post_channels_stats:{rule_id}:{saved_post_id}:{next_offset}"))
    if nav:
        rows.append(nav)
    rows.extend([
        [InlineKeyboardButton(text="📊 Excel XLSX", callback_data=f"rule_repost_campaign_post_export_xlsx:{rule_id}:{saved_post_id}")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data=f"rule_repost_campaign_post_export_csv:{rule_id}:{saved_post_id}"), InlineKeyboardButton(text="📄 Экспорт TXT", callback_data=f"rule_repost_campaign_post_export_txt:{rule_id}:{saved_post_id}")],
        [InlineKeyboardButton(text="📄 К посту", callback_data=f"rule_repost_campaign_post_stats:{rule_id}:{saved_post_id}")],
        [InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_post_stats_loading_view(*, rule_id: int, saved_post_id: int) -> tuple[str, InlineKeyboardMarkup]:
    _ = saved_post_id
    text = (
        "📄 Рекламный пост\n\n"
        "⏳ Собираю статистику просмотров…\n\n"
        "ViMi проверяет размещения этого поста в каналах/группах.\n"
        "Обычно это занимает несколько секунд.\n\n"
        "Что сейчас происходит:\n"
        "• проверяем опубликованные сообщения;\n"
        "• получаем просмотры из Telegram;\n"
        "• собираем топ каналов.\n\n"
        "Экран обновится автоматически."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 К библиотеке", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb

def extract_campaign_run_delete_after_at(messages: list[dict]) -> str | None:
    with_active_delete = []
    all_delete_dates = []
    for message in messages or []:
        delete_after_at = message.get("delete_after_at")
        if not delete_after_at:
            continue
        all_delete_dates.append(delete_after_at)
        delete_status = (message.get("delete_status") or "").strip().lower()
        if delete_status in {"pending", "processing", ""}:
            with_active_delete.append(delete_after_at)
    if with_active_delete:
        return min(with_active_delete)
    if all_delete_dates:
        return min(all_delete_dates)
    return None


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
    summary_sent = int(summary.get("sent") or run.get("targets_success") or 0)
    summary_total = int(summary.get("total") or run.get("targets_total") or 0)
    summary_failed = int(summary.get("failed") or run.get("targets_failed") or 0)
    summary_delete_pending = int(summary.get("delete_pending") or 0)
    summary_delete_processing = int(summary.get("delete_processing") or 0)
    summary_delete_failed = int(summary.get("delete_failed") or 0)
    messages = details.get("messages") or []
    has_delete_aggregates = any(key in summary for key in ("delete_pending", "delete_processing", "delete_failed"))
    if not has_delete_aggregates:
        for msg in messages:
            status = (msg.get("delete_status") or "").strip().lower()
            if status == "pending":
                summary_delete_pending += 1
            elif status == "processing":
                summary_delete_processing += 1
            elif status == "failed":
                summary_delete_failed += 1
    delete_after_at = extract_campaign_run_delete_after_at(messages)
    is_active_placement = summary_delete_pending > 0 or summary_delete_processing > 0
    lines = [
        "📄 Активное размещение" if is_active_placement else f"📄 Запуск #{run.get('id') or run_id}",
        "",
        f"✅ Опубликовано: {summary_sent} из {summary_total}",
        f"⚠️ Ошибки отправки: {summary_failed}",
        "",
        f"⏳ Время показа: {format_campaign_show_seconds_text(run.get('show_seconds'))}",
        f"🕒 Запущено: {format_campaign_datetime_text(run.get('started_at'))}",
        "",
        "Каналы/Группы:",
    ]
    if delete_after_at:
        lines.insert(8, f"🧹 Удаление ожидается: {format_campaign_datetime_text(delete_after_at)}")
    if is_active_placement:
        lines.extend([
            f"🧹 Ожидают удаления: {summary_delete_pending + summary_delete_processing}",
            f"⚠️ Ошибки удаления: {summary_delete_failed}",
        ])
    else:
        def _sort_key(msg: dict) -> tuple[int, str]:
            send_status = (msg.get("send_status") or "").strip().lower()
            delete_status = (msg.get("delete_status") or "").strip().lower()
            if send_status == "failed":
                return (0, "")
            if delete_status == "failed":
                return (1, "")
            if delete_status == "processing":
                return (2, "")
            if delete_status == "pending":
                return (3, "")
            return (4, "")
        lines.append("")
        visible_messages = sorted(messages, key=_sort_key)[:RUN_DETAILS_VISIBLE_MESSAGES_LIMIT]
        for idx, msg in enumerate(visible_messages, 1):
            view = build_campaign_run_message_view(msg, index=idx)
            channel = str(msg.get("target_title") or "Канал/Группа")
            send_status = (msg.get("send_status") or "").strip().lower()
            send_line = "✅ опубликовано" if send_status == "sent" else "⚠️ ошибка отправки"
            lines.append(f"{send_line.split()[0]} {channel} — {send_line.split(' ', 1)[1]}")
            if view["send_error_text"]:
                lines.append(f"Причина: {view['send_error_text'].replace('Ошибка отправки: ', '')}")
            delete_status = (msg.get("delete_status") or "").strip().lower()
            if delete_status == "failed":
                lines.append("⚠️ ошибка удаления")
                lines.append(f"Причина: {format_campaign_error_text(msg.get('delete_error_text')) or 'не указано'}")
            elif delete_status == "pending":
                lines.append("🧹 ожидает удаления")
            elif delete_status == "processing":
                lines.append("🧹 удаление выполняется")
            lines.append("")
        if len(messages) > RUN_DETAILS_VISIBLE_MESSAGES_LIMIT:
            lines.extend([f"Показаны первые {RUN_DETAILS_VISIBLE_MESSAGES_LIMIT} из {len(messages)}.", ""])
    kb_rows = []
    if is_active_placement:
        kb_rows.append([InlineKeyboardButton(text="🧹 Удалить сейчас", callback_data=f"rule_repost_campaign_run_delete_confirm:{rule_id}:{run.get('id') or run_id}")])
    elif summary_delete_failed > 0:
        kb_rows.append([InlineKeyboardButton(text="🔁 Повторить удаление", callback_data=f"rule_repost_campaign_run_delete_confirm:{rule_id}:{run.get('id') or run_id}")])
    kb_rows.extend([
        [InlineKeyboardButton(text="📊 Отчёт просмотров", callback_data=f"rule_repost_campaign_views_report:{rule_id}:{run.get('id') or run_id}")],
        [InlineKeyboardButton(text="📣 Каналы/Группы", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="🔄 Обновить статус", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run.get('id') or run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    return trim_campaign_text_for_telegram("\n".join(lines).rstrip()), kb


def build_repost_campaign_run_delete_confirm_view(*, rule_id: int, run_id: int, details: dict) -> tuple[str, InlineKeyboardMarkup]:
    summary = (details or {}).get("summary") or {}
    deletable = int(summary.get("delete_pending") or 0) + int(summary.get("delete_processing") or 0) + int(summary.get("delete_failed") or 0)
    published = int(summary.get("sent") or 0)
    total = int(summary.get("total") or 0)
    pending_processing = int(summary.get("delete_pending") or 0) + int(summary.get("delete_processing") or 0)
    failed = int(summary.get("delete_failed") or 0)
    text = (
        "🧹 Удалить активное размещение?\n\n"
        "Рекламный пост будет удалён из каналов/групп, где он был опубликован.\n\n"
        f"✅ Опубликовано: {published} из {total}\n"
        f"🧹 Ожидают удаления: {pending_processing}\n"
        f"⚠️ Ошибки удаления: {failed}\n\n"
        "Это действие нельзя отменить."
    )
    rows = []
    if deletable > 0:
        rows.append([InlineKeyboardButton(text="✅ Да, удалить сейчас", callback_data=f"rule_repost_campaign_run_delete_now:{rule_id}:{run_id}")])
    rows.extend([
        [InlineKeyboardButton(text="↩️ Назад к размещению", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_run_delete_loading_view(*, rule_id: int, run_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = "🧹 Удаляю активное размещение…\n\nПожалуйста, подождите."
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 К размещению", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_run_delete_result_view(*, rule_id: int, run_id: int, result) -> tuple[str, InlineKeyboardMarkup]:
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result or {})
    extra = payload.get("extra") or {}
    if payload.get("ok"):
        text = (
            "✅ Удаление размещения завершено\n\n"
            f"🧹 Удалено: {int(extra.get('deleted') or 0)}\n"
            f"↩️ Пропущено: {int(extra.get('skipped') or 0)}"
        )
    else:
        text = (
            "⚠️ Удаление завершено с ошибками\n\n"
            f"🧹 Удалено: {int(extra.get('deleted') or 0)}\n"
            f"❌ Ошибок: {int(extra.get('failed') or 0)}\n"
            f"↩️ Пропущено: {int(extra.get('skipped') or 0)}"
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 К размещению", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])
    return text, kb


def build_repost_campaign_views_report_view(*, rule_id: int, run_id: int, report: dict) -> tuple[str, InlineKeyboardMarkup]:
    vm = build_campaign_views_report_view_model(report=report or {})
    lines = [
        vm["title"],
        "",
        vm["status_line"],
        "",
        vm["total_views_line"],
        vm["coverage_line"],
        "",
        vm["post_line"],
        vm["run_line"],
        vm["show_seconds_line"],
        "",
        "Каналы/Группы:",
    ]
    lines.extend(vm.get("channel_lines") or ["—"])
    if vm.get("top_lines"):
        lines.extend(["", "Топ размещений:"] + vm["top_lines"])
    if vm.get("problem_lines"):
        lines.extend(["", "Где нет данных:"] + vm["problem_lines"])
    lines.extend(["", vm.get("delete_note_line") or "", vm.get("summary_line") or ""])
    rows = [
        [InlineKeyboardButton(text="📊 Excel XLSX", callback_data=f"rule_repost_campaign_views_export_xlsx:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data=f"rule_repost_campaign_views_export_csv:{rule_id}:{run_id}"), InlineKeyboardButton(text="📄 Экспорт TXT", callback_data=f"rule_repost_campaign_views_export_txt:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="🔄 Обновить просмотры", callback_data=f"rule_repost_campaign_views_report:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="📄 Детали запуска", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="📊 История размещений", callback_data=f"rule_repost_campaign_history:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
    return "\n".join([x for x in lines if x is not None]).strip(), InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_views_report_loading_view(*, rule_id: int, run_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "📊 Отчёт просмотров\n\n"
        "⏳ Собираю просмотры…\n\n"
        "ViMi проверяет опубликованные сообщения в каналах/группах.\n"
        "Обычно это занимает несколько секунд.\n\n"
        "Что сейчас происходит:\n"
        "• находим публикации этого запуска;\n"
        "• проверяем сообщения в Telegram;\n"
        "• собираем просмотры по каналам.\n\n"
        "Экран обновится автоматически."
    )
    rows = [
        [InlineKeyboardButton(text="📄 К размещению", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_repost_campaign_views_report_error_view(*, rule_id: int, run_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "📊 Отчёт просмотров\n\n"
        "⚠️ Сейчас не удалось собрать просмотры.\n\n"
        "Telegram временно не вернул данные по публикациям.\n"
        "Попробуйте обновить отчёт через несколько секунд."
    )
    rows = [
        [InlineKeyboardButton(text="🔄 Обновить отчёт", callback_data=f"rule_repost_campaign_views_report:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="📄 К размещению", callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{run_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


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


def build_repost_campaign_target_check_result_view(*, rule_id: int, result: dict, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    payload = result or {}
    safe_page = max(0, int(page or 0))
    ok = bool(payload.get("ok"))
    check_ok = bool(payload.get("check_ok", ok))
    saved = bool(payload.get("saved", True))
    title = (result or {}).get("target_title") or (result or {}).get("target_id") or "—"
    target_id = (result or {}).get("target_id") or "—"
    p = (result or {}).get("publish_status") or ("confirmed" if (result or {}).get("can_publish") is True else "unknown")
    d = (result or {}).get("delete_status")
    if not d:
        d = "confirmed" if (result or {}).get("can_delete") is True else ("denied" if (result or {}).get("can_delete") is False else "unknown")
    publish_line = "Публикация: ✅ подтверждена" if p == "confirmed" else ("Публикация: ❌ нет права" if p == "denied" else "Публикация: 🟡 не удалось подтвердить")
    delete_line = "Удаление: ✅ подтверждено" if d == "confirmed" else ("Удаление: ❌ нет права" if d == "denied" else "Удаление: 🟡 не удалось подтвердить")
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
    rows = [
        [InlineKeyboardButton(text="📋 К списку каналов/групп", callback_data=f"rule_repost_campaign_targets_list:{rule_id}:{safe_page}")],
        [InlineKeyboardButton(text="🔄 Проверить ещё раз", callback_data=f"rule_repost_campaign_target_check:{rule_id}:{row_id}:{safe_page}")],
    ]
    if row_id > 0:
        rows.append([InlineKeyboardButton(text="📣 К каналу/группе", callback_data=f"rule_repost_campaign_target_card:{rule_id}:{row_id}:{safe_page}")])
    rows.append([InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    return text, kb


def build_repost_campaign_targets_check_result_view(*, rule_id: int, result: dict) -> tuple[str, InlineKeyboardMarkup]:
    items = (result or {}).get("items") or []
    blocked = sum(1 for i in items if (i.get("publish_status") in {"denied", "unknown"} or i.get("delete_status") == "denied"))
    delete_warn = sum(1 for i in items if i.get("publish_status") == "confirmed" and i.get("delete_status") == "unknown")
    lines = [
        "🧪 Проверка прав завершена",
        "",
        f"Проверено: {int((result or {}).get('checked') or 0)}",
        f"✅ Готово к размещению: {int((result or {}).get('passed') or 0)}",
        f"❌ Заблокировано: {blocked}",
        f"🟡 Не подтверждено удаление: {delete_warn}",
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

def build_repost_campaign_schedule_preview_view(*, rule_id: int, readiness: dict, scheduled_at_utc: datetime, timezone_offset_minutes: int = 180, timezone_label: str = "UTC+3") -> tuple[str, InlineKeyboardMarkup]:
    from app.repost_campaign_schedule_service import format_campaign_schedule_datetime
    show_seconds = int(readiness.get("show_seconds") or 0)
    expected_delete = scheduled_at_utc + timedelta(seconds=show_seconds) if show_seconds > 0 else None
    will_send_total = int(readiness.get("will_send_total") or 0)
    will_skip_total = int(readiness.get("will_skip_total") or 0)
    targets_total = int(readiness.get("targets_total") or 0)
    if targets_total <= 0:
        targets_total = will_send_total + will_skip_total
    text = (
        "👁 Предпросмотр запланированного запуска\n\n"
        "Старт:\n"
        f"🕒 {format_campaign_schedule_datetime(scheduled_at_utc, timezone_offset_minutes=timezone_offset_minutes, timezone_label=timezone_label)}\n\n"
        "Рекламный пост:\n"
        f"{'✅ Готов к публикации' if readiness.get('saved_post_id') else '❌ Не готов к публикации'}\n\n"
        "Публикация:\n"
        f"📣 Каналов/групп: {targets_total}\n"
        f"✅ Готовы: {will_send_total}\n"
        f"⚠️ Требуют внимания: {will_skip_total}\n\n"
        "Срок размещения:\n"
        f"⏳ {format_campaign_show_seconds_text(show_seconds)}\n"
        f"🕒 Ожидаемое удаление: {format_campaign_schedule_datetime(expected_delete, timezone_offset_minutes=timezone_offset_minutes, timezone_label=timezone_label)}\n\n"
        "После запуска ViMi:\n"
        "• опубликует пост в готовые каналы;\n"
        "• сохранит результат по каждому получателю;\n"
        "• соберёт просмотры перед удалением;\n"
        "• удалит копии после срока размещения;\n"
        "• подготовит отчёт XLSX/CSV/TXT.\n\n"
        "Если всё верно — запланируйте запуск."
    )
    can=bool(readiness.get('can_launch'))
    rows=[[InlineKeyboardButton(text='✅ Запланировать запуск', callback_data=f"rule_repost_campaign_schedule_confirm:{rule_id}:{int(scheduled_at_utc.timestamp())}")]] if can else []
    rows.append([InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_schedule_menu:{rule_id}')])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_repost_campaign_scheduled_launch_detail_view(*, rule_id:int, scheduled_launch:dict) -> tuple[str, InlineKeyboardMarkup]:
    from app.repost_campaign_schedule_service import format_campaign_schedule_datetime
    status = str(scheduled_launch.get("status") or "scheduled").lower()
    if status == "launched":
        return ("🕒 Запланированный запуск\n\n🚀 Запуск выполнен", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='📄 Открыть размещение', callback_data=f"rule_repost_campaign_history_detail:{rule_id}:{scheduled_launch.get('campaign_run_id')}")]]))
    status_map = {
        "scheduled": "🕒 ожидает запуска",
        "processing": "🟡 запускается",
        "failed": "❌ ошибка запуска",
        "needs_review": "⚠️ Требуется проверка",
        "cancelled": "⛔ отменён",
        "expired": "⚪ просрочен",
    }
    text = (
        "🕒 Запланированный запуск\n\n"
        f"Статус: {status_map.get(status, status)}\n"
        f"Старт: {format_campaign_schedule_datetime(scheduled_launch.get('scheduled_at'))}\n"
        f"Пост: #{int(scheduled_launch.get('saved_post_id') or 0)}\n"
        f"Срок размещения: {format_campaign_show_seconds_text(int(scheduled_launch.get('show_seconds') or 0))}\n"
        f"Ожидаемое удаление: {format_campaign_schedule_datetime((datetime.fromisoformat(str(scheduled_launch.get('scheduled_at')).replace('Z','+00:00')) + timedelta(seconds=int(scheduled_launch.get('show_seconds') or 0))))}"
    )
    if status == "needs_review":
        text += (
            "\n\nЗапуск был прерван после создания campaign_run.\n"
            "Автоматический повтор остановлен, чтобы не отправить рекламу дважды."
        )

    rows = []
    if status == "scheduled":
        rows.append([InlineKeyboardButton(text='❌ Отменить запуск', callback_data=f"rule_repost_campaign_scheduled_cancel_confirm:{rule_id}:{scheduled_launch.get('id')}")])
    if status == "needs_review":
        rows.append([InlineKeyboardButton(text='🔄 Обновить', callback_data=f"rule_repost_campaign_scheduled_detail:{rule_id}:{scheduled_launch.get('id')}")])
        rows.append([InlineKeyboardButton(text='⬅️ Назад к кампании', callback_data=f'rule_repost_campaign_menu:{rule_id}')])
    else:
        rows.append([InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_schedule_menu:{rule_id}')])
    return (text, InlineKeyboardMarkup(inline_keyboard=rows))


def build_repost_campaign_schedule_result_view(*, rule_id: int, scheduled_launch: dict) -> tuple[str, InlineKeyboardMarkup]:
    from app.repost_campaign_schedule_service import format_campaign_schedule_datetime
    dt = scheduled_launch.get("scheduled_at")
    show_seconds = int(scheduled_launch.get("show_seconds") or 0)
    expected = datetime.fromisoformat(str(dt).replace("Z", "+00:00")) + timedelta(seconds=show_seconds)
    text = (
        "✅ Запуск запланирован\n\n"
        f"Старт: {format_campaign_schedule_datetime(dt)}\n"
        f"Ожидаемое удаление: {format_campaign_schedule_datetime(expected)}\n\n"
        "ViMi автоматически запустит кампанию в указанное время."
    )
    return text, InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Открыть запланированный запуск", callback_data=f"rule_repost_campaign_scheduled_detail:{rule_id}:{scheduled_launch.get('id')}")],
        [InlineKeyboardButton(text="💎 К VIP функциям", callback_data=f"rule_repost_campaign_vip_features:{rule_id}")],
        [InlineKeyboardButton(text="💰 К кампании", callback_data=f"rule_repost_campaign_menu:{rule_id}")],
    ])


def build_repost_campaign_scheduled_launch_cancel_confirm_view(*, rule_id: int, scheduled_launch_id: int) -> tuple[str, InlineKeyboardMarkup]:
    return ("❌ Отменить запланированный запуск?\n\nДействие нельзя отменить.", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отменить запуск", callback_data=f"rule_repost_campaign_scheduled_cancel:{rule_id}:{scheduled_launch_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_detail:{rule_id}:{scheduled_launch_id}")],
    ]))


def build_repost_campaign_scheduled_launch_cancel_result_view(*, rule_id: int, ok: bool) -> tuple[str, InlineKeyboardMarkup]:
    return ("✅ Запланированный запуск отменён" if ok else "❌ Не удалось отменить запуск", InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕒 К расписанию", callback_data=f"rule_repost_campaign_schedule_menu:{rule_id}")],
        [InlineKeyboardButton(text="💎 К VIP функциям", callback_data=f"rule_repost_campaign_vip_features:{rule_id}")],
    ]))

# =========================================================
# VIP SCHEDULED POSTS UI
# =========================================================

def build_vip_scheduled_posts_screen_view(*, rule_id: int, posts: list[dict] | None = None, active_placement: dict | None = None) -> tuple[str, InlineKeyboardMarkup]:
    active = active_placement or {}
    has_active = bool(active.get("active_placement"))
    delete_failed = int(active.get("delete_failed") or 0)
    lines = [
        '🕒 Запланированные посты',
        '',
        'Планируйте рекламные публикации заранее: ViMi сам отправит пост в выбранное время, выдержит срок показа и подготовит отчёт.',
        '',
    ]
    if has_active:
        lines.extend([
            '🟢 Сейчас активно размещение',
            '',
            'Пост будет удалён:',
            f"🕘 {active.get('active_delete_after_text') or 'в ближайшее время'}",
        ])
        if active.get("active_left_text"):
            lines.extend([
                '',
                f"До удаления:\n⏳ {active.get('active_left_text')}",
            ])
        lines.extend([
            '',
            'VIP-режим: публикация не блокируется активной рекламой.',
            '',
        ])
    if delete_failed > 0:
        lines.extend([
            '⚠️ Есть размещение с ошибкой удаления',
            '',
            'VIP-пост можно опубликовать поверх неё; проверьте удаление предыдущего размещения отдельно.',
            '',
        ])
    lines.extend([
        'Как это работает:',
        '1. Добавьте рекламный пост.',
        '2. Выберите каналы/группы.',
        '3. Укажите срок показа.',
        '4. Выберите время запуска.',
        '',
        'После подтверждения пост появится в списке отложенных постов.',
    ])
    rows = [
        [InlineKeyboardButton(text='➕ Запланировать пост', callback_data=f'rule_repost_campaign_scheduled_post_new:{rule_id}')],
        [InlineKeyboardButton(text='📄 Все запланированные посты', callback_data=f'rule_repost_campaign_scheduled_posts_list:{rule_id}:0')],
    ]
    if has_active or delete_failed > 0:
        rows.append([InlineKeyboardButton(text='🧹 Удалить активный пост', callback_data=f'rule_repost_campaign_vip_delete_active:{rule_id}')])
    rows.append([InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_vip_features:{rule_id}')])
    return '\n'.join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def build_vip_scheduled_posts_list_view(
    *,
    rule_id: int,
    posts: list[dict],
    page: int = 0,
    page_size: int = 10,
) -> tuple[str, InlineKeyboardMarkup]:
    visible_posts = [post for post in posts if str(post.get("status") or "").strip().lower() in {"scheduled", "processing", "launched", "failed", "cancelled", "expired"}]
    visible_posts = [post for post in visible_posts if post.get("scheduled_at")]
    total = len(visible_posts)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(0, page), total_pages - 1)
    start = page * page_size
    page_items = visible_posts[start:start + page_size]
    lines = ["📄 Все запланированные посты", "", "Пока нет отложенных постов.", "", "Создайте пост и пройдите 4 шага настройки — после подтверждения он появится здесь."] if total == 0 else ["📄 Все запланированные посты", "", "Ваши отложенные публикации:", f"Страница: {page + 1} / {total_pages}"]
    rows = []
    for post in page_items:
        post_id = int(post.get("id") or 0)
        scheduled_at = post.get("scheduled_at")
        button_text = f"🕒 Отложенный пост от {format_campaign_datetime_text(scheduled_at, timezone_offset_hours=3)}"
        rows.append([InlineKeyboardButton(text=button_text, callback_data=f"rule_repost_campaign_scheduled_post_detail:{rule_id}:{post_id}")])
    if total > page_size:
        pager = []
        if page > 0:
            pager.append(InlineKeyboardButton(text="⬅️ Предыдущая", callback_data=f"rule_repost_campaign_scheduled_posts_list:{rule_id}:{page - 1}"))
        if page < total_pages - 1:
            pager.append(InlineKeyboardButton(text="➡️ Следующая", callback_data=f"rule_repost_campaign_scheduled_posts_list:{rule_id}:{page + 1}"))
        if pager:
            rows.append(pager)
    rows += [
        [InlineKeyboardButton(text="➕ Запланировать пост", callback_data=f"rule_repost_campaign_scheduled_post_new:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_posts:{rule_id}")],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)



def build_vip_scheduled_post_create_choice_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "🧙 Новый запланированный пост\n\n"
        "Что запланировать?\n\n"
        "📝 Создать новый рекламный пост\n"
        "Создайте новый материал для будущей публикации.\n\n"
        "📌 Использовать текущий пост кампании\n"
        "Взять рекламный пост, который уже выбран в текущей кампании.\n\n"
        "📚 Выбрать из библиотеки\n"
        "Выбрать один из ранее сохранённых рекламных постов."
    )
    rows = [
        [InlineKeyboardButton(text='📝 Создать новый рекламный пост', callback_data=f'rule_repost_campaign_scheduled_post_new_create_material:{rule_id}')],
        [InlineKeyboardButton(text='📌 Использовать текущий пост кампании', callback_data=f'rule_repost_campaign_scheduled_post_new_from_current:{rule_id}')],
        [InlineKeyboardButton(text='📚 Выбрать из библиотеки', callback_data=f'rule_repost_campaign_scheduled_post_new_from_library:{rule_id}')],
        [InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_posts:{rule_id}')],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_vip_scheduled_post_create_material_help_view(*, rule_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "📝 Создать новый рекламный пост\n\n"
        "Сейчас создание нового материала выполняется через библиотеку рекламных постов.\n"
        "Создайте пост в библиотеке, затем вернитесь сюда и выберите его для расписания."
    )
    rows = [
        [InlineKeyboardButton(text='📚 Открыть библиотеку постов', callback_data=f'rule_repost_campaign_history:{rule_id}')],
        [InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_create_choice:{rule_id}')],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)
def build_vip_scheduled_post_wizard_post_view(*, rule_id:int, scheduled_post:dict, saved_posts:list[dict], readiness:dict, title: str = '🧙 Запланированный пост · Шаг 1/4')->tuple[str,InlineKeyboardMarkup]:
    selected = scheduled_post.get('saved_post_id')
    current = f'✅ Пост #{int(selected)}' if selected else '❌ Пост не выбран'
    text = f'{title}\n📝 Рекламный пост\nВыберите материал, который ViMi опубликует в указанное время.\nТекущий выбор:\n{current}\n\n📚 Выберите рекламный пост из библиотеки'
    rows=[]
    for sp in saved_posts[:10]:
        sid=int(sp.get('id') or sp.get('saved_post_id') or 0)
        rows.append([InlineKeyboardButton(text=f'Пост #{sid}', callback_data=f'rule_repost_campaign_scheduled_post_pick_post:{rule_id}:{int(scheduled_post.get("id") or 0)}:{sid}')])
    rows += [[InlineKeyboardButton(text='📚 Открыть библиотеку постов', callback_data=f'rule_repost_campaign_history:{rule_id}')],[InlineKeyboardButton(text='✅ Далее', callback_data=f'rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{int(scheduled_post.get("id") or 0)}')],[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_posts:{rule_id}')]]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_vip_scheduled_post_wizard_targets_view(*, rule_id:int, scheduled_post:dict, targets:list[dict], readiness:dict)->tuple[str,InlineKeyboardMarkup]:
    selected_targets = targets or []
    selected_count = len(selected_targets)
    sid = int(scheduled_post.get('id') or 0)
    lines = [
        '🧙 Запланированный пост · Шаг 2/4',
        '📋 Каналы/группы',
        '',
        'Выберите, куда ViMi опубликует этот отложенный пост.',
        '',
        f'Выбрано: {selected_count}',
    ]
    if selected_targets:
        lines.extend(['', 'Выбранные каналы:'])
        for target in selected_targets[:5]:
            title = str(target.get('target_title') or target.get('target_id') or 'Канал/группа')
            lines.append(f'✅ {title}')
        if selected_count > 5:
            lines.append(f'…и ещё {selected_count - 5}')
    text = "\n".join(lines)

    rows = [
        [InlineKeyboardButton(text='➕ Добавить канал/группу', callback_data=f'rule_repost_campaign_scheduled_post_add_target:{rule_id}:{sid}')],
        [InlineKeyboardButton(text='📋 Выбрать из известных', callback_data=f'rule_repost_campaign_scheduled_post_pick_targets:{rule_id}:{sid}')],
    ]
    if selected_count > 0:
        rows.append([InlineKeyboardButton(text='✅ Далее', callback_data=f'rule_repost_campaign_scheduled_post_step_show:{rule_id}:{sid}')])
    rows.append([InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_step_post:{rule_id}:{sid}')])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_vip_scheduled_post_add_target_view(*, rule_id: int, scheduled_post_id: int) -> tuple[str, InlineKeyboardMarkup]:
    text = (
        "➕ Добавить канал/группу\n"
        "Отправьте ссылку или ID канала/группы, куда нужно опубликовать этот запланированный пост.\n"
        "Можно отправить:\n"
        "• @channelname\n"
        "• https://t.me/channelname\n"
        "• -1001234567890\n\n"
        "После добавления ViMi вернёт вас на Шаг 2."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{scheduled_post_id}")]])
    return text, kb

def build_vip_scheduled_post_pick_targets_view(
    *,
    rule_id: int,
    scheduled_post_id: int,
    known_targets: list[dict],
    selected_targets: list[dict],
    page: int = 0,
    page_size: int = 10,
) -> tuple[str, InlineKeyboardMarkup]:
    if not known_targets:
        text = (
            "📋 Выбрать канал/группу\n\n"
            "Пока нет известных каналов/групп.\n\n"
            "Добавьте канал вручную через кнопку:\n"
            "➕ Добавить канал/группу"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить канал/группу", callback_data=f"rule_repost_campaign_scheduled_post_add_target:{rule_id}:{scheduled_post_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{scheduled_post_id}")],
        ])
        return text, kb
    page = max(0, int(page))
    page_size = max(1, int(page_size))
    total_pages = max(1, (len(known_targets) + page_size - 1) // page_size)
    page = min(page, total_pages - 1)
    start = page * page_size
    end = start + page_size
    page_items = known_targets[start:end]
    selected_keys = {f"{str(t.get('target_id') or '')}|{str(t.get('target_thread_id') or '')}" for t in (selected_targets or [])}
    text = (
        "📋 Выбрать канал/группу\n\n"
        "Выберите каналы/группы для этого запланированного поста.\n\n"
        "Можно выбрать один канал, несколько или все.\n\n"
        f"Выбрано: {len(selected_keys)}\n"
        f"Страница: {page + 1} / {total_pages}"
    )
    rows: list[list[InlineKeyboardButton]] = []
    for idx, t in enumerate(page_items, start=start):
        tkey = f"{str(t.get('target_id') or '')}|{str(t.get('target_thread_id') or '')}"
        prefix = "✅" if tkey in selected_keys else "➕"
        title = str(t.get("target_title") or t.get("target_id") or "Канал/группа")
        rows.append([InlineKeyboardButton(text=f"{prefix} {title}", callback_data=f"rule_repost_campaign_scheduled_post_add_known_target:{rule_id}:{scheduled_post_id}:{idx}:{page}")])
    if total_pages > 1:
        rows.append([InlineKeyboardButton(text="➕ Добавить все на странице", callback_data=f"rule_repost_campaign_scheduled_post_add_known_page:{rule_id}:{scheduled_post_id}:{page}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить все", callback_data=f"rule_repost_campaign_scheduled_post_add_known_all:{rule_id}:{scheduled_post_id}")])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Предыдущая", callback_data=f"rule_repost_campaign_scheduled_post_pick_targets:{rule_id}:{scheduled_post_id}:{page-1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️ Следующая", callback_data=f"rule_repost_campaign_scheduled_post_pick_targets:{rule_id}:{scheduled_post_id}:{page+1}"))
        if nav:
            rows.append(nav)
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{scheduled_post_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{scheduled_post_id}")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_vip_scheduled_post_wizard_show_view(*, rule_id:int, scheduled_post:dict, readiness:dict)->tuple[str,InlineKeyboardMarkup]:
    sid=int(scheduled_post.get('id') or 0)
    current = format_campaign_show_seconds_text(scheduled_post.get('show_seconds'))
    text=f"🧙 Запланированный пост · Шаг 3/4\n⏳ Срок показа\nСколько времени рекламный пост должен оставаться в каналах?\nТекущий срок:\n✅ {current}"
    rows=[[InlineKeyboardButton(text='1 час', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:3600'),InlineKeyboardButton(text='2 часа', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:7200')],[InlineKeyboardButton(text='6 часов', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:21600'),InlineKeyboardButton(text='12 часов', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:43200')],[InlineKeyboardButton(text='24 часа', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:86400'),InlineKeyboardButton(text='48 часов', callback_data=f'rule_repost_campaign_scheduled_post_pick_show:{rule_id}:{sid}:172800')],[InlineKeyboardButton(text='✅ Далее', callback_data=f'rule_repost_campaign_scheduled_post_step_time:{rule_id}:{sid}')],[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_step_targets:{rule_id}:{sid}')]]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_vip_scheduled_post_wizard_time_view(*, rule_id:int, scheduled_post:dict, readiness:dict)->tuple[str,InlineKeyboardMarkup]:
    sid=int(scheduled_post.get('id') or 0)
    dt=format_campaign_datetime_text(scheduled_post.get('scheduled_at'), timezone_offset_hours=3)
    text=f"🧙 Запланированный пост · Шаг 4/4\n🕒 Время запуска\nКогда ViMi должен опубликовать этот рекламный пост?\nЧасовой пояс: UTC+3\nТекущее время:\n✅ {dt} UTC+3"
    rows=[[InlineKeyboardButton(text='Сегодня в 20:00', callback_data=f'rule_repost_campaign_scheduled_post_quick_time:{rule_id}:{sid}:today_20')],[InlineKeyboardButton(text='Завтра в 12:00', callback_data=f'rule_repost_campaign_scheduled_post_quick_time:{rule_id}:{sid}:tomorrow_12')],[InlineKeyboardButton(text='Завтра в 18:00', callback_data=f'rule_repost_campaign_scheduled_post_quick_time:{rule_id}:{sid}:tomorrow_18')],[InlineKeyboardButton(text='✍️ Ввести дату и время', callback_data=f'rule_repost_campaign_scheduled_post_input_time:{rule_id}:{sid}')],[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_step_show:{rule_id}:{sid}')]]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def format_human_count(value: int | None) -> str:
    return f"{max(0, int(value or 0)):,}".replace(",", " ")


def format_vip_scheduled_run_summary(run: dict) -> list[str]:
    run_row = run.get("run") or {}
    sent = int(run_row.get("targets_success") or 0)
    total = int(run_row.get("targets_total") or 0)
    failed = int(run_row.get("targets_failed") or 0)
    status_line = "Статус: отправлено" if failed <= 0 else "Статус: отправлено частично"
    lines = ["📤 Публикация", "", status_line, f"Отправлено: {sent} из {total} каналов/групп"]
    if failed > 0:
        lines.append(f"Ошибки отправки: {failed}")
    return lines


def format_vip_scheduled_delete_summary(run: dict) -> list[str]:
    summary = run.get("summary") or {}
    messages = run.get("messages") or []
    done = int(summary.get("delete_done") or 0)
    failed = int(summary.get("delete_failed") or 0)
    pending = int(summary.get("delete_pending") or 0)
    processing = int(summary.get("delete_processing") or 0)
    delete_after_values = [m.get("delete_after_at") for m in messages if m.get("delete_after_at")]
    delete_after = max(delete_after_values) if delete_after_values else None
    delete_error_text = next((str(m.get("delete_error_text") or "") for m in messages if str(m.get("delete_status") or "") == "failed" and m.get("delete_error_text")), "")
    total = done + failed + pending + processing
    lines = ["🧹 Удаление", ""]
    if failed > 0:
        lines.append(f"Не удалось удалить: {failed}")
        if delete_error_text:
            lines.append(f"Причина: {delete_error_text}")
        return lines
    if done > 0 and total > 0 and pending <= 0 and processing <= 0:
        lines.append(f"Удалено: {done} из {total}")
        return lines
    if delete_after:
        lines.append(f"Пост будет удалён: {format_campaign_datetime_text(delete_after, timezone_offset_hours=3)} UTC+3")
    if pending > 0:
        lines.append(f"Ожидает удаления: {pending}")
    elif processing > 0:
        lines.append(f"Удаление выполняется: {processing}")
    return lines if len(lines) > 2 else []


def format_vip_scheduled_views_summary(run: dict) -> list[str]:
    views = run.get("views") or {}
    total_views = int(views.get("total_views") or 0)
    if total_views <= 0:
        return []
    return ["👁 Просмотры", "", f"Всего: {format_human_count(total_views)}"]

def build_vip_scheduled_post_preview_view(*, rule_id:int, scheduled_post:dict, targets:list[dict], readiness:dict)->tuple[str,InlineKeyboardMarkup]:
    sid=int(scheduled_post.get('id') or 0)
    status='\n'.join([f"• {x}" for x in (readiness.get('block_reasons') or [])])
    warning_lines='\n'.join([str(x) for x in (readiness.get('warnings') or []) if x])
    text=(f"👁 Предпросмотр запланированного поста\nРекламный пост:\n✅ Пост #{int(scheduled_post.get('saved_post_id') or 0)}\n"
          f"Срок показа:\n⏳ {format_campaign_show_seconds_text(scheduled_post.get('show_seconds'))}\n"
          f"Запуск:\n🕒 {format_campaign_datetime_text(scheduled_post.get('scheduled_at'), timezone_offset_hours=3)} UTC+3")
    if warning_lines:
        text += f"\n\n{warning_lines}"
    text += "\n\nVIP-режим: публикация не блокируется активной рекламой."
    rows=[]
    if readiness.get('can_schedule'):
        rows.append([InlineKeyboardButton(text='✅ Запланировать пост', callback_data=f'rule_repost_campaign_scheduled_post_confirm:{rule_id}:{sid}')])
    elif status:
        text += f"\n\nПост пока не готов.\nЧто нужно исправить:\n{status}"
    rows += [[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_step_time:{rule_id}:{sid}')]]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)

def build_vip_scheduled_post_detail_view(*, rule_id: int, details: dict) -> tuple[str, InlineKeyboardMarkup]:
    post = details.get('post') or {}
    readiness = details.get('readiness') or {}
    run = details.get('campaign_run') or {}
    sid = int(post.get('id') or 0)
    st = str(post.get('status') or 'draft')
    status = "запланирован" if st in {"draft", "ready", "scheduled"} else format_vip_scheduled_post_status_text(st)
    run_id = int(post.get("campaign_run_id") or 0)
    launch_line = f"{format_campaign_datetime_text(post.get('scheduled_at'), timezone_offset_hours=3)} UTC+3"
    blocks = [
        "🕒 Отложенный пост",
        f"Статус: {status}\nЗапуск: {launch_line}\nСрок показа: {format_campaign_show_seconds_text(post.get('show_seconds'))}",
    ]
    if st == "processing":
        blocks[1] = "Статус: запускается\nЗапуск: сейчас\nСрок показа: " + format_campaign_show_seconds_text(post.get("show_seconds"))
        blocks.append("ViMi отправляет пост в выбранные каналы/группы.\nИзменения сейчас недоступны.")
    elif run and run_id > 0:
        run_summary = format_vip_scheduled_run_summary(run)
        delete_summary = format_vip_scheduled_delete_summary(run)
        views_summary = format_vip_scheduled_views_summary(run)
        if run_summary:
            blocks.append("\n".join(run_summary))
        if delete_summary:
            blocks.append("\n".join(delete_summary))
        if views_summary:
            blocks.append("\n".join(views_summary))
    elif st in {"draft", "ready", "scheduled"}:
        blocks.append("Публикация ещё не запускалась.\nПосле запуска здесь появится отчёт по отправке, удалению и просмотрам.")
        blocks.append("VIP-режим: публикация не блокируется активной рекламой.")
        warnings = [str(x) for x in (readiness.get("warnings") or []) if x]
        if warnings:
            blocks.append("\n".join(warnings))
    if post.get("error_text"):
        blocks.append(f"⚠️ Есть проблема\n\nОшибка: {post.get('error_text')}")
    text = "\n\n".join([b for b in blocks if b])
    rows = []
    if st in {'draft', 'ready'}:
        rows += [[InlineKeyboardButton(text='🚀 Отправить сейчас', callback_data=f'rule_repost_campaign_scheduled_post_send_now_confirm:{rule_id}:{sid}')],[InlineKeyboardButton(text='✏️ Изменить пост', callback_data=f'rule_repost_campaign_scheduled_post_edit:{rule_id}:{sid}')],[InlineKeyboardButton(text='📋 Дублировать пост', callback_data=f'rule_repost_campaign_scheduled_post_duplicate:{rule_id}:{sid}')],[InlineKeyboardButton(text='🗑 Удалить пост', callback_data=f'rule_repost_campaign_scheduled_post_cancel_confirm:{rule_id}:{sid}')]]
    elif st == 'scheduled':
        rows += [[InlineKeyboardButton(text='🚀 Отправить сейчас', callback_data=f'rule_repost_campaign_scheduled_post_send_now_confirm:{rule_id}:{sid}')],[InlineKeyboardButton(text='📋 Дублировать пост', callback_data=f'rule_repost_campaign_scheduled_post_duplicate:{rule_id}:{sid}')],[InlineKeyboardButton(text='🗑 Отменить пост', callback_data=f'rule_repost_campaign_scheduled_post_cancel_confirm:{rule_id}:{sid}')]]
    elif st == 'processing':
        text += "\n\nПост сейчас запускается. Изменения недоступны."
        rows += [[InlineKeyboardButton(text='📋 Дублировать пост', callback_data=f'rule_repost_campaign_scheduled_post_duplicate:{rule_id}:{sid}')]]
    elif st == 'launched':
        if run_id > 0:
            rows += [
                [InlineKeyboardButton(text='📊 Открыть отчёт', callback_data=f'rule_repost_campaign_views_report:{rule_id}:{run_id}')],
            ]
        rows += [[InlineKeyboardButton(text='📋 Дублировать пост', callback_data=f'rule_repost_campaign_scheduled_post_duplicate:{rule_id}:{sid}')]]
    else:
        rows += [[InlineKeyboardButton(text='📋 Дублировать пост', callback_data=f'rule_repost_campaign_scheduled_post_duplicate:{rule_id}:{sid}')]]
    rows.append([InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_posts_list:{rule_id}:0')])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_vip_scheduled_post_cancel_confirm_view(*, rule_id: int, scheduled_post: dict) -> tuple[str, InlineKeyboardMarkup]:
    sid = int(scheduled_post.get('id') or 0)
    text = "🗑 Удалить отложенный пост?\n\nViMi не отправит его по расписанию."
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='✅ Удалить пост', callback_data=f'rule_repost_campaign_scheduled_post_cancel:{rule_id}:{sid}')],[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_detail:{rule_id}:{sid}')]])
    return text, kb

def build_vip_scheduled_post_send_now_confirm_view(*, rule_id: int, scheduled_post: dict) -> tuple[str, InlineKeyboardMarkup]:
    sid = int(scheduled_post.get("id") or 0)
    text = "🚀 Отправить отложенный пост сейчас?\n\nViMi запустит этот пост немедленно, не дожидаясь выбранного времени.\n\n⚠️ В этих целях уже может быть активная реклама.\nПосле подтверждения VIP-пост будет опубликован поверх неё."
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='✅ Да, отправить сейчас', callback_data=f'rule_repost_campaign_scheduled_post_send_now:{rule_id}:{sid}')],[InlineKeyboardButton(text='⬅️ Назад', callback_data=f'rule_repost_campaign_scheduled_post_detail:{rule_id}:{sid}')]])
    return text, kb
