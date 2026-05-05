from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_repost_campaign_menu_view(*, rule_id: int, summary: dict, saved_post_line: str) -> tuple[str, InlineKeyboardMarkup]:
    show_seconds_text = str((summary or {}).get("show_seconds_text") or "не задан")
    targets_active = int((summary or {}).get("targets_active") or 0)
    targets_ready = int((summary or {}).get("targets_ready") or 0)
    saved_post_id = (summary or {}).get("saved_post_id")
    text = (
        "💰 Рекламная кампания\n\n"
        "Тестовый режим для админа.\n\n"
        "Новый репост из этого правила будет опубликован в основной канал и каналы кампании.\n"
        "После окончания срока показа бот автоматически удалит рекламные публикации.\n\n"
        f"⏳ Срок показа: {show_seconds_text}\n"
        f"📣 Каналы кампании: {targets_active}\n"
        f"🧪 Готовность: {targets_ready} / {targets_active}\n"
        f"{saved_post_line}"
    )
    rows = [
        [InlineKeyboardButton(text="⏳ Срок показа", callback_data=f"rule_repost_campaign_show_menu:{rule_id}")],
        [InlineKeyboardButton(text="📣 Каналы кампании", callback_data=f"rule_repost_campaign_targets:{rule_id}")],
        [InlineKeyboardButton(text="📝 Рекламный пост", callback_data=f"rule_repost_campaign_post_menu:{rule_id}")],
        [InlineKeyboardButton(text="👁 Предпросмотр кампании", callback_data=f"rule_repost_campaign_preview:{rule_id}")],
        [InlineKeyboardButton(text="📊 История кампаний", callback_data=f"rule_repost_campaign_history:{rule_id}")],
    ]
    if saved_post_id:
        rows.append([InlineKeyboardButton(text="🚀 Тестовый запуск", callback_data=f"rule_repost_campaign_test_send:{rule_id}")])
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
