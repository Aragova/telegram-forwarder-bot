from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def build_rule_reactions_text(rule_id: int, settings: dict[str, Any] | None, accounts: list[dict[str, Any]]) -> str:
    enabled = bool(settings and settings.get("enabled"))
    mode = str((settings or {}).get("mode") or "premium_then_normal")
    total = len(accounts)
    active = sum(1 for row in accounts if str(row.get("status") or "").strip().lower() == "active")
    premium = sum(1 for row in accounts if bool(row.get("is_premium")))
    ordinary = max(total - premium, 0)
    status = "🟢 Включены" if enabled else "⚪️ Выключены"
    lines = [
        f"⚙️ Реакции правила #{rule_id}",
        "",
        f"Статус: {status}",
        f"Режим: {mode}",
        f"Аккаунтов-реакторов: {total}",
        f"Активных: {active}",
        f"Premium: {premium}",
        f"Обычных: {ordinary}",
        "",
        "Как это работает:",
        "Реакции должны ставиться аккаунтами, которые принадлежат владельцу этого правила. "
        "Глобальные аккаунты сервиса используются только в legacy/dev режиме.",
    ]
    if total == 0:
        lines.extend(["", "Пока нет подключённых аккаунтов-реакторов."])
    return "\n".join(lines)


def build_rule_reactions_keyboard(rule_id: int, enabled: bool) -> InlineKeyboardMarkup:
    toggle_text = "⚪️ Выключить реакции" if enabled else "🟢 Включить реакции"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f"rule_reactions_toggle:{rule_id}")],
            [InlineKeyboardButton(text="➕ Подключить аккаунт-реактор", callback_data=f"rule_reactions_add_account:{rule_id}")],
            [InlineKeyboardButton(text="👥 Мои аккаунты-реакторы", callback_data=f"rule_reactions_accounts:{rule_id}")],
            [InlineKeyboardButton(text="🎭 Набор реакций", callback_data=f"rule_reactions_preset:{rule_id}")],
            [InlineKeyboardButton(text="🧪 Тест реакции", callback_data=f"rule_reactions_test:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад в дополнительные функции", callback_data=f"rule_extra_menu:{rule_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"rule_reactions_refresh:{rule_id}")],
        ]
    )


def build_rule_reaction_accounts_text(accounts: list[dict[str, Any]]) -> str:
    lines = ["👥 Аккаунты-реакторы", "", "Аккаунты, подключённые к этому workspace/tenant.", ""]
    if not accounts:
        lines.append("Нет подключённых аккаунтов.")
    else:
        for row in accounts:
            username = row.get("username")
            tg_uid = row.get("telegram_user_id")
            ident = f"@{username}" if username else (f"id:{tg_uid}" if tg_uid else "без username")
            reactions = row.get("fixed_reactions_json") or "[]"
            reactions_short = str(reactions)
            if len(reactions_short) > 40:
                reactions_short = reactions_short[:37] + "..."
            lines.extend([
                f"#{row.get('id')} · {ident}",
                f"Premium: {'да' if row.get('is_premium') else 'нет'} · Статус: {row.get('status') or 'unknown'}",
                f"Набор: {reactions_short}",
                "",
            ])
    return "\n".join(lines).strip()


def build_rule_reaction_accounts_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Подключить аккаунт", callback_data=f"rule_reactions_add_account:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"rule_reactions:{rule_id}")],
        ]
    )


def build_rule_reaction_back_keyboard(rule_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"rule_reactions:{rule_id}")]]
    )


def build_rule_reaction_connect_text() -> str:
    return (
        "➕ Подключение аккаунта-реактора\n\n"
        "Аккаунт-реактор — это Telegram-аккаунт владельца канала или команды, который будет автоматически "
        "ставить реакции под публикациями ваших правил.\n\n"
        "Важно:\n"
        "• используйте только свои аккаунты или аккаунты вашей команды;\n"
        "• не подключайте чужие аккаунты;\n"
        "• коды Telegram и 2FA-пароли нельзя показывать третьим лицам;\n"
        "• глобальные аккаунты сервиса не используются для чужих клиентов.\n\n"
        "Статус:\n"
        "🚧 Подключение аккаунтов будет включено следующим обновлением."
    )


def build_rule_reaction_preset_text() -> str:
    return (
        "🎭 Набор реакций\n\n"
        "Сейчас правило использует будущий SaaS-набор:\n"
        "premium_then_normal\n\n"
        "В следующем обновлении здесь можно будет выбрать:\n"
        "• premium набор до 3 реакций;\n"
        "• ordinary pool;\n"
        "• количество аккаунтов;\n"
        "• задержку между реакциями."
    )


def build_rule_reaction_test_text() -> str:
    return (
        "🧪 Тест реакции\n\n"
        "Тест будет доступен после подключения аккаунтов-реакторов в этом workspace."
    )
