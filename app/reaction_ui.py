from __future__ import annotations

import json
import unicodedata
from datetime import datetime
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


def build_rule_reactions_keyboard(
    rule_id: int,
    enabled: bool,
    *,
    callback_prefix: str = "user_rule_reactions",
    back_callback: str | None = None,
) -> InlineKeyboardMarkup:
    toggle_text = "⚪️ Выключить реакции" if enabled else "🟢 Включить реакции"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f"{callback_prefix}_toggle:{rule_id}")],
            [InlineKeyboardButton(text="➕ Подключить аккаунт-реактор", callback_data=f"{callback_prefix}_add_account:{rule_id}")],
            [InlineKeyboardButton(text="👥 Мои аккаунты-реакторы", callback_data=f"{callback_prefix}_accounts:{rule_id}")],
            [InlineKeyboardButton(text="🧪 Тест реакции", callback_data=f"{callback_prefix}_test:{rule_id}")],
            [InlineKeyboardButton(text="⬅️ Назад в дополнительные функции", callback_data=back_callback or f"user_rule_extra:{rule_id}")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"{callback_prefix}_refresh:{rule_id}")],
        ]
    )




def _status_label(status_raw: Any) -> str:
    status = str(status_raw or "").strip().lower()
    if status == "active":
        return "🟢 активен"
    if status in {"disabled", "inactive"}:
        return "⚪️ отключён"
    if status in {"error", "failed", "banned", "auth_required"}:
        return "🔴 ошибка"
    return f"⚪️ {status or 'unknown'}"


def _account_identity(row: dict[str, Any]) -> str:
    username = (row.get("username") or "").strip()
    if username:
        return f"@{username}"
    phone = str(row.get("phone_hint") or "").strip()
    if phone:
        return phone
    tg_uid = row.get("telegram_user_id")
    if tg_uid:
        return f"ID {tg_uid}"
    return "без имени"


def _format_reactions(value: Any) -> str:
    raw = value
    parsed: list[str] = []
    if isinstance(raw, list):
        parsed = [str(x) for x in raw if str(x).strip()]
    elif isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                parsed = [str(x) for x in data if str(x).strip()]
        except Exception:
            parsed = []
    return " ".join(parsed) if parsed else "по умолчанию"


def _format_connected_at(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "н/д"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return raw

def build_rule_reaction_accounts_text(accounts: list[dict[str, Any]]) -> str:
    lines = ["👥 Аккаунты-реакторы", "", "Аккаунты, подключённые к этому workspace/tenant.", ""]
    if not accounts:
        lines.append("Нет подключённых аккаунтов.")
    else:
        for row in accounts:
            lines.extend([
                f"#{row.get('id')} · {_account_identity(row)}",
                f"Статус: {_status_label(row.get('status'))}",
                f"Реакции: {_format_reactions(row.get('fixed_reactions_json'))}",
                f"Подключён: {_format_connected_at(row.get('created_at'))}",
                "",
            ])
    return "\n".join(lines).strip()


def build_rule_reaction_accounts_keyboard(rule_id: int, *, callback_prefix: str = "user_rule_reactions") -> InlineKeyboardMarkup:
    return build_rule_reaction_accounts_keyboard_with_items(rule_id, [], callback_prefix=callback_prefix)


def build_rule_reaction_accounts_keyboard_with_items(
    rule_id: int,
    accounts: list[dict[str, Any]],
    *,
    callback_prefix: str = "user_rule_reactions",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for row in accounts:
        label = _account_identity(row)
        status = _status_label(row.get("status"))
        rows.append([InlineKeyboardButton(text=f"👤 {label} · {status}", callback_data=f"{callback_prefix}_account:{rule_id}:{row.get('id')}")])
    rows.extend([
        [InlineKeyboardButton(text="➕ Подключить аккаунт", callback_data=f"{callback_prefix}_add_account:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"{callback_prefix}:{rule_id}")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_rule_reaction_account_detail_text(account: dict[str, Any]) -> str:
    return (
        "👤 Аккаунт-реактор\n\n"
        f"Account ID: {account.get('id')}\n"
        f"Аккаунт: {_account_identity(account)}\n"
        f"Telegram ID: {account.get('telegram_user_id')}\n"
        f"Статус: {_status_label(account.get('status'))}\n"
        f"Premium: {'да' if account.get('is_premium') else 'нет'}\n"
        f"Текущие реакции: {_format_reactions(account.get('fixed_reactions_json'))}\n"
        f"Подключён: {_format_connected_at(account.get('created_at'))}"
    )


def build_rule_reaction_account_detail_keyboard(rule_id: int, account_id: int, status: str) -> InlineKeyboardMarkup:
    is_active = str(status or "").strip().lower() == "active"
    toggle_text = "⛔ Отключить" if is_active else "✅ Включить"
    toggle_cb = f"user_rule_reactions_account_disable:{rule_id}:{account_id}" if is_active else f"user_rule_reactions_account_enable:{rule_id}:{account_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎭 Изменить реакции", callback_data=f"user_rule_reactions_account_reactions:{rule_id}:{account_id}")],
            [InlineKeyboardButton(text=toggle_text, callback_data=toggle_cb)],
            [InlineKeyboardButton(text="🔄 Переподключить", callback_data=f"user_rule_reactions_account_reconnect:{rule_id}:{account_id}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"user_rule_reactions_account_delete_confirm:{rule_id}:{account_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к аккаунтам", callback_data=f"user_rule_reactions_accounts:{rule_id}")],
        ]
    )


def build_rule_reaction_account_delete_confirm_text(account: dict[str, Any]) -> str:
    username = account.get("username")
    ident = f"@{username}" if username else f"ID {account.get('telegram_user_id')}"
    return (
        "🗑 Удаление аккаунта-реактора\n\n"
        f"Вы действительно хотите удалить {ident}?\n\n"
        "Будет удалена запись аккаунта и session-файл на сервере.\n"
        "Если нужно, вы сможете подключить аккаунт заново через защищённую страницу."
    )


def build_rule_reaction_account_delete_confirm_keyboard(rule_id: int, account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"user_rule_reactions_account_delete:{rule_id}:{account_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"user_rule_reactions_account:{rule_id}:{account_id}")],
        ]
    )


def build_rule_reaction_back_keyboard(rule_id: int, *, callback_prefix: str = "user_rule_reactions") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"{callback_prefix}:{rule_id}")]]
    )


def _looks_like_emoji_cluster(cluster: str) -> bool:
    if not cluster:
        return False
    for ch in cluster:
        code = ord(ch)
        if ch in {"\ufe0f", "\u200d"} or 0x1F3FB <= code <= 0x1F3FF:
            continue
        if unicodedata.category(ch) == "So":
            return True
        if (
            0x1F300 <= code <= 0x1FAFF
            or 0x2600 <= code <= 0x27BF
            or 0x2300 <= code <= 0x23FF
            or 0x1F1E6 <= code <= 0x1F1FF
        ):
            return True
    return False


def normalize_fixed_reactions_input(text: str, *, is_premium: bool) -> list[str]:
    cleaned = (text or "").replace(",", " ").strip()
    if not cleaned:
        raise ValueError("Введите хотя бы одну реакцию.")
    normalized_text = cleaned.replace("\ufe0f", "")
    candidates = normalized_text.split() if " " in normalized_text else list(normalized_text)
    result: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        cluster = item.strip()
        if not cluster or not _looks_like_emoji_cluster(cluster):
            continue
        if cluster in seen:
            continue
        seen.add(cluster)
        result.append(cluster)
    if not result:
        raise ValueError("Введите хотя бы одну реакцию.")
    limit = 3 if is_premium else 1
    return result[:limit]


def build_reaction_account_reactions_text(account: dict[str, Any]) -> str:
    username = account.get("username")
    ident = f"@{username}" if username else f"ID {account.get('telegram_user_id')}"
    current_raw = account.get("fixed_reactions_json") or "[]"
    current_set = str(current_raw)
    is_premium = bool(account.get("is_premium"))
    account_type = "Premium" if is_premium else "обычный"
    limit_line = "Для Premium: до 3 emoji." if is_premium else "Для обычного аккаунта: 1 emoji."
    return (
        "🎭 Набор реакций\n\n"
        f"Аккаунт: {ident}\n"
        f"Тип: {account_type}\n\n"
        "Текущий набор:\n"
        f"{current_set}\n\n"
        "Введите новый набор реакций сообщением.\n\n"
        f"{limit_line}\n"
        "Пример:\n"
        "🔥 ❤️ 🥰"
    )


def build_reaction_account_reactions_keyboard(rule_id: int, account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Очистить набор", callback_data=f"user_rule_reactions_account_reactions_clear:{rule_id}:{account_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к аккаунту", callback_data=f"user_rule_reactions_account:{rule_id}:{account_id}")],
        ]
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


def build_reaction_web_onboarding_text(rule_id: int, *, web_enabled: bool) -> str:
    return (
        "➕ Подключение аккаунта-реактора\n\n"
        "Подключение Telegram-аккаунта нельзя выполнять через чат бота: Telegram может заблокировать попытку входа, "
        "если login-code отправить в другой Telegram-чат.\n\n"
        "Правильный способ подключения:\n"
        "• открыть защищённую HTTPS-страницу ViMi;\n"
        "• ввести код только на этой странице;\n"
        "• после успешного входа аккаунт будет привязан к вашему workspace.\n\n"
                + ("✅ Защищённая страница подключения доступна по кнопке ниже.\n\n" if web_enabled else "🚧 Защищённая страница подключения пока не включена.\n\n")
        + "Ваши коды Telegram и 2FA-пароли не должны отправляться в этот чат."
    )


def build_reaction_web_onboarding_keyboard(
    rule_id: int,
    onboarding_url: str | None = None,
    *,
    callback_prefix: str = "user_rule_reactions",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if onboarding_url:
        rows.append([InlineKeyboardButton(text="🌐 Открыть защищённое подключение", url=onboarding_url)])
    rows.extend([
        [InlineKeyboardButton(text="👥 Мои аккаунты-реакторы", callback_data=f"{callback_prefix}_accounts:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"{callback_prefix}:{rule_id}")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


def build_reaction_auth_phone_text(rule_id: int) -> str:
    return (
        "➕ Подключение аккаунта-реактора\n\n"
        "Введите номер Telegram-аккаунта, который принадлежит вам или вашей команде.\n\n"
        "Формат:\n"
        "+79991234567\n\n"
        "Важно:\n"
        "• не подключайте чужие аккаунты;\n"
        "• коды Telegram и 2FA-пароли не сохраняются;\n"
        "• аккаунт будет привязан только к вашему workspace."
    )


def build_reaction_auth_code_text(phone_hint: str) -> str:
    return f"📩 Код отправлен в Telegram\n\nВведите код, который пришёл на аккаунт {phone_hint}."


def build_reaction_auth_password_text(phone_hint: str) -> str:
    return (
        "🔐 Требуется 2FA-пароль\n\n"
        f"На аккаунте {phone_hint} включена двухэтапная защита.\n"
        "Введите 2FA-пароль.\n"
        "Пароль не сохраняется."
    )


def build_reaction_auth_success_text(account: dict[str, Any]) -> str:
    username = account.get("username")
    ident = f"@{username}" if username else f"ID {account.get('telegram_user_id')}"
    return (
        "✅ Аккаунт-реактор подключён\n\n"
        f"Аккаунт: {ident}\n"
        f"Premium: {'да' if account.get('is_premium') else 'нет'}\n"
        "Статус: active\n\n"
        "Теперь он отображается в разделе “👥 Мои аккаунты-реакторы”."
    )


def build_reaction_auth_error_text(error: str) -> str:
    return f"❌ Ошибка подключения: {error}"


def build_reaction_auth_cancel_keyboard(rule_id: int, *, callback_prefix: str = "user_rule_reactions") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить подключение", callback_data=f"{callback_prefix}_auth_cancel:{rule_id}")]])


def build_reaction_auth_success_keyboard(rule_id: int, *, callback_prefix: str = "user_rule_reactions") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Мои аккаунты-реакторы", callback_data=f"{callback_prefix}_accounts:{rule_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к реакциям", callback_data=f"{callback_prefix}:{rule_id}")],
    ])
