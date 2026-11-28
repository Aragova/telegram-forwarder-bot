from __future__ import annotations

from math import isfinite
from html import escape

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .context import AdminHandlersContext
from app.billing_catalog import USD_PRICES
from app.payments.fixed_prices import format_crypto_price, format_stars_price


def _normalize_button_text(text: str | None) -> str:
    # Убираем variation selector, чтобы одинаково обрабатывать emoji-style/unstyled кнопки.
    return (text or "").replace("\ufe0f", "").strip()


def _cancel_price_kb(back_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data=back_callback)],
    ])


def _billing_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💵 Цены USD", callback_data="admin_billing_usd")],
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="admin_fixed_prices_kind:stars")],
        [InlineKeyboardButton(text="₿ Crypto", callback_data="admin_fixed_prices_kind:crypto")],
        [InlineKeyboardButton(text="💱 Курсы валют", callback_data="admin_billing_rates")],
        [InlineKeyboardButton(text="⬅️ Назад в систему", callback_data="admin_billing_home_back")],
    ])


BILLING_HOME_TEXT = (
    "<b>💳 БИЛЛИНГ И ТАРИФЫ</b>\n\n"
    "💎 <b>BASIC</b>\n🚀 <b>PRO</b>\n\n"
    "USD и курсы валют, отдельные фиксированные цены Stars и Crypto."
)


class _FixedPriceSnapshot:
    """Read each price history once per screen; reuse existing pricing rules."""

    def __init__(self, repo, kinds):
        self.prices = {kind: repo.get_billing_fixed_prices(kind) or {} for kind in kinds} if hasattr(repo, "get_billing_fixed_prices") else {}

    def get_billing_fixed_prices(self, kind):
        return self.prices.get(kind, {})


def _fixed_price_repo(repo, kinds):
    return _FixedPriceSnapshot(repo, kinds) if hasattr(repo, "get_billing_fixed_prices") else repo


def _build_rates_text(ctx: AdminHandlersContext) -> str:
    rates = {}
    if hasattr(ctx.db, "get_billing_exchange_rates"):
        rates = ctx.db.get_billing_exchange_rates() or {}
    rub = rates.get("USD_TO_RUB", 95.0)
    eur = rates.get("USD_TO_EUR", 0.9)
    uah = rates.get("USD_TO_UAH", 40.0)
    return (
        "<b>💱 КУРСЫ ВАЛЮТ</b>\n\n"
        "Базовая валюта: <b>USD</b>\n\n"
        "Ручные курсы:\n"
        f"🇷🇺 RUB: <b>{escape(str(rub))}</b>\n"
        f"🇪🇺 EUR: <b>{escape(str(eur))}</b>\n"
        f"🇺🇦 UAH: <b>{escape(str(uah))}</b>\n\n"
        "Stars и Crypto не пересчитываются автоматически."
    )


def _build_rates_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🇷🇺 Изменить RUB", callback_data="admin_billing_rate_edit:RUB")],[InlineKeyboardButton(text="🇪🇺 Изменить EUR", callback_data="admin_billing_rate_edit:EUR")],[InlineKeyboardButton(text="🇺🇦 Изменить UAH", callback_data="admin_billing_rate_edit:UAH")],[InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_billing_rates")],[InlineKeyboardButton(text="⬅️ Назад к биллингу", callback_data="admin_billing_home")]])


def _build_tariff_prices_text(ctx: AdminHandlersContext) -> str:
    saved = ctx.db.get_billing_usd_prices() if hasattr(ctx.db, "get_billing_usd_prices") else {}
    basic = saved.get("basic", {}) if isinstance(saved, dict) else {}
    pro = saved.get("pro", {}) if isinstance(saved, dict) else {}
    def _line(code: str, period: int, source: dict) -> str:
        value = source.get(period, USD_PRICES[code][period])
        value_num = float(value)
        return f"{period} месяц{'а' if period in (3, 6) else 'ев' if period == 12 else ''} — {value_num:g} USD"
    return (
        "<b>💵 ЦЕНЫ ТАРИФОВ</b>\n\n"
        "Базовая валюта: <b>USD</b>\n\n"
        "💎 <b>BASIC</b>\n"
        f"{_line('basic', 1, basic)}\n{_line('basic', 3, basic)}\n{_line('basic', 6, basic)}\n{_line('basic', 12, basic)}\n\n"
        "🚀 <b>PRO</b>\n"
        f"{_line('pro', 1, pro)}\n{_line('pro', 3, pro)}\n{_line('pro', 6, pro)}\n{_line('pro', 12, pro)}"
    )




def _build_fixed_prices_text(ctx: AdminHandlersContext) -> str:
    repo = _fixed_price_repo(ctx.db, ("stars", "crypto"))
    lines = ["<b>💫 ФИКС-ЦЕНЫ STARS/CRYPTO</b>", "",
             "Цены задаются отдельно от курсов RUB/EUR/UAH."]
    for kind, heading, formatter in (
        ("stars", "⭐ Telegram Stars", format_stars_price),
        ("crypto", "₿ Crypto", format_crypto_price),
    ):
        lines.extend(["", f"<b>{heading}</b>"])
        for code in ("basic", "pro"):
            lines.append(f"<b>{code.upper()}</b>")
            for period in (1, 3, 6, 12):
                lines.append(f"{period} мес. — {escape(formatter(code, period, repo=repo))}")
    return "\n".join(lines)


def _build_fixed_plan_kb(ctx, kind, code):
    repo = _fixed_price_repo(ctx.db, (kind,))
    formatter = format_stars_price if kind == "stars" else format_crypto_price
    rows = [[InlineKeyboardButton(
        text=f"{period} мес. — {formatter(code, period, repo=repo)}",
        callback_data=f"admin_fixed_price_edit:{kind}:{code}:{period}",
    )] for period in (1, 3, 6, 12)]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_fixed_prices_kind:{kind}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_fixed_prices_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⭐ Stars', callback_data='admin_fixed_prices_kind:stars')],[InlineKeyboardButton(text='₿ Crypto', callback_data='admin_fixed_prices_kind:crypto')],[InlineKeyboardButton(text='🔄 Обновить', callback_data='admin_fixed_prices')],[InlineKeyboardButton(text='⬅️ Назад к биллингу', callback_data='admin_billing_home')]])

def _build_tariff_prices_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 BASIC", callback_data="admin_billing_usd_plan:basic")],
        [InlineKeyboardButton(text="🚀 PRO", callback_data="admin_billing_usd_plan:pro")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_billing_usd")],
        [InlineKeyboardButton(text="⬅️ Назад к биллингу", callback_data="admin_billing_home")],
    ])


def register_admin_system_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: _normalize_button_text(m.text) in {"⚙ Система", "Система"})
    async def handle_system_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text="⚙️ Раздел: Система", reply_markup=ctx.get_system_menu())

    @dp.message(lambda m: _normalize_button_text(m.text) == "💳 Биллинг и тарифы")
    async def handle_billing_home(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text=BILLING_HOME_TEXT,
                                    parse_mode="HTML", reply_markup=_billing_home_kb())

    @dp.callback_query(lambda c: c.data in {"admin_billing_home", "admin_billing_rates_back",
                                            "admin_billing_usd_back", "admin_fixed_prices_back"})
    async def handle_billing_home_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text=BILLING_HOME_TEXT,
                                         parse_mode="HTML", reply_markup=_billing_home_kb())

    @dp.callback_query(lambda c: c.data == "admin_billing_home_back")
    async def handle_billing_home_back(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        await ctx.answer_callback_safe_once(callback)
        await ctx.edit_message_text_safe(message=callback.message, text="Настройки биллинга закрыты.")
        await ctx.send_message_safe(chat_id=callback.message.chat.id, text="⚙️ Раздел: Система",
                                    reply_markup=ctx.get_system_menu())

    @dp.message(lambda m: _normalize_button_text(m.text) == "💱 Курсы валют")
    async def handle_billing_rates_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text=await ctx.run_db(_build_rates_text, ctx), parse_mode="HTML", reply_markup=_build_rates_kb())

    @dp.message(lambda m: _normalize_button_text(m.text) == "💫 Фикс-цены Stars/Crypto")
    async def handle_billing_fixed_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text=await ctx.run_db(_build_fixed_prices_text, ctx), parse_mode="HTML", reply_markup=_build_fixed_prices_kb())

    @dp.message(lambda m: _normalize_button_text(m.text) == "💵 Цены тарифов")
    async def handle_billing_usd_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text=await ctx.run_db(_build_tariff_prices_text, ctx), parse_mode="HTML", reply_markup=_build_tariff_prices_kb())

    @dp.callback_query(lambda c: c.data == "admin_billing_usd")
    async def handle_billing_usd_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        await ctx.edit_message_text_safe(message=callback.message, text=await ctx.run_db(_build_tariff_prices_text, ctx), parse_mode="HTML", reply_markup=_build_tariff_prices_kb())
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_usd_plan:"))
    async def handle_billing_usd_plan(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        code = str(callback.data).split(":", 1)[1].lower()
        plan_icon = "💎" if code == "basic" else "🚀"
        plan_name = "BASIC" if code == "basic" else "PRO"
        if code not in USD_PRICES:
            await ctx.answer_callback_safe(callback, "Некорректный тариф", show_alert=True)
            return
        saved = await ctx.run_db(ctx.db.get_billing_usd_prices) if hasattr(ctx.db, "get_billing_usd_prices") else {}
        plan_prices = (saved or {}).get(code, {})
        lines = []
        for period in (1, 3, 6, 12):
            value = plan_prices.get(period, USD_PRICES[code][period])
            lines.append([InlineKeyboardButton(text=f"{period} мес — {float(value):g} USD", callback_data=f"admin_billing_usd_edit:{code}:{period}")])
        lines.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_billing_usd")])
        await ctx.edit_message_text_safe(message=callback.message, text=f"{plan_icon} {plan_name} — цены", reply_markup=InlineKeyboardMarkup(inline_keyboard=lines))
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_usd_edit:"))
    async def handle_billing_usd_edit(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        parts = str(callback.data).split(":")
        if len(parts) != 3 or parts[1] not in USD_PRICES or parts[2] not in {"1", "3", "6", "12"}:
            await ctx.answer_callback_safe(callback, "Некорректный тариф или период", show_alert=True)
            return
        _, code, period = parts
        user_id = callback.from_user.id
        ctx.user_states[user_id] = {"action": "admin_billing_usd_price_input", "tariff_code": code, "period_months": int(period)}
        await ctx.edit_message_text_safe(
            message=callback.message, text="Введите новую цену в USD.\n\nПример:\n11\nили\n11.5",
            reply_markup=_cancel_price_kb(f"admin_billing_usd_plan:{code}"),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data == "admin_billing_rates")
    async def handle_billing_rates_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        await ctx.edit_message_text_safe(message=callback.message, text=await ctx.run_db(_build_rates_text, ctx), parse_mode="HTML", reply_markup=_build_rates_kb())
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_rate_edit:"))
    async def handle_billing_rate_edit(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        currency = str(callback.data).split(":", 1)[1].upper()
        if currency not in {"RUB", "EUR", "UAH"}:
            await ctx.answer_callback_safe(callback, "Некорректная валюта", show_alert=True)
            return
        user_id = callback.from_user.id
        ctx.user_states[user_id] = {"action": "admin_billing_rate_input", "currency": currency}
        await ctx.edit_message_text_safe(
            message=callback.message, text=f"Введите новый курс USD→{currency} (положительное число):",
            reply_markup=_cancel_price_kb("admin_billing_rates"),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.message(lambda m: m.from_user is not None and ctx.user_states.get(m.from_user.id, {}).get("action") == "admin_billing_rate_input")
    async def handle_billing_rate_input(message: Message):
        if not await ctx.is_admin(message):
            return
        state = ctx.user_states.get(message.from_user.id, {})
        currency = str(state.get("currency") or "").upper()
        raw = str(message.text or "").strip().replace(",", ".")
        try:
            value = float(raw)
        except Exception:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: введите положительное число.")
            return
        if not isfinite(value) or value <= 0:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: курс должен быть больше 0.")
            return
        if value > 100000:
            await ctx.send_message_safe(chat_id=message.chat.id, text="⚠️ Слишком большой курс. Введите более реалистичное значение.")
            return
        admin_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        ok = await ctx.run_db(ctx.db.set_billing_exchange_rate, currency=currency, new_value=value, admin_id=admin_id)
        if not ok:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить курс.")
            return
        ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text="✅ Курс сохранён.")
        await ctx.send_message_safe(chat_id=message.chat.id, text=await ctx.run_db(_build_rates_text, ctx), parse_mode="HTML", reply_markup=_build_rates_kb())

    @dp.message(lambda m: m.from_user is not None and ctx.user_states.get(m.from_user.id, {}).get("action") == "admin_billing_usd_price_input")
    async def handle_billing_usd_input(message: Message):
        if not await ctx.is_admin(message):
            return
        state = ctx.user_states.get(message.from_user.id, {})
        raw = str(message.text or "").strip().replace(",", ".")
        try:
            value = float(raw)
        except Exception:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: введите положительное число.")
            return
        if not isfinite(value) or value <= 0:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: цена должна быть больше 0.")
            return
        if value > 100000:
            await ctx.send_message_safe(chat_id=message.chat.id, text="⚠️ Слишком большое значение. Введите более реалистичную цену.")
            return
        tariff_code = str(state.get("tariff_code") or "").lower()
        period_months = int(state.get("period_months") or 0)
        admin_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        ok = await ctx.run_db(ctx.db.set_billing_usd_price, tariff_code=tariff_code, period_months=period_months, new_price=value, admin_id=admin_id)
        if not ok:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить цену.")
            return
        ctx.logger.info(
            "Обновлена USD-цена тарифа admin_id=%s tariff_code=%s period_months=%s new_price=%s",
            admin_id, tariff_code, period_months, value,
        )
        ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text="✅ Цена сохранена.")
        await ctx.send_message_safe(chat_id=message.chat.id, text=await ctx.run_db(_build_tariff_prices_text, ctx), parse_mode="HTML", reply_markup=_build_tariff_prices_kb())


    @dp.callback_query(lambda c: c.data == "admin_fixed_prices")
    async def handle_fixed_prices_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        await ctx.edit_message_text_safe(message=callback.message, text=await ctx.run_db(_build_fixed_prices_text, ctx), parse_mode="HTML", reply_markup=_build_fixed_prices_kb())
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_fixed_prices_kind:"))
    async def handle_fixed_prices_kind(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        kind = str(callback.data).split(":", 1)[1]
        if kind not in {"stars", "crypto"}:
            await ctx.answer_callback_safe(callback, "Некорректный способ оплаты", show_alert=True)
            return
        title = "⭐ Telegram Stars — фикс-цены" if kind == "stars" else "₿ Crypto — фикс-цены"
        lines = []
        for code, label in (("basic", "💎 BASIC"), ("pro", "🚀 PRO")):
            lines.append([InlineKeyboardButton(text=label, callback_data=f"admin_fixed_prices_plan:{kind}:{code}")])
        lines.append([InlineKeyboardButton(text="⬅️ Назад к биллингу", callback_data="admin_billing_home")])
        await ctx.edit_message_text_safe(message=callback.message, text=title, reply_markup=InlineKeyboardMarkup(inline_keyboard=lines))
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_fixed_prices_plan:"))
    async def handle_fixed_prices_plan(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        parts = str(callback.data).split(":")
        if len(parts) != 3:
            await ctx.answer_callback_safe(callback, "Некорректные параметры", show_alert=True)
            return
        _, kind, code = parts
        icon = "⭐" if kind == "stars" else "₿"
        if kind not in {"stars", "crypto"} or code not in USD_PRICES:
            await ctx.answer_callback_safe(callback, "Некорректные параметры", show_alert=True)
            return
        kb = await ctx.run_db(_build_fixed_plan_kb, ctx, kind, code)
        await ctx.edit_message_text_safe(message=callback.message, text=f"{icon} {code.upper()} — {'Stars' if kind=='stars' else 'Crypto'}", reply_markup=kb)
        await ctx.answer_callback_safe_once(callback)

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_fixed_price_edit:"))
    async def handle_fixed_price_edit(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        parts = str(callback.data).split(":")
        if (len(parts) != 4 or parts[1] not in {"stars", "crypto"}
                or parts[2] not in USD_PRICES or parts[3] not in {"1", "3", "6", "12"}):
            await ctx.answer_callback_safe(callback, "Некорректные параметры", show_alert=True)
            return
        _, kind, code, period = parts
        user_id = callback.from_user.id
        ctx.user_states[user_id] = {"action": "admin_fixed_price_input", "kind": kind, "tariff_code": code, "period_months": int(period)}
        text = (
            "Введите новую цену в Telegram Stars.\n\nПример:\n900"
            if kind == "stars"
            else "Введите фиксированную цену для Crypto.\n\nПримеры:\n9\n9.5\n$9\n$9.50"
        )
        await ctx.edit_message_text_safe(
            message=callback.message, text=text,
            reply_markup=_cancel_price_kb(f"admin_fixed_prices_plan:{kind}:{code}"),
        )
        await ctx.answer_callback_safe_once(callback)

    @dp.message(lambda m: m.from_user is not None and ctx.user_states.get(m.from_user.id, {}).get("action") == "admin_fixed_price_input")
    async def handle_fixed_price_input(message: Message):
        if not await ctx.is_admin(message):
            return
        state = ctx.user_states.get(message.from_user.id, {})
        kind = str(state.get("kind") or "")
        code = str(state.get("tariff_code") or "")
        period = int(state.get("period_months") or 0)
        formatter = format_stars_price if kind == "stars" else format_crypto_price
        old_value = await ctx.run_db(formatter, code, period, repo=ctx.db)
        raw = str(message.text or "").strip().replace(",", ".").replace("$", "")
        admin_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        value = None
        if kind == "stars":
            if not raw.isascii() or not raw.isdigit() or len(raw) > 8:
                await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: введите целое положительное число.")
                return
            amount = int(raw)
            if amount <= 0 or amount > 10000000:
                await ctx.send_message_safe(chat_id=message.chat.id, text="⚠️ Некорректное значение. Введите число от 1 до 10000000.")
                return
            value = {"amount": amount}
        else:
            try:
                amount_f = float(raw)
            except Exception:
                await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Ошибка: введите число больше 0.")
                return
            if not isfinite(amount_f) or amount_f <= 0 or amount_f > 10000000:
                await ctx.send_message_safe(chat_id=message.chat.id, text="⚠️ Некорректное значение. Введите число больше 0.")
                return
            amount = f"{amount_f:g}"
            value = {"amount": amount, "display": f"${amount}"}
        try:
            ok = await ctx.run_db(ctx.db.set_billing_fixed_price, kind=kind, tariff_code=code, period_months=period, value=value, admin_id=admin_id)
        except Exception:
            ctx.logger.exception(
                "Ошибка сохранения фикс-цены kind=%s tariff_code=%s period=%s admin_id=%s",
                kind,
                code,
                period,
                admin_id,
            )
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить фикс-цену. Ошибка записана в лог.")
            return
        if not ok:
            await ctx.send_message_safe(chat_id=message.chat.id, text="❌ Не удалось сохранить фикс-цену.")
            return
        new_value = await ctx.run_db(formatter, code, period, repo=ctx.db)
        ctx.logger.info("Обновлена фикс-цена billing admin_id=%s kind=%s tariff_code=%s period_months=%s old_value=%s new_value=%s", admin_id, kind, code, period, old_value, new_value)
        ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text=
            "✅ Цена %s сохранена\n\n%s / %s месяц:\n%s → %s"
            % ("Stars" if kind == "stars" else "Crypto", code.upper(), period, old_value, new_value)
        )

    @dp.message(
        lambda m: _normalize_button_text(m.text)
        in {"⬅ Назад в меню", "🔙 Главное меню", "📋 Меню"}
    )
    async def handle_back_to_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        if message.from_user:
            ctx.reset_user_state(message.from_user.id)
        await ctx.send_message_safe(chat_id=message.chat.id, text="📋 Главное меню", reply_markup=ctx.get_main_menu())
