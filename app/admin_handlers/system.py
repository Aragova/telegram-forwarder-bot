from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .context import AdminHandlersContext
from app.billing_catalog import USD_PRICES


def _normalize_button_text(text: str | None) -> str:
    # Убираем variation selector, чтобы одинаково обрабатывать emoji-style/unstyled кнопки.
    return (text or "").replace("\ufe0f", "").strip()


def _build_rates_text(ctx: AdminHandlersContext) -> str:
    rates = {}
    if hasattr(ctx.db, "get_billing_exchange_rates"):
        rates = ctx.db.get_billing_exchange_rates() or {}
    rub = rates.get("USD_TO_RUB", 95.0)
    eur = rates.get("USD_TO_EUR", 0.9)
    uah = rates.get("USD_TO_UAH", 40.0)
    return (
        "💱 Курсы валют\n\n"
        "Базовая валюта: USD\n\n"
        "Текущие курсы:\n"
        f"🇷🇺 RUB: {rub}\n"
        f"🇪🇺 EUR: {eur}\n"
        f"🇺🇦 UAH: {uah}\n\n"
        "Stars и Crypto не пересчитываются автоматически."
    )


def _build_rates_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🇷🇺 Изменить RUB", callback_data="admin_billing_rate_edit:RUB")],[InlineKeyboardButton(text="🇪🇺 Изменить EUR", callback_data="admin_billing_rate_edit:EUR")],[InlineKeyboardButton(text="🇺🇦 Изменить UAH", callback_data="admin_billing_rate_edit:UAH")],[InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_billing_rates")],[InlineKeyboardButton(text="⬅️ Назад в систему", callback_data="admin_billing_rates_back")]])


def _build_tariff_prices_text(ctx: AdminHandlersContext) -> str:
    saved = ctx.db.get_billing_usd_prices() if hasattr(ctx.db, "get_billing_usd_prices") else {}
    basic = saved.get("basic", {}) if isinstance(saved, dict) else {}
    pro = saved.get("pro", {}) if isinstance(saved, dict) else {}
    def _line(code: str, period: int, source: dict) -> str:
        value = source.get(period, USD_PRICES[code][period])
        value_num = float(value)
        return f"{period} месяц{'а' if period in (3, 6) else 'ев' if period == 12 else ''} — {value_num:g} USD"
    return (
        "💵 Цены тарифов\n\n"
        "Базовая валюта: USD\n\n"
        "BASIC:\n"
        f"{_line('basic', 1, basic)}\n{_line('basic', 3, basic)}\n{_line('basic', 6, basic)}\n{_line('basic', 12, basic)}\n\n"
        "PRO:\n"
        f"{_line('pro', 1, pro)}\n{_line('pro', 3, pro)}\n{_line('pro', 6, pro)}\n{_line('pro', 12, pro)}"
    )


def _build_tariff_prices_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 BASIC", callback_data="admin_billing_usd_plan:basic")],
        [InlineKeyboardButton(text="🚀 PRO", callback_data="admin_billing_usd_plan:pro")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_billing_usd")],
        [InlineKeyboardButton(text="⬅️ Назад в систему", callback_data="admin_billing_usd_back")],
    ])


def register_admin_system_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: _normalize_button_text(m.text) in {"⚙ Система", "Система"})
    async def handle_system_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("⚙️ Раздел: Система", reply_markup=ctx.get_system_menu())

    @dp.message(
        lambda m: _normalize_button_text(m.text) in {
            "▶ Запуск",
            "▶ Запустить пересылку",
            "Запуск",
            "Запустить пересылку",
        }
    )
    async def handle_global_start(message: Message):
        if not await ctx.is_admin(message):
            return

        if ctx.is_posting_active and ctx.is_posting_active():
            await message.reply("ℹ️ Пересылка уже запущена.")
            return

        try:
            await ctx.start_forwarding()
            await message.reply(
                "▶️ Пересылка запущена.\n"
                "Новые задачи будут обрабатываться.",
                reply_markup=ctx.get_main_menu(),
            )
            ctx.logger.info(
                "Пересылка запущена через системное меню admin_id=%s",
                message.from_user.id if message.from_user else None,
            )
        except Exception as exc:
            ctx.logger.exception("Ошибка запуска пересылки: %s", exc)
            await message.reply("❌ Ошибка запуска пересылки")

    @dp.message(
        lambda m: _normalize_button_text(m.text) in {
            "⏸ Стоп",
            "⏸ Остановить пересылку",
            "Стоп",
            "Остановить пересылку",
        }
    )
    async def handle_global_stop(message: Message):
        if not await ctx.is_admin(message):
            return

        if ctx.is_posting_active and not ctx.is_posting_active():
            await message.reply("ℹ️ Пересылка уже остановлена.")
            return

        try:
            await ctx.stop_forwarding()
            await message.reply(
                "⏸ Пересылка остановлена.\n"
                "Новые задачи запускаться не будут.",
                reply_markup=ctx.get_main_menu(),
            )
            ctx.logger.info(
                "Пересылка остановлена через системное меню admin_id=%s",
                message.from_user.id if message.from_user else None,
            )
        except Exception as exc:
            ctx.logger.exception("Ошибка остановки пересылки: %s", exc)
            await message.reply("❌ Ошибка остановки пересылки")


    @dp.message(lambda m: _normalize_button_text(m.text) == "💱 Курсы валют")
    async def handle_billing_rates_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        await message.reply(_build_rates_text(ctx), reply_markup=_build_rates_kb())

    @dp.message(lambda m: _normalize_button_text(m.text) == "💵 Цены тарифов")
    async def handle_billing_usd_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        await message.reply(_build_tariff_prices_text(ctx), reply_markup=_build_tariff_prices_kb())

    @dp.callback_query(lambda c: c.data == "admin_billing_usd")
    async def handle_billing_usd_callback(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        await callback.message.edit_text(_build_tariff_prices_text(ctx), reply_markup=_build_tariff_prices_kb())
        await callback.answer()

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_usd_plan:"))
    async def handle_billing_usd_plan(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        code = str(callback.data).split(":", 1)[1].lower()
        plan_icon = "💎" if code == "basic" else "🚀"
        plan_name = "BASIC" if code == "basic" else "PRO"
        saved = ctx.db.get_billing_usd_prices() if hasattr(ctx.db, "get_billing_usd_prices") else {}
        plan_prices = (saved or {}).get(code, {})
        lines = []
        for period in (1, 3, 6, 12):
            value = plan_prices.get(period, USD_PRICES[code][period])
            lines.append([InlineKeyboardButton(text=f"{period} мес — {float(value):g} USD", callback_data=f"admin_billing_usd_edit:{code}:{period}")])
        lines.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_billing_usd")])
        await callback.message.edit_text(f"{plan_icon} {plan_name} — цены", reply_markup=InlineKeyboardMarkup(inline_keyboard=lines))
        await callback.answer()

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_usd_edit:"))
    async def handle_billing_usd_edit(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        _, code, period = str(callback.data).split(":")
        user_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        ctx.user_states[user_id] = {"action": "admin_billing_usd_price_input", "tariff_code": code, "period_months": int(period)}
        await callback.message.edit_text("Введите новую цену в USD.\n\nПример:\n11\nили\n11.5")
        await callback.answer()

    @dp.callback_query(lambda c: c.data == "admin_billing_rates")
    async def handle_billing_rates_callback(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        await callback.message.edit_text(_build_rates_text(ctx), reply_markup=_build_rates_kb())
        await callback.answer()

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin_billing_rate_edit:"))
    async def handle_billing_rate_edit(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        currency = str(callback.data).split(":", 1)[1].upper()
        user_id = callback.from_user.id if callback.from_user else ctx.settings.admin_id
        ctx.user_states[user_id] = {"action": "admin_billing_rate_input", "currency": currency}
        await callback.message.edit_text(f"Введите новый курс USD→{currency} (положительное число):")
        await callback.answer()

    @dp.callback_query(lambda c: c.data == "admin_billing_rates_back")
    async def handle_billing_rates_back(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        if callback.from_user:
            ctx.reset_user_state(callback.from_user.id)
        await callback.message.answer("⚙️ Раздел: Система", reply_markup=ctx.get_system_menu())
        await callback.answer()

    @dp.callback_query(lambda c: c.data == "admin_billing_usd_back")
    async def handle_billing_usd_back(callback: CallbackQuery):
        if not await ctx.is_admin(callback):
            return
        if callback.from_user:
            ctx.reset_user_state(callback.from_user.id)
        await callback.message.answer("⚙️ Раздел: Система", reply_markup=ctx.get_system_menu())
        await callback.answer()

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
            await message.reply("❌ Ошибка: введите положительное число.")
            return
        if value <= 0:
            await message.reply("❌ Ошибка: курс должен быть больше 0.")
            return
        if value > 100000:
            await message.reply("⚠️ Слишком большой курс. Введите более реалистичное значение.")
            return
        admin_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        ok = await ctx.run_db(ctx.db.set_billing_exchange_rate, currency=currency, new_value=value, admin_id=admin_id)
        if not ok:
            await message.reply("❌ Не удалось сохранить курс.")
            return
        ctx.reset_user_state(message.from_user.id)
        await message.reply("✅ Курс сохранён.")
        await message.reply(_build_rates_text(ctx), reply_markup=_build_rates_kb())

    @dp.message(lambda m: m.from_user is not None and ctx.user_states.get(m.from_user.id, {}).get("action") == "admin_billing_usd_price_input")
    async def handle_billing_usd_input(message: Message):
        if not await ctx.is_admin(message):
            return
        state = ctx.user_states.get(message.from_user.id, {})
        raw = str(message.text or "").strip().replace(",", ".")
        try:
            value = float(raw)
        except Exception:
            await message.reply("❌ Ошибка: введите положительное число.")
            return
        if value <= 0:
            await message.reply("❌ Ошибка: цена должна быть больше 0.")
            return
        if value > 100000:
            await message.reply("⚠️ Слишком большое значение. Введите более реалистичную цену.")
            return
        tariff_code = str(state.get("tariff_code") or "").lower()
        period_months = int(state.get("period_months") or 0)
        admin_id = message.from_user.id if message.from_user else ctx.settings.admin_id
        ok = await ctx.run_db(ctx.db.set_billing_usd_price, tariff_code=tariff_code, period_months=period_months, new_price=value, admin_id=admin_id)
        if not ok:
            await message.reply("❌ Не удалось сохранить цену.")
            return
        ctx.logger.info(
            "Обновлена USD-цена тарифа admin_id=%s tariff_code=%s period_months=%s new_price=%s",
            admin_id, tariff_code, period_months, value,
        )
        ctx.reset_user_state(message.from_user.id)
        await message.reply("✅ Цена сохранена.")
        await message.reply(_build_tariff_prices_text(ctx), reply_markup=_build_tariff_prices_kb())

    @dp.message(
        lambda m: _normalize_button_text(m.text)
        in {"⬅ Назад в меню", "🔙 Главное меню", "📋 Меню"}
    )
    async def handle_back_to_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await message.reply("📋 Главное меню", reply_markup=ctx.get_main_menu())
