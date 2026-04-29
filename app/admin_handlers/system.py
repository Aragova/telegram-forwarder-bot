from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .context import AdminHandlersContext


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

    @dp.message(
        lambda m: _normalize_button_text(m.text)
        in {"⬅ Назад в меню", "🔙 Главное меню", "📋 Меню"}
    )
    async def handle_back_to_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await message.reply("📋 Главное меню", reply_markup=ctx.get_main_menu())
