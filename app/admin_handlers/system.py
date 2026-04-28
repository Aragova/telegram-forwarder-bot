from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import Message

from .context import AdminHandlersContext


def _normalize_button_text(text: str | None) -> str:
    # Убираем variation selector, чтобы одинаково обрабатывать emoji-style/unstyled кнопки.
    return (text or "").replace("\ufe0f", "").strip()


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

    @dp.message(
        lambda m: _normalize_button_text(m.text)
        in {"⬅ Назад в меню", "🔙 Главное меню", "📋 Меню"}
    )
    async def handle_back_to_menu(message: Message):
        if not await ctx.is_admin(message):
            return
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        await message.reply("📋 Главное меню", reply_markup=ctx.get_main_menu())
