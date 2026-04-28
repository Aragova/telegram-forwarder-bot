from __future__ import annotations

from aiogram import Dispatcher
from aiogram.types import Message

from .context import AdminHandlersContext


def register_admin_system_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(lambda m: m.text == "⚙️ Система")
    async def handle_system_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("⚙️ Раздел: Система", reply_markup=ctx.get_system_menu())

    @dp.message(lambda m: m.text in ("▶️ Запуск", "▶️ Запустить пересылку"))
    async def handle_global_start(message: Message):
        if not await ctx.is_admin(message):
            return
        await ctx.start_forwarding()
        await message.reply("✅ Пересылка запущена", reply_markup=ctx.get_main_menu())

    @dp.message(lambda m: m.text in ("⏸ Стоп", "⏸ Остановить пересылку"))
    async def handle_global_stop(message: Message):
        if not await ctx.is_admin(message):
            return
        await ctx.stop_forwarding()
        await message.reply("⏸ Пересылка остановлена", reply_markup=ctx.get_main_menu())
