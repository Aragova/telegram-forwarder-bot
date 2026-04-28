from __future__ import annotations

from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardRemove

from .context import AdminHandlersContext


def register_admin_menu_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    @dp.message(Command("menu"))
    async def cmd_menu(message: Message):
        await handle_start(message)

    @dp.message(lambda m: m.text == "📋 Меню")
    async def handle_start(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if ctx.is_admin_user(message.from_user.id if message.from_user else None):
            await message.reply("📋 Главное меню", reply_markup=ctx.get_main_menu())
            return
        await message.answer(" ", reply_markup=ReplyKeyboardRemove())
        await ctx.show_public_user_menu_message(message)

    @dp.message(lambda m: m.text in ("🔙 Главное меню", "⬅️ Назад в меню"))
    async def handle_main_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if ctx.is_admin_user(message.from_user.id if message.from_user else None):
            await message.reply("📋 Главное меню", reply_markup=ctx.get_main_menu())
            return
        await message.answer(" ", reply_markup=ReplyKeyboardRemove())
        await ctx.show_public_user_menu_message(message)

    @dp.message(lambda m: m.text == "🔄 Правила")
    async def handle_rules_menu_open(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        await message.reply("🔄 Раздел: Правила", reply_markup=ctx.get_rules_menu())
