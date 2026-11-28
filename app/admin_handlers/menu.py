from __future__ import annotations

from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from .context import AdminHandlersContext
from app.rule_folder_handlers import clear_folder_input


def register_admin_menu_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
    # Historical Telegram messages can survive a deployment. Consume retired
    # callback IDs without touching deliveries, jobs, or any repository method.
    @dp.callback_query(lambda c: (c.data or "") == "reset_back" or (c.data or "").startswith("reset_source:"))
    async def handle_retired_queue_callback(callback: CallbackQuery):
        if not await ctx.is_admin_callback(callback):
            return
        ctx.reset_user_state(callback.from_user.id)
        clear_folder_input(callback.from_user.id)
        await ctx.answer_callback_safe(callback, "Раздел очереди удалён. Используйте меню правил.", show_alert=True)
        if callback.message is not None and callback.message.chat.type == "private":
            await ctx.send_message_safe(chat_id=callback.message.chat.id, text="📋 Главное меню",
                                        reply_markup=ctx.get_main_menu())

    @dp.message(Command("menu"))
    async def cmd_menu(message: Message):
        await handle_start(message)

    @dp.message(lambda m: m.text == "📋 Меню")
    async def handle_start(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        clear_folder_input(message.from_user.id if message.from_user else None)
        if ctx.is_admin_user(message.from_user.id if message.from_user else None):
            await ctx.send_message_safe(chat_id=message.chat.id, text="📋 Главное меню", reply_markup=ctx.get_main_menu())
            return
        await ctx.send_message_safe(chat_id=message.chat.id, text="Меню открыто.", reply_markup=ReplyKeyboardRemove())
        await ctx.show_public_user_menu_message(message)

    @dp.message(lambda m: m.text in ("🔙 Главное меню", "⬅️ Назад в меню"))
    async def handle_main_menu(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        clear_folder_input(message.from_user.id if message.from_user else None)
        if ctx.is_admin_user(message.from_user.id if message.from_user else None):
            await ctx.send_message_safe(chat_id=message.chat.id, text="📋 Главное меню", reply_markup=ctx.get_main_menu())
            return
        await ctx.send_message_safe(chat_id=message.chat.id, text="Меню открыто.", reply_markup=ReplyKeyboardRemove())
        await ctx.show_public_user_menu_message(message)

    @dp.message(lambda m: m.text == "🔄 Правила")
    async def handle_rules_menu_open(message: Message):
        ctx.reset_user_state(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        user_id = message.from_user.id if message.from_user else 0
        if ctx.show_admin_rule_manager is None:
            await ctx.send_message_safe(
                chat_id=message.chat.id,
                text="⚠️ Не удалось открыть менеджер правил",
                reply_markup=ctx.get_main_menu(),
            )
            return
        await ctx.show_admin_rule_manager(message, user_id)
