from __future__ import annotations

from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardRemove

from .context import AdminHandlersContext
from app.rule_folders_ui import root_view
from app.rule_folder_handlers import clear_new_rule_folder, clear_folder_input


def register_admin_menu_handlers(dp: Dispatcher, ctx: AdminHandlersContext) -> None:
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
        clear_folder_input(message.from_user.id if message.from_user else None)
        if not await ctx.is_admin(message):
            return
        user_id = message.from_user.id if message.from_user else 0
        clear_new_rule_folder(user_id)
        clear_folder_input(user_id)
        tenant_id = await ctx.run_db(ctx.ensure_user_tenant, user_id)
        folders = await ctx.run_db(ctx.db.list_rule_folders, tenant_id)
        rules = await ctx.run_db(ctx.db.get_all_rules)
        text, markup = root_view(total_rules=len(rules), folders_count=len(folders), admin=True)
        # Reply keyboards cannot be dismissed and replaced with inline markup in
        # the same Telegram message. Remove the old keyboard via a temporary
        # service message, then keep only the navigable rich screen.
        cleanup = await ctx.send_message_safe(
            chat_id=message.chat.id, text="🔄 Открываю правила",
            reply_markup=ReplyKeyboardRemove(),
        )
        await ctx.send_message_safe(
            chat_id=message.chat.id, text=text,
            reply_markup=markup, parse_mode="HTML",
        )
        if cleanup:
            try:
                await ctx.bot.delete_message(message.chat.id, cleanup.message_id)
            except Exception:
                pass
