"""Standalone ViMi support bot.

Only actual customer messages create support requests. Telegram slash commands
(including /start from bot links and deep links) never reach the operator inbox.
"""

import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO)

SUPPORT_BOT_TOKEN = os.getenv("SUPPORT_BOT_TOKEN")
SUPPORT_ADMIN_ID = int(os.getenv("SUPPORT_ADMIN_ID", "0"))

# Do not instantiate Bot with an unset token at module import: this lets the
# same module be imported safely for unit tests and configuration checks.
bot: Bot | None = None
dp = Dispatcher()

# Existing operator reply routing. Stored in memory, as before this change.
user_map: dict[int, int] = {}

CUSTOMER_WELCOME = (
    "👋 <b>Добро пожаловать в ViMi | Support!</b>\n\n"
    "🛟 Здесь мы помогаем с вопросами по <b>ViMi Pay</b>, "
    "<b>ViMi Autoposting</b> и <b>ViMi Social</b>.\n\n"
    "✍️ <b>Как написать нам?</b>\n"
    "Просто отправьте ваш вопрос следующим сообщением. "
    "Если понадобится, прикрепите скриншот или файл.\n\n"
    "💬 <i>Ответ поддержки придёт сюда, в этот же чат.</i>"
)

ADMIN_WELCOME = (
    "🛟 <b>ViMi | Support · Панель ответов</b>\n\n"
    "📩 Когда пользователь обратится в поддержку, его сообщение появится здесь.\n"
    "↩️ <b>Чтобы ответить:</b> используйте функцию «Ответить» "
    "на пересланном сообщении.\n\n"
    "🔐 Команды /start и /help не создают новых обращений."
)


@dp.message(CommandStart())
async def start_command(message: Message) -> None:
    """Opening the bot is not a support ticket."""
    is_admin = message.from_user is not None and message.from_user.id == SUPPORT_ADMIN_ID
    await message.answer(
        ADMIN_WELCOME if is_admin else CUSTOMER_WELCOME,
        parse_mode="HTML",
    )


@dp.message(Command("help", "menu"))
async def help_command(message: Message) -> None:
    """Show the same guidance without forwarding a request."""
    await start_command(message)


@dp.message(F.text.startswith("/"))
@dp.message(F.caption.startswith("/"))
async def unknown_command(message: Message) -> None:
    # Catch remaining slash commands before the generic forwarding handler.
    await message.answer(
        "ℹ️ <b>Это команда, а не обращение.</b>\n"
        "Чтобы написать в поддержку, отправьте обычное сообщение "
        "без символа / в начале.\n"
        "Подсказка: /help",
        parse_mode="HTML",
    )


@dp.message(F.from_user.id != SUPPORT_ADMIN_ID)
async def from_user(message: Message) -> None:
    """Forward genuine customer requests only."""
    if bot is None:
        raise RuntimeError("Support bot is not initialized")

    forwarded = await bot.forward_message(
        chat_id=SUPPORT_ADMIN_ID,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
    )
    user_map[forwarded.message_id] = message.chat.id

    await message.answer(
        "✅ <b>Обращение отправлено!</b>\n"
        "Спасибо за сообщение. 💬 Мы ответим здесь, в этом чате.",
        parse_mode="HTML",
    )


@dp.message(F.from_user.id == SUPPORT_ADMIN_ID)
async def from_admin(message: Message) -> None:
    if not message.reply_to_message:
        await message.answer(
            "↩️ <b>Как ответить пользователю</b>\n"
            "Нажмите «Ответить» на пересланном обращении и напишите сообщение.",
            parse_mode="HTML",
        )
        return

    user_id = user_map.get(message.reply_to_message.message_id)
    if not user_id:
        await message.answer(
            "⚠️ Не удалось найти адресата. "
            "После перезапуска бота связь со старыми обращениями может "
            "быть недоступна.",
        )
        return

    if bot is None:
        raise RuntimeError("Support bot is not initialized")

    await bot.send_message(
        chat_id=user_id,
        text=message.text or "Сообщение без текста",
    )
    await message.answer("✅ Ответ доставлен пользователю.")


async def main() -> None:
    global bot
    if not SUPPORT_BOT_TOKEN:
        raise RuntimeError("SUPPORT_BOT_TOKEN не задан в .env")
    if not SUPPORT_ADMIN_ID:
        raise RuntimeError("SUPPORT_ADMIN_ID не задан в .env")

    bot = Bot(token=SUPPORT_BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
