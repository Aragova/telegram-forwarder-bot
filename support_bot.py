"""Standalone ViMi support bot.

Only genuine customer messages create support requests. Telegram commands
(including /start deep links) never reach the operator inbox.

Support copy is Russian for Telegram ru locale and English for other locales.
No automatic translation of customer messages or operator replies takes place.
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

# Import-safe until configuration is validated at runtime.
bot: Bot | None = None
dp = Dispatcher()

# Reply routing and locale hints are kept only in RAM as before.
user_map: dict[int, int] = {}
user_languages: dict[int, str] = {}

CUSTOMER_WELCOME_RU = (
    "👋 <b>Добро пожаловать в ViMi | Support!</b>\n\n"
    "🛟 Здесь мы помогаем с вопросами по <b>ViMi Pay</b>, "
    "<b>ViMi Autoposting</b> и <b>ViMi Social</b>.\n\n"
    "✍️ <b>Как написать нам?</b>\n"
    "Просто отправьте ваш вопрос следующим сообщением. "
    "Если понадобится, прикрепите скриншот или файл.\n\n"
    "💬 <i>Ответ поддержки придёт сюда, в этот же чат.</i>"
)
CUSTOMER_WELCOME_EN = (
    "👋 <b>Welcome to ViMi | Support!</b>\n\n"
    "🛟 We're here to help with <b>ViMi Pay</b>, "
    "<b>ViMi Autoposting</b> and <b>ViMi Social</b>.\n\n"
    "✍️ <b>How can we help?</b>\n"
    "Send your question as your next message. "
    "You can attach a screenshot or file if needed.\n\n"
    "💬 <i>Our support team will reply right here in this chat.</i>"
)
ADMIN_WELCOME_RU = (
    "🛟 <b>ViMi | Support · Панель ответов</b>\n\n"
    "📩 Когда пользователь обратится в поддержку, его сообщение появится здесь.\n"
    "↩️ <b>Чтобы ответить:</b> используйте функцию «Ответить» "
    "на пересланном сообщении.\n\n"
    "🔐 Команды /start и /help не создают новых обращений."
)
ADMIN_WELCOME_EN = (
    "🛟 <b>ViMi | Support · Operator inbox</b>\n\n"
    "📩 Customer support requests will arrive here.\n"
    "↩️ <b>To reply:</b> use Telegram's Reply function "
    "on the forwarded customer message.\n\n"
    "🔐 /start and /help never create support requests."
)


def _language(message: Message) -> str:
    user = getattr(message, "from_user", None)
    code = str(getattr(user, "language_code", None) or "en").lower()
    return "ru" if code == "ru" or code.startswith("ru-") else "en"


def _text(message: Message, ru: str, en: str) -> str:
    return ru if _language(message) == "ru" else en


@dp.message(CommandStart())
async def start_command(message: Message) -> None:
    """Opening the bot must never create a support ticket."""
    is_admin = message.from_user is not None and message.from_user.id == SUPPORT_ADMIN_ID
    await message.answer(
        (_text(message, ADMIN_WELCOME_RU, ADMIN_WELCOME_EN)
         if is_admin else _text(message, CUSTOMER_WELCOME_RU, CUSTOMER_WELCOME_EN)),
        parse_mode="HTML",
    )


@dp.message(Command("help", "menu"))
async def help_command(message: Message) -> None:
    """Show bilingual guidance without forwarding a request."""
    await start_command(message)


@dp.message(F.text.startswith("/"))
@dp.message(F.caption.startswith("/"))
async def unknown_command(message: Message) -> None:
    """Do not create support tickets for any slash commands."""
    await message.answer(
        _text(
            message,
            "ℹ️ <b>Это команда, а не обращение.</b>\n"
            "Чтобы написать в поддержку, отправьте обычное сообщение "
            "без символа / в начале.\nПодсказка: /help",
            "ℹ️ <b>This is a command, not a support request.</b>\n"
            "To contact support, send a regular message "
            "without / at the beginning.\nTip: /help",
        ),
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
    user_languages[message.chat.id] = _language(message)

    await message.answer(
        _text(
            message,
            "✅ <b>Обращение отправлено!</b>\n"
            "Спасибо за сообщение. 💬 Мы ответим здесь, в этом чате.",
            "✅ <b>Your request has been sent!</b>\n"
            "Thanks for reaching out. 💬 We'll reply right here in this chat.",
        ),
        parse_mode="HTML",
    )


@dp.message(F.from_user.id == SUPPORT_ADMIN_ID)
async def from_admin(message: Message) -> None:
    if not message.reply_to_message:
        await message.answer(
            _text(
                message,
                "↩️ <b>Как ответить пользователю</b>\n"
                "Нажмите «Ответить» на пересланном обращении и напишите сообщение.",
                "↩️ <b>How to reply to a customer</b>\n"
                "Use Reply on the forwarded request and type your response.",
            ),
            parse_mode="HTML",
        )
        return

    user_id = user_map.get(message.reply_to_message.message_id)
    if not user_id:
        await message.answer(
            _text(
                message,
                "⚠️ Не удалось найти адресата. "
                "После перезапуска бота связь со старыми обращениями может "
                "быть недоступна.",
                "⚠️ The customer could not be found. "
                "After restarting the bot, replies to older requests "
                "may be unavailable.",
            ),
        )
        return

    if bot is None:
        raise RuntimeError("Support bot is not initialized")

    await bot.send_message(
        chat_id=user_id,
        text=message.text or (
            "Сообщение без текста"
            if user_languages.get(user_id, "en") == "ru"
            else "Message without text"
        ),
    )
    await message.answer(
        _text(
            message,
            "✅ Ответ доставлен пользователю.",
            "✅ Reply delivered to the customer.",
        ),
    )


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
