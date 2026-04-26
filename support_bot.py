import os
import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO)

SUPPORT_BOT_TOKEN = os.getenv("SUPPORT_BOT_TOKEN")
SUPPORT_ADMIN_ID = int(os.getenv("SUPPORT_ADMIN_ID", "0"))

bot = Bot(token=SUPPORT_BOT_TOKEN)
dp = Dispatcher()

user_map = {}


@dp.message(F.from_user.id != SUPPORT_ADMIN_ID)
async def from_user(message: Message):
    forwarded = await bot.forward_message(
        chat_id=SUPPORT_ADMIN_ID,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
    )

    user_map[forwarded.message_id] = message.chat.id

    await message.answer(
        "✅ Сообщение отправлено в поддержку.\n"
        "Мы ответим вам здесь."
    )


@dp.message(F.from_user.id == SUPPORT_ADMIN_ID)
async def from_admin(message: Message):
    if not message.reply_to_message:
        await message.answer("Ответь reply на сообщение пользователя.")
        return

    user_id = user_map.get(message.reply_to_message.message_id)

    if not user_id:
        await message.answer("Не нашёл пользователя для ответа.")
        return

    await bot.send_message(
        chat_id=user_id,
        text=message.text or "Сообщение без текста"
    )

    await message.answer("✅ Ответ отправлен пользователю.")


async def main():
    if not SUPPORT_BOT_TOKEN:
        raise RuntimeError("SUPPORT_BOT_TOKEN не задан в .env")

    if not SUPPORT_ADMIN_ID:
        raise RuntimeError("SUPPORT_ADMIN_ID не задан в .env")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
