from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

api_id = 31490108
api_hash = "75c1e179e8604b6ab6604d95b0463d5a"

session_name = "reactor_2"
phone = "+380936317798"

client = TelegramClient(session_name, api_id, api_hash)

async def main():
    await client.connect()

    if await client.is_user_authorized():
        print("Сессия уже существует и авторизована.")
        return

    sent = await client.send_code_request(phone)
    code = input("Введи код из Telegram: ").strip()

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        password = input("Введи пароль 2FA: ").strip()
        await client.sign_in(password=password)

    me = await client.get_me()

    print("\nУСПЕШНО")
    print(f"Создан файл: {session_name}.session")
    print(f"Аккаунт: id={me.id}, username={me.username}, phone={me.phone}")

with client:
    client.loop.run_until_complete(main())
