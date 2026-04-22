from dataclasses import dataclass
from telethon import TelegramClient
from .config import settings


# ЖЁСТКО ЗАКРЕПЛЁННЫЕ НАБОРЫ ДЛЯ PREMIUM-СЕССИЙ
# ключ = имя session БЕЗ .session
PREMIUM_REACTION_SETS: dict[str, list[str]] = {
    "reactor_premium_1": ["🔥", "❤️", "🥰"],
    "reactor_premium_2": ["😍", "🔥", "😘"],
}


@dataclass(slots=True)
class ReactionClientInfo:
    session_name: str
    client: TelegramClient
    is_premium: bool
    fixed_reactions: list[str]


async def create_telethon_client() -> TelegramClient:
    client = TelegramClient(
        settings.session_name,
        settings.api_id,
        settings.api_hash,
        connection_retries=8,
        retry_delay=2,
        request_retries=8,
        auto_reconnect=True,
    )
    await client.connect()
    return client


async def create_reaction_clients() -> list[ReactionClientInfo]:
    reaction_clients: list[ReactionClientInfo] = []

    for session_name in settings.reaction_sessions:
        client = TelegramClient(
            session_name,
            settings.api_id,
            settings.api_hash,
            connection_retries=8,
            retry_delay=2,
            request_retries=8,
            auto_reconnect=True,
        )
        await client.connect()

        me = await client.get_me()
        is_premium = bool(getattr(me, "premium", False))
        fixed_reactions = PREMIUM_REACTION_SETS.get(session_name, [])

        reaction_clients.append(
            ReactionClientInfo(
                session_name=session_name,
                client=client,
                is_premium=is_premium,
                fixed_reactions=fixed_reactions,
            )
        )

    return reaction_clients
