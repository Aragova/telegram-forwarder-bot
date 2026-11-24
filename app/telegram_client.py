import logging
from dataclasses import dataclass
from telethon import TelegramClient
from .config import settings

logger = logging.getLogger("forwarder")


async def _close_failed_client(client: TelegramClient) -> None:
    try:
        await client.disconnect()
    except Exception:
        logger.exception("TELETHON_STARTUP_CLOSE_FAILED")


# ЖЁСТКО ЗАКРЕПЛЁННЫЕ НАБОРЫ ДЛЯ PREMIUM-СЕССИЙ
# ключ = имя session БЕЗ .session
# LEGACY/DEV MODE: глобальные reaction sessions используются только для текущего
# single-tenant/dev режима. В SaaS runtime нужно использовать tenant-scoped
# reaction_accounts.
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
    try:
        await client.connect()
    except BaseException:
        await _close_failed_client(client)
        raise
    return client


async def create_reaction_clients() -> list[ReactionClientInfo]:
    reaction_clients: list[ReactionClientInfo] = []

    opened_clients: list[TelegramClient] = []
    try:
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
            opened_clients.append(client)
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
    except BaseException:
        for opened in reversed(opened_clients):
            await _close_failed_client(opened)
        raise

    return reaction_clients
