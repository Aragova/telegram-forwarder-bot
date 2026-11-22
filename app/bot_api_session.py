"""Build an actual aiogram HTTP session for the configured local Bot API server.

Passing base_url= to aiogram.Bot does not change its transport endpoint.
"""

from urllib.parse import urlsplit

from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer


def create_bot_api_session(base_url: str) -> AiohttpSession:
    base = str(base_url or "").strip().rstrip("/")
    parsed = urlsplit(base)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("BOT_API_BASE must be a valid HTTP(S) URL")

    return AiohttpSession(
        api=TelegramAPIServer.from_base(base, is_local=True),
    )
