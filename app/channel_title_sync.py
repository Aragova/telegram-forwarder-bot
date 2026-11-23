"""Keep saved channel names current without touching rule delivery state.

Runs once immediately after the UI runtime starts, then once every six hours.
All saved chat/group titles are synchronized, including rows with forum topics.
The topics' own names are not modified.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

TITLE_SYNC_INTERVAL_SECONDS = 6 * 60 * 60
TITLE_SYNC_LOOKUP_TIMEOUT_SECONDS = 12.0
TITLE_SYNC_CONCURRENCY = 3


@dataclass(slots=True)
class TitleSyncResult:
    channels: int = 0
    checked: int = 0
    updated: int = 0
    inaccessible: int = 0


def unique_channel_ids(rows: list[Any]) -> list[str]:
    """Deduplicate by chat ID, including groups saved only as forum topics."""
    ids: set[str] = set()
    for row in rows:
        channel_id = str(row["channel_id"] or "").strip()
        if channel_id:
            ids.add(channel_id)
    return sorted(ids)


def _rate_limited(exc: Exception) -> bool:
    return bool(getattr(exc, "retry_after", None) or getattr(exc, "seconds", None))


async def _resolve_title(
    channel_id: str,
    *,
    bot: Any,
    telethon_client: Any,
    bot_flooded: asyncio.Event,
    telethon_flooded: asyncio.Event,
    logger: logging.Logger,
) -> str | None:
    """Bot API first; Telethon fallback for private sources inaccessible to bot."""
    if bot is not None and not bot_flooded.is_set():
        try:
            chat = await asyncio.wait_for(
                bot.get_chat(channel_id), timeout=TITLE_SYNC_LOOKUP_TIMEOUT_SECONDS,
            )
            title = getattr(chat, "title", None)
            if isinstance(title, str) and title.strip():
                return title.strip()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _rate_limited(exc):
                bot_flooded.set()
                logger.warning("CHANNEL_TITLE_SYNC | Telegram Bot API ограничил запросы")

    if telethon_client is not None and not telethon_flooded.is_set():
        # Do not import or initialize additional Telegram sessions.
        try:
            entity_id: int | str = (
                int(channel_id) if channel_id.lstrip("-").isdigit() else channel_id
            )
            chat = await asyncio.wait_for(
                telethon_client.get_entity(entity_id),
                timeout=TITLE_SYNC_LOOKUP_TIMEOUT_SECONDS,
            )
            title = getattr(chat, "title", None)
            if isinstance(title, str) and title.strip():
                return title.strip()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _rate_limited(exc):
                telethon_flooded.set()
                logger.warning("CHANNEL_TITLE_SYNC | Telegram-сессия ограничила запросы")
    return None


async def refresh_channel_titles_once(
    *,
    db: Any,
    bot: Any,
    telethon_client: Any,
    run_db: Callable[..., Awaitable[Any]],
    logger: logging.Logger,
) -> TitleSyncResult:
    """Refresh distinct saved channels only; failures never affect publication.

    The database update uses IS DISTINCT FROM and commits only changed titles.
    Existing rule IDs, forum topic IDs, queues and delivery markers stay intact.
    """
    rows = await run_db(db.get_channels)
    channel_ids = unique_channel_ids(rows)
    result = TitleSyncResult(channels=len(channel_ids))
    bot_flooded = asyncio.Event()
    telethon_flooded = asyncio.Event()
    semaphore = asyncio.Semaphore(TITLE_SYNC_CONCURRENCY)

    async def refresh_one(channel_id: str) -> None:
        async with semaphore:
            title = await _resolve_title(
                channel_id, bot=bot, telethon_client=telethon_client,
                bot_flooded=bot_flooded, telethon_flooded=telethon_flooded,
                logger=logger,
            )
            if title is None:
                result.inaccessible += 1
                return
            result.checked += 1
            try:
                result.updated += int(
                    await run_db(db.refresh_channel_title, channel_id, title),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "CHANNEL_TITLE_SYNC | не удалось обновить название channel_id=%s",
                    channel_id,
                )

    await asyncio.gather(*(refresh_one(channel_id) for channel_id in channel_ids))
    logger.info(
        "CHANNEL_TITLE_SYNC | всего=%d проверено=%d изменено=%d недоступно=%d",
        result.channels, result.checked, result.updated, result.inaccessible,
    )
    return result


async def channel_title_sync_loop(
    *,
    db: Any,
    bot: Any,
    telethon_client: Any,
    run_db: Callable[..., Awaitable[Any]],
    logger: logging.Logger,
    interval_seconds: float = TITLE_SYNC_INTERVAL_SECONDS,
) -> None:
    """Run the first synchronization immediately, then every six hours."""
    while True:
        try:
            await refresh_channel_titles_once(
                db=db, bot=bot, telethon_client=telethon_client,
                run_db=run_db, logger=logger,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("CHANNEL_TITLE_SYNC | ошибка синхронизации, повтор позже")
        await asyncio.sleep(interval_seconds)
