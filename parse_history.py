#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio

from app.config import settings
from app.logging_setup import setup_logging
from app.parser import parse_channel_history, parse_group_history
from app.repository import RepositoryProtocol
from app.repository_factory import create_repository
from app.telegram_client import create_telethon_client


async def main():
    parser = argparse.ArgumentParser(
        description="Парсинг истории канала или темы в PostgreSQL"
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--thread", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    settings.validate()
    logger = setup_logging(settings.log_level, "parse_history.log")

    db: RepositoryProtocol = create_repository()
    db.init()

    ok, msg = db.integrity_check()
    if not ok:
        raise RuntimeError(f"PostgreSQL недоступен: {msg}")

    client = await create_telethon_client()

    try:
        if args.thread is None:
            saved = await parse_channel_history(
                client,
                db,
                args.source,
                clean_start=args.clean,
            )
        else:
            saved = await parse_group_history(
                client,
                db,
                args.source,
                args.thread,
                clean_start=args.clean,
            )

        logger.info("Готово. Новых записей: %s", saved)

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
