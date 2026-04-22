from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from aiogram import Bot
from aiogram.methods import CopyMessages
from telethon import TelegramClient

from app.config import settings
from app.repository_factory import create_repository
from app.telegram_client import create_telethon_client


RULE_ID = 9
LOGICAL_POSITION = 3          # <-- ставь нужную логическую позицию альбома
CHECK_TARGET_RECENT = True    # проверить последние сообщения в целевом канале
TARGET_RECENT_LIMIT = 10      # сколько последних сообщений смотреть


def print_header(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def row_to_dict(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        return row


async def copy_album_via_bot(bot: Bot, source_channel, target_id, message_ids, target_thread_id):
    try:
        sent_messages = await bot(
            CopyMessages(
                chat_id=target_id,
                from_chat_id=source_channel,
                message_ids=message_ids,
                message_thread_id=target_thread_id,
            )
        )

        if sent_messages and len(sent_messages) > 0:
            first_message_id = getattr(sent_messages[0], "message_id", None)
            return {
                "ok": True,
                "sent_count": len(sent_messages),
                "first_message_id": first_message_id,
                "raw": sent_messages,
                "error": None,
            }

        return {
            "ok": False,
            "sent_count": 0,
            "first_message_id": None,
            "raw": sent_messages,
            "error": "CopyMessages вернул пустой результат",
        }

    except Exception as exc:
        return {
            "ok": False,
            "sent_count": 0,
            "first_message_id": None,
            "raw": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


async def fetch_message(telethon_client: TelegramClient, source_channel, message_id: int):
    try:
        entity = int(source_channel) if str(source_channel).lstrip("-").isdigit() else source_channel
        msg = await telethon_client.get_messages(entity, ids=message_id)
        return msg
    except Exception as exc:
        print(f"[FETCH ERROR] {source_channel}/{message_id}: {type(exc).__name__}: {exc}")
        return None


async def get_recent_target_messages(telethon_client: TelegramClient, target_id, limit: int = 10):
    try:
        entity = int(target_id) if str(target_id).lstrip("-").isdigit() else target_id
        msgs = await telethon_client.get_messages(entity, limit=limit)
        if not msgs:
            return []
        if not isinstance(msgs, list):
            msgs = [msgs]
        return msgs
    except Exception as exc:
        print(f"[TARGET READ ERROR] {target_id}: {type(exc).__name__}: {exc}")
        return []


async def main():
    print_header("СТАРТ ДИАГНОСТИКИ ПОЛНОЙ ВЕТКИ _deliver_album()")

    print(f"RULE_ID = {RULE_ID}")
    print(f"LOGICAL_POSITION = {LOGICAL_POSITION}")
    print(f"APP_DB_BACKEND = {os.getenv('APP_DB_BACKEND')}")
    print(f"DATA_READ_BACKEND = {os.getenv('DATA_READ_BACKEND')}")
    print(f"BOT_API_BASE = {os.getenv('BOT_API_BASE')}")
    print(f"DB_PATH = {settings.db_path}")

    db = create_repository()

    rule = db.get_rule(RULE_ID)
    if not rule:
        print(f"[FATAL] Правило #{RULE_ID} не найдено")
        return

    print_header("ПРАВИЛО")
    rule_dict = {
        "id": rule.id,
        "source_id": rule.source_id,
        "source_thread_id": rule.source_thread_id,
        "target_id": rule.target_id,
        "target_thread_id": rule.target_thread_id,
        "interval": rule.interval,
        "schedule_mode": rule.schedule_mode,
        "fixed_times_json": rule.fixed_times_json,
        "fixed_times": rule.fixed_times() if hasattr(rule, "fixed_times") else [],
        "is_active": rule.is_active,
        "created_date": rule.created_date,
        "next_run_at": rule.next_run_at,
        "last_sent_at": rule.last_sent_at,
        "source_title": rule.source_title,
        "target_title": rule.target_title,
        "mode": rule.mode,
    }
    print(json.dumps(rule_dict, ensure_ascii=False, indent=2))

    items = db.get_rule_queue_logical_items(RULE_ID)

    print_header("ОЧЕРЕДЬ ПРАВИЛА")
    print(f"logical_items total = {len(items)}")

    if not items:
        print("[FATAL] У правила пустая логическая очередь")
        return

    if LOGICAL_POSITION < 1 or LOGICAL_POSITION > len(items):
        print(f"[FATAL] Позиция {LOGICAL_POSITION} вне диапазона 1..{len(items)}")
        return

    item = items[LOGICAL_POSITION - 1]

    print_header("ВЫБРАННЫЙ LOGICAL ITEM")
    print(json.dumps(item, ensure_ascii=False, indent=2))

    if item.get("kind") != "album":
        print(f"[WARN] Выбранный item kind={item.get('kind')}, но скрипт заточен под album-ветку")
        return

    source_channel = str(item["source_channel"])
    target_id = str(rule.target_id)
    target_thread_id = rule.target_thread_id
    message_ids = [int(x) for x in item["message_ids"]]

    print_header("ДАННЫЕ ДЛЯ ОТПРАВКИ")
    print(f"source_channel = {source_channel}")
    print(f"source_thread_id = {item.get('source_thread_id')}")
    print(f"target_id = {target_id}")
    print(f"target_thread_id = {target_thread_id}")
    print(f"media_group_id = {item.get('media_group_id')}")
    print(f"message_ids = {message_ids}")
    print(f"count = {len(message_ids)}")

    bot = Bot(
        token=settings.bot_token,
        base_url=f"{settings.bot_api_base}/bot",
    )
    telethon_client = await create_telethon_client()

    try:
        print_header("ШАГ 1. ПОВТОРЯЕМ _copy_album_via_bot()")
        copy_result = await copy_album_via_bot(
            bot=bot,
            source_channel=source_channel,
            target_id=target_id,
            message_ids=message_ids,
            target_thread_id=target_thread_id,
        )

        print(json.dumps(
            {
                "ok": copy_result["ok"],
                "sent_count": copy_result["sent_count"],
                "first_message_id": copy_result["first_message_id"],
                "error": copy_result["error"],
            },
            ensure_ascii=False,
            indent=2,
        ))

        if copy_result["ok"]:
            print("\n[RESULT] Ветка _deliver_album() в текущем sender.py на этом месте завершилась бы УСПЕХОМ.")
        else:
            print("\n[RESULT] CopyMessages не сработал. Идём дальше точно как в текущем sender.py.")

        print_header("ШАГ 2. SELF-LOOP ПРОВЕРКА")
        is_self_loop = (
            str(rule.source_id) == str(rule.target_id)
            and rule.source_thread_id == rule.target_thread_id
        )
        print(f"is_self_loop = {is_self_loop}")

        if not copy_result["ok"] and is_self_loop:
            print("[RESULT] Ветка sender.py здесь бы завершилась SKIP без faulty.")
            return

        print_header("ШАГ 3. ПОВТОРЯЕМ _fetch_message() ДЛЯ ВСЕГО АЛЬБОМА")
        fetched = []
        for mid in message_ids:
            msg = await fetch_message(telethon_client, source_channel, mid)
            if not msg:
                print(f"[FETCH] message_id={mid}: НЕ ПОЛУЧЕН")
                break

            media = getattr(msg, "media", None)
            text_preview = (msg.text or msg.message or "").strip().replace("\n", " ")[:120]

            fetched.append(msg)
            print(json.dumps(
                {
                    "message_id": mid,
                    "ok": True,
                    "has_media": bool(media),
                    "grouped_id": getattr(msg, "grouped_id", None),
                    "text_preview": text_preview,
                },
                ensure_ascii=False,
                indent=2,
            ))

        fetched_ok = len(fetched) == len(message_ids)
        print(f"\nfetched_ok = {fetched_ok} ({len(fetched)}/{len(message_ids)})")

        print_header("ШАГ 4. ЧТО СДЕЛАЕТ ИМЕННО ТЕКУЩИЙ sender.py")
        if copy_result["ok"]:
            print("Текущий sender.py завершится УСПЕХОМ на этапе _copy_album_via_bot().")
        else:
            if not fetched_ok:
                print("Текущий sender.py завершится ОШИБКОЙ:")
                print("❌ Не удалось получить весь альбом через MTProto")
            else:
                print("Текущий sender.py завершится ОШИБКОЙ:")
                print("❌ Не удалось скопировать альбом через Bot API")
                print("")
                print("Причина: в текущей версии _deliver_album() после успешного fetch всех сообщений")
                print("нет следующего рабочего fallback-метода отправки альбома.")
                print("То есть код реально дойдёт до faulty-ветки.")

        if CHECK_TARGET_RECENT:
            print_header("ШАГ 5. ПРОВЕРКА ПОСЛЕДНИХ СООБЩЕНИЙ В ЦЕЛЕВОМ КАНАЛЕ")
            recent = await get_recent_target_messages(
                telethon_client=telethon_client,
                target_id=target_id,
                limit=TARGET_RECENT_LIMIT,
            )

            if not recent:
                print("[INFO] Не удалось прочитать последние сообщения цели или цель пуста.")
            else:
                for idx, msg in enumerate(recent, start=1):
                    text_preview = (msg.text or msg.message or "").strip().replace("\n", " ")[:120]
                    print(json.dumps(
                        {
                            "n": idx,
                            "message_id": getattr(msg, "id", None),
                            "grouped_id": getattr(msg, "grouped_id", None),
                            "media": bool(getattr(msg, "media", None)),
                            "text_preview": text_preview,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ))

                if copy_result["ok"] and copy_result["first_message_id"]:
                    print("")
                    print("[CHECK] Если среди последних сообщений есть message_id >= returned first_message_id,")
                    print("значит CopyMessages реально отправил альбом в цель.")

    finally:
        try:
            await bot.session.close()
        except Exception:
            pass
        try:
            await telethon_client.disconnect()
        except Exception:
            pass

    print_header("ДИАГНОСТИКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    asyncio.run(main())
