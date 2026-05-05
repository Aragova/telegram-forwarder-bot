from __future__ import annotations

import asyncio
import logging
from typing import Any


class SavedPostAlbumCaptureBuffer:
    def __init__(self, *, delay_seconds: float = 1.2, logger_=None):
        self.delay_seconds = delay_seconds
        self.logger = logger_ or logging.getLogger("forwarder")
        self._buckets: dict[tuple[int, str], dict[str, Any]] = {}

    async def add_message(self, *, admin_id: int, message, on_album_ready) -> bool:
        media_group_id = getattr(message, "media_group_id", None)
        if not media_group_id:
            raise ValueError("У сообщения отсутствует media_group_id")
        key = (int(admin_id), str(media_group_id))
        bucket = self._buckets.get(key)
        is_new_bucket = bucket is None
        if bucket is None:
            bucket = {"messages": [], "message_ids": set(), "task": None, "on_album_ready": on_album_ready}
            self._buckets[key] = bucket
        message_id = int(getattr(message, "message_id", 0) or 0)
        if message_id and message_id in bucket["message_ids"]:
            return is_new_bucket
        if message_id:
            bucket["message_ids"].add(message_id)
        bucket["messages"].append(message)
        self.logger.info("SAVED_POST_ALBUM_BUFFER_ADD | admin_id=%s | media_group_id=%s | count=%s", admin_id, media_group_id, len(bucket["messages"]))
        if bucket["task"] is None:
            bucket["task"] = asyncio.create_task(self._flush_later(key))
        return is_new_bucket

    async def _flush_later(self, key: tuple[int, str]) -> None:
        await asyncio.sleep(self.delay_seconds)
        bucket = self._buckets.get(key)
        if not bucket:
            return
        messages = list(bucket["messages"])
        admin_id, media_group_id = key
        try:
            self.logger.info("SAVED_POST_ALBUM_BUFFER_READY | admin_id=%s | media_group_id=%s | count=%s", admin_id, media_group_id, len(messages))
            await bucket["on_album_ready"](admin_id=admin_id, messages=messages)
        except Exception as exc:
            self.logger.warning("SAVED_POST_ALBUM_BUFFER_FAILED | admin_id=%s | media_group_id=%s | error=%s", admin_id, media_group_id, exc)
        finally:
            self._buckets.pop(key, None)
