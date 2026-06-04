import asyncio
import time

from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

from app.telegram_flood_locks import TelegramFloodLockStore
from app.ui_error_policy import UIErrorPolicy


class FakeMediaBot:
    def __init__(self) -> None:
        self.send_photo_calls = 0
        self.copy_message_calls = 0
        self.forward_message_calls = 0
        self.send_document_calls = 0

    async def send_photo(self, **kwargs):
        self.send_photo_calls += 1
        return {"ok": True, **kwargs}

    async def copy_message(self, **kwargs):
        self.copy_message_calls += 1
        raise TelegramRetryAfter(method=None, message="retry after", retry_after=6402)

    async def forward_message(self, **kwargs):
        self.forward_message_calls += 1
        raise TelegramRetryAfter(method=None, message="retry after", retry_after=6402)

    async def send_document(self, **kwargs):
        self.send_document_calls += 1
        raise TelegramForbiddenError(method=None, message="forbidden")


def test_send_photo_respects_persistent_chat_lock(tmp_path):
    bot = FakeMediaBot()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    store.set_lock(
        chat_id=123,
        retry_after_seconds=6402,
        method="bot.send_photo",
        now_epoch=time.time(),
    )
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    result = asyncio.run(policy.send_photo(chat_id=123, photo="file_id"))

    assert result.skipped is True
    assert result.reason == "chat_retry_after_active"
    assert bot.send_photo_calls == 0


def test_copy_message_retry_after_sets_persistent_lock(tmp_path):
    bot = FakeMediaBot()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    first = asyncio.run(policy.copy_message(chat_id=123, from_chat_id=456, message_id=10))

    assert first.skipped is True
    assert first.reason == "retry_after"
    assert store.get_remaining_seconds(123) > 6000

    second = asyncio.run(policy.copy_message(chat_id=123, from_chat_id=456, message_id=10))

    assert bot.copy_message_calls == 1
    assert second.reason == "chat_retry_after_active"


def test_forward_message_retry_after_sets_persistent_lock(tmp_path):
    bot = FakeMediaBot()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    first = asyncio.run(policy.forward_message(chat_id=123, from_chat_id=456, message_id=10))

    assert first.skipped is True
    assert first.reason == "retry_after"
    assert store.get_remaining_seconds(123) > 6000

    second = asyncio.run(policy.forward_message(chat_id=123, from_chat_id=456, message_id=10))

    assert bot.forward_message_calls == 1
    assert second.reason == "chat_retry_after_active"


def test_send_document_forbidden_is_suppressed(tmp_path):
    bot = FakeMediaBot()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    result = asyncio.run(policy.send_document(chat_id=123, document="file_id"))

    assert result.skipped is True
    assert result.reason == "forbidden"
    assert bot.send_document_calls == 1
