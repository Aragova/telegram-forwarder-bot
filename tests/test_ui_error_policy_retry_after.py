import asyncio
import time

from aiogram.exceptions import TelegramRetryAfter

from app.telegram_flood_locks import TelegramFloodLockStore
from app.ui_error_policy import UIErrorPolicy


class FakeBotRetryAfter:
    def __init__(self) -> None:
        self.send_message_calls = 0
        self.edit_message_text_calls = 0

    async def send_message(self, **kwargs):
        self.send_message_calls += 1
        raise TelegramRetryAfter(method=None, message="retry after", retry_after=6402)

    async def edit_message_text(self, **kwargs):
        self.edit_message_text_calls += 1
        raise TelegramRetryAfter(method=None, message="retry after", retry_after=6402)


def test_send_message_retry_after_sets_chat_lock(tmp_path):
    bot = FakeBotRetryAfter()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    first = asyncio.run(policy.send_message(chat_id=123, text="Первое сообщение"))

    assert first.ok is False
    assert first.skipped is True
    assert first.reason == "retry_after"
    assert bot.send_message_calls == 1

    second = asyncio.run(policy.send_message(chat_id=123, text="Второе сообщение"))

    assert second.ok is False
    assert second.skipped is True
    assert second.reason == "chat_retry_after_active"
    assert bot.send_message_calls == 1


def test_edit_text_retry_after_sets_chat_lock(tmp_path):
    bot = FakeBotRetryAfter()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    policy = UIErrorPolicy(bot, flood_lock_store=store)

    first = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Первый текст")
    )

    assert first.ok is False
    assert first.skipped is True
    assert first.reason == "retry_after"
    assert bot.edit_message_text_calls == 1

    second = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Второй текст")
    )

    assert second.ok is False
    assert second.skipped is True
    assert second.reason == "chat_retry_after_active"
    assert bot.edit_message_text_calls == 1


class FakeBotSuccess:
    def __init__(self) -> None:
        self.send_message_calls = 0

    async def send_message(self, **kwargs):
        self.send_message_calls += 1
        return {"ok": True, **kwargs}


def test_ui_policy_restores_persistent_chat_lock(tmp_path):
    bot = FakeBotSuccess()
    store = TelegramFloodLockStore(tmp_path / "locks.json")
    store.set_lock(
        chat_id=123,
        retry_after_seconds=6402,
        method="bot.send_message",
        now_epoch=time.time(),
    )

    policy = UIErrorPolicy(bot, flood_lock_store=store)
    result = asyncio.run(policy.send_message(chat_id=123, text="hello"))

    assert bot.send_message_calls == 0
    assert result.skipped is True
    assert result.reason == "chat_retry_after_active"
