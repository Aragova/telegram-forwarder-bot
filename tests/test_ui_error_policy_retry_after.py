import asyncio

from aiogram.exceptions import TelegramRetryAfter

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


def test_send_message_retry_after_sets_chat_lock():
    bot = FakeBotRetryAfter()
    policy = UIErrorPolicy(bot)

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


def test_edit_text_retry_after_sets_chat_lock():
    bot = FakeBotRetryAfter()
    policy = UIErrorPolicy(bot)

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
