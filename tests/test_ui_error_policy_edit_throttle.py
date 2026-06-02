import asyncio
from time import monotonic

from app.ui_error_policy import UIErrorPolicy


class FakeBotEditSuccess:
    def __init__(self) -> None:
        self.edit_message_text_calls = 0

    async def edit_message_text(self, **kwargs):
        self.edit_message_text_calls += 1
        return {"ok": True, **kwargs}


def test_edit_same_signature_skipped():
    bot = FakeBotEditSuccess()
    policy = UIErrorPolicy(bot)
    reply_markup = {"inline_keyboard": [[{"text": "Назад", "callback_data": "back"}]]}

    first = asyncio.run(
        policy.edit_text(
            chat_id=123,
            message_id=456,
            text="Одинаковый текст",
            reply_markup=reply_markup,
        )
    )
    second = asyncio.run(
        policy.edit_text(
            chat_id=123,
            message_id=456,
            text="Одинаковый текст",
            reply_markup=reply_markup,
        )
    )

    assert first.ok is True
    assert first.skipped is False
    assert second.ok is False
    assert second.skipped is True
    assert second.reason == "same_message_signature"
    assert bot.edit_message_text_calls == 1


def test_edit_message_throttled():
    bot = FakeBotEditSuccess()
    policy = UIErrorPolicy(bot)

    first = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Первый текст")
    )
    second = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Второй текст")
    )

    assert first.ok is True
    assert first.skipped is False
    assert second.ok is False
    assert second.skipped is True
    assert second.reason == "message_edit_throttled"
    assert bot.edit_message_text_calls == 1


def test_edit_message_throttle_allows_edit_after_window():
    bot = FakeBotEditSuccess()
    policy = UIErrorPolicy(bot)

    first = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Первый текст")
    )
    second = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Второй текст")
    )

    policy._last_edit_at[(123, 456)] = (
        monotonic() - policy.MESSAGE_EDIT_THROTTLE_SECONDS - 0.001
    )
    third = asyncio.run(
        policy.edit_text(chat_id=123, message_id=456, text="Третий текст")
    )

    assert first.ok is True
    assert first.skipped is False
    assert second.ok is False
    assert second.skipped is True
    assert second.reason == "message_edit_throttled"
    assert third.ok is True
    assert third.skipped is False
    assert bot.edit_message_text_calls == 2
