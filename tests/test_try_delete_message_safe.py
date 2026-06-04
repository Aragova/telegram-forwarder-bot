from __future__ import annotations

import asyncio

from aiogram.exceptions import TelegramRetryAfter

import bot
from app.ui_error_policy import UIActionResult


class _FakeRawBot:
    def __init__(self, *, retry_after: bool = False) -> None:
        self.delete_message_calls: list[dict] = []
        self.retry_after = retry_after

    async def delete_message(self, **kwargs):
        self.delete_message_calls.append(kwargs)
        if self.retry_after:
            raise TelegramRetryAfter(
                method=None,
                message="retry after",
                retry_after=6402,
            )
        return True


class _FakeUIPolicy:
    def __init__(self, result: UIActionResult) -> None:
        self.result = result
        self.delete_message_calls: list[dict] = []

    async def delete_message(self, **kwargs) -> UIActionResult:
        self.delete_message_calls.append(kwargs)
        return self.result


def test_try_delete_message_safe_uses_ui_policy(monkeypatch):
    fake_raw_bot = _FakeRawBot()
    fake_ui_policy = _FakeUIPolicy(UIActionResult(ok=True))
    monkeypatch.setattr(bot, "bot", fake_raw_bot)
    monkeypatch.setattr(bot, "ui_policy", fake_ui_policy)

    result = asyncio.run(bot.try_delete_message_safe(chat_id=123, message_id=456))

    assert result is True
    assert fake_ui_policy.delete_message_calls == [
        {"chat_id": 123, "message_id": 456}
    ]
    assert fake_raw_bot.delete_message_calls == []


def test_try_delete_message_safe_returns_false_on_chat_retry_after_active(monkeypatch):
    fake_raw_bot = _FakeRawBot()
    fake_ui_policy = _FakeUIPolicy(
        UIActionResult(
            ok=False,
            skipped=True,
            reason="chat_retry_after_active",
            result=None,
        )
    )
    monkeypatch.setattr(bot, "bot", fake_raw_bot)
    monkeypatch.setattr(bot, "ui_policy", fake_ui_policy)

    result = asyncio.run(bot.try_delete_message_safe(chat_id=123, message_id=456))

    assert result is False
    assert fake_ui_policy.delete_message_calls == [
        {"chat_id": 123, "message_id": 456}
    ]
    assert fake_raw_bot.delete_message_calls == []


def test_try_delete_message_safe_fallback_suppresses_retry_after(monkeypatch):
    fake_raw_bot = _FakeRawBot(retry_after=True)
    monkeypatch.setattr(bot, "bot", fake_raw_bot)
    monkeypatch.setattr(bot, "ui_policy", None)

    result = asyncio.run(bot.try_delete_message_safe(chat_id=123, message_id=456))

    assert result is False
    assert fake_raw_bot.delete_message_calls == [
        {"chat_id": 123, "message_id": 456}
    ]
