import asyncio
from unittest.mock import AsyncMock

from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from app.telegram_flood_locks import TelegramFloodLockStore
from app.ui_error_policy import UIErrorPolicy
from app.ui_navigation import edit_result_status


def make_policy(tmp_path):
    bot = AsyncMock()
    policy = UIErrorPolicy(bot, TelegramFloodLockStore(tmp_path / "locks.json"))
    policy.MESSAGE_EDIT_THROTTLE_SECONDS = 0.01
    return bot, policy


def test_edit_same_signature_skipped(tmp_path):
    bot, policy = make_policy(tmp_path)

    async def scenario():
        first = await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        second = await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        assert first.ok is True
        assert second.skipped is True
        assert second.reason == "same_message_signature"
        assert edit_result_status(second) == "not_modified"

    asyncio.run(scenario())
    assert bot.edit_message_text.await_count == 1


def test_quick_back_navigation_is_delivered(tmp_path):
    bot, policy = make_policy(tmp_path)

    async def scenario():
        for screen in ("Карточка", "Настройки", "Карточка"):
            assert (await policy.edit_text(chat_id=123, message_id=456, text=screen)).ok

    asyncio.run(scenario())
    assert [c.kwargs["text"] for c in bot.edit_message_text.await_args_list] == [
        "Карточка", "Настройки", "Карточка",
    ]


def test_edit_signature_includes_parse_mode_and_markup(tmp_path):
    bot, policy = make_policy(tmp_path)

    async def scenario():
        await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        await policy.edit_text(chat_id="123", message_id=456, text="Карточка", parse_mode="HTML")
        await policy.edit_message_reply_markup(chat_id=123, message_id=456, reply_markup=None)
        await policy.edit_text(chat_id=123, message_id=456, text="Карточка", parse_mode="HTML")

    asyncio.run(scenario())
    assert bot.edit_message_text.await_count == 3
    assert bot.edit_message_reply_markup.await_count == 1


def test_confirmed_not_modified_is_successful_but_retry_after_is_not(tmp_path):
    bot, policy = make_policy(tmp_path)
    bot.edit_message_text.side_effect = [
        TelegramBadRequest(method=None, message="message is not modified"),
        TelegramRetryAfter(method=None, message="retry after", retry_after=6402),
    ]

    async def scenario():
        first = await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        assert edit_result_status(first) == "not_modified"
        second = await policy.edit_text(chat_id=123, message_id=456, text="Настройки")
        assert second.reason == "retry_after"
        assert edit_result_status(second) == "failed"
        third = await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        assert third.reason == "chat_retry_after_active"
        assert edit_result_status(third) == "failed"

    asyncio.run(scenario())
    assert bot.edit_message_text.await_count == 2


def test_short_retry_after_retries_distinct_screen(tmp_path, monkeypatch):
    import app.ui_error_policy as module

    monkeypatch.setattr(module, "RETRY_AFTER_SAFETY_SECONDS", 0)
    bot, policy = make_policy(tmp_path)
    bot.edit_message_text.side_effect = [
        TelegramRetryAfter(method=None, message="retry after", retry_after=0.02),
        True,
    ]
    result = asyncio.run(policy.edit_text(chat_id=123, message_id=456, text="Карточка"))
    assert result.ok
    assert bot.edit_message_text.await_count == 2


def test_rich_and_text_edits_share_lock_and_signature(tmp_path):
    bot, policy = make_policy(tmp_path)

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def rich():
            started.set()
            await release.wait()
            return True

        await policy.edit_text(chat_id=123, message_id=456, text="Карточка")
        older = asyncio.create_task(policy.edit_operation(
            chat_id=123, message_id=456, action="bot.edit_rich_catalog",
            signature=("rich", "Список"), func=rich,
        ))
        await started.wait()
        newer = asyncio.create_task(policy.edit_text(chat_id=123, message_id=456, text="Карточка"))
        await asyncio.sleep(0)
        assert bot.edit_message_text.await_count == 1
        release.set()
        assert (await older).ok
        assert (await newer).ok

    asyncio.run(scenario())
    assert bot.edit_message_text.await_count == 2
