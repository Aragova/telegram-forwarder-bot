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


def test_acknowledged_alert_waits_for_cooldown_and_coalesces(tmp_path):
    from unittest.mock import AsyncMock

    async def scenario():
        bot = AsyncMock()
        policy = UIErrorPolicy(bot, TelegramFloodLockStore(tmp_path / "alerts.json"))
        policy._chat_retry_after_until["123"] = time.monotonic() + 0.05
        assert policy.notify_after_ack(chat_id=123, text="Нет доступа")
        assert policy.notify_after_ack(chat_id="123", text="Нет доступа")
        assert policy.notify_after_ack(chat_id=123, text="Повторите попытку")
        task = policy._alert_tasks["123"]
        await asyncio.sleep(0)
        bot.send_message.assert_not_awaited()
        await asyncio.wait_for(task, timeout=2)
        assert [c.kwargs["text"] for c in bot.send_message.await_args_list] == [
            "Нет доступа", "Повторите попытку",
        ]
        assert not policy._alert_tasks and not policy._pending_alerts

    asyncio.run(scenario())


def test_deferred_alert_rechecks_new_retry_after(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    import app.ui_error_policy as module

    monkeypatch.setattr(module, "RETRY_AFTER_SAFETY_SECONDS", 0)
    async def scenario():
        bot = AsyncMock()
        bot.send_message.side_effect = [
            TelegramRetryAfter(method=None, message="retry after", retry_after=0.03),
            True,
        ]
        policy = UIErrorPolicy(bot, TelegramFloodLockStore(tmp_path / "alerts.json"))
        assert policy.notify_after_ack(chat_id=123, text="Нет доступа")
        await asyncio.wait_for(policy._alert_tasks["123"], timeout=2)
        assert bot.send_message.await_count == 2
        assert not policy._pending_alerts

    asyncio.run(scenario())


def test_deferred_alert_shutdown_cancels_pending_notification(tmp_path):
    from unittest.mock import AsyncMock
    from app.background_tasks import stop_background_tasks

    async def scenario():
        bot = AsyncMock()
        policy = UIErrorPolicy(bot, TelegramFloodLockStore(tmp_path / "alerts.json"))
        policy._chat_retry_after_until["123"] = time.monotonic() + 6402
        assert policy.notify_after_ack(chat_id=123, text="Нет доступа")
        task = policy._alert_tasks["123"]
        await stop_background_tasks()
        assert task.cancelled()
        assert not policy._pending_alerts and not policy._alert_tasks
        bot.send_message.assert_not_awaited()

    asyncio.run(scenario())
