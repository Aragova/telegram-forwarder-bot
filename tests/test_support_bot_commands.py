"""Regression contracts for the standalone ViMi Support bot.

No Telegram network calls. The same handler ordering is checked that aiogram
uses for first-match routing of message commands versus customer tickets.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import support_bot


def _message(*, user_id: int, chat_id: int = 101, message_id: int = 17,
             text: str | None = None, reply_to=None):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id),
        chat=SimpleNamespace(id=chat_id),
        message_id=message_id,
        text=text,
        reply_to_message=reply_to,
        answer=AsyncMock(),
    )


def test_commands_are_registered_before_generic_forwarding():
    callbacks = [handler.callback.__name__ for handler in support_bot.dp.message.handlers]
    assert callbacks.count("start_command") == 1
    assert callbacks.count("help_command") == 1
    assert callbacks.count("unknown_command") == 2  # text & media captions
    assert callbacks.index("start_command") < callbacks.index("from_user")
    assert callbacks.index("help_command") < callbacks.index("from_user")
    assert max(i for i, v in enumerate(callbacks) if v == "unknown_command") < (
        callbacks.index("from_user")
    )
    assert callbacks.index("unknown_command") < callbacks.index("from_admin")


def test_start_greets_customer_without_creating_ticket(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.forward_message = AsyncMock()
    monkeypatch.setattr(support_bot, "bot", fake_bot)
    support_bot.user_map.clear()

    message = _message(user_id=3001, text="/start")
    asyncio.run(support_bot.start_command(message))
    text = message.answer.await_args.args[0]
    assert "Добро пожаловать" in text
    assert "ViMi Pay" in text
    assert "ViMi Autoposting" in text
    assert "ViMi Social" in text
    assert "следующим сообщением" in text
    assert message.answer.await_args.kwargs["parse_mode"] == "HTML"
    assert "Обращение отправлено" not in text
    fake_bot.forward_message.assert_not_awaited()
    assert support_bot.user_map == {}


def test_start_admin_shows_operator_help_without_forwarding(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.forward_message = AsyncMock()
    monkeypatch.setattr(support_bot, "bot", fake_bot)

    message = _message(user_id=9001, text="/start")
    asyncio.run(support_bot.start_command(message))
    text = message.answer.await_args.args[0]
    assert "Панель ответов" in text
    assert "пересланном сообщении" in text
    fake_bot.forward_message.assert_not_awaited()


def test_help_and_unknown_commands_do_not_forward(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.forward_message = AsyncMock()
    monkeypatch.setattr(support_bot, "bot", fake_bot)
    for text in ("/help", "/menu", "/start campaign"):
        message = _message(user_id=3001, text=text)
        asyncio.run(support_bot.help_command(message))
        assert "Добро пожаловать" in message.answer.await_args.args[0]

    unknown = _message(user_id=3001, text="/not-a-ticket")
    asyncio.run(support_bot.unknown_command(unknown))
    assert "Это команда, а не обращение" in unknown.answer.await_args.args[0]
    fake_bot.forward_message.assert_not_awaited()


def test_real_text_photo_and_file_requests_still_reach_operator(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.forward_message = AsyncMock(
        side_effect=lambda **kwargs: SimpleNamespace(
            message_id=kwargs["message_id"] + 400
        )
    )
    monkeypatch.setattr(support_bot, "bot", fake_bot)
    support_bot.user_map.clear()

    for user_id, text, message_id in (
        (3001, "Помогите с оплатой", 18),
        (3002, None, 19),  # photo or document, forwarded unchanged
    ):
        message = _message(user_id=user_id, text=text, message_id=message_id)
        asyncio.run(support_bot.from_user(message))
        fake_bot.forward_message.assert_any_await(
            chat_id=9001, from_chat_id=101, message_id=message_id,
        )
        assert support_bot.user_map[message_id + 400] == 101
        assert "Обращение отправлено" in message.answer.await_args.args[0]
    assert fake_bot.forward_message.await_count == 2


def test_operator_reply_routes_to_original_chat(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.send_message = AsyncMock()
    monkeypatch.setattr(support_bot, "bot", fake_bot)
    monkeypatch.setattr(support_bot, "user_map", {418: 3010})
    message = _message(
        user_id=9001, chat_id=9001, text="Ответ от поддержки",
        reply_to=SimpleNamespace(message_id=418),
    )
    asyncio.run(support_bot.from_admin(message))
    fake_bot.send_message.assert_awaited_once_with(
        chat_id=3010, text="Ответ от поддержки",
    )
    assert "Ответ доставлен" in message.answer.await_args.args[0]


def test_operator_gets_clear_reply_instruction_or_stale_warning(monkeypatch):
    monkeypatch.setattr(support_bot, "SUPPORT_ADMIN_ID", 9001)
    fake_bot = MagicMock()
    fake_bot.send_message = AsyncMock()
    monkeypatch.setattr(support_bot, "bot", fake_bot)
    monkeypatch.setattr(support_bot, "user_map", {})

    without_reply = _message(user_id=9001, chat_id=9001, text="reply")
    asyncio.run(support_bot.from_admin(without_reply))
    assert "Нажмите «Ответить»" in without_reply.answer.await_args.args[0]

    after_restart = _message(
        user_id=9001, chat_id=9001, text="reply",
        reply_to=SimpleNamespace(message_id=999),
    )
    asyncio.run(support_bot.from_admin(after_restart))
    assert "После перезапуска" in after_restart.answer.await_args.args[0]
    fake_bot.send_message.assert_not_awaited()
