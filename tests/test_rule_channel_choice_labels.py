import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram import Dispatcher

from app import rule_folder_handlers
from app.keyboards import format_rule_channel_choice


@pytest.mark.parametrize(
    ("title", "thread_id", "expected"),
    [
        ("Канал", None, "Канал"),
        ("Группа", 0, "Группа"),
        ("Тайная", 1, "Тайная · ID темы: 1"),
        ("Тайная", 59887, "Тайная · ID темы: 59887"),
        ("Тайная", 59888, "Тайная · ID темы: 59888"),
    ],
)
def test_rule_channel_choice_label(title, thread_id, expected):
    assert format_rule_channel_choice(title, thread_id) == expected


@pytest.mark.parametrize("action", ["rf:add", "rf:add_in:9"])
def test_admin_folder_rule_source_picker_distinguishes_topics(monkeypatch, action):
    user_id = 990029
    rows = [
        {"channel_id": "-1001", "thread_id": 125, "title": "Тайная"},
        {"channel_id": "-1001", "thread_id": 248, "title": "Тайная"},
        {"channel_id": "-1002", "thread_id": None, "title": "Канал"},
    ]

    async def run_db(fn, *args):
        return fn(*args)

    ctx = SimpleNamespace(
        db=SimpleNamespace(
            get_channels=lambda kind: rows if kind == "source" else [],
            get_rule_folder=lambda tenant_id, folder_id: {"id": folder_id},
        ),
        run_db=run_db,
        channel_choice_cls=lambda channel_id, thread_id, title: SimpleNamespace(
            channel_id=channel_id, thread_id=thread_id, title=title,
        ),
        user_states={},
        answer_callback_safe_once=AsyncMock(),
        edit_message_text_safe=AsyncMock(),
        send_message_safe=AsyncMock(),
    )
    monkeypatch.setattr(rule_folder_handlers, "_actor", AsyncMock(return_value=(1, True)))
    dp = Dispatcher()
    rule_folder_handlers.register_rule_folder_handlers(dp, ctx)
    callback = SimpleNamespace(
        data=action,
        from_user=SimpleNamespace(id=user_id),
        message=SimpleNamespace(chat=SimpleNamespace(id=user_id, type="private")),
    )

    try:
        asyncio.run(dp.callback_query.handlers[0].callback(callback))
        keyboard = ctx.send_message_safe.await_args.kwargs["reply_markup"].keyboard
        assert [row[0].text for row in keyboard] == [
            "📤 1. Тайная · ID темы: 125",
            "📤 2. Тайная · ID темы: 248",
            "📤 3. Канал",
            "❌ Отмена",
        ]
        state = ctx.user_states[user_id]
        assert state["action"] == "pick_rule_source"
        assert [(s.channel_id, s.thread_id) for s in state["sources"]] == [
            ("-1001", 125), ("-1001", 248), ("-1002", None),
        ]
    finally:
        rule_folder_handlers.clear_new_rule_folder(user_id)
